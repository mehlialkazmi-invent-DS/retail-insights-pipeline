"""Persist pipeline outputs to Delta with incremental merge support.

Output layout per table::

    {PATH_OUTPUT_ROOT}/{table_name}/run_date={OUTPUT_RUN_DATE}/

``OUTPUT_RUN_DATE`` defaults to ``reporting_window.as_of_date`` (override via ``output.run_date``).

Incremental merge reads the **latest existing run_date partition on or before** the run being
written — not the partition being written — so weekly runs (whose ``run_date`` advances with
``as_of_date``) accumulate history instead of writing isolated single-window snapshots. Each
run_date partition is therefore a self-contained snapshot of the full merged history as of that
run. Comparison tables are recomputed from that merged ``kpi_long`` so YoY/YTD reflect the full
saved history, not just the current run window.

The same pattern applies to the comparable (like-for-like) YTD table: ``comparable_kpi_long`` is
merged incrementally across runs just like ``kpi_long``, and ``comparable_comparison_ytd`` is then
recomputed from the merged ``comparable_kpi_long``. A single-week refresh therefore produces a
comparable YTD comparison relative to the full saved history. Comparable is YTD-only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from kpi_pipeline.comparisons import _selected_comparison_kinds
from kpi_pipeline.context import KPIContext


@dataclass
class TableSavePlan:
    name: str
    path: str
    exists: bool
    new_rows: int
    append_rows: int = 0
    overwrite_rows: int = 0
    skipped_rows: int = 0
    merge_source_run_date: Optional[str] = None


@dataclass
class SavePlan:
    output_root: str
    save_mode: str
    allow_overwrite_existing: bool
    run_date: str = ""
    tables: List[TableSavePlan] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def has_skipped_overlaps(self) -> bool:
        return any(t.skipped_rows > 0 for t in self.tables)

    def print_summary(self) -> None:
        print(f"Save mode: {self.save_mode} | root: {self.output_root}")
        if self.run_date:
            print(f"Run date partition: run_date={self.run_date}")
        print(f"Allow overwrite existing: {self.allow_overwrite_existing}")
        for table in self.tables:
            src = f" | merge_from=run_date={table.merge_source_run_date}" if table.merge_source_run_date else ""
            print(
                f"  {table.name}: exists={table.exists} | new={table.new_rows} | "
                f"append={table.append_rows} | overwrite={table.overwrite_rows} | skip={table.skipped_rows}{src}"
            )
        for warning in self.warnings:
            print(f"WARNING: {warning}")


TABLE_ROW_KEYS: Dict[str, Sequence[str]] = {
    # "root" included everywhere dimension/dimension_value appears: a cut's own breakdown is
    # computed independently within every root (e.g. "brand"="KNG" exists once under root=
    # "overall" and again under root="nvrout", each a genuinely different row) -- without root in
    # the key, those would collide as if they were the same row. See kpi_pipeline/kpi_long.py.
    "kpi_long": ("period_type", "period", "root", "dimension", "dimension_value"),
    "comparison_yoy": ("comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period"),
    "comparison_ytd": ("comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period"),
    "scope_diff": ("Year", "metric"),
    # link_prior_year/link_current_year included: the same year's row can legitimately carry a
    # different metric value per link it participates in (each link has its own pair
    # restriction), so the link identifies which occurrence a given row is.
    "comparable_kpi_long": (
        "comparison_type", "period_type", "period", "root", "dimension", "dimension_value",
        "link_prior_year", "link_current_year",
    ),
    "comparable_comparison_ytd": (
        "comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period",
    ),
}

# Comparison tables are a deterministic function of the saved kpi_long snapshot. They are
# recomputed from the merged kpi_long and overwritten wholesale into the current run_date
# partition (never key-merged), so the comparisons in a partition always match its kpi_long.
COMPARISON_TABLES: Tuple[str, ...] = ("comparison_yoy", "comparison_ytd")

# comparable_kpi_long is period-grain (same shape as kpi_long) and is merged incrementally across
# runs. The comparable comparison table is then recomputed from the merged comparable_kpi_long —
# the same pattern as regular comparisons from kpi_long. Comparable is YTD-only.
COMPARABLE_COMPARISON_TABLES: Tuple[str, ...] = ("comparable_comparison_ytd",)


def _output_frames(ctx: KPIContext) -> Dict[str, pd.DataFrame]:
    """Tables to persist, in save order.

    Only the comparison kinds selected via ``comparisons.enabled`` are persisted; the others
    are skipped entirely. Comparable tables (YTD-only) are included only when comparable_pairs
    is gated on AND "ytd" is among the selected kinds.
    """
    kinds = _selected_comparison_kinds(ctx)
    frames: Dict[str, pd.DataFrame] = {"kpi_long": ctx.kpi_long}
    for kind in kinds:
        frames[f"comparison_{kind}"] = getattr(ctx, f"comparison_{kind}")
    frames["scope_diff"] = ctx.scope_diff
    if ctx.settings.get("COMPARABLE_PAIRS_ENABLED", False) and "ytd" in kinds:
        frames["comparable_kpi_long"] = ctx.comparable_kpi_long
        frames["comparable_comparison_ytd"] = ctx.comparable_comparison_ytd
    return frames


def _table_path(output_root: str, name: str, run_date: str, fund_paste) -> str:
    """Resolve Delta path: {output_root}/{table_name}/run_date={run_date}/"""
    return fund_paste(output_root, name, f"run_date={run_date}")


def _list_run_date_dirs(spark, base_path: str) -> List[str]:
    """Best-effort list of run_date partition values under {output_root}/{table_name}/.

    Tries Databricks ``dbutils.fs.ls`` first, then a local/DBFS filesystem listing. Returns
    an empty list when the base path does not exist yet (first-ever save).
    """
    names: List[str] = []
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        names = [fi.name for fi in dbutils.fs.ls(base_path)]
    except Exception:
        from pathlib import Path

        candidates = []
        if base_path.startswith("/mnt/"):
            candidates.append("/dbfs" + base_path)
        candidates.append(base_path)
        for candidate in candidates:
            try:
                p = Path(candidate)
                if p.is_dir():
                    names = [child.name for child in p.iterdir()]
                    break
            except Exception:
                continue

    run_dates = []
    for raw in names:
        leaf = raw.rstrip("/").split("/")[-1]
        if leaf.startswith("run_date="):
            run_dates.append(leaf[len("run_date="):])
    return sorted(run_dates)


def _latest_run_date_on_or_before(spark, output_root: str, name: str, fund_paste, run_date: str) -> Optional[str]:
    """Latest existing run_date partition ``<= run_date`` for a table, or None if none exist.

    ISO dates sort lexicographically, so a plain string comparison gives chronological order.
    """
    base = fund_paste(output_root, name)
    candidates = [d for d in _list_run_date_dirs(spark, base) if d <= run_date]
    return candidates[-1] if candidates else None


def _delta_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False


def _load_existing_table(spark, path: str) -> pd.DataFrame:
    try:
        return spark.read.format("delta").load(path).toPandas()
    except Exception:
        return pd.DataFrame()


def _row_tuples(pdf: pd.DataFrame, key_cols: Sequence[str]) -> set:
    if pdf is None or pdf.empty:
        return set()
    missing = [c for c in key_cols if c not in pdf.columns]
    if missing:
        raise ValueError(f"Missing key columns for merge: {missing}")
    return set(map(tuple, pdf[list(key_cols)].astype(str).itertuples(index=False, name=None)))


def _filter_by_keys(pdf: pd.DataFrame, key_cols: Sequence[str], keys: Iterable[Tuple]) -> pd.DataFrame:
    if pdf.empty or not keys:
        return pdf.iloc[0:0].copy()
    key_set = set(keys)
    mask = pdf[list(key_cols)].astype(str).apply(tuple, axis=1).isin(key_set)
    return pdf[mask].copy()


def _drop_keys(pdf: pd.DataFrame, key_cols: Sequence[str], keys: Iterable[Tuple]) -> pd.DataFrame:
    if pdf.empty or not keys:
        return pdf.copy()
    key_set = set(keys)
    mask = pdf[list(key_cols)].astype(str).apply(tuple, axis=1).isin(key_set)
    return pdf[~mask].copy()


def merge_table_incremental(
    existing: pd.DataFrame,
    new: pd.DataFrame,
    key_cols: Sequence[str],
    allow_overwrite_existing: bool,
) -> Tuple[pd.DataFrame, TableSavePlan]:
    plan = TableSavePlan(
        name="",
        path="",
        exists=not existing.empty,
        new_rows=0 if new is None else len(new),
        append_rows=0,
        overwrite_rows=0,
        skipped_rows=0,
    )
    if new is None or new.empty:
        return existing.copy(), plan

    new_keys = _row_tuples(new, key_cols)
    if existing.empty:
        plan.append_rows = len(new)
        return new.copy(), plan

    existing_keys = _row_tuples(existing, key_cols)
    append_keys = new_keys - existing_keys
    overlap_keys = new_keys & existing_keys

    append_df = _filter_by_keys(new, key_cols, append_keys)
    overlap_df = _filter_by_keys(new, key_cols, overlap_keys)

    merged = existing.copy()
    if append_keys:
        merged = pd.concat([merged, append_df], ignore_index=True)
        plan.append_rows = len(append_df)

    if overlap_keys:
        if allow_overwrite_existing:
            merged = _drop_keys(merged, key_cols, overlap_keys)
            merged = pd.concat([merged, overlap_df], ignore_index=True)
            plan.overwrite_rows = len(overlap_df)
        else:
            plan.skipped_rows = len(overlap_df)

    return merged, plan


def _annotate_run_metadata(pdf: pd.DataFrame, run_as_of: str) -> pd.DataFrame:
    out = pdf.copy()
    now = pd.Timestamp.now(tz="UTC").isoformat()
    if "_run_as_of" in out.columns:
        out["_run_as_of"] = out["_run_as_of"].fillna(run_as_of)
    else:
        out["_run_as_of"] = run_as_of
    if "_saved_at" in out.columns:
        out["_saved_at"] = out["_saved_at"].fillna(now)
    else:
        out["_saved_at"] = now
    return out


def build_save_plan(ctx: KPIContext, fund_paste) -> SavePlan:
    settings = ctx.settings
    output_root = settings["PATH_OUTPUT_ROOT"]
    run_date = settings["OUTPUT_RUN_DATE"]
    save_mode = settings["OUTPUT_SAVE_MODE"]
    allow_overwrite = settings["ALLOW_OVERWRITE_EXISTING"]
    recompute_enabled = save_mode == "incremental" and settings.get("RECOMPUTE_COMPARISONS_FROM_HISTORY", True)

    # Comparisons recomputed only when incremental AND a prior kpi_long partition exists to merge onto.
    kpi_long_source = _latest_run_date_on_or_before(ctx.spark, output_root, "kpi_long", fund_paste, run_date)
    recompute = recompute_enabled and kpi_long_source is not None

    # Comparable comparisons recomputed the same way, from merged comparable_kpi_long.
    comparable_kpi_long_source = (
        _latest_run_date_on_or_before(ctx.spark, output_root, "comparable_kpi_long", fund_paste, run_date)
        if ctx.settings.get("COMPARABLE_PAIRS_ENABLED", False)
        else None
    )
    recompute_comparable = recompute_enabled and comparable_kpi_long_source is not None

    outputs = _output_frames(ctx)

    plan = SavePlan(
        output_root=output_root,
        save_mode=save_mode,
        allow_overwrite_existing=allow_overwrite,
        run_date=run_date,
    )

    for name, pdf in outputs.items():
        path = _table_path(output_root, name, run_date, fund_paste)
        source_run_date = _latest_run_date_on_or_before(ctx.spark, output_root, name, fund_paste, run_date)
        exists = source_run_date is not None or _delta_exists(ctx.spark, path)
        key_cols = TABLE_ROW_KEYS[name]

        if save_mode == "full_refresh":
            plan.tables.append(
                TableSavePlan(
                    name=name,
                    path=path,
                    exists=exists,
                    new_rows=0 if pdf is None else len(pdf),
                    append_rows=0 if pdf is None else len(pdf),
                )
            )
            continue

        if save_mode == "initial" and exists:
            plan.warnings.append(
                f"{name} already exists at {path}. Use save_mode='incremental' to append missing periods "
                "or 'full_refresh' to replace everything."
            )
            plan.tables.append(
                TableSavePlan(name=name, path=path, exists=True, new_rows=0 if pdf is None else len(pdf))
            )
            continue

        # Incremental preview. Comparison/comparable-comparison tables are recomputed from the
        # merged kpi_long / comparable_kpi_long at save time and overwritten wholesale.
        if recompute and name in COMPARISON_TABLES:
            plan.tables.append(
                TableSavePlan(
                    name=name,
                    path=path,
                    exists=exists,
                    new_rows=0 if pdf is None else len(pdf),
                    overwrite_rows=0 if pdf is None else len(pdf),
                    merge_source_run_date=kpi_long_source,
                )
            )
            continue

        if recompute_comparable and name in COMPARABLE_COMPARISON_TABLES:
            plan.tables.append(
                TableSavePlan(
                    name=name,
                    path=path,
                    exists=exists,
                    new_rows=0 if pdf is None else len(pdf),
                    overwrite_rows=0 if pdf is None else len(pdf),
                    merge_source_run_date=comparable_kpi_long_source,
                )
            )
            continue

        existing = (
            _load_existing_table(ctx.spark, _table_path(output_root, name, source_run_date, fund_paste))
            if source_run_date
            else pd.DataFrame()
        )
        _, table_plan = merge_table_incremental(existing, pdf, key_cols, allow_overwrite)
        table_plan.name = name
        table_plan.path = path
        table_plan.merge_source_run_date = source_run_date
        plan.tables.append(table_plan)

        if table_plan.skipped_rows > 0 and not allow_overwrite:
            plan.warnings.append(
                f"{name}: {table_plan.skipped_rows} row(s) already exist and were skipped. "
                "Set output.allow_overwrite_existing=True to replace them."
            )

    if recompute:
        plan.warnings.append(
            "Comparison tables (YoY/YTD) will be recomputed from the merged kpi_long history "
            "and overwritten in this run_date partition (full saved history, not just this run window)."
        )
    if recompute_comparable:
        plan.warnings.append(
            "Comparable comparison tables will be recomputed from the merged comparable_kpi_long history "
            "and overwritten in this run_date partition."
        )

    return plan


def save_pandas_table(
    ctx: KPIContext,
    name: str,
    pdf: pd.DataFrame,
    output_root: str,
    run_date: str,
    fund_paste,
    save_mode: str,
    allow_overwrite_existing: bool,
    run_as_of: str,
) -> TableSavePlan:
    path = _table_path(output_root, name, run_date, fund_paste)
    key_cols = TABLE_ROW_KEYS[name]

    if pdf is None or pdf.empty:
        print(f"skip save {name}: empty")
        return TableSavePlan(name=name, path=path, exists=_delta_exists(ctx.spark, path), new_rows=0)

    source_run_date = _latest_run_date_on_or_before(ctx.spark, output_root, name, fund_paste, run_date)
    exists = source_run_date is not None or _delta_exists(ctx.spark, path)

    if save_mode == "full_refresh":
        merged = pdf.copy()
        table_plan = TableSavePlan(
            name=name,
            path=path,
            exists=exists,
            new_rows=len(pdf),
            append_rows=len(pdf),
        )
    elif save_mode == "initial":
        if exists:
            raise ValueError(
                f"initial save blocked for {name}: data already exists at {path}. "
                "Use save_mode='incremental' or 'full_refresh'."
            )
        merged = pdf.copy()
        table_plan = TableSavePlan(
            name=name,
            path=path,
            exists=False,
            new_rows=len(pdf),
            append_rows=len(pdf),
        )
    else:
        # Incremental: accumulate onto the latest existing partition (<= run_date), then write
        # the full merged result into this run's run_date partition.
        existing = (
            _load_existing_table(ctx.spark, _table_path(output_root, name, source_run_date, fund_paste))
            if source_run_date
            else pd.DataFrame()
        )
        merged, table_plan = merge_table_incremental(existing, pdf, key_cols, allow_overwrite_existing)
        table_plan.name = name
        table_plan.path = path
        table_plan.merge_source_run_date = source_run_date

    merged = _annotate_run_metadata(merged, run_as_of)
    ctx.spark.createDataFrame(merged).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    src = f" | merge_from=run_date={source_run_date}" if table_plan.merge_source_run_date else ""
    print(
        f"saved {name} -> {path} | rows={len(merged)} | append={table_plan.append_rows} | "
        f"overwrite={table_plan.overwrite_rows} | skip={table_plan.skipped_rows}{src}"
    )
    return table_plan


_SAVED_OUTPUT_TABLES = {
    "kpi_long": "kpi_long",
    "comparison_yoy": "comparison_yoy",
    "comparison_ytd": "comparison_ytd",
    "scope_diff": "scope_diff",
    # Comparable (like-for-like) tables — optional; absent unless comparable_pairs was enabled.
    # Comparable is YTD-only.
    "comparable_kpi_long": "comparable_kpi_long",
    "comparable_comparison_ytd": "comparable_comparison_ytd",
}


def _resolve_read_run_date(ctx: KPIContext, fund_paste) -> str:
    """Run_date partition to load saved outputs from: the requested one if present, else the
    latest existing partition on or before it (so html_only finds the most recent snapshot)."""
    output_root = ctx.settings["PATH_OUTPUT_ROOT"]
    requested = ctx.settings["OUTPUT_RUN_DATE"]
    path = _table_path(output_root, "kpi_long", requested, fund_paste)
    if _delta_exists(ctx.spark, path):
        return requested
    latest = _latest_run_date_on_or_before(ctx.spark, output_root, "kpi_long", fund_paste, requested)
    if latest and latest != requested:
        print(f"run_date={requested} not found for kpi_long — loading latest available partition run_date={latest}")
        return latest
    return requested


def load_saved_outputs(ctx: KPIContext, fund_paste) -> None:
    """Load previously saved Delta output tables into ctx (for run.mode=html_only)."""
    output_root = ctx.settings["PATH_OUTPUT_ROOT"]
    run_date = _resolve_read_run_date(ctx, fund_paste)
    spark = ctx.spark

    for attr, name in _SAVED_OUTPUT_TABLES.items():
        path = _table_path(output_root, name, run_date, fund_paste)
        if not _delta_exists(spark, path):
            # Comparison tables are skipped at save time when empty (e.g. fewer than
            # two periods for YoY). Only kpi_long is mandatory; the rest default empty.
            if name == "kpi_long":
                raise ValueError(
                    f"Saved output table {name!r} not found at {path}. "
                    "Run the full pipeline with output.save_outputs=True first, "
                    f"or set output.run_date to match an existing partition."
                )
            setattr(ctx, attr, pd.DataFrame())
            continue
        pdf = _load_existing_table(spark, path)
        setattr(ctx, attr, pdf)

    if ctx.kpi_long is None or ctx.kpi_long.empty:
        raise ValueError("Saved kpi_long is empty — nothing to render in the HTML report.")

    print(f"Loaded saved outputs from {output_root} (run_date={run_date})")
    print("kpi_long shape:", ctx.kpi_long.shape)


def _recompute_comparisons_from_saved_history(ctx: KPIContext, fund_paste) -> None:
    """Re-read the merged kpi_long just written and recompute comparison tables from it.

    After an incremental save, the current run_date partition holds the full merged history
    (this run's window unioned onto the latest prior partition). Reloading it and rebuilding
    comparisons makes YoY/YTD reflect the full saved history rather than only the current run
    window. ``ctx.comparison_*`` and the overall display tables are updated in place so the
    notebook comparison cells and the HTML report also reflect the merged history.

    ``ctx.kpi_long`` is set to the full merged frame (not trimmed) — the kpi_long Delta save
    already happened before this runs, so this only affects what the notebook/HTML sees
    afterward. ``ctx.kpi_long_display`` gets the trimmed-for-HTML copy instead.
    """
    from kpi_pipeline.comparisons import build_comparisons
    from kpi_pipeline.kpi_long import trim_periods_to_recent

    output_root = ctx.settings["PATH_OUTPUT_ROOT"]
    run_date = ctx.settings["OUTPUT_RUN_DATE"]
    merged = _load_existing_table(ctx.spark, _table_path(output_root, "kpi_long", run_date, fund_paste))
    if merged is None or merged.empty:
        return

    ctx.kpi_long = merged
    build_comparisons(ctx)
    ctx.kpi_long_display = trim_periods_to_recent(merged, ctx)
    print("recomputed comparisons from merged kpi_long history (run_date=%s)" % run_date)


def _recompute_comparable_comparisons_from_saved_history(ctx: KPIContext, fund_paste) -> None:
    """Re-read the merged comparable_kpi_long and recompute the comparable YTD comparison from it.

    Mirrors ``_recompute_comparisons_from_saved_history``: after ``comparable_kpi_long`` has been
    incrementally merged onto prior history, reload it and rebuild the comparison from every
    link present (each link's rows already carry that link's own pair-restricted metric values,
    tagged via link_prior_year/link_current_year) so the saved comparable numbers reflect the
    full accumulated history, not just the current run window. Comparable is YTD-only.
    """
    from kpi_pipeline.comparable import rebuild_comparable_ytd_from_saved_rows

    output_root = ctx.settings["PATH_OUTPUT_ROOT"]
    run_date = ctx.settings["OUTPUT_RUN_DATE"]
    merged = _load_existing_table(
        ctx.spark, _table_path(output_root, "comparable_kpi_long", run_date, fund_paste)
    )
    if merged is None or merged.empty:
        return

    ctx.comparable_kpi_long = merged

    if "ytd" not in set(_selected_comparison_kinds(ctx)):
        return
    display, save = rebuild_comparable_ytd_from_saved_rows(ctx, merged)
    ctx.comparable_comparison_ytd = save
    ctx.comparable_ytd_display = display

    print("recomputed comparable comparisons from merged comparable_kpi_long history (run_date=%s)" % run_date)


def save_outputs(ctx: KPIContext, fund_paste) -> SavePlan:
    if not ctx.settings["SAVE_OUTPUTS"]:
        print(
            "SAVE_OUTPUTS is False — skipping writes. "
            "Set CONFIG['output']['save_outputs'] = True in config.py to persist outputs."
        )
        return SavePlan(
            output_root=ctx.settings["PATH_OUTPUT_ROOT"],
            save_mode=ctx.settings["OUTPUT_SAVE_MODE"],
            allow_overwrite_existing=ctx.settings["ALLOW_OVERWRITE_EXISTING"],
            run_date=ctx.settings["OUTPUT_RUN_DATE"],
        )

    plan = build_save_plan(ctx, fund_paste)
    plan.print_summary()

    if plan.save_mode == "initial" and plan.warnings:
        raise ValueError(
            "Initial save blocked because output tables already exist. "
            "See warnings above; switch to save_mode='incremental' or 'full_refresh'."
        )

    if plan.has_skipped_overlaps and not plan.allow_overwrite_existing:
        print(
            "Overlapping periods detected. Existing rows were left unchanged. "
            "Set output.allow_overwrite_existing=True and re-run save to replace them."
        )

    output_root = ctx.settings["PATH_OUTPUT_ROOT"]
    run_date = ctx.settings["OUTPUT_RUN_DATE"]
    save_mode = ctx.settings["OUTPUT_SAVE_MODE"]
    allow_overwrite = ctx.settings["ALLOW_OVERWRITE_EXISTING"]
    run_as_of = str(ctx.settings["AS_OF_DATE"])
    recompute_enabled = save_mode == "incremental" and ctx.settings.get("RECOMPUTE_COMPARISONS_FROM_HISTORY", True)

    # Decided before the write so the current run_date partition doesn't count as its own source.
    kpi_long_history_source = _latest_run_date_on_or_before(ctx.spark, output_root, "kpi_long", fund_paste, run_date)
    recompute = recompute_enabled and kpi_long_history_source is not None

    comparable_enabled = ctx.settings.get("COMPARABLE_PAIRS_ENABLED", False)
    comparable_kpi_long_source = (
        _latest_run_date_on_or_before(ctx.spark, output_root, "comparable_kpi_long", fund_paste, run_date)
        if comparable_enabled
        else None
    )
    recompute_comparable = recompute_enabled and comparable_kpi_long_source is not None

    def _save(name: str, pdf: pd.DataFrame, mode: str) -> None:
        table_plan = save_pandas_table(
            ctx, name, pdf, output_root, run_date, fund_paste, mode, allow_overwrite, run_as_of
        )
        for existing in plan.tables:
            if existing.name == name:
                existing.append_rows = table_plan.append_rows
                existing.overwrite_rows = table_plan.overwrite_rows
                existing.skipped_rows = table_plan.skipped_rows
                existing.merge_source_run_date = table_plan.merge_source_run_date

    # 1. Save kpi_long first (accumulates onto the latest prior partition under incremental).
    _save("kpi_long", ctx.kpi_long, save_mode)

    # 2. When merging onto prior history, recompute comparisons from the full merged kpi_long so
    #    they reflect saved history (e.g. a single-week run can still produce a YoY vs last year).
    comparison_mode = save_mode
    if recompute:
        _recompute_comparisons_from_saved_history(ctx, fund_paste)
        # Recomputed comparisons are the authoritative full-history snapshot for this partition;
        # overwrite wholesale rather than key-merge against a stale partition.
        comparison_mode = "full_refresh"

    # 3. Save the selected comparison tables + scope_diff.
    selected_kinds = _selected_comparison_kinds(ctx)
    for kind in selected_kinds:
        _save(f"comparison_{kind}", getattr(ctx, f"comparison_{kind}"), comparison_mode)
    _save("scope_diff", ctx.scope_diff, save_mode)

    # 4. Comparable (like-for-like) tables (YTD-only): comparable_kpi_long merges incrementally
    #    like kpi_long; comparable_comparison_ytd is then recomputed from the merged comparable_kpi_long.
    if comparable_enabled and "ytd" in selected_kinds:
        _save("comparable_kpi_long", ctx.comparable_kpi_long, save_mode)

        comparable_comp_mode = save_mode
        if recompute_comparable:
            _recompute_comparable_comparisons_from_saved_history(ctx, fund_paste)
            comparable_comp_mode = "full_refresh"

        _save("comparable_comparison_ytd", ctx.comparable_comparison_ytd, comparable_comp_mode)

    ctx.save_plan = plan
    return plan

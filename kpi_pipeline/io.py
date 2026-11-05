"""Persist pipeline outputs to Delta, with incremental merge.

Layout per table: ``{PATH_OUTPUT_ROOT}/{table_name}/run_date={OUTPUT_RUN_DATE}/`` (OUTPUT_RUN_DATE defaults to
reporting_window.as_of_date; override with output.run_date).

An incremental merge reads the latest existing run_date partition on or before the one being written, so
weekly runs accumulate history: each partition is a full snapshot of the merged history as of its run.
kpi_long and comparable_kpi_long are key-merged; the comparison tables are then recomputed from the merged
frame and overwritten whole. With report_end="latest_day" an incremental merge always replaces existing
"ytd" rows (their window moves with as_of_date), whatever allow_overwrite_existing says.
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
    # root is part of every key: the same cut value under two roots is two rows.
    "kpi_long": ("period_type", "period", "root", "dimension", "dimension_value"),
    "comparison_yoy": ("comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period"),
    "comparison_ytd": ("comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period"),
    "scope_diff": ("Year", "metric"),
    # A year is "current" in one link and "prior" in the next: the link years tell those rows apart.
    "comparable_kpi_long": (
        "comparison_type", "period_type", "period", "root", "dimension", "dimension_value",
        "link_prior_year", "link_current_year",
    ),
    "comparable_comparison_ytd": (
        "comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period",
    ),
    "comparable_comparison_yoy": (
        "comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period",
    ),
    "comparable_comparison_quarter": (
        "comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period",
        "quarter_number",
    ),
    "comparable_comparison_half": (
        "comparison_type", "root", "dimension", "dimension_value", "metric_key", "current_period",
        "half_number",
    ),
}

# Recomputed from the merged kpi_long and overwritten whole (never key-merged), so a partition's
# comparisons always match its kpi_long.
COMPARISON_TABLES: Tuple[str, ...] = ("comparison_yoy", "comparison_ytd")

# One per comparable_pairs.kinds entry, recomputed from the merged comparable_kpi_long.
COMPARABLE_COMPARISON_TABLES: Tuple[str, ...] = (
    "comparable_comparison_ytd", "comparable_comparison_yoy", "comparable_comparison_quarter",
    "comparable_comparison_half",
)


def _comparable_save_kinds(ctx: KPIContext) -> List[str]:
    """comparable_pairs.kinds when comparable_pairs is enabled, else none."""
    if not ctx.settings.get("COMPARABLE_PAIRS_ENABLED", False):
        return []
    return list(ctx.settings.get("COMPARABLE_KINDS") or [])


def _output_table_names(ctx: KPIContext) -> List[str]:
    """Tables to persist, in save order: the comparison kinds of comparisons.enabled, and the comparable
    tables when comparable_pairs is on with at least one kind (the two settings are independent). Each name
    is also the ctx attribute holding the table."""
    names = ["kpi_long"] + [f"comparison_{kind}" for kind in _selected_comparison_kinds(ctx)] + ["scope_diff"]
    comparable_kinds = _comparable_save_kinds(ctx)
    if comparable_kinds:
        names += ["comparable_kpi_long"] + [f"comparable_comparison_{kind}" for kind in comparable_kinds]
    return names


def _output_frames(ctx: KPIContext) -> Dict[str, pd.DataFrame]:
    return {name: getattr(ctx, name) for name in _output_table_names(ctx)}


def _table_path(output_root: str, name: str, run_date: str, fund_paste) -> str:
    """Resolve Delta path: {output_root}/{table_name}/run_date={run_date}/"""
    return fund_paste(output_root, name, f"run_date={run_date}")


def _list_run_date_dirs(spark, base_path: str) -> List[str]:
    """run_date partition values under {output_root}/{table_name}/ (dbutils.fs.ls, else a filesystem
    listing); empty when the path does not exist yet."""
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
    """Latest existing run_date partition ``<= run_date`` of a table (ISO dates compare as strings), or None."""
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


# Tables whose period_type column holds the YTD rows that report_end="latest_day" always replaces.
_YTD_ROW_TABLES: Tuple[str, ...] = ("kpi_long", "comparable_kpi_long")


def _always_overwrite_period_types(ctx: KPIContext, name: str) -> Tuple[str, ...]:
    """period_type values of table ``name`` an incremental merge replaces even without
    allow_overwrite_existing: "ytd" under report_end="latest_day" (its window moves with as_of_date),
    none otherwise."""
    if ctx.settings["REPORT_END_MODE"] == "latest_day" and name in _YTD_ROW_TABLES:
        return ("ytd",)
    return ()


def merge_table_incremental(
    existing: pd.DataFrame,
    new: pd.DataFrame,
    key_cols: Sequence[str],
    allow_overwrite_existing: bool,
    always_overwrite_period_types: Sequence[str] = (),
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
            overwrite_df = overlap_df
        elif always_overwrite_period_types:
            overwrite_df = overlap_df[overlap_df["period_type"].isin(always_overwrite_period_types)]
        else:
            overwrite_df = overlap_df.iloc[0:0]
        if not overwrite_df.empty:
            merged = _drop_keys(merged, key_cols, _row_tuples(overwrite_df, key_cols))
            merged = pd.concat([merged, overwrite_df], ignore_index=True)
        plan.overwrite_rows = len(overwrite_df)
        plan.skipped_rows = len(overlap_df) - len(overwrite_df)

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

        # Incremental preview; the comparison tables are recomputed at save time and overwritten whole.
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
        _, table_plan = merge_table_incremental(
            existing, pdf, key_cols, allow_overwrite, _always_overwrite_period_types(ctx, name)
        )
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
        merged, table_plan = merge_table_incremental(
            existing, pdf, key_cols, allow_overwrite_existing, _always_overwrite_period_types(ctx, name)
        )
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


# Saved table names, each loaded onto the ctx attribute of the same name.
_SAVED_OUTPUT_TABLES: Tuple[str, ...] = (
    "kpi_long",
    "comparison_yoy",
    "comparison_ytd",
    "scope_diff",
    # Comparable (like-for-like) tables — optional; absent unless comparable_pairs was enabled.
    "comparable_kpi_long",
    *COMPARABLE_COMPARISON_TABLES,
)


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

    for name in _SAVED_OUTPUT_TABLES:
        path = _table_path(output_root, name, run_date, fund_paste)
        if not _delta_exists(spark, path):
            # Empty tables are not saved (e.g. YoY with one year): only kpi_long is required.
            if name == "kpi_long":
                raise ValueError(
                    f"Saved output table {name!r} not found at {path}. "
                    "Run the full pipeline with output.save_outputs=True first, "
                    f"or set output.run_date to match an existing partition."
                )
            setattr(ctx, name, pd.DataFrame())
            continue
        setattr(ctx, name, _load_existing_table(spark, path))

    if ctx.kpi_long is None or ctx.kpi_long.empty:
        raise ValueError("Saved kpi_long is empty — nothing to render in the HTML report.")

    print(f"Loaded saved outputs from {output_root} (run_date={run_date})")
    print("kpi_long shape:", ctx.kpi_long.shape)


def _recompute_comparisons_from_saved_history(ctx: KPIContext, fund_paste) -> None:
    """Reload the merged kpi_long just written and recompute the comparisons from the full saved history.
    ctx.kpi_long becomes the merged frame (already saved) and ctx.kpi_long_display its trimmed copy, so the
    notebook and the HTML report show the merged history too."""
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
    """Reload the merged comparable_kpi_long and rebuild every enabled comparable kind's comparison from all
    its links (each link's rows carry their own restricted values)."""
    from kpi_pipeline.comparable import _KIND_CTX_ATTRS, rebuild_comparable_kind_from_saved_rows

    output_root = ctx.settings["PATH_OUTPUT_ROOT"]
    run_date = ctx.settings["OUTPUT_RUN_DATE"]
    merged = _load_existing_table(
        ctx.spark, _table_path(output_root, "comparable_kpi_long", run_date, fund_paste)
    )
    if merged is None or merged.empty:
        return

    ctx.comparable_kpi_long = merged

    for kind in ctx.settings.get("COMPARABLE_KINDS") or []:
        _, save_attr, display_attr = _KIND_CTX_ATTRS[kind]
        display, save = rebuild_comparable_kind_from_saved_rows(ctx, merged, kind)
        setattr(ctx, save_attr, save)
        setattr(ctx, display_attr, display)

    print("recomputed comparable comparisons from merged comparable_kpi_long history (run_date=%s)" % run_date)


def save_outputs(ctx: KPIContext, fund_paste) -> SavePlan:
    """Write every output table at once: after a run(save=False), or to save what an interrupted run built
    (runner.ctx keeps the finished tables; an empty or missing table is skipped, never written empty).
    run(save=True) writes each table as it is built instead (OutputSaver)."""
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

    build_save_plan(ctx, fund_paste).print_summary()
    saver = OutputSaver(ctx, fund_paste)
    saver.save_kpi_long()
    saver.save_comparisons()
    saver.save_scope_diff()
    saver.save_comparable()
    return saver.plan


class OutputSaver:
    """Writes each output table as soon as its pipeline step has built it (KPIRunner.run with save=True),
    so a long or interrupted run keeps every table it finished.

    The incremental history sources are looked up once, before the first write, so this run's own run_date
    partition never counts as its own source. save_mode="initial" checks every table up front, before the
    run computes anything. ctx.save_plan collects each written table's counts.
    """

    def __init__(self, ctx: KPIContext, fund_paste):
        settings = ctx.settings
        self.ctx = ctx
        self.fund_paste = fund_paste
        self.output_root = settings["PATH_OUTPUT_ROOT"]
        self.run_date = settings["OUTPUT_RUN_DATE"]
        self.save_mode = settings["OUTPUT_SAVE_MODE"]
        self.allow_overwrite = settings["ALLOW_OVERWRITE_EXISTING"]
        self.run_as_of = str(settings["AS_OF_DATE"])
        self.comparable_kinds = _comparable_save_kinds(ctx)

        recompute_enabled = self.save_mode == "incremental" and settings.get("RECOMPUTE_COMPARISONS_FROM_HISTORY", True)
        self.recompute = recompute_enabled and self._history_source("kpi_long") is not None
        self.recompute_comparable = (
            recompute_enabled
            and bool(self.comparable_kinds)
            and self._history_source("comparable_kpi_long") is not None
        )

        if self.save_mode == "initial":
            existing = [name for name in _output_table_names(ctx) if self._exists(name)]
            if existing:
                raise ValueError(
                    f"Initial save blocked because output tables already exist: {existing}. "
                    "Switch to save_mode='incremental' or 'full_refresh'."
                )

        self.plan = SavePlan(
            output_root=self.output_root,
            save_mode=self.save_mode,
            allow_overwrite_existing=self.allow_overwrite,
            run_date=self.run_date,
        )
        ctx.save_plan = self.plan
        print(
            f"Saving outputs | mode: {self.save_mode} | run_date={self.run_date} | "
            f"tables: {_output_table_names(ctx)}"
        )

    def _history_source(self, name: str) -> Optional[str]:
        return _latest_run_date_on_or_before(self.ctx.spark, self.output_root, name, self.fund_paste, self.run_date)

    def _exists(self, name: str) -> bool:
        path = _table_path(self.output_root, name, self.run_date, self.fund_paste)
        return self._history_source(name) is not None or _delta_exists(self.ctx.spark, path)

    def _save(self, name: str, pdf: Optional[pd.DataFrame], mode: str) -> None:
        table_plan = save_pandas_table(
            self.ctx, name, pdf, self.output_root, self.run_date, self.fund_paste, mode,
            self.allow_overwrite, self.run_as_of,
        )
        self.plan.tables.append(table_plan)
        if table_plan.skipped_rows > 0 and not self.allow_overwrite:
            print(
                f"{name}: {table_plan.skipped_rows} overlapping row(s) left unchanged. "
                "Set output.allow_overwrite_existing=True and re-save to replace them."
            )

    def save_kpi_long(self) -> None:
        """kpi_long (accumulates onto the latest prior partition under incremental)."""
        self._save("kpi_long", self.ctx.kpi_long, self.save_mode)

    def save_comparisons(self) -> None:
        """The comparisons.enabled tables. Merging onto prior history, they are first recomputed from the
        merged kpi_long (save_kpi_long must have run) and overwritten whole."""
        mode = self.save_mode
        if self.recompute:
            _recompute_comparisons_from_saved_history(self.ctx, self.fund_paste)
            mode = "full_refresh"
        for kind in _selected_comparison_kinds(self.ctx):
            name = f"comparison_{kind}"
            self._save(name, getattr(self.ctx, name), mode)

    def save_scope_diff(self) -> None:
        self._save("scope_diff", self.ctx.scope_diff, self.save_mode)

    def save_comparable(self) -> None:
        """comparable_kpi_long (merges like kpi_long), then each kind's comparison, recomputed from the merged
        comparable_kpi_long when merging onto prior history. A no-op while comparable_pairs is off."""
        if not self.comparable_kinds:
            return
        self._save("comparable_kpi_long", self.ctx.comparable_kpi_long, self.save_mode)
        mode = self.save_mode
        if self.recompute_comparable:
            _recompute_comparable_comparisons_from_saved_history(self.ctx, self.fund_paste)
            mode = "full_refresh"
        for kind in self.comparable_kinds:
            name = f"comparable_comparison_{kind}"
            self._save(name, getattr(self.ctx, name), mode)

"""Persist pipeline outputs to Delta with incremental merge support."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd

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


@dataclass
class SavePlan:
    output_root: str
    save_mode: str
    allow_overwrite_existing: bool
    tables: List[TableSavePlan] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def has_skipped_overlaps(self) -> bool:
        return any(t.skipped_rows > 0 for t in self.tables)

    def print_summary(self) -> None:
        print(f"Save mode: {self.save_mode} | root: {self.output_root}")
        print(f"Allow overwrite existing: {self.allow_overwrite_existing}")
        for table in self.tables:
            print(
                f"  {table.name}: exists={table.exists} | new={table.new_rows} | "
                f"append={table.append_rows} | overwrite={table.overwrite_rows} | skip={table.skipped_rows}"
            )
        for warning in self.warnings:
            print(f"WARNING: {warning}")


TABLE_ROW_KEYS: Dict[str, Sequence[str]] = {
    "kpi_long": ("period_type", "period", "dimension", "dimension_value"),
    "comparison_yoy": ("comparison_type", "dimension", "dimension_value", "metric_key", "current_period"),
    "comparison_qoq": ("comparison_type", "dimension", "dimension_value", "metric_key", "current_period"),
    "comparison_wow": ("comparison_type", "dimension", "dimension_value", "metric_key", "current_period"),
    "scope_diff": ("Year", "metric"),
}


def _table_path(output_root: str, name: str, fund_paste) -> str:
    return fund_paste(output_root, name)


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
    mask = pdf[key_cols].astype(str).apply(tuple, axis=1).isin(key_set)
    return pdf[mask].copy()


def _drop_keys(pdf: pd.DataFrame, key_cols: Sequence[str], keys: Iterable[Tuple]) -> pd.DataFrame:
    if pdf.empty or not keys:
        return pdf.copy()
    key_set = set(keys)
    mask = pdf[key_cols].astype(str).apply(tuple, axis=1).isin(key_set)
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
    save_mode = settings["OUTPUT_SAVE_MODE"]
    allow_overwrite = settings["ALLOW_OVERWRITE_EXISTING"]

    outputs = {
        "kpi_long": ctx.kpi_long,
        "comparison_yoy": ctx.comparison_yoy,
        "comparison_qoq": ctx.comparison_qoq,
        "comparison_wow": ctx.comparison_wow,
        "scope_diff": ctx.scope_diff,
    }

    plan = SavePlan(
        output_root=output_root,
        save_mode=save_mode,
        allow_overwrite_existing=allow_overwrite,
    )

    for name, pdf in outputs.items():
        path = _table_path(output_root, name, fund_paste)
        exists = _delta_exists(ctx.spark, path)
        key_cols = TABLE_ROW_KEYS[name]

        if save_mode == "full_refresh":
            table_plan = TableSavePlan(
                name=name,
                path=path,
                exists=exists,
                new_rows=0 if pdf is None else len(pdf),
                append_rows=0 if pdf is None else len(pdf),
            )
            plan.tables.append(table_plan)
            continue

        if save_mode == "initial" and exists:
            plan.warnings.append(
                f"{name} already exists at {path}. Use save_mode='incremental' to append missing periods "
                "or 'full_refresh' to replace everything."
            )
            table_plan = TableSavePlan(name=name, path=path, exists=True, new_rows=0 if pdf is None else len(pdf))
            plan.tables.append(table_plan)
            continue

        existing = _load_existing_table(ctx.spark, path) if exists else pd.DataFrame()
        _, table_plan = merge_table_incremental(existing, pdf, key_cols, allow_overwrite)
        table_plan.name = name
        table_plan.path = path
        plan.tables.append(table_plan)

        if table_plan.skipped_rows > 0 and not allow_overwrite:
            plan.warnings.append(
                f"{name}: {table_plan.skipped_rows} row(s) already exist and were skipped. "
                "Set output.allow_overwrite_existing=True to replace them."
            )

    return plan


def save_pandas_table(
    ctx: KPIContext,
    name: str,
    pdf: pd.DataFrame,
    output_root: str,
    fund_paste,
    save_mode: str,
    allow_overwrite_existing: bool,
    run_as_of: str,
) -> TableSavePlan:
    path = _table_path(output_root, name, fund_paste)
    key_cols = TABLE_ROW_KEYS[name]

    if pdf is None or pdf.empty:
        print(f"skip save {name}: empty")
        return TableSavePlan(name=name, path=path, exists=_delta_exists(ctx.spark, path), new_rows=0)

    exists = _delta_exists(ctx.spark, path)

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
        existing = _load_existing_table(ctx.spark, path) if exists else pd.DataFrame()
        merged, table_plan = merge_table_incremental(existing, pdf, key_cols, allow_overwrite_existing)
        table_plan.name = name
        table_plan.path = path

    merged = _annotate_run_metadata(merged, run_as_of)
    ctx.spark.createDataFrame(merged).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print(
        f"saved {name} -> {path} | append={table_plan.append_rows} | "
        f"overwrite={table_plan.overwrite_rows} | skip={table_plan.skipped_rows}"
    )
    return table_plan


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
    save_mode = ctx.settings["OUTPUT_SAVE_MODE"]
    allow_overwrite = ctx.settings["ALLOW_OVERWRITE_EXISTING"]
    run_as_of = str(ctx.settings["AS_OF_DATE"])

    outputs = {
        "kpi_long": ctx.kpi_long,
        "comparison_yoy": ctx.comparison_yoy,
        "comparison_qoq": ctx.comparison_qoq,
        "comparison_wow": ctx.comparison_wow,
        "scope_diff": ctx.scope_diff,
    }

    for name, pdf in outputs.items():
        table_plan = save_pandas_table(
            ctx,
            name,
            pdf,
            output_root,
            fund_paste,
            save_mode,
            allow_overwrite,
            run_as_of,
        )
        for existing in plan.tables:
            if existing.name == name:
                existing.append_rows = table_plan.append_rows
                existing.overwrite_rows = table_plan.overwrite_rows
                existing.skipped_rows = table_plan.skipped_rows

    ctx.save_plan = plan
    return plan

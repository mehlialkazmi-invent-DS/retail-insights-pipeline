"""Gated 'comparable pairs' (like-for-like) KPIs and comparisons.

For each comparison kind (YoY / QoQ / MoM / WoW) the metrics are recomputed over only the
``(product_id, store_id)`` pairs present in **both** compared periods — the same fixed universe v4
built with ``_pairs_same_calendar_years``. This isolates like-for-like movement (same pairs, both
periods) from mix shifts caused by newly listed or closed pairs.

Because every slice dimension is a product attribute, the comparable-pair universe computed once
across the two periods and then grouped by slice is identical to computing it per slice value — so a
single intersection per comparison kind is correct for overall and every slice.

Pair-level data only exists for the current run window, so a comparable comparison is produced only
when the run window spans both compared periods (e.g. a multi-year window for comparable YoY).
Gated by ``comparable_pairs.enabled`` — a no-op otherwise.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.comparisons import _build_comparison_for_dimension, _comparison_dimensions
from kpi_pipeline.context import KPIContext
from kpi_pipeline.kpi_long import _filter_frames_for_dimension, _period_frames, _period_label
from kpi_pipeline.metrics import build_kpi_table

# (comparison_kind, period_type, period_col) — period_col matches kpi_long.PERIODS.
_COMPARABLE_KINDS: List[Tuple[str, str, str]] = [
    ("yoy", "annual", "Year"),
    ("qoq", "quarter", "period_key"),
    ("mom", "monthly", "month_key"),
    ("wow", "weekly", "Year_Week"),
]

_KIND_SAVE_ATTR = {
    "yoy": "comparable_comparison_yoy",
    "qoq": "comparable_comparison_qoq",
    "mom": "comparable_comparison_mom",
    "wow": "comparable_comparison_wow",
}
_KIND_DISPLAY_ATTR = {
    "yoy": "comparable_yoy_display",
    "qoq": "comparable_qoq_display",
    "mom": "comparable_mom_display",
    "wow": "comparable_wow_display",
}

_PAIR_KEYS = ["product_id", "store_id"]
_RESTRICT_FRAMES = ("scoped_daily", "inst_data", "lost_base", "scope_pairs", "scope_pair_weeks")


def _last_two_periods(scoped_daily: DataFrame, period_name: str, period_col: str) -> List:
    """The two most recent period values present in the run window, chronologically ordered."""
    if period_name == "annual":
        ordered = scoped_daily.select(F.col(period_col).alias("p")).distinct().toPandas().sort_values("p")
    elif period_name == "quarter":
        ordered = (
            scoped_daily.select("Year", "Fiscal_Quarter", F.col(period_col).alias("p"))
            .distinct()
            .toPandas()
            .sort_values(["Year", "Fiscal_Quarter"])
        )
    elif period_name == "monthly":
        ordered = (
            scoped_daily.select("Year", "Fiscal_Month", F.col(period_col).alias("p"))
            .distinct()
            .toPandas()
            .sort_values(["Year", "Fiscal_Month"])
        )
    else:  # weekly
        ordered = (
            scoped_daily.select("week_start_date", F.col(period_col).alias("p"))
            .distinct()
            .toPandas()
            .sort_values("week_start_date")
        )
    return ordered["p"].tolist()[-2:]


def _comparable_pairs(scoped_daily: DataFrame, period_col: str, prior_p, current_p) -> DataFrame:
    prior_pairs = scoped_daily.filter(F.col(period_col) == prior_p).select(*_PAIR_KEYS).distinct()
    current_pairs = scoped_daily.filter(F.col(period_col) == current_p).select(*_PAIR_KEYS).distinct()
    return prior_pairs.intersect(current_pairs)


def _restrict_frames(period_frames: Dict[str, DataFrame], comparable_pairs: DataFrame) -> Dict[str, DataFrame]:
    out = dict(period_frames)
    for key in _RESTRICT_FRAMES:
        if key in out and out[key] is not None:
            out[key] = out[key].join(comparable_pairs, on=_PAIR_KEYS, how="inner")
    return out


def _comparable_period_rows(
    ctx: KPIContext,
    frames: Dict[str, DataFrame],
    period_name: str,
    period_col: str,
    period_vals: Sequence,
    metric_cols: List[str],
) -> pd.DataFrame:
    """kpi_long-shaped rows for the two comparable periods (overall + each active slice)."""
    period_filter = F.col(period_col).isin(list(period_vals))
    value_filters = ctx.settings.get("SLICE_VALUE_FILTERS", {}) or {}
    slices: List[Tuple[str, List[str]]] = [("overall", [])] + [(d, [d]) for d in ctx.active_slice_dimensions]
    rows: List[dict] = []
    for slice_name, gk in slices:
        sf = _filter_frames_for_dimension(frames, gk[0], value_filters) if gk else frames
        tbl = build_kpi_table(ctx, sf, period_col, gk, period_filter)
        for _, r in tbl.iterrows():
            rec = {
                "period_type": period_name,
                "period": _period_label(period_name, r),
                "dimension": slice_name,
                "dimension_value": ("ALL" if not gk else r[gk[0]]),
            }
            for m in metric_cols:
                rec[m] = r.get(m)
            rows.append(rec)
    return pd.DataFrame(rows, columns=["period_type", "period", "dimension", "dimension_value"] + metric_cols)


def _comparisons_from_rows(ctx: KPIContext, kind: str, comparable_rows: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run the standard comparison builder for one kind over a comparable kpi_long subset."""
    saved = ctx.kpi_long
    ctx.kpi_long = comparable_rows
    try:
        saves: List[pd.DataFrame] = []
        display = pd.DataFrame()
        for dimension in _comparison_dimensions(ctx):
            disp, save = _build_comparison_for_dimension(ctx, dimension, kind)
            if not save.empty:
                saves.append(save)
            if dimension == "overall" and not disp.empty:
                display = disp
        save_all = pd.concat(saves, ignore_index=True) if saves else pd.DataFrame()
        return display, save_all
    finally:
        ctx.kpi_long = saved


def build_comparable_pairs(ctx: KPIContext) -> None:
    """Populate comparable (like-for-like) kpi_long + comparison tables when gated on."""
    for attr in list(_KIND_SAVE_ATTR.values()) + list(_KIND_DISPLAY_ATTR.values()):
        setattr(ctx, attr, pd.DataFrame())
    ctx.comparable_kpi_long = pd.DataFrame()

    if not ctx.settings.get("COMPARABLE_PAIRS_ENABLED", False):
        print("comparable pairs: skipped (comparable_pairs.enabled=False)")
        return
    if ctx.hybrid_frames is None:
        raise RuntimeError("comparable_pairs.enabled=True but pipeline frames are missing — run build_kpis first.")

    metric_cols = ctx.settings["METRIC_COLS"]
    kpi_parts: List[pd.DataFrame] = []
    counts: List[str] = []

    for kind, period_name, period_col in _COMPARABLE_KINDS:
        pf = _period_frames(ctx.hybrid_frames, period_name)
        period_vals = _last_two_periods(pf["scoped_daily"], period_name, period_col)
        if len(period_vals) < 2:
            counts.append(f"{kind}=n/a(<2 periods in window)")
            continue
        prior_p, current_p = period_vals[0], period_vals[1]

        comparable_pairs = _comparable_pairs(pf["scoped_daily"], period_col, prior_p, current_p).cache()
        pair_count = comparable_pairs.count()
        if pair_count == 0:
            comparable_pairs.unpersist()
            counts.append(f"{kind}=0 common pairs")
            continue

        restricted = _restrict_frames(pf, comparable_pairs)
        comparable_rows = _comparable_period_rows(ctx, restricted, period_name, period_col, period_vals, metric_cols)

        tagged = comparable_rows.copy()
        tagged.insert(0, "comparison_type", kind)
        tagged["comparable_pair_count"] = pair_count
        kpi_parts.append(tagged)

        display, save = _comparisons_from_rows(ctx, kind, comparable_rows)
        setattr(ctx, _KIND_SAVE_ATTR[kind], save)
        setattr(ctx, _KIND_DISPLAY_ATTR[kind], display)
        counts.append(f"{kind}={pair_count} pairs ({prior_p}->{current_p})")
        comparable_pairs.unpersist()

    ctx.comparable_kpi_long = (
        pd.concat(kpi_parts, ignore_index=True) if kpi_parts else pd.DataFrame()
    )
    print("comparable pairs:", " | ".join(counts) if counts else "(none)")

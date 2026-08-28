"""Gated 'comparable pairs' (like-for-like) KPIs and comparisons.

For YoY / WoW, the metrics are recomputed over only the (product_id, store_id) pairs present in
**both** of the last two compared periods, then compared — isolating like-for-like movement (same
pairs, both periods) from mix shifts caused by newly listed or closed pairs.

For QoQ / MoM / YTD, the comparison itself is still the SAME fiscal quarter/month/YTD-window
chained across consecutive years (see comparisons.py), but the comparable-pair universe is the
intersection across EVERY year present for that quarter-number / month-number / YTD window (not
just the two years being compared in one chain link) — the same "same pairs across all the years
specified" universe the reference implementation calls ``sameytd``/``sameyears``. One fixed
universe is used for every chain link within a kind so the pair set stays consistent across links.

Pair-level data only exists for the current run window, so a comparable comparison is produced only
when the run window spans enough periods (e.g. a multi-year window for comparable YoY/YTD, or a
quarter-number/month-number present in at least 2 years for comparable QoQ/MoM).
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
    ("ytd", "ytd", "Year"),
]

# Kinds whose comparable-pair universe spans EVERY year present (grouped by this number column;
# None means no sub-grouping — use every year in the whole period frame, as YTD does). Kinds not
# listed here (yoy, wow) keep the original last-two-periods-only universe.
_ALL_YEARS_NUMBER_COL: Dict[str, Optional[str]] = {"qoq": "Fiscal_Quarter", "mom": "Fiscal_Month", "ytd": None}

_KIND_SAVE_ATTR = {
    "yoy": "comparable_comparison_yoy",
    "qoq": "comparable_comparison_qoq",
    "mom": "comparable_comparison_mom",
    "wow": "comparable_comparison_wow",
    "ytd": "comparable_comparison_ytd",
}
_KIND_DISPLAY_ATTR = {
    "yoy": "comparable_yoy_display",
    "qoq": "comparable_qoq_display",
    "mom": "comparable_mom_display",
    "wow": "comparable_wow_display",
    "ytd": "comparable_ytd_display",
}

_PAIR_KEYS = ["product_id", "store_id"]
_RESTRICT_FRAMES = ("scoped_daily", "inst_data", "lost_base", "scope_pairs", "scope_pair_weeks")


def _last_two_periods(scoped_daily: DataFrame, period_name: str, period_col: str) -> List:
    """The two most recent period values present in the run window, chronologically ordered."""
    if period_name in ("annual", "ytd"):
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


def _all_years_common_pairs(scoped_daily: DataFrame) -> Optional[DataFrame]:
    """(product_id, store_id) pairs present in EVERY year represented in ``scoped_daily`` (by its
    ``Year`` column). None if fewer than 2 distinct years are present — nothing to compare."""
    years = sorted(r["Year"] for r in scoped_daily.select("Year").distinct().collect())
    if len(years) < 2:
        return None
    pairs = None
    for y in years:
        year_pairs = scoped_daily.filter(F.col("Year") == y).select(*_PAIR_KEYS).distinct()
        pairs = year_pairs if pairs is None else pairs.intersect(year_pairs)
    return pairs


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
    """kpi_long-shaped rows for the given comparable periods (overall + each active slice)."""
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


def _build_last_two_periods_comparable_rows(
    ctx: KPIContext, pf: Dict[str, DataFrame], period_name: str, period_col: str, metric_cols: List[str]
) -> Tuple[Optional[pd.DataFrame], int, Optional[Tuple]]:
    """The original YoY/WoW universe: intersect pairs across just the last two periods present."""
    period_vals = _last_two_periods(pf["scoped_daily"], period_name, period_col)
    if len(period_vals) < 2:
        return None, 0, None
    prior_p, current_p = period_vals[0], period_vals[1]

    comparable_pairs = _comparable_pairs(pf["scoped_daily"], period_col, prior_p, current_p).cache()
    pair_count = comparable_pairs.count()
    if pair_count == 0:
        comparable_pairs.unpersist()
        return None, 0, (prior_p, current_p)

    restricted = _restrict_frames(pf, comparable_pairs)
    rows = _comparable_period_rows(ctx, restricted, period_name, period_col, period_vals, metric_cols)
    comparable_pairs.unpersist()
    return rows, pair_count, (prior_p, current_p)


def _build_all_years_comparable_rows(
    ctx: KPIContext, pf: Dict[str, DataFrame], period_name: str, period_col: str, metric_cols: List[str], number_col: Optional[str]
) -> Tuple[List[pd.DataFrame], List[pd.DataFrame], List[int]]:
    """QoQ/MoM/YTD universe: for each number_col value present (or the whole frame when
    number_col is None, as for YTD), intersect pairs across EVERY year present for that number,
    then build comparable rows for EVERY year present for that number (not just the last two) —
    the multi-year chaining in comparisons.py (qoq/mom/ytd_comparison_long) picks the consecutive
    pairs out of this. A number with fewer than 2 years anywhere, or 0 common pairs, is skipped.
    """
    scoped_daily = pf["scoped_daily"]
    if number_col is None:
        numbers: List = [None]
    else:
        numbers = sorted(r[number_col] for r in scoped_daily.select(number_col).distinct().collect())

    rows_parts: List[pd.DataFrame] = []
    tagged_parts: List[pd.DataFrame] = []
    pair_counts: List[int] = []
    for number_val in numbers:
        sd = scoped_daily if number_val is None else scoped_daily.filter(F.col(number_col) == number_val)
        common_pairs = _all_years_common_pairs(sd)
        if common_pairs is None:
            continue
        common_pairs = common_pairs.cache()
        pair_count = common_pairs.count()
        if pair_count == 0:
            common_pairs.unpersist()
            continue

        period_vals = [r["p"] for r in sd.select(F.col(period_col).alias("p")).distinct().collect()]
        restricted = _restrict_frames(pf, common_pairs)
        rows = _comparable_period_rows(ctx, restricted, period_name, period_col, period_vals, metric_cols)
        rows_parts.append(rows)
        tagged = rows.copy()
        tagged["comparable_pair_count"] = pair_count
        tagged_parts.append(tagged)
        pair_counts.append(pair_count)
        common_pairs.unpersist()

    return rows_parts, tagged_parts, pair_counts


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
    selected_kinds = set(ctx.settings.get("COMPARISON_KINDS") or _KIND_SAVE_ATTR.keys())
    kpi_parts: List[pd.DataFrame] = []
    counts: List[str] = []

    for kind, period_name, period_col in _COMPARABLE_KINDS:
        if kind not in selected_kinds:
            continue
        pf = _period_frames(ctx, ctx.hybrid_frames, period_name)

        if kind in _ALL_YEARS_NUMBER_COL:
            rows_parts, tagged_parts, pair_counts = _build_all_years_comparable_rows(
                ctx, pf, period_name, period_col, metric_cols, _ALL_YEARS_NUMBER_COL[kind]
            )
            if not rows_parts:
                counts.append(f"{kind}=n/a(<2 years anywhere)")
                continue

            comparable_rows = pd.concat(rows_parts, ignore_index=True)
            tagged = pd.concat(tagged_parts, ignore_index=True)
            tagged.insert(0, "comparison_type", kind)
            kpi_parts.append(tagged)

            display, save = _comparisons_from_rows(ctx, kind, comparable_rows)
            setattr(ctx, _KIND_SAVE_ATTR[kind], save)
            setattr(ctx, _KIND_DISPLAY_ATTR[kind], display)
            counts.append(f"{kind}=pairs:{pair_counts}")
        else:
            rows, pair_count, period_pair = _build_last_two_periods_comparable_rows(
                ctx, pf, period_name, period_col, metric_cols
            )
            if rows is None:
                if period_pair is None:
                    counts.append(f"{kind}=n/a(<2 periods in window)")
                else:
                    counts.append(f"{kind}=0 common pairs")
                continue

            tagged = rows.copy()
            tagged.insert(0, "comparison_type", kind)
            tagged["comparable_pair_count"] = pair_count
            kpi_parts.append(tagged)

            display, save = _comparisons_from_rows(ctx, kind, rows)
            setattr(ctx, _KIND_SAVE_ATTR[kind], save)
            setattr(ctx, _KIND_DISPLAY_ATTR[kind], display)
            prior_p, current_p = period_pair
            counts.append(f"{kind}={pair_count} pairs ({prior_p}->{current_p})")

    ctx.comparable_kpi_long = (
        pd.concat(kpi_parts, ignore_index=True) if kpi_parts else pd.DataFrame()
    )
    print("comparable pairs:", " | ".join(counts) if counts else "(none)")

"""Tidy long KPI output across slices and periods.

Each row: period_type | period | root | dimension | dimension_value | METRIC_COLS...
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.filters import apply_value_filter as _apply_value_filter
from kpi_pipeline.fiscal import _period_key_columns, last_saturday_on_or_before, week_day_counts
from kpi_pipeline.metrics import build_kpi_table

PERIODS: List[Tuple[str, str]] = [
    ("annual", "Year"),
    ("quarter", "period_key"),
    ("half", "half_key"),
    ("monthly", "month_key"),
    ("weekly", "Year_Week"),
    ("ytd", "Year"),
]


def _with_period_key(df: DataFrame) -> DataFrame:
    return df.withColumn("period_key", F.concat_ws("-", F.col("Year").cast("string"), F.col("Fiscal_Quarter").cast("string")))


def _with_half_key(df: DataFrame) -> DataFrame:
    return df.withColumn("half_key", F.concat_ws("-", F.col("Year").cast("string"), F.col("Fiscal_Half").cast("string")))


def _with_month_key(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "month_key",
        F.concat(F.col("Year").cast("string"), F.lit("-"), F.format_string("%02d", F.col("Fiscal_Month"))),
    )


def _with_ytd_filter(df: DataFrame, available_months: List[int]) -> DataFrame:
    return df.filter(F.col("Fiscal_Month").isin(available_months))


# The frames build_kpi_table reads, period-framed and value-filtered alike (scope_pairs /
# scope_pair_weeks feed no metric).
_METRIC_FRAMES = ("scoped_daily", "inst_data", "lost_base", "dc_daily", "dc_inst")


def _drop_incomplete_periods(
    ctx: KPIContext, frames: Dict[str, DataFrame], period_col: str
) -> Dict[str, DataFrame]:
    """Keep only the rows of (Year, ``period_col``) periods fully inside the window
    (fiscal.complete_fiscal_periods), so the Quarter / Half / Monthly tabs never show a trailing period
    only a week or two in. report_end="latest_day" also uses it for "Year" (Annual: the current fiscal
    year is in YTD only) and "Week" (Weekly: the trailing partial week is dropped).
    """
    complete = F.broadcast(ctx.complete_fiscal_periods[period_col])
    out = dict(frames)
    for key in _METRIC_FRAMES:
        out[key] = out[key].join(complete, on=_period_key_columns(period_col), how="left_semi")
    return out


def _drop_partial_trailing_week(ctx: KPIContext, frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """Weekly tab: keep only the weeks ending on or before the last Saturday on or before REPORT_END_DATE
    (report_end="complete_month" on the civil calendar cuts at a month end, usually mid-week)."""
    last_saturday = last_saturday_on_or_before(ctx.settings["REPORT_END_DATE"])
    whole_weeks = F.broadcast(
        ctx.fiscal_week.filter(F.col("week_end_date") <= F.lit(last_saturday)).select("Year_Week")
    )
    out = dict(frames)
    for key in _METRIC_FRAMES:
        out[key] = out[key].join(whole_weeks, on="Year_Week", how="left_semi")
    return out


def _ytd_latest_day_frames(ctx: KPIContext, frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """report_end="latest_day" YTD: days 1..K of the fiscal year (K = ctx.ytd_through_day) of every year in
    ctx.ytd_years. Daily frames filter on day_index, the pair-week in-stock frames on last_day_index (the
    week containing K was split at K when built). Lost sales is weekly and only reaches the last Saturday,
    so its YTD is whole weeks 1..ctx.ytd_lost_sales_last_week.
    """
    in_ytd_year = F.col("Year").isin(ctx.ytd_years)
    through_day = ctx.ytd_through_day
    # The YTD part of the week containing K has fewer than 7 days; WOS weights by it (week_days).
    ytd_week_days = F.broadcast(
        week_day_counts(ctx).select("Year", "Week", F.col("ytd_week_days").alias("week_days"))
    )
    out = dict(frames)
    out["scoped_daily"] = (
        frames["scoped_daily"]
        .filter(in_ytd_year & (F.col("day_index") <= through_day))
        .drop("week_days")
        .join(ytd_week_days, on=["Year", "Week"], how="inner")
    )
    out["dc_daily"] = frames["dc_daily"].filter(in_ytd_year & (F.col("day_index") <= through_day))
    out["inst_data"] = frames["inst_data"].filter(in_ytd_year & (F.col("last_day_index") <= through_day))
    out["dc_inst"] = frames["dc_inst"].filter(in_ytd_year & (F.col("last_day_index") <= through_day))
    out["lost_base"] = frames["lost_base"].filter(in_ytd_year & (F.col("Week") <= ctx.ytd_lost_sales_last_week))
    return out


# quarter / half / monthly: the period-key column each frame gets, and the fiscal column its
# completeness set (ctx.complete_fiscal_periods) is keyed on.
_PERIOD_KEYED = {
    "quarter": (_with_period_key, "Fiscal_Quarter"),
    "half": (_with_half_key, "Fiscal_Half"),
    "monthly": (_with_month_key, "Fiscal_Month"),
}


def _period_frames(ctx: KPIContext, frames: Dict[str, DataFrame], period_name: str) -> Dict[str, DataFrame]:
    latest_day = ctx.settings["REPORT_END_MODE"] == "latest_day"
    if latest_day and period_name == "annual":
        return _drop_incomplete_periods(ctx, frames, "Year")
    if latest_day and period_name == "weekly":
        return _drop_incomplete_periods(ctx, frames, "Week")
    if latest_day and period_name == "ytd":
        return _ytd_latest_day_frames(ctx, frames)
    if period_name == "weekly" and ctx.settings["REPORT_END_MODE"] == "complete_month" and not ctx.settings["USE_FISCAL_CALENDAR"]:
        return _drop_partial_trailing_week(ctx, frames)
    if period_name in _PERIOD_KEYED:
        with_key, period_col = _PERIOD_KEYED[period_name]
        out = dict(frames)
        for key in _METRIC_FRAMES:
            out[key] = with_key(frames[key])
        return _drop_incomplete_periods(ctx, out, period_col)
    if period_name == "ytd":
        # The fiscal months fully elapsed in the latest year, applied to every year (apples-to-apples YTD).
        available_months = ctx.available_fiscal_months or []
        out = dict(frames)
        for key in _METRIC_FRAMES:
            out[key] = _with_ytd_filter(frames[key], available_months)
        return out
    return frames


def _period_label(period_name: str, row: pd.Series) -> str:
    if period_name == "annual":
        return str(int(row["Year"]))
    if period_name == "quarter":
        y, q = str(row["period_key"]).split("-")
        return f"{int(y)}-Q{int(q)}"
    if period_name == "half":
        y, h = str(row["half_key"]).split("-")
        return f"{int(y)}-H{int(h)}"
    if period_name == "monthly":
        return str(row["month_key"])
    if period_name == "ytd":
        return f"YTD-{int(row['Year'])}"
    return str(row["Year_Week"])


def trim_periods_to_recent(kpi_long: pd.DataFrame, ctx: KPIContext) -> pd.DataFrame:
    """kpi_long with each period_type cut to its N most recent periods (html_report's *_display_* settings),
    for the HTML report; weekly counts fiscal weeks, not only the weeks present."""
    settings = ctx.settings
    trim_cfg: List[Tuple[str, object, bool]] = [
        ("weekly", settings.get("HTML_REPORT_WEEKLY_DISPLAY_WEEKS"), True),
        ("monthly", settings.get("HTML_REPORT_MONTHLY_DISPLAY_MONTHS"), False),
        ("quarter", settings.get("HTML_REPORT_QUARTERLY_DISPLAY_QUARTERS"), False),
        ("half", settings["HTML_REPORT_HALF_DISPLAY_HALVES"], False),
        ("annual", settings.get("HTML_REPORT_YEARLY_DISPLAY_YEARS"), False),
    ]

    if not any(n for _, n, _ in trim_cfg):
        return kpi_long

    parts = []
    for period_type in kpi_long["period_type"].unique():
        chunk = kpi_long[kpi_long["period_type"] == period_type]
        n = next((n for pt, n, _ in trim_cfg if pt == period_type), None)
        use_fiscal_week = next((u for pt, _, u in trim_cfg if pt == period_type), False)

        if not n:
            parts.append(chunk)
            continue

        if use_fiscal_week:
            fiscal_week = ctx.fiscal_week
            if settings["REPORT_END_MODE"] == "latest_day":
                # The trailing partial week is not a Weekly column and takes no slot.
                fiscal_week = fiscal_week.filter(
                    F.col("week_end_date") <= F.lit(last_saturday_on_or_before(settings["REPORT_END_DATE"]))
                )
            fw_pd = fiscal_week.select("Year_Week", "week_start_date").toPandas()
            recent = set(fw_pd.sort_values("week_start_date", ascending=False).head(n)["Year_Week"].tolist())
        else:
            all_periods = sorted(chunk["period"].unique(), reverse=True)
            recent = set(all_periods[:n])

        parts.append(chunk[chunk["period"].isin(recent)])

    return pd.concat(parts, ignore_index=True) if parts else kpi_long.iloc[0:0].copy()


def _filter_frames_for_dimension(
    frames: Dict[str, DataFrame], dim: str, value_filters: Dict[str, object]
) -> Dict[str, DataFrame]:
    """Restrict the metric frames to ``value_filters[dim]`` (list = include-only, or a dict with include /
    exclude / keep_null; filters.normalize_value_filter); unchanged when ``dim`` has no entry."""
    if dim not in value_filters:
        return frames
    spec = value_filters[dim]
    out = dict(frames)
    for key in _METRIC_FRAMES:
        out[key] = _apply_value_filter(out[key], dim, spec)
    return out


def build_kpi_long(ctx: KPIContext, frames: Dict[str, DataFrame]) -> pd.DataFrame:
    """kpi_long for every (root, cut) across the annual / quarter / half / monthly / weekly / ytd periods (half
    only with fiscal_calendar.half_periods; complete periods per _period_frames).

    Roots: "overall" plus one per ctx.root_definitions entry, each restricting the population to one
    dimension_source value. Cuts: "overall" plus each of ctx.cut_dimensions, within every root.
    """
    rows: List[dict] = []
    for period_name, period_col in PERIODS:
        if period_name == "half" and not ctx.settings["HALF_PERIODS"]:
            continue
        pf = _period_frames(ctx, frames, period_name)
        ctx.progress.section = period_name
        rows.extend(kpi_rows(ctx, pf, period_name, period_col, F.lit(True)))
    return kpi_long_frame(ctx, rows)


def kpi_rows(
    ctx: KPIContext, frames: Dict[str, DataFrame], period_type: str, period_col: str, period_filter
) -> List[dict]:
    """kpi_long records of one period type for every (root, cut), ``frames`` already period-framed and
    restricted to ``period_filter`` rows (shared with comparable._comparable_period_rows)."""
    metric_cols = ctx.settings["METRIC_COLS"]
    cuts: List[Tuple[str, List[str]]] = [("overall", [])] + [
        (dim, [dim]) for dim in ctx.cut_dimensions
    ]
    roots: List[Optional[Dict[str, str]]] = [None] + list(ctx.root_definitions)
    value_filters = ctx.settings.get("SLICE_VALUE_FILTERS", {}) or {}
    rows: List[dict] = []
    for root_def in roots:
        if root_def is None:
            root_name, rf = "overall", frames
        else:
            root_name = root_def["root"]
            rf = _filter_frames_for_dimension(
                frames, root_def["dim_col"], {root_def["dim_col"]: [root_def["value"]]}
            )
        for cut_name, gk in cuts:
            sf = _filter_frames_for_dimension(rf, gk[0], value_filters) if gk else rf
            tbl = build_kpi_table(ctx, sf, period_col, gk, period_filter)
            ctx.progress.table_done(f"{root_name} · {cut_name}")
            for _, r in tbl.iterrows():
                rec = {
                    "period_type": period_type,
                    "period": _period_label(period_type, r),
                    "root": root_name,
                    "dimension": cut_name,
                    "dimension_value": ("ALL" if not gk else r[gk[0]]),
                }
                for m in metric_cols:
                    rec[m] = r.get(m)
                rows.append(rec)
    return rows


def kpi_long_frame(ctx: KPIContext, rows: List[dict]) -> pd.DataFrame:
    """kpi_long-shaped pandas frame of ``kpi_rows`` records."""
    return pd.DataFrame(
        rows, columns=["period_type", "period", "root", "dimension", "dimension_value"] + ctx.settings["METRIC_COLS"]
    )

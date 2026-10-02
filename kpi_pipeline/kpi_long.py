"""Tidy long KPI output across slices and periods.

Each row: period_type | period | dimension | dimension_value | METRIC_COLS...
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


# The metric-source frames build_kpi_table reads — the same five frames _VALUE_FILTERED_FRAMES
# lists below. scope_pairs/scope_pair_weeks are excluded on purpose: they carry no fiscal period
# column and feed no metric.
_PERIOD_METRIC_FRAMES = ("scoped_daily", "inst_data", "lost_base", "dc_daily", "dc_inst")


def _drop_incomplete_periods(
    ctx: KPIContext, frames: Dict[str, DataFrame], period_col: str
) -> Dict[str, DataFrame]:
    """Drop every row belonging to a (Year, ``period_col``) that has not fully elapsed as of
    REPORT_END_DATE (fiscal.complete_fiscal_periods).

    Why the Quarter/Half/Monthly trend tabs need it: REPORT_END_DATE is a week boundary that almost
    never lands on a quarter, half or month boundary (report_end="complete_month" aligns it to a
    month, not to a quarter or half), so without this the trailing row of those tabs is
    routinely a period only a week or two in — a 1-week "quarter" plotted next to full 13-week
    ones. That row now simply doesn't appear until the period closes.

    Weekly needs no equivalent while REPORT_END_DATE is a Saturday (the trailing week is whole);
    see _drop_partial_trailing_week for the civil-calendar complete_month cut. YTD has its own
    apples-to-apples month filter in _period_frames.

    report_end="latest_day" (REPORT_END_DATE is any day) uses it for every view but YTD: ``period_col``
    may also be "Year" (the Annual tab: the current fiscal year appears in YTD only) and "Week" (the
    Weekly tab: the trailing partial week is dropped).
    """
    complete = F.broadcast(ctx.complete_fiscal_periods[period_col])
    out = dict(frames)
    for key in _PERIOD_METRIC_FRAMES:
        out[key] = out[key].join(complete, on=_period_key_columns(period_col), how="left_semi")
    return out


def _drop_partial_trailing_week(ctx: KPIContext, frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """Weekly tab: keep only weeks that end on or before the last Saturday on or before
    REPORT_END_DATE.

    report_end="complete_month" without a fiscal calendar cuts REPORT_END_DATE at a calendar month
    end, which is usually mid-week; the week straddling it is clipped to a partial week that would
    otherwise sit next to whole ones. A no-op when REPORT_END_DATE is already a Saturday.
    """
    last_saturday = last_saturday_on_or_before(ctx.settings["REPORT_END_DATE"])
    whole_weeks = F.broadcast(
        ctx.fiscal_week.filter(F.col("week_end_date") <= F.lit(last_saturday)).select("Year_Week")
    )
    out = dict(frames)
    for key in _PERIOD_METRIC_FRAMES:
        out[key] = out[key].join(whole_weeks, on="Year_Week", how="left_semi")
    return out


def _ytd_latest_day_frames(ctx: KPIContext, frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """report_end="latest_day" YTD: days 1..K of the fiscal year (K = ctx.ytd_through_day, the day of
    the fiscal year of REPORT_END_DATE) for every year in ctx.ytd_years -- the same fiscal day for every
    year, summed by "Year" alone downstream.

    Daily frames (scoped_daily, dc_daily) filter on day_index. The pair-week in-stock frames (inst_data,
    dc_inst) filter on last_day_index: the week containing day K was split at K when they were built
    (pipeline.build_instock_daily / build_dc_inst), so the part up to K counts and the part after does
    not. Lost sales is weekly and only reaches the last Saturday, so its YTD is the whole weeks
    1..ctx.ytd_lost_sales_last_week for every year.
    """
    in_ytd_year = F.col("Year").isin(ctx.ytd_years)
    through_day = ctx.ytd_through_day
    # The week containing day K is cut at K in every year: its YTD part has fewer than 7 days, which
    # compute_kpis's WOS needs (week_days, see fiscal.week_day_counts).
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
    if period_name == "quarter":
        out = dict(frames)
        out["scoped_daily"] = _with_period_key(frames["scoped_daily"])
        out["inst_data"] = _with_period_key(frames["inst_data"])
        out["lost_base"] = _with_period_key(frames["lost_base"])
        out["dc_daily"] = _with_period_key(frames["dc_daily"])
        out["dc_inst"] = _with_period_key(frames["dc_inst"])
        return _drop_incomplete_periods(ctx, out, "Fiscal_Quarter")
    if period_name == "half":
        out = dict(frames)
        out["scoped_daily"] = _with_half_key(frames["scoped_daily"])
        out["inst_data"] = _with_half_key(frames["inst_data"])
        out["lost_base"] = _with_half_key(frames["lost_base"])
        out["dc_daily"] = _with_half_key(frames["dc_daily"])
        out["dc_inst"] = _with_half_key(frames["dc_inst"])
        return _drop_incomplete_periods(ctx, out, "Fiscal_Half")
    if period_name == "monthly":
        out = dict(frames)
        out["scoped_daily"] = _with_month_key(frames["scoped_daily"])
        out["inst_data"] = _with_month_key(frames["inst_data"])
        out["lost_base"] = _with_month_key(frames["lost_base"])
        out["dc_daily"] = _with_month_key(frames["dc_daily"])
        out["dc_inst"] = _with_month_key(frames["dc_inst"])
        return _drop_incomplete_periods(ctx, out, "Fiscal_Month")
    if period_name == "ytd":
        # Only the fiscal MONTHS that have fully elapsed for the latest year (see
        # fiscal._compute_available_fiscal_months), applied identically to every year, so
        # summing by "Year" alone below gives an apples-to-apples YTD window across years.
        # Month-grain, not quarter-grain: a quarter still in progress can still have one or more
        # of its own months already fully closed (e.g. Q3 in progress but its first month done) —
        # quarter-grain would have stopped YTD at the end of the PRIOR quarter instead, understating
        # it by up to two months' worth of otherwise-complete data.
        available_months = ctx.available_fiscal_months or []
        out = dict(frames)
        out["scoped_daily"] = _with_ytd_filter(frames["scoped_daily"], available_months)
        out["inst_data"] = _with_ytd_filter(frames["inst_data"], available_months)
        out["lost_base"] = _with_ytd_filter(frames["lost_base"], available_months)
        out["dc_daily"] = _with_ytd_filter(frames["dc_daily"], available_months)
        out["dc_inst"] = _with_ytd_filter(frames["dc_inst"], available_months)
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
    """Trim each period_type to the N most recent periods in kpi_long and saved Delta."""
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
                # The trailing partial week is not a Weekly column, so it must not take one of the N slots.
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


# Frames that carry the slice dimension columns and therefore get value-filtered.
_VALUE_FILTERED_FRAMES = ("scoped_daily", "inst_data", "lost_base", "dc_daily", "dc_inst")


def _filter_frames_for_dimension(
    frames: Dict[str, DataFrame], dim: str, value_filters: Dict[str, object]
) -> Dict[str, DataFrame]:
    """Restrict a slice's frames to the dimension values configured in ``value_filters``.

    Only a per-slice breakdown is filtered; the 'overall' totals never pass through here
    (build_kpi_long / comparable._comparable_period_rows call this only when a slice
    dimension is present). See ``_normalize_value_filter`` for the accepted shapes: a plain
    list (include-only) or a dict with include / exclude / keep_null.
    """
    if dim not in value_filters:
        return frames
    spec = value_filters[dim]
    out = dict(frames)
    for key in _VALUE_FILTERED_FRAMES:
        df = out.get(key)
        if df is None:
            continue
        out[key] = _apply_value_filter(df, dim, spec)
    return out


def build_kpi_long(ctx: KPIContext, frames: Dict[str, DataFrame]) -> pd.DataFrame:
    """Build kpi_long for every (root, cut) combination across annual/quarter/half/monthly/weekly/ytd periods
    (half only when fiscal_calendar.half_periods is on). With report_end="latest_day" the annual,
    quarter, half, monthly and weekly rows are complete periods only and ytd runs to the latest day
    (see _period_frames).

    Roots: "overall" (no restriction, always first) plus one per ctx.root_definitions entry (e.g.
    "nvrout", "comp") -- each restricts the population to rows where that dimension_source's
    column equals the configured/discovered value (see fiscal._resolve_root_definitions). Cuts:
    "overall" (the root's own total, no further breakdown) plus each of ctx.cut_dimensions (e.g.
    "brand", "smw") -- applied identically within EVERY root, including "overall" itself, so
    every root gets both its own aggregate total and a breakdown by each cut dimension (mirrors
    kpi-skill-toolkit's segment x {none, brand, smw} cut_specs).

    Root population restriction reuses _filter_frames_for_dimension (an include-only value_filter
    of exactly one value) -- the same machinery that already restricts a cut's own breakdown.
    """
    metric_cols = ctx.settings["METRIC_COLS"]
    cuts: List[Tuple[str, List[str]]] = [("overall", [])] + [
        (dim, [dim]) for dim in ctx.cut_dimensions
    ]
    roots: List[Optional[Dict[str, str]]] = [None] + list(ctx.root_definitions)
    value_filters = ctx.settings.get("SLICE_VALUE_FILTERS", {}) or {}
    rows: List[dict] = []
    for period_name, period_col in PERIODS:
        if period_name == "half" and not ctx.settings["HALF_PERIODS"]:
            continue
        pf = _period_frames(ctx, frames, period_name)
        for root_def in roots:
            if root_def is None:
                root_name, rf = "overall", pf
            else:
                root_name = root_def["root"]
                rf = _filter_frames_for_dimension(
                    pf, root_def["dim_col"], {root_def["dim_col"]: [root_def["value"]]}
                )
            for cut_name, gk in cuts:
                sf = _filter_frames_for_dimension(rf, gk[0], value_filters) if gk else rf
                tbl = build_kpi_table(ctx, sf, period_col, gk)
                for _, r in tbl.iterrows():
                    rec = {
                        "period_type": period_name,
                        "period": _period_label(period_name, r),
                        "root": root_name,
                        "dimension": cut_name,
                        "dimension_value": ("ALL" if not gk else r[gk[0]]),
                    }
                    for m in metric_cols:
                        rec[m] = r.get(m)
                    rows.append(rec)
    return pd.DataFrame(
        rows, columns=["period_type", "period", "root", "dimension", "dimension_value"] + metric_cols
    )

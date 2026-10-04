"""Fiscal calendar, reporting-window periods and product dimensions.

USE_FISCAL_CALENDAR reads one_time_uploads/fiscal_cal. Otherwise the time grain comes from noob/daily-data:
Year is the calendar year of `date` and Week the native week column (never the source 'year', which can
be the ISO week-year and label late-December weeks as the next year). A week straddling Jan 1 is then two
partial weeks; quarter, month and annual rollups stay correct.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import get_daily_data_raw, read_csv_source


def _build_fiscal_week_frame(
    daily_grain: DataFrame,
    quarter_col: Optional[str] = None,
    month_col: Optional[str] = None,
    month_name_col: Optional[str] = None,
) -> DataFrame:
    """One row per (Year, Week) of day-level rows: week_start_date / week_end_date, Year_Week, Fiscal_Month,
    Fiscal_Quarter, Fiscal_Half[, Fiscal_Month_Name].

    quarter_col / month_col / month_name_col are used when present on ``daily_grain`` (fiscal_calendar.
    column_map); otherwise Fiscal_Month is the calendar month of week_start_date (exact on the civil
    calendar), Fiscal_Quarter is ceil(Fiscal_Month / 3), and Fiscal_Month_Name is left to
    html_report._build_month_display_labels. Fiscal_Half is always 1 for Q1-Q2 and 2 for Q3-Q4.
    """
    has_quarter = bool(quarter_col) and quarter_col in daily_grain.columns
    has_month = bool(month_col) and month_col in daily_grain.columns
    has_month_name = bool(month_name_col) and month_name_col in daily_grain.columns

    agg_exprs = [
        F.min("date").alias("week_start_date"),
        F.max("date").alias("week_end_date"),
    ]
    if has_quarter:
        agg_exprs.append(F.first(quarter_col, ignorenulls=True).alias("_raw_quarter"))
    if has_month:
        agg_exprs.append(F.first(month_col, ignorenulls=True).alias("_raw_month"))
    if has_month_name:
        # The upload's own month label (e.g. "August"), shown verbatim on the Monthly tab.
        agg_exprs.append(F.first(month_name_col, ignorenulls=True).alias("Fiscal_Month_Name"))

    frame = (
        daily_grain.groupBy("Year", "Week")
        .agg(*agg_exprs)
        .withColumn(
            "Year_Week",
            F.concat_ws("-W", F.col("Year").cast("string"), F.format_string("%02d", F.col("Week"))),
        )
    )

    if has_month:
        # Extract numeric month from the raw month column (e.g. "M01" → 1, "1" → 1).
        frame = (
            frame
            .withColumn("Fiscal_Month", F.regexp_extract(F.col("_raw_month"), r"(\d+)", 1).cast("int"))
            .drop("_raw_month")
        )
    else:
        frame = frame.withColumn("Fiscal_Month", F.month("week_start_date"))

    if has_quarter:
        # Extract numeric quarter from the raw quarter column (e.g. "Q1" → 1, "1" → 1).
        frame = (
            frame
            .withColumn("Fiscal_Quarter", F.regexp_extract(F.col("_raw_quarter"), r"(\d+)", 1).cast("int"))
            .drop("_raw_quarter")
        )
    else:
        frame = frame.withColumn(
            "Fiscal_Quarter", ((F.col("Fiscal_Month") - F.lit(1)) / F.lit(3)).cast("int") + F.lit(1)
        )

    return frame.withColumn("Fiscal_Half", ((F.col("Fiscal_Quarter") - F.lit(1)) / F.lit(2)).cast("int") + F.lit(1))


def _fiscal_upload_column_map(ctx: KPIContext) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """(quarter_col, month_col, month_name_col) from config's fiscal_calendar.column_map."""
    s = ctx.settings
    return s.get("FISCAL_QUARTER_COL"), s.get("FISCAL_MONTH_COL"), s.get("FISCAL_MONTH_NAME_COL")


def _read_fiscal_cal_upload(
    ctx: KPIContext,
    path: str,
    report_start_date: Optional[datetime.date] = None,
    report_end_date: Optional[datetime.date] = None,
) -> DataFrame:
    """The fiscal_cal upload as date, Year, Week and the configured fiscal attribute columns, clipped to
    [report_start_date, report_end_date] when both are given (ctx.fiscal_cal); unclipped for the readers
    that need a period's real bounds beyond the window."""
    raw = ctx.spark.read.format("delta").load(path)
    select_cols = ["date", "Year", "Week"]
    for col in _fiscal_upload_column_map(ctx):
        if col and col in raw.columns and col not in select_cols:
            select_cols.append(col)

    out = raw.select(*select_cols).withColumn("date", F.to_date("date"))
    if report_start_date is not None and report_end_date is not None:
        out = out.filter(F.col("date").between(F.lit(report_start_date), F.lit(report_end_date)))
    return out


def _compute_available_fiscal_months(ctx: KPIContext) -> List[int]:
    """Fiscal-month numbers fully elapsed for the latest window year, applied to every year so YTD stays
    apples-to-apples. Month grain, not quarter: an in-progress quarter can already have closed months.
    Uses complete_fiscal_periods, not the window-clipped ctx.fiscal_week, which makes every period look
    complete."""
    fw = ctx.fiscal_week
    latest_year = fw.agg(F.max("Year")).collect()[0][0]
    complete = complete_fiscal_periods(ctx, "Fiscal_Month").filter(F.col("Year") == latest_year)
    return sorted(int(r["Fiscal_Month"]) for r in complete.select("Fiscal_Month").collect())


# Period columns with a (Year, period) completeness set: the Quarter / Half / Monthly tabs' rollups.
COMPLETE_PERIOD_COLUMNS = ("Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")

# report_end="latest_day" also keeps complete years (Annual tab, key (Year,)) and whole weeks (Weekly tab).
LATEST_DAY_COMPLETE_PERIOD_COLUMNS = ("Year", "Week")

# Civil-calendar path only: calendar months per period column, to derive period bounds analytically.
_MONTHS_PER_PERIOD = {"Fiscal_Month": 1, "Fiscal_Quarter": 3, "Fiscal_Half": 6}


def _period_key_columns(period_col: str) -> List[str]:
    """Key columns of one period: (Year,) for the annual "Year" grain, else (Year, period_col)."""
    return ["Year"] if period_col == "Year" else ["Year", period_col]


def last_saturday_on_or_before(day: datetime.date) -> datetime.date:
    """The last Saturday on or before ``day`` (``day`` itself when it is a Saturday)."""
    return day - datetime.timedelta(days=(day.weekday() + 2) % 7)


def _fiscal_period_bounds(ctx: KPIContext, period_col: str) -> DataFrame:
    """One row per (Year, ``period_col``) with the period's REAL period_start / period_end; ``period_col``
    may also be "Year" or "Week".

    Not from ctx.fiscal_week: it is clipped to the window, so a period cut at either window edge would show
    the window boundary as its bound and look complete. Fiscal calendar: the unclipped upload, which must
    extend beyond the window (with quarter / month columns filled there) for the edge periods to be caught.
    Civil calendar: computed (calendar months, Jan 1 - Dec 31 years, 7-day weeks from the first date).
    """
    if ctx.settings["USE_FISCAL_CALENDAR"]:
        quarter_col, month_col, month_name_col = _fiscal_upload_column_map(ctx)
        full_weeks = _build_fiscal_week_frame(
            _read_fiscal_cal_upload(ctx, ctx.settings["PATH_FISCAL"]),
            quarter_col,
            month_col,
            month_name_col,
        )
        if period_col == "Week":
            return full_weeks.select(
                "Year",
                "Week",
                F.col("week_start_date").alias("period_start"),
                F.col("week_end_date").alias("period_end"),
            )
        return full_weeks.groupBy(*_period_key_columns(period_col)).agg(
            F.min("week_start_date").alias("period_start"),
            F.max("week_end_date").alias("period_end"),
        )

    if period_col == "Year":
        return (
            ctx.fiscal_week.select("Year").distinct()
            .withColumn("period_start", F.to_date(F.format_string("%04d-01-01", F.col("Year").cast("int"))))
            .withColumn("period_end", F.to_date(F.format_string("%04d-12-31", F.col("Year").cast("int"))))
        )
    if period_col == "Week":
        # Civil weeks run Sunday to Saturday: a week is whole when its 7 days from its first date fit.
        return (
            ctx.fiscal_week.select("Year", "Week", F.col("week_start_date").alias("period_start"))
            .withColumn("period_end", F.date_add(F.col("period_start"), 6))
        )

    months_in_period = _MONTHS_PER_PERIOD[period_col]
    last_month_number = F.col(period_col) * F.lit(months_in_period)
    first_month_number = last_month_number - F.lit(months_in_period) + F.lit(1)
    return (
        ctx.fiscal_week.select("Year", period_col).distinct()
        .withColumn(
            "period_start",
            F.to_date(F.format_string("%04d-%02d-01", F.col("Year").cast("int"), first_month_number.cast("int"))),
        )
        .withColumn(
            "period_end",
            F.last_day(
                F.to_date(
                    F.format_string("%04d-%02d-01", F.col("Year").cast("int"), last_month_number.cast("int"))
                )
            ),
        )
    )


def complete_fiscal_periods(ctx: KPIContext, period_col: str) -> DataFrame:
    """(Year, ``period_col``) keys of the periods fully inside the window (period_start >=
    EFFECTIVE_REPORT_START_DATE and period_end <= REPORT_END_DATE). Semi-joined onto the metric frames
    (kpi_long._period_frames) so no in-progress or truncated period shows on the Quarter / Half / Monthly
    tabs, nor, with report_end="latest_day", on the Annual and Weekly tabs."""
    start = ctx.settings["EFFECTIVE_REPORT_START_DATE"]
    end = ctx.settings["REPORT_END_DATE"]
    return (
        _fiscal_period_bounds(ctx, period_col)
        .filter((F.col("period_start") >= F.lit(start)) & (F.col("period_end") <= F.lit(end)))
        .select(*_period_key_columns(period_col))
    )


def _civil_month_cut(end: datetime.date) -> datetime.date:
    """Last day of the most recent complete calendar month on or before ``end``."""
    if (end + datetime.timedelta(days=1)).day == 1:
        return end
    return end.replace(day=1) - datetime.timedelta(days=1)


def apply_report_end_mode(ctx: KPIContext) -> None:
    """report_end="complete_month": cut REPORT_END_DATE back to the last day of the most recent fully elapsed
    month (idempotent; must run before anything reads REPORT_END_DATE). "as_of" / "latest_day" change nothing.

    Fiscal calendar: month bounds from the unclipped upload (_fiscal_period_bounds), which must extend past
    REPORT_END_DATE; the cut stays a Saturday. Civil calendar: the last day of the previous calendar month
    (or REPORT_END_DATE when it is a month end), usually mid-week; kpi_long drops the clipped trailing week.
    Raises when no month ends inside the window or the cut month starts before the window.
    """
    s = ctx.settings
    if s["REPORT_END_MODE"] in ("as_of", "latest_day"):
        return
    end = s["REPORT_END_DATE"]
    if s["USE_FISCAL_CALENDAR"]:
        upload_end = _read_fiscal_cal_upload(ctx, s["PATH_FISCAL"]).agg(F.max("date")).collect()[0][0]
        if upload_end is None or upload_end <= end:
            raise ValueError(
                f"report_end='complete_month': the fiscal_cal upload ends {upload_end}; it must extend past "
                f"REPORT_END_DATE {end} so the month containing it can be classified."
            )
        cut_month = (
            _fiscal_period_bounds(ctx, "Fiscal_Month")
            .filter(F.col("period_end") <= F.lit(end))
            .orderBy(F.col("period_end").desc())
            .select("period_start", "period_end")
            .first()
        )
        cut, cut_month_start = (None, None) if cut_month is None else (cut_month["period_end"], cut_month["period_start"])
    else:
        cut = _civil_month_cut(end)
        cut_month_start = cut.replace(day=1)
    start = s["EFFECTIVE_REPORT_START_DATE"]
    if cut is None or cut < start:
        raise ValueError(
            f"report_end='complete_month': no month ends between {start} and {end}; "
            "widen run_min_date or move as_of_date."
        )
    if cut_month_start < start:
        raise ValueError(
            f"report_end='complete_month': the cut month {cut_month_start}..{cut} starts before the report "
            f"start {start}, so its first days are outside the window; widen run_min_date to {cut_month_start} "
            "or earlier, or move as_of_date."
        )
    print(f"report_end=complete_month: REPORT_END_DATE {end} -> {cut}")
    s["REPORT_END_DATE"] = cut


def build_latest_day_windows(ctx: KPIContext) -> None:
    """report_end="latest_day": set ctx.ytd_through_day (K, the day of the fiscal year of REPORT_END_DATE,
    from each year's first date in the unclipped upload, or Jan 1 on the civil calendar), ctx.ytd_years
    (years whose days 1..K are all inside the window), ctx.day_calendar (window dates with day_index and
    last_day_index; the week containing K is split at K) and ctx.ytd_lost_sales_last_week (fiscal week of
    the last Saturday on or before REPORT_END_DATE, 0 when that Saturday is in the previous fiscal year).
    """
    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    if s["USE_FISCAL_CALENDAR"]:
        year_first = (
            _read_fiscal_cal_upload(ctx, s["PATH_FISCAL"]).groupBy("Year").agg(F.min("date").alias("year_first_date"))
        )
    else:
        year_first = ctx.fiscal_week.select("Year").distinct().withColumn(
            "year_first_date", F.to_date(F.format_string("%04d-01-01", F.col("Year").cast("int")))
        )
    dated = (
        ctx.fiscal_cal.select("date", "Year", "Week")
        .join(broadcast(year_first), on="Year", how="inner")
        .withColumn("day_index", F.datediff(F.col("date"), F.col("year_first_date")) + F.lit(1))
        .drop("year_first_date")
    )
    end_row = dated.filter(F.col("date") == F.lit(end)).first()
    through_day, end_year = int(end_row["day_index"]), end_row["Year"]
    ctx.ytd_through_day = through_day
    ctx.day_calendar = (
        dated.withColumn(
            "last_day_index",
            F.max("day_index").over(Window.partitionBy("Year", "Week", F.col("day_index") <= F.lit(through_day))),
        )
        .select("date", "Year", "Week", "day_index", "last_day_index")
        .cache()
    )

    first_dates = {r["Year"]: r["year_first_date"] for r in year_first.collect()}
    window_years = sorted(r["Year"] for r in ctx.fiscal_week.select("Year").distinct().collect())
    ctx.ytd_years = [
        int(y)
        for y in window_years
        if first_dates[y] >= start and first_dates[y] + datetime.timedelta(days=through_day - 1) <= end
    ]

    last_saturday = last_saturday_on_or_before(end)
    saturday_row = ctx.fiscal_cal.filter(F.col("date") == F.lit(last_saturday)).first()
    if saturday_row is None:
        raise ValueError(
            f"report_end='latest_day': the last Saturday on or before REPORT_END_DATE {end} ({last_saturday}) "
            f"is before the report start {start}; lost sales has no complete week in the window. Widen "
            "run_min_date or move as_of_date."
        )
    ctx.ytd_lost_sales_last_week = int(saturday_row["Week"]) if saturday_row["Year"] == end_year else 0
    print(
        f"report_end=latest_day: YTD through fiscal day {through_day} of REPORT_END_DATE {end} | YTD years: "
        f"{ctx.ytd_years} (years not in the window from day 1: "
        f"{[int(y) for y in window_years if int(y) not in ctx.ytd_years]}) | lost sales through {last_saturday} "
        f"(YTD weeks 1..{ctx.ytd_lost_sales_last_week})"
    )


def week_day_counts(ctx: KPIContext) -> DataFrame:
    """report_end="latest_day": (Year, Week, week_days, ytd_week_days), the days of each fiscal week inside
    the window and how many fall on or before day K. WOS weights a part week's inventory by days / 7."""
    return ctx.day_calendar.groupBy("Year", "Week").agg(
        F.count(F.lit(1)).alias("week_days"),
        F.sum((F.col("day_index") <= F.lit(ctx.ytd_through_day)).cast("int")).alias("ytd_week_days"),
    )


def require_complete_time_grain(
    ctx: KPIContext, fiscal_cal: DataFrame, start: datetime.date, end: datetime.date, grain_label: str
) -> None:
    """Raise when ``fiscal_cal`` lacks a date between ``start`` and ``end``: a missing date would drop out of
    every daily metric and shift the in-stock week bounds (on the daily-data path it means missing data)."""
    expected = ctx.spark.range(1).select(F.explode(F.sequence(F.lit(start), F.lit(end))).alias("date"))
    missing = [
        r["date"]
        for r in expected.join(fiscal_cal.select("date").distinct(), on="date", how="left_anti")
        .orderBy("date")
        .collect()
    ]
    if missing:
        raise ValueError(
            f"time grain ({grain_label}) is missing {len(missing)} date(s) between {start} and {end}: "
            f"{[str(d) for d in missing[:20]]}"
        )


def build_fiscal_cal_and_week_from_upload(
    ctx: KPIContext,
    path: str,
    report_start_date: datetime.date,
    report_end_date: datetime.date,
) -> Tuple[DataFrame, DataFrame]:
    quarter_col, month_col, month_name_col = _fiscal_upload_column_map(ctx)
    fiscal_cal_out = _read_fiscal_cal_upload(ctx, path, report_start_date, report_end_date)
    return fiscal_cal_out, _build_fiscal_week_frame(fiscal_cal_out, quarter_col, month_col, month_name_col)


def build_time_grain_from_daily_data(
    ctx: KPIContext,
    time_cols: Dict[str, str],
    report_start_date: datetime.date,
    report_end_date: datetime.date,
) -> Tuple[DataFrame, DataFrame]:
    date_col, week_col = time_cols["date"], time_cols["week"]
    # Year = calendar year of `date` (not the ISO week-year 'year'). get_daily_data_raw, so
    # input_filters.daily_data applies to the period labels too.
    daily_rows = get_daily_data_raw(ctx)
    if ctx.settings["SALES_BASIS"] == "gross":
        # Sales-only days borrow their week from these rows, so they cannot also be what completes the grain.
        daily_rows = daily_rows.filter(~F.col("gross_only"))
    daily_time = (
        daily_rows
        .select(date_col, week_col)
        .withColumn(date_col, F.to_date(F.col(date_col)))
        .filter(F.col(date_col).between(F.lit(report_start_date), F.lit(report_end_date)))
        .select(
            F.col(date_col).alias("date"),
            F.year(F.col(date_col)).alias("Year"),
            F.col(week_col).cast("int").alias("Week"),
        )
        .distinct()
    )
    return daily_time, _build_fiscal_week_frame(daily_time)


def _build_time_grain(ctx: KPIContext) -> str:
    """Set ctx.fiscal_cal / ctx.fiscal_week for the report window (fiscal_cal upload, or the daily-data
    year/week on the civil calendar), cached and checked for missing dates. Returns the grain label."""
    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]

    if s["USE_FISCAL_CALENDAR"]:
        fiscal_cal, fiscal_week = build_fiscal_cal_and_week_from_upload(ctx, s["PATH_FISCAL"], start, end)
        grain_label = "fiscal_cal upload"
    else:
        fiscal_cal, fiscal_week = build_time_grain_from_daily_data(
            ctx, s["DAILY_TIME_COLUMNS"], start, end
        )
        grain_label = "daily-data year/week"

    ctx.fiscal_cal = fiscal_cal.cache()
    ctx.fiscal_week = fiscal_week.cache()
    require_complete_time_grain(ctx, ctx.fiscal_cal, start, end, grain_label)
    return grain_label


def build_fiscal_week_only(ctx: KPIContext) -> None:
    """Load fiscal_cal and fiscal_week only — used by html_only run mode for weekly column order."""
    grain_label = _build_time_grain(ctx)
    print("html_only time grain:", grain_label, "| fiscal weeks:", ctx.fiscal_week.count())


def _join_dimension_sources(
    ctx: KPIContext,
    products_proj: DataFrame,
    dimension_sources: List[Dict[str, Any]],
    taken_dims: List[str],
) -> Tuple[DataFrame, List[str]]:
    """Left-join the enabled dimension_sources onto the product attribute table; returns it and the added
    dimension names.

    Each source adds its ``columns`` and ``derived`` SQL expressions (evaluated on the source table), one row
    per ``join_key`` (a column of ``products_proj``). An enabled source fails loudly on a bad path, column or
    expression. ``fillna`` ({dim: value}) coalesces the NULLs of products absent from the source (a
    ``derived`` CASE never runs for them), e.g. {"is_comp": "yes"} for a partial source.
    """
    source_dims: List[str] = []
    for src in dimension_sources:
        if not src.get("enabled"):
            continue
        label = src.get("label", "dimension_source")
        join_key = src.get("join_key", "product_id")
        if join_key not in products_proj.columns:
            raise ValueError(
                f"dimension_source {label!r} join_key {join_key!r} is not a column on the "
                f"product attribute table {products_proj.columns}; use 'product_id' or a "
                "column carried from the products master."
            )
        path = src.get("path")
        if not path:
            raise ValueError(
                f"dimension_source {label!r} requires a resolved 'path' "
                "(set 'path' or 'path_segments' in config)."
            )

        src_type = (src.get("source") or "").strip().lower()
        if src_type not in {"delta", "csv"}:
            src_type = "csv" if path.lower().endswith(".csv") else "delta"
        if src_type == "csv":
            print(f"  dimension_source {label!r}: csv ({path})")
            raw = read_csv_source(
                ctx.spark, path, src.get("csv_options") or {}, src.get("location", "datastore")
            )
        else:
            print(f"  dimension_source {label!r}: delta ({path})")
            raw = ctx.spark.read.format("delta").load(path)

        derived = dict(src.get("derived", {}) or {})
        raw_cols = list(src.get("columns", []) or [])
        wanted = [c for c in (raw_cols + list(derived)) if c not in taken_dims and c not in source_dims]
        if not wanted:
            print(
                f"NOTE: dimension_source {label!r} enabled but adds no new dimensions "
                "(all its columns are already provided elsewhere)."
            )
            continue

        sel = [F.col(join_key)]
        for name in wanted:
            sel.append(F.expr(derived[name]).alias(name) if name in derived else F.col(name))
        src_proj = raw.select(*sel).dropDuplicates([join_key])
        products_proj = products_proj.join(broadcast(src_proj), on=join_key, how="left")

        fillna_cfg = dict(src.get("fillna", {}) or {})
        unknown_fillna = set(fillna_cfg) - set(wanted)
        if unknown_fillna:
            raise ValueError(
                f"dimension_source {label!r} fillna has key(s) {sorted(unknown_fillna)} not "
                f"among its own dimensions {wanted}."
            )
        for name, default in fillna_cfg.items():
            products_proj = products_proj.withColumn(name, F.coalesce(F.col(name), F.lit(default)))

        source_dims.extend(wanted)
        if fillna_cfg:
            print(
                f"  dimension_source {label!r}: joined on '{join_key}' -> dims {wanted} "
                f"(fillna: {fillna_cfg})"
            )
        else:
            print(f"  dimension_source {label!r}: joined on '{join_key}' -> dims {wanted}")

    return products_proj, source_dims


def _resolve_root_definitions(
    ctx: KPIContext, products_proj: DataFrame, root_specs: List[Dict[str, Any]]
) -> List[Dict[str, str]]:
    """ROOT_SPECS resolved to {"root", "dim_col", "value"} definitions ("overall" is implicit). root_values
    ({raw value: root name}) gives exactly those roots; without it, one root per distinct non-null value in
    the data. A dim_col missing from ``products_proj`` (its source contributed nothing) is skipped."""
    definitions: List[Dict[str, str]] = []
    for spec in root_specs:
        dim_col = spec["dim_col"]
        if dim_col not in products_proj.columns:
            continue
        root_values = spec.get("root_values")
        if root_values:
            for raw_value, root_name in root_values.items():
                definitions.append({"root": str(root_name), "dim_col": dim_col, "value": raw_value})
        else:
            distinct_values = [
                row[dim_col]
                for row in products_proj.select(dim_col).distinct().collect()
                if row[dim_col] is not None
            ]
            for raw_value in distinct_values:
                definitions.append({"root": str(raw_value), "dim_col": dim_col, "value": raw_value})
    return definitions


def build_fiscal_and_products(ctx: KPIContext) -> None:
    """Populate ctx.fiscal_cal, ctx.fiscal_week, ctx.products_attr, ctx.active_slice_dimensions."""
    s = ctx.settings
    grain_label = _build_time_grain(ctx)

    null_quarter_weeks = ctx.fiscal_week.filter(F.col("Fiscal_Quarter").isNull()).count()
    if null_quarter_weeks > 0:
        raise ValueError(
            f"{null_quarter_weeks} fiscal week(s) in the report window have a null Fiscal_Quarter "
            f"(unparseable value in fiscal_calendar.column_map.quarter_col = {s.get('FISCAL_QUARTER_COL')!r}). "
            "Fix that column in the fiscal_cal upload, or set quarter_col to None to derive from "
            "Fiscal_Month instead."
        )

    null_month_weeks = ctx.fiscal_week.filter(F.col("Fiscal_Month").isNull()).count()
    if null_month_weeks > 0:
        raise ValueError(
            f"{null_month_weeks} fiscal week(s) in the report window have a null Fiscal_Month "
            f"(missing/unparseable value in fiscal_calendar.column_map.month_col = {s.get('FISCAL_MONTH_COL')!r} "
            "for that week). Extend that column through the report window before running -- "
            "otherwise those weeks silently drop out of the monthly rollup."
        )

    latest_day = s["REPORT_END_MODE"] == "latest_day"
    if latest_day:
        build_latest_day_windows(ctx)
    else:
        ctx.available_fiscal_months = _compute_available_fiscal_months(ctx)
        print("available (fully elapsed) fiscal months for YTD:", ctx.available_fiscal_months)

    # Cached once per run; kpi_long._period_frames semi-joins them onto every metric frame.
    complete_columns = COMPLETE_PERIOD_COLUMNS + (LATEST_DAY_COMPLETE_PERIOD_COLUMNS if latest_day else ())
    ctx.complete_fiscal_periods = {
        period_col: complete_fiscal_periods(ctx, period_col).cache()
        for period_col in complete_columns
        if period_col != "Fiscal_Half" or s["HALF_PERIODS"]
    }
    latest_complete = {
        period_col: pairs.agg(F.max(F.struct(*_period_key_columns(period_col))).alias("latest")).collect()[0]["latest"]
        for period_col, pairs in ctx.complete_fiscal_periods.items()
    }
    print("latest fully elapsed fiscal period (Quarter/Half/Monthly trend tabs end here):", latest_complete)

    products_raw = ctx.spark.read.format("delta").load(s["PATH_PRODUCTS"])
    slice_dims = s["SLICE_DIMENSIONS"]
    derived_dims_cfg = s["DERIVED_SLICE_DIMENSIONS"]
    dimension_sources = s.get("DIMENSION_SOURCES", []) or []

    # Dimensions an enabled dimension_source supplies: not "missing" from the products master.
    source_provided = [
        c
        for src in dimension_sources
        if src.get("enabled")
        for c in (list(src.get("columns", []) or []) + list((src.get("derived", {}) or {}).keys()))
    ]

    existing_dims = [c for c in slice_dims if c in products_raw.columns]
    missing_existing = [
        c for c in slice_dims if c not in products_raw.columns and c not in source_provided
    ]
    if missing_existing:
        print(
            "NOTE: skipping SLICE_DIMENSIONS not found in products table or any enabled "
            "dimension_source:",
            missing_existing,
        )

    derived_dims = []
    derived_exprs = {}
    for name, sql in derived_dims_cfg.items():
        try:
            products_raw.select(F.expr(sql).alias(name)).schema
            derived_dims.append(name)
            derived_exprs[name] = sql
        except Exception as exc:
            print(f"NOTE: skipping derived dimension {name!r} (expression failed to resolve): {exc}")

    products_proj = products_raw.select(
        "product_id",
        "cogs",
        "price_without_tax",
        *existing_dims,
        *[F.expr(derived_exprs[n]).alias(n) for n in derived_dims],
    ).dropDuplicates(["product_id"])

    # Gated: joins nothing unless a dimension_source has enabled=True.
    products_proj, source_dims = _join_dimension_sources(
        ctx, products_proj, dimension_sources, existing_dims + derived_dims
    )

    ctx.active_slice_dimensions = existing_dims + derived_dims + source_dims
    products_proj = products_proj.cache()
    ctx.products_attr = broadcast(products_proj)
    ctx.product_dims = broadcast(products_proj.select("product_id", *ctx.active_slice_dimensions))

    root_specs = s.get("ROOT_SPECS", []) or []
    ctx.root_definitions = _resolve_root_definitions(ctx, products_proj, root_specs)
    root_dim_cols = {spec["dim_col"] for spec in root_specs}
    ctx.cut_dimensions = [d for d in ctx.active_slice_dimensions if d not in root_dim_cols]

    print("time grain:", grain_label)
    print("fiscal weeks:", ctx.fiscal_week.count())
    print(
        "ROOTS:",
        ["overall"] + [r["root"] for r in ctx.root_definitions],
        "| CUT_DIMENSIONS:",
        ctx.cut_dimensions,
    )
    print(
        "ACTIVE_SLICE_DIMENSIONS:",
        ctx.active_slice_dimensions,
        "(existing:",
        existing_dims,
        "| derived:",
        derived_dims,
        "| dimension_sources:",
        source_dims,
        ")",
    )

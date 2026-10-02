"""Fiscal calendar and product dimension setup.

When USE_FISCAL_CALENDAR is True, reads one_time_uploads/fiscal_cal.
Otherwise derives the time grain from noob/daily-data: Year is the CALENDAR year of
`date`, and Week is the native fiscal week column. The source 'year' column is NOT
used, because it can carry the ISO week-year (late-December weeks labelled as the next
year), which would mislabel e.g. December 2025 as Q4 2026. A fiscal week straddling
Jan 1 is therefore reported as two partial weeks (one per calendar year); quarter,
month and annual rollups remain correct.
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
    """Aggregate day-level Year/Week rows to one row per (Year, Week).

    Shared by both time-grain paths: the fiscal_cal upload (build_fiscal_cal_and_week_from_upload)
    and the civil-calendar daily-data fallback (build_time_grain_from_daily_data, which never has
    quarter_col/month_col/month_name_col to pass). Each of quarter_col/month_col/month_name_col is
    used when it names a column actually present on daily_grain; otherwise that fiscal attribute is
    derived instead. This lets every client's fiscal_cal upload -- with or without any of these
    columns -- and the no-upload fallback both resolve to the same output shape (Fiscal_Quarter,
    Fiscal_Half, Fiscal_Month[, Fiscal_Month_Name]). See config.py's fiscal_calendar.column_map.

    Derivation fallbacks:
      * Fiscal_Month: F.month(week_start_date) -- the real calendar month. This IS correct as-is
        on the civil-calendar path (there is no separate fiscal month there); on a fiscal-calendar
        upload missing a month column, it's the best available substitute.
      * Fiscal_Quarter: ceil(Fiscal_Month / 3), computed from Fiscal_Month (whichever source
        produced it above) rather than from `date` directly, so quarter and month stay internally
        consistent with each other regardless of which one actually came from the upload.
      * Fiscal_Half: Fiscal_Quarter 1-2 -> 1, 3-4 -> 2, always derived from Fiscal_Quarter.
      * Fiscal_Month_Name: no derivation here -- see html_report._build_month_display_labels,
        which derives a display name from the majority real calendar month across each fiscal
        month's actual dates when this column is absent.
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
        # Verbatim display month label from the fiscal_cal upload (e.g. "August") -- trusts the
        # client's own fiscal calendar instead of deriving one. Consumed by html_report's Monthly
        # tab when present.
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
    """The fiscal_cal upload projected to date + Year/Week + whichever fiscal attribute columns
    config names, optionally clipped to the report window.

    Both bounds are optional so the same projection serves two readers: ctx.fiscal_cal (clipped to
    [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE] -- what every downstream consumer reads), and
    _fiscal_period_bounds's UNCLIPPED read, which needs each quarter's/month's REAL first/last
    week, including weeks on either side of the window that the clipped calendar deliberately drops.
    """
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
    """Fiscal-month numbers fully elapsed (as of REPORT_END_DATE) for the latest year in the
    report window, applied identically to every year so YTD stays apples-to-apples once the
    current year is only partially reported (e.g. months 01-07 closed -> YTD sums 01-07 for
    every year, not the in-progress month 08).

    Month-grain, not quarter-grain: a quarter in progress can still have already-closed months
    (e.g. Q3 in progress but its first month done) -- quarter-grain would understate YTD by up to
    two months every time the current quarter is in progress, which is virtually always
    (REPORT_END_DATE is a week boundary, essentially never a quarter boundary).

    Delegates to complete_fiscal_periods -- NOT ctx.fiscal_week directly, which is itself clipped
    to [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE] and so would trivially treat every period in
    the window as complete. This was a live bug (in-progress periods always read as elapsed) fixed
    alongside comparable.py's analogous _complete_period_years.
    """
    fw = ctx.fiscal_week
    latest_year = fw.agg(F.max("Year")).collect()[0][0]
    complete = complete_fiscal_periods(ctx, "Fiscal_Month").filter(F.col("Year") == latest_year)
    return sorted(int(r["Fiscal_Month"]) for r in complete.select("Fiscal_Month").collect())


# Period columns a (Year, period) completeness set is computed for — the fiscal rollups the
# Quarter/Half/Monthly value-trend tabs group by (see kpi_long._period_frames).
COMPLETE_PERIOD_COLUMNS = ("Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")

# report_end="latest_day" also keeps only complete years and whole weeks: "Year" is a complete fiscal
# year (the Annual tab), "Week" a complete (Year, Week) (the Weekly tab). Their key columns are
# (Year,) and (Year, Week); every other period column's are (Year, period_col).
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
    """One row per (Year, ``period_col``) with ``period_start``/``period_end`` — that period's
    REAL first/last date. ``period_col`` may also be "Year" (one row per fiscal / calendar year) or
    "Week" (one row per (Year, Week)), the grains report_end="latest_day" keeps complete only.

    Deliberately NOT read from ctx.fiscal_week. ctx.fiscal_week is clipped to
    [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE] (see build_fiscal_cal_and_week_from_upload), so
    a period truncated at EITHER edge of the window reports a start/end exactly at the window's own
    boundary and would compare as "fully inside" against any window-boundary-based test:

      * a period still in progress at the window's leading (most recent) edge always shows
        period_end == REPORT_END_DATE regardless of when it really ends;
      * a period whose real start predates EFFECTIVE_REPORT_START_DATE (e.g. run_min_date doesn't
        land on a period boundary) always shows period_start == EFFECTIVE_REPORT_START_DATE
        regardless of when it really starts.

    Both need the calendar BEYOND the window to detect:

      * fiscal-calendar upload (use_fiscal_calendar=True) — re-read unclipped, so a period still in
        progress carries its true (future) end date, and one truncated at the window's start
        carries its true (earlier) start date. This needs the upload to actually cover the weeks
        before/after the window, with its quarter/month columns populated there: an upload that
        stops at the window edge (or leaves those columns null past it) makes the truncated period
        look complete again and nothing is excluded. Only IN-WINDOW weeks are validated for null
        quarter/month (see build_fiscal_and_products), so extend the upload through the fiscal
        year on both sides to get this guard.
      * daily-data time grain (use_fiscal_calendar=False) — no calendar exists beyond the data, but
        on that path Fiscal_Month IS the real calendar month of the week start and Fiscal_Quarter
        is ceil(month/3) (see _build_fiscal_week_frame's derivation fallbacks), so the period
        start/end are the first/last day of that calendar month / of the quarter's or half's first
        and last month — computable analytically, no calendar lookup needed. A "Year" is Jan 1 to
        Dec 31 and a "Week" is its first date plus 6 days.
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
    """(Year, ``period_col``) pairs that have FULLY ELAPSED **and** are fully contained in the
    report window: ``period_start >= EFFECTIVE_REPORT_START_DATE`` and
    ``period_end <= REPORT_END_DATE``.

    Semi-join this onto a metric frame to drop every row belonging to a period that is either
    still in progress at the window's trailing edge or truncated at its leading edge — what keeps
    an incomplete quarter/half/month off the Quarter, Half and Monthly value-trend tabs
    (kpi_long._period_frames). Every pair well inside the window is trivially complete; only the
    one or two nearest either edge are ever at risk.

    This is a PER-PAIR test ("is THIS year's Q3 fully inside the window?"), distinct from
    _compute_available_fiscal_months's per-NUMBER test for YTD ("which fiscal MONTH numbers are
    over for the latest year", then applied to every year so YTD stays apples-to-apples) — that
    function DOES delegate to this same helper (at Fiscal_Month grain) for its own per-pair check,
    it just then reduces the result to a plain list of month numbers for the latest year only. The
    Weekly tab needs no completeness set: REPORT_END_DATE is a Saturday, except with
    report_end="complete_month" on the civil calendar, where kpi_long._period_frames drops the
    trailing partial week instead.

    report_end="latest_day" (REPORT_END_DATE is any day) also uses it for the Annual ("Year") and
    Weekly ("Week") tabs, so the current fiscal year and the trailing partial week are dropped.
    """
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
    """report_end="complete_month": cut REPORT_END_DATE back to the last day of the most recent
    fully elapsed month on or before it.

    Must run before anything reads REPORT_END_DATE (build_fiscal_and_products, the scopes, the
    daily in-stock and blocked-days windows, the saved-outputs HTML in html_only mode): they all
    read the cut date from ctx.settings. Running it again on an already cut date changes nothing.

    Fiscal calendar (use_fiscal_calendar=True): month bounds come from _fiscal_period_bounds, i.e.
    the UNCLIPPED fiscal_cal upload, so a month still in progress at REPORT_END_DATE reports its
    real (later) last day and is excluded. The upload must extend past REPORT_END_DATE: an upload
    ending earlier would make its own last date look like a month end. Fiscal months are whole
    weeks, so the cut date stays the Saturday that ends a week.

    Civil calendar (use_fiscal_calendar=False): no calendar exists beyond the data, so the cut is
    the last day of the previous calendar month (or REPORT_END_DATE itself when it is a month end).
    That is usually not a Saturday: the last week is clipped at the cut, and kpi_long drops the
    clipped trailing Weekly column. Months are bucketed by each week's start date, so this is a
    calendar-month cut, not exact calendar-month totals.

    Raises when no month ends inside the window, and when the cut month starts before
    EFFECTIVE_REPORT_START_DATE (fiscal: the month's period_start; civil: the 1st of the cut month):
    that month would be reported with its first days missing.

    "as_of" and "latest_day" leave REPORT_END_DATE as materialize resolved it (the last completed
    Saturday, or as_of_date itself for "latest_day"), so they return without changing anything.
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
    """report_end="latest_day": resolve the same-fiscal-day YTD window and the lost-sales YTD weeks.

    K is the day of the fiscal year (1-based: days since that year's first date, plus 1) of
    REPORT_END_DATE. Fiscal calendar: each year's first date is read from the UNCLIPPED fiscal_cal
    upload (the window's own calendar is clipped); civil calendar: Jan 1 (day of the calendar year).
    YTD of every year in ctx.ytd_years is its days 1..K. A year qualifies only when its first date is
    inside the report window (its days 1..K are all reported); the latest year's day K is
    REPORT_END_DATE itself.

    Sets ctx.day_calendar (the window's (date, Year, Week) + day_index + last_day_index; the week
    containing day K splits into the days <= K and the days after, each with its own last_day_index, so
    the pair-week in-stock frames can be cut at K), ctx.ytd_through_day, ctx.ytd_years and
    ctx.ytd_lost_sales_last_week. Lost sales only reaches the last Saturday on or before
    REPORT_END_DATE; its YTD is the whole weeks 1..(fiscal week of that Saturday) for every year, or no
    week at all when that Saturday falls in the previous fiscal year (day K is in week 1).
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
    """report_end="latest_day": (Year, Week, week_days, ytd_week_days) from ctx.day_calendar -- the days
    of each fiscal week inside the report window, and how many of them fall on or before the YTD cut
    day K (ctx.ytd_through_day).

    Every week is 7 days except the week containing K in the latest year, which the report window ends
    inside, and in the YTD views the week containing K in every year, which stops at day K. metrics.
    compute_kpis weights WOS's weekly inventory by days / 7 so such a part week is not counted as a full
    week of inventory against a part week of sales (pipeline._with_week_days attaches week_days;
    kpi_long._ytd_latest_day_frames swaps in ytd_week_days).
    """
    return ctx.day_calendar.groupBy("Year", "Week").agg(
        F.count(F.lit(1)).alias("week_days"),
        F.sum((F.col("day_index") <= F.lit(ctx.ytd_through_day)).cast("int")).alias("ytd_week_days"),
    )


def require_complete_time_grain(
    ctx: KPIContext, fiscal_cal: DataFrame, start: datetime.date, end: datetime.date, grain_label: str
) -> None:
    """Raise when ``fiscal_cal`` lacks any date between ``start`` and ``end``.

    A missing date silently drops out of every daily metric and shifts the week bounds the
    in-stock denominator is counted from. On the daily-data path the calendar is only the dates
    present in daily-data, so a gap there means missing source data.
    """
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
    # Year is the CALENDAR year of `date`, not the source 'year' column: that column can
    # carry the ISO week-year, which labels late-December weeks as the following year
    # (e.g. Dec 2025 -> 2026) and mismatches the Quarter/Month derived from `date`.
    #
    # Reads via get_daily_data_raw -- the same cached, config-filtered daily_data read every
    # other consumer uses -- rather than loading PATH_DAILY_DATA directly, so
    # input_filters.daily_data (e.g. "usable = 1") applies here too. This is the function that
    # derives Year/Week/Quarter/Month on the civil-calendar path (use_fiscal_calendar=False), so
    # an unfiltered read here would leak excluded rows into every downstream period label.
    daily_time = (
        get_daily_data_raw(ctx)
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
    # No quarter_col/month_col here -- _build_fiscal_week_frame's derivation fallbacks (real
    # calendar month of week_start_date, quarter = ceil(that month / 3)) are exactly the civil
    # calendar's Quarter/Month, so there's nothing to source from this path's raw daily-data table.
    return daily_time, _build_fiscal_week_frame(daily_time)


def build_fiscal_week_only(ctx: KPIContext) -> None:
    """Load fiscal_cal and fiscal_week only — used by html_only run mode for weekly column order."""
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
    print("html_only time grain:", grain_label, "| fiscal weeks:", ctx.fiscal_week.count())


def _join_dimension_sources(
    ctx: KPIContext,
    products_proj: DataFrame,
    dimension_sources: List[Dict[str, Any]],
    taken_dims: List[str],
) -> Tuple[DataFrame, List[str]]:
    """Left-join optional external dimension sources onto the product attribute table.

    Gated feature: only sources with ``enabled=True`` are read — by default this does
    nothing and slices come from the products master alone. Each enabled source
    contributes its raw ``columns`` and ``derived`` SQL expressions (evaluated against
    the *source* table) as new slice dimensions, joined by ``join_key`` (must already be
    a column on ``products_proj`` — normally ``product_id``). The source is reduced to one
    row per ``join_key`` before the join so it cannot fan out the product rows.

    Unlike products ``derived_dimensions`` (best-effort, skipped on error), an *enabled*
    dimension source fails loudly on a bad path, missing column, or unresolved
    expression — a silently dropped segment would misreport the breakdown it was added
    to produce.

    ``fillna`` (optional, per source): ``{dim_name: default_value}``. Because the join is
    a LEFT join, a product with no row in the source table gets ``NULL`` for that source's
    dimensions — a ``CASE ... ELSE`` in ``derived`` never fires for it, since it has no row
    to evaluate the expression against. ``fillna`` runs *after* the join and coalesces those
    NULLs to the given literal, e.g. ``{"is_comp": "yes"}`` treats every product absent from
    a partial (non-full-universe) source as the complement value.
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
    """Resolve config.py's ROOT_SPECS (dim_col + optional explicit root_values) to concrete root
    definitions: {"root": name, "dim_col": col, "value": raw_value}, one per root population.

    "overall" (no restriction) is implicit everywhere else and never appears in this list. A
    dim_col missing from products_proj (its source disabled, or nothing actually joined) is
    skipped rather than erroring -- root_specs already validated dim_col names against each
    source's own declared columns at config time; a still-missing column just means that source
    contributed nothing this run.

    root_values present (dict of raw_value -> root name) -> exactly those roots, restricted to
    the listed values. root_values absent/empty -> AUTO: one root per distinct non-null value
    actually found in products_proj, named after the raw value itself -- this needs real data,
    which is why it's resolved here (runtime, Spark available) rather than in config.py
    (pure-Python, no data access).
    """
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

    # Computed once per run and cached: kpi_long._period_frames semi-joins these onto every metric
    # frame for the Quarter and Monthly trend tabs, so an in-progress trailing period never renders.
    # report_end="latest_day" adds the Annual ("Year") and Weekly ("Week") grains.
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

    # Dimensions an enabled external source will supply — excluded from the products
    # "not found" warning below, since they intentionally live outside the products master.
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
    # Cache the deduplicated projection so repeated downstream joins reuse it without re-scanning Delta.
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

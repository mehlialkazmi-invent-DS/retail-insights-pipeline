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

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import read_csv_source


def _build_fiscal_week_frame(
    daily_grain: DataFrame,
    quarter_col: Optional[str] = None,
    month_col: Optional[str] = None,
    month_name_col: Optional[str] = None,
) -> DataFrame:
    """Aggregate day-level fiscal_cal rows to one row per (Year, Week).

    Each of quarter_col/month_col/month_name_col is used when it names a column actually present
    on daily_grain; otherwise that fiscal attribute is derived instead. This lets every client's
    fiscal_cal upload -- with or without any of these columns -- resolve to the same output shape
    (Fiscal_Quarter, Fiscal_Month[, Fiscal_Month_Name]). See config.py's fiscal_calendar.column_map.

    Derivation fallbacks:
      * Fiscal_Month: F.month(week_start_date) -- the real calendar month. This IS correct as-is
        on the civil-calendar path (there is no separate fiscal month there); on a fiscal-calendar
        upload missing a month column, it's the best available substitute.
      * Fiscal_Quarter: ceil(Fiscal_Month / 3), computed from Fiscal_Month (whichever source
        produced it above) rather than from `date` directly, so quarter and month stay internally
        consistent with each other regardless of which one actually came from the upload.
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

    return frame


def _compute_available_fiscal_quarters(ctx: KPIContext) -> List[int]:
    """Fiscal-quarter numbers fully elapsed (as of REPORT_END_DATE) for the latest year in the
    report window. Applied identically to every year for the "ytd" period (see kpi_long.py) so
    the YTD comparison stays apples-to-apples once the current year is only partially reported —
    e.g. if only Q1 has fully closed for the latest year, YTD sums Q1 for every year, not the
    calendar-to-date weeks of an in-progress Q2.

    Handles a single-year or single-quarter report window the same way — it only looks at the
    latest year's own weeks, so nothing else needs to exist.
    """
    report_end = ctx.settings["REPORT_END_DATE"]
    fw = ctx.fiscal_week
    latest_year = fw.agg(F.max("Year")).collect()[0][0]
    quarter_ends = (
        fw.filter(F.col("Year") == latest_year)
        .groupBy("Fiscal_Quarter")
        .agg(F.max("week_end_date").alias("quarter_end"))
        .collect()
    )
    return sorted(
        int(row["Fiscal_Quarter"])
        for row in quarter_ends
        if row["quarter_end"] is not None and row["quarter_end"] <= report_end
    )


def build_fiscal_cal_and_week_from_upload(
    ctx: KPIContext,
    path: str,
    report_start_date: datetime.date,
    report_end_date: datetime.date,
) -> Tuple[DataFrame, DataFrame]:
    raw = ctx.spark.read.format("delta").load(path)
    quarter_col = ctx.settings.get("FISCAL_QUARTER_COL")
    month_col = ctx.settings.get("FISCAL_MONTH_COL")
    month_name_col = ctx.settings.get("FISCAL_MONTH_NAME_COL")

    select_cols = ["date", "Year", "Week"]
    for col in (quarter_col, month_col, month_name_col):
        if col and col in raw.columns and col not in select_cols:
            select_cols.append(col)

    fiscal_cal_out = (
        raw.select(*select_cols)
        .withColumn("date", F.to_date("date"))
        .filter(F.col("date").between(F.lit(report_start_date), F.lit(report_end_date)))
    )
    return fiscal_cal_out, _build_fiscal_week_frame(fiscal_cal_out, quarter_col, month_col, month_name_col)


def build_time_grain_from_daily_data(
    ctx: KPIContext,
    path: str,
    time_cols: Dict[str, str],
    report_start_date: datetime.date,
    report_end_date: datetime.date,
) -> Tuple[DataFrame, DataFrame]:
    date_col, week_col = time_cols["date"], time_cols["week"]
    # Year is the CALENDAR year of `date`, not the source 'year' column: that column can
    # carry the ISO week-year, which labels late-December weeks as the following year
    # (e.g. Dec 2025 -> 2026) and mismatches the Quarter/Month derived from `date`.
    daily_time = (
        ctx.spark.read.format("delta")
        .load(path)
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
            ctx, s["PATH_DAILY_DATA"], s["DAILY_TIME_COLUMNS"], start, end
        )
        grain_label = "daily-data year/week"

    ctx.fiscal_cal = fiscal_cal.cache()
    ctx.fiscal_week = fiscal_week.cache()
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


def build_fiscal_and_products(ctx: KPIContext) -> None:
    """Populate ctx.fiscal_cal, ctx.fiscal_week, ctx.products_attr, ctx.active_slice_dimensions."""
    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]

    if s["USE_FISCAL_CALENDAR"]:
        fiscal_cal, fiscal_week = build_fiscal_cal_and_week_from_upload(ctx, s["PATH_FISCAL"], start, end)
        grain_label = "fiscal_cal upload"
    else:
        fiscal_cal, fiscal_week = build_time_grain_from_daily_data(
            ctx, s["PATH_DAILY_DATA"], s["DAILY_TIME_COLUMNS"], start, end
        )
        grain_label = "daily-data year/week"

    ctx.fiscal_cal = fiscal_cal.cache()
    ctx.fiscal_week = fiscal_week.cache()

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

    ctx.available_fiscal_quarters = _compute_available_fiscal_quarters(ctx)
    print("available (fully elapsed) fiscal quarters for YTD:", ctx.available_fiscal_quarters)

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

    print("time grain:", grain_label)
    print("fiscal weeks:", ctx.fiscal_week.count())
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

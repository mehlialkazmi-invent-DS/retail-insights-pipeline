"""Fiscal calendar and product dimension setup.

When USE_FISCAL_CALENDAR is True, reads one_time_uploads/fiscal_cal.
Otherwise derives Year/Week from noob/daily-data native columns.
"""

from __future__ import annotations

import datetime
from typing import Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from kpi_pipeline.context import KPIContext


def _build_fiscal_week_frame(daily_grain: DataFrame) -> DataFrame:
    has_month = "Month" in daily_grain.columns
    agg_exprs = [
        F.min("date").alias("week_start_date"),
        F.max("date").alias("week_end_date"),
        F.first("Quarter", ignorenulls=True).alias("Quarter"),
    ]
    if has_month:
        agg_exprs.append(F.first("Month", ignorenulls=True).alias("_raw_month"))

    frame = (
        daily_grain.groupBy("Year", "Week")
        .agg(*agg_exprs)
        .withColumn(
            "Year_Week",
            F.concat_ws("-W", F.col("Year").cast("string"), F.format_string("%02d", F.col("Week"))),
        )
        .withColumn("Fiscal_Quarter", F.regexp_extract(F.col("Quarter"), r"Q(\d+)", 1).cast("int"))
    )

    if has_month:
        # Extract numeric month from the fiscal calendar Month column (e.g. "M01" → 1, "1" → 1).
        frame = (
            frame
            .withColumn("Fiscal_Month", F.regexp_extract(F.col("_raw_month"), r"(\d+)", 1).cast("int"))
            .drop("_raw_month")
        )
    else:
        frame = frame.withColumn("Fiscal_Month", F.month("week_start_date"))

    return frame


def build_fiscal_cal_and_week_from_upload(
    ctx: KPIContext,
    path: str,
    report_start_date: datetime.date,
    report_end_date: datetime.date,
) -> Tuple[DataFrame, DataFrame]:
    raw = ctx.spark.read.format("delta").load(path)
    select_cols = ["date", "Year", "Week", "Quarter"]
    if "Month" in raw.columns:
        select_cols.append("Month")
    fiscal_cal_out = (
        raw.select(*select_cols)
        .withColumn("date", F.to_date("date"))
        .filter(F.col("date").between(F.lit(report_start_date), F.lit(report_end_date)))
    )
    return fiscal_cal_out, _build_fiscal_week_frame(fiscal_cal_out)


def build_time_grain_from_daily_data(
    ctx: KPIContext,
    path: str,
    time_cols: Dict[str, str],
    report_start_date: datetime.date,
    report_end_date: datetime.date,
) -> Tuple[DataFrame, DataFrame]:
    date_col, year_col, week_col = time_cols["date"], time_cols["year"], time_cols["week"]
    daily_time = (
        ctx.spark.read.format("delta")
        .load(path)
        .select(date_col, year_col, week_col)
        .withColumn(date_col, F.to_date(F.col(date_col)))
        .filter(F.col(date_col).between(F.lit(report_start_date), F.lit(report_end_date)))
        .select(
            F.col(date_col).alias("date"),
            F.col(year_col).cast("int").alias("Year"),
            F.col(week_col).cast("int").alias("Week"),
        )
        .distinct()
    )
    fiscal_cal_out = daily_time.withColumn(
        "Quarter",
        F.concat(F.lit("Q"), (((F.month("date") - 1) / 3 + 1).cast("int")).cast("string")),
    )
    return fiscal_cal_out, _build_fiscal_week_frame(fiscal_cal_out)


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
            "(unparseable 'Quarter' value). Fix the fiscal calendar / Quarter column before running."
        )

    products_raw = ctx.spark.read.format("delta").load(s["PATH_PRODUCTS"])
    slice_dims = s["SLICE_DIMENSIONS"]
    derived_dims_cfg = s["DERIVED_SLICE_DIMENSIONS"]

    existing_dims = [c for c in slice_dims if c in products_raw.columns]
    missing_existing = [c for c in slice_dims if c not in products_raw.columns]
    if missing_existing:
        print("NOTE: skipping SLICE_DIMENSIONS not found in products table:", missing_existing)

    derived_dims = []
    derived_exprs = {}
    for name, sql in derived_dims_cfg.items():
        try:
            products_raw.select(F.expr(sql).alias(name)).schema
            derived_dims.append(name)
            derived_exprs[name] = sql
        except Exception as exc:
            print(f"NOTE: skipping derived dimension {name!r} (expression failed to resolve): {exc}")

    ctx.active_slice_dimensions = existing_dims + derived_dims
    # Cache the deduplicated projection so repeated downstream joins reuse it without re-scanning Delta.
    products_proj = (
        products_raw.select(
            "product_id",
            "cogs",
            "price_without_tax",
            *existing_dims,
            *[F.expr(derived_exprs[n]).alias(n) for n in derived_dims],
        )
        .dropDuplicates(["product_id"])
        .cache()
    )
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
        ")",
    )

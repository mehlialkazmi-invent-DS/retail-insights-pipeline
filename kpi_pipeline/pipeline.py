"""Pipeline input frames for a given scope."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import get_daily_data_raw, read_lost_sales_source


def is_service_store(ctx: KPIContext) -> F.Column:
    """True for stores included in service metrics (excludes e-com IDs from config)."""
    return ~F.col("store_id").isin(ctx.settings["EXCLUDED_STORE_IDS_FOR_SERVICE_METRICS"])


def _enrich_lost_sales_with_time_grain(ctx: KPIContext, lost_sales_pair: DataFrame) -> DataFrame:
    fw_cols = ["week_start_date", "week_end_date", "Year", "Week", "Year_Week", "Fiscal_Quarter"]
    if ctx.settings["USE_FISCAL_CALENDAR"]:
        return lost_sales_pair.join(broadcast(ctx.fiscal_week.select(*fw_cols)), on="week_start_date", how="inner")
    if "_ls_year" in lost_sales_pair.columns and "_ls_week" in lost_sales_pair.columns:
        enriched = (
            lost_sales_pair.withColumn("Year", F.col("_ls_year"))
            .withColumn("Week", F.col("_ls_week"))
            .drop("_ls_year", "_ls_week")
        )
    else:
        enriched = lost_sales_pair.join(
            broadcast(ctx.fiscal_cal.select(F.col("date").alias("week_start_date"), "Year", "Week")),
            on="week_start_date",
            how="inner",
        )
    return enriched.drop("week_start_date").join(broadcast(ctx.fiscal_week.select(*fw_cols)), on=["Year", "Week"], how="inner")


def read_lost_sales_weekly(ctx: KPIContext, path: Optional[str] = None) -> DataFrame:
    """Weekly lost-sales aggregates for the report window (cached per run)."""
    if ctx.lost_sales_weekly_base is not None:
        return ctx.lost_sales_weekly_base

    path = path or ctx.settings["PATH_LOST_SALES"]
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    raw = read_lost_sales_source(ctx.spark, ctx.settings, path, quiet=True)
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
    agg_exprs = [
        F.sum(F.col("lost_sales").cast("double")).alias("lost_sales"),
        F.sum(F.col("in_stock").cast("double")).alias("in_stock_days"),
        F.sum(F.col("details.total_days").cast("double")).alias("total_days"),
    ]
    if not ctx.settings["USE_FISCAL_CALENDAR"]:
        if "year" in raw.columns and "week" in raw.columns:
            agg_exprs.append(F.max(F.col("year").cast("int")).alias("_ls_year"))
            agg_exprs.append(F.max(F.col("week").cast("int")).alias("_ls_week"))
    deduped = filtered.groupBy("product_id", "store_id", "week_start_date").agg(*agg_exprs)
    ctx.lost_sales_weekly_base = _enrich_lost_sales_with_time_grain(ctx, deduped).withColumn(
        "fiscal_week_days",
        F.datediff(F.col("week_end_date"), F.col("week_start_date")) + 1,
    ).cache()
    return ctx.lost_sales_weekly_base


def build_scoped_daily(ctx: KPIContext, scope_core: DataFrame, scope_pairs_in: DataFrame, has_store: bool) -> DataFrame:
    """Daily sales/inventory for scoped pairs, with product cost/price and fiscal week attributes."""
    s = ctx.settings
    time_cols = s["DAILY_TIME_COLUMNS"]
    date_col, year_col, week_col = time_cols["date"], time_cols["year"], time_cols["week"]
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    select_cols = ["product_id", "store_id", date_col, "sales_revenue", "sales_quantity", "inventory"]
    if not s["USE_FISCAL_CALENDAR"]:
        select_cols.extend([year_col, week_col])

    daily = (
        get_daily_data_raw(ctx)
        .select(*select_cols)
        .withColumn(date_col, F.to_date(F.col(date_col)))
        .withColumnRenamed(date_col, "date")
        .filter(F.col("date").between(F.lit(start), F.lit(end)))
    )
    if has_store:
        daily = daily.join(scope_pairs_in, on=["product_id", "store_id"], how="left_semi")
    if not s["USE_FISCAL_CALENDAR"]:
        daily = (
            daily.withColumnRenamed(year_col, "Year")
            .withColumnRenamed(week_col, "Week")
            .withColumn("Year", F.col("Year").cast("int"))
            .withColumn("Week", F.col("Week").cast("int"))
        )
    else:
        daily = daily.join(broadcast(ctx.fiscal_cal.select("date", "Year", "Week")), on="date", how="inner")

    scope_keys = ["product_id", "store_id", "Year", "Week"] if has_store else ["product_id", "Year", "Week"]
    daily = daily.join(scope_core, on=scope_keys, how="left_semi")
    return (
        daily.join(ctx.products_attr, on="product_id", how="inner")
        .withColumn("inventory_retail", F.round(F.col("inventory") * F.col("price_without_tax"), 2))
        .withColumn("inventory_cost", F.round(F.col("inventory") * F.col("cogs"), 2))
        .withColumn("sales_cost", F.round(F.col("sales_quantity") * F.col("cogs"), 2))
        .join(
            broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter")),
            on=["Year", "Week"],
            how="inner",
        )
    )


def build_pipeline_frames(ctx: KPIContext, scope_in: DataFrame) -> Dict[str, DataFrame]:
    """Build scoped_daily, inst_data, lost_base, and scope helper frames for one scope variant."""
    has_store = "store_id" in scope_in.columns
    scope_keys = ["product_id", "store_id", "Year", "Week"] if has_store else ["product_id", "Year", "Week"]
    scope_core = scope_in.select(*scope_keys).distinct().cache()

    lost_sales_weekly = read_lost_sales_weekly(ctx).join(scope_core, on=scope_keys, how="left_semi").cache()

    if has_store:
        scope_pair_weeks = scope_core.select("product_id", "store_id", "Year", "Week").distinct().cache()
        scope_pairs = scope_core.select("product_id", "store_id").distinct().cache()
    else:
        scope_pair_weeks = lost_sales_weekly.select("product_id", "store_id", "Year", "Week").distinct().cache()
        scope_pairs = lost_sales_weekly.select("product_id", "store_id").distinct().cache()

    inst_data = (
        lost_sales_weekly.filter(is_service_store(ctx))
        .select(
            "product_id",
            "store_id",
            "Year",
            "Week",
            "Year_Week",
            "Fiscal_Quarter",
            F.col("in_stock_days").alias("stocked_pairs"),
            F.coalesce(F.col("total_days"), F.col("fiscal_week_days")).alias("available_days"),
        )
        .filter(F.col("available_days") > 0)
        .join(ctx.product_dims, on="product_id", how="left")
    ).cache()

    scoped_daily = build_scoped_daily(ctx, scope_core, scope_pairs, has_store).cache()
    weekly_pair = scoped_daily.groupBy("product_id", "store_id", "Year", "Week").agg(
        F.sum("sales_quantity").alias("weekly_sales")
    )
    weekly_sales_for_lost = (
        weekly_pair.filter(is_service_store(ctx))
        .join(scope_pair_weeks, on=["product_id", "store_id", "Year", "Week"], how="left_semi")
        .select("product_id", "store_id", "Year", "Week", F.col("weekly_sales").alias("sales_quantity_weekly"))
    )
    lost_base = (
        lost_sales_weekly.filter(is_service_store(ctx))
        .select("product_id", "store_id", "Year", "Week", "Year_Week", "Fiscal_Quarter", "lost_sales")
        .join(weekly_sales_for_lost, on=["product_id", "store_id", "Year", "Week"], how="left")
        .withColumn("sales_quantity_weekly", F.coalesce(F.col("sales_quantity_weekly"), F.lit(0.0)))
        .withColumn(
            "TY_sales_quantity_weekly_corrected_lost_sales",
            F.floor(F.col("sales_quantity_weekly") + F.col("lost_sales")),
        )
        .join(ctx.product_dims, on="product_id", how="left")
    ).cache()

    return {
        "scoped_daily": scoped_daily,
        "inst_data": inst_data,
        "lost_base": lost_base,
        "scope_pairs": scope_pairs,
        "scope_pair_weeks": scope_pair_weeks,
        "lost_sales_weekly": lost_sales_weekly,
    }

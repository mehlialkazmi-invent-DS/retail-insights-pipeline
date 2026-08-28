"""Pipeline input frames for a given scope."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import (
    DEFAULT_INSTOCK_SOURCE_COLUMN_MAP,
    DEFAULT_LOST_SALES_COLUMN_MAP,
    get_daily_data_raw,
    read_instock_source,
    read_lost_sales_source,
    read_speed_cluster_source,
)


def is_service_store(ctx: KPIContext) -> F.Column:
    """True for stores included in service metrics (excludes e-com IDs from config)."""
    return ~F.col("store_id").isin(ctx.settings["EXCLUDED_STORE_IDS_FOR_SERVICE_METRICS"])


def _enrich_lost_sales_with_time_grain(ctx: KPIContext, lost_sales_pair: DataFrame) -> DataFrame:
    fw_cols = ["week_start_date", "week_end_date", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Month"]
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


def _aggregate_lost_sales_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """Aggregate ONE raw lost-sales model to (product_id, store_id, week_start_date) grain.

    lost_sales/in_stock/total_days source column names come from LOST_SALES_COLUMN_MAP
    (see config.py's lost_sales_source). in_stock/total_days are skipped here entirely
    when INSTOCK_SOURCE_ENABLED — they come from the separate instock_source table
    instead (see read_lost_sales_weekly).
    """
    col_map = ctx.settings.get("LOST_SALES_COLUMN_MAP") or DEFAULT_LOST_SALES_COLUMN_MAP
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
    agg_exprs = [F.sum(F.col(col_map["lost_sales_col"]).cast("double")).alias("lost_sales")]
    if not ctx.settings.get("INSTOCK_SOURCE_ENABLED", False):
        agg_exprs.append(F.sum(F.col(col_map["in_stock_col"]).cast("double")).alias("in_stock_days"))
        agg_exprs.append(F.sum(F.col(col_map["total_days_col"]).cast("double")).alias("total_days"))
    ls_native_week = not ctx.settings["USE_FISCAL_CALENDAR"] and "week" in raw.columns
    if ls_native_week:
        # Keep the native fiscal week number, but derive Year from week_start_date
        # (calendar year) rather than the source 'year' column, which can carry the ISO
        # week-year (late-December weeks labelled as the next year). See fiscal.py.
        agg_exprs.append(F.first(F.col("week").cast("int"), ignorenulls=True).alias("_ls_week"))
    return filtered.groupBy("product_id", "store_id", "week_start_date").agg(*agg_exprs)


def _aggregate_instock_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """Aggregate the standalone instock_source table to (product_id, store_id, week_start_date)."""
    col_map = ctx.settings.get("INSTOCK_SOURCE_COLUMN_MAP") or DEFAULT_INSTOCK_SOURCE_COLUMN_MAP
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
    agg_exprs = [
        F.sum(F.col(col_map["in_stock_col"]).cast("double")).alias("in_stock_days"),
        F.sum(F.col(col_map["total_days_col"]).cast("double")).alias("total_days"),
    ]
    return filtered.groupBy("product_id", "store_id", "week_start_date").agg(*agg_exprs)


def read_lost_sales_weekly(ctx: KPIContext, path: Optional[str] = None) -> DataFrame:
    """Weekly lost-sales aggregates for the report window (cached per run).

    OFF (default, lost_sales_ensemble.enabled=False): reads the single fast-mover
    model at PATH_LOST_SALES exactly as before.

    ON (lost_sales_ensemble.enabled=True): blends the fast (120-day) and slow
    (365-day) models by product sales-speed cluster — products whose cluster is in
    FAST_MOVER_CLUSTERS take the fast model; everyone else (other clusters AND
    products with no/NULL cluster row) takes the slow model. A single boolean drives
    all three aggregate fields (lost_sales, in_stock_days, total_days) for a given
    pair-week, so they always come from the SAME chosen model. Mutually exclusive with
    instock_source (see config.py's validation).

    instock_source.enabled=True (mutually exclusive with the ensemble above): in_stock_days
    and total_days are read from a separate table instead of PATH_LOST_SALES, left-joined
    onto the lost-sales pair-weeks by (product_id, store_id, week_start_date).
    """
    if ctx.lost_sales_weekly_base is not None:
        return ctx.lost_sales_weekly_base

    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]

    if not s["LOST_SALES_ENSEMBLE_ENABLED"]:
        path = path or s["PATH_LOST_SALES"]
        raw = read_lost_sales_source(ctx.spark, s, path, quiet=True)
        deduped = _aggregate_lost_sales_pairweek(ctx, raw, start, end)
        if not s["USE_FISCAL_CALENDAR"] and "week" in raw.columns:
            deduped = deduped.withColumn("_ls_year", F.year("week_start_date"))
    else:
        fast_raw = read_lost_sales_source(ctx.spark, s, s["PATH_LOST_SALES"], quiet=True)
        slow_raw = read_lost_sales_source(ctx.spark, s, s["PATH_LOST_SALES_SLOW"], quiet=True)
        native_week = not s["USE_FISCAL_CALENDAR"] and "week" in fast_raw.columns

        fast_cols = [
            "product_id", "store_id", "week_start_date",
            F.col("lost_sales").alias("lost_sales_fast"),
            F.col("in_stock_days").alias("in_stock_days_fast"),
            F.col("total_days").alias("total_days_fast"),
        ]
        slow_cols = [
            "product_id", "store_id", "week_start_date",
            F.col("lost_sales").alias("lost_sales_slow"),
            F.col("in_stock_days").alias("in_stock_days_slow"),
            F.col("total_days").alias("total_days_slow"),
        ]
        if native_week:
            fast_cols.append(F.col("_ls_week").alias("_ls_week_fast"))
            slow_cols.append(F.col("_ls_week").alias("_ls_week_slow"))
        fast = _aggregate_lost_sales_pairweek(ctx, fast_raw, start, end).select(*fast_cols)
        slow = _aggregate_lost_sales_pairweek(ctx, slow_raw, start, end).select(*slow_cols)

        cluster = read_speed_cluster_source(ctx.spark, s, quiet=True)
        merged = (
            fast.join(slow, on=["product_id", "store_id", "week_start_date"], how="fullouter")
            .join(cluster, on="product_id", how="left")
        )
        # ONE shared boolean drives ALL field selections -> fields never mix across models.
        use_fast = F.col("sales_speed_cluster").isin(*s["FAST_MOVER_CLUSTERS"])
        merged = (
            merged.withColumn("_use_fast", use_fast)
            .withColumn(
                "lost_sales",
                F.when(F.col("_use_fast"), F.col("lost_sales_fast")).otherwise(F.col("lost_sales_slow")),
            )
            .withColumn(
                "in_stock_days",
                F.when(F.col("_use_fast"), F.col("in_stock_days_fast")).otherwise(F.col("in_stock_days_slow")),
            )
            .withColumn(
                "total_days",
                F.when(F.col("_use_fast"), F.col("total_days_fast")).otherwise(F.col("total_days_slow")),
            )
            # Keep the legacy invariant: a pair-week exists ONLY if the CHOSEN model has a
            # row. If the selected side is absent (full-outer null), all three fields are
            # null together -> drop the row (do NOT coalesce to 0).
            .filter(F.col("total_days").isNotNull())
        )
        if native_week:
            merged = merged.withColumn("_ls_week", F.coalesce(F.col("_ls_week_fast"), F.col("_ls_week_slow")))
        select_cols = ["product_id", "store_id", "week_start_date", "lost_sales", "in_stock_days", "total_days"]
        if native_week:
            select_cols.append("_ls_week")
        deduped = merged.select(*select_cols)
        if native_week:
            deduped = deduped.withColumn("_ls_year", F.year("week_start_date"))

    if s.get("INSTOCK_SOURCE_ENABLED", False):
        instock_raw = read_instock_source(ctx.spark, s, quiet=True)
        instock_agg = _aggregate_instock_pairweek(ctx, instock_raw, start, end)
        deduped = deduped.join(instock_agg, on=["product_id", "store_id", "week_start_date"], how="left")

    ctx.lost_sales_weekly_base = _enrich_lost_sales_with_time_grain(ctx, deduped).withColumn(
        "fiscal_week_days",
        F.datediff(F.col("week_end_date"), F.col("week_start_date")) + 1,
    ).cache()
    return ctx.lost_sales_weekly_base


def build_scoped_daily(ctx: KPIContext, scope_core: DataFrame, scope_pairs_in: DataFrame, has_store: bool) -> DataFrame:
    """Daily sales/inventory for scoped pairs, with product cost/price and fiscal week attributes."""
    s = ctx.settings
    time_cols = s["DAILY_TIME_COLUMNS"]
    date_col, week_col = time_cols["date"], time_cols["week"]
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    select_cols = ["product_id", "store_id", date_col, "sales_revenue", "sales_quantity", "inventory"]
    if not s["USE_FISCAL_CALENDAR"]:
        select_cols.append(week_col)

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
        # Year = calendar year of `date`; Week = native fiscal week column. Avoids the
        # source 'year' column's ISO week-year mislabel (Dec -> next year). See fiscal.py.
        daily = (
            daily.withColumn("Year", F.year(F.col("date")))
            .withColumnRenamed(week_col, "Week")
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
            broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Month")),
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

    # Fall back to the fiscal week's day-count only when total_days can legitimately be missing
    # for reasons unrelated to instock_source (e.g. a null in the lost-sales table itself). Under
    # instock_source.enabled=True, a null total_days specifically means "no matching row in the
    # separate instock table" -- coalescing it to a full week would silently pad available_days
    # for an unmatched pair-week instead of excluding it via the filter below, deflating
    # in_stock_rate/weighted_instock_rate for exactly the coverage gaps instock_source expects.
    available_days_expr = (
        F.col("total_days")
        if ctx.settings.get("INSTOCK_SOURCE_ENABLED", False)
        else F.coalesce(F.col("total_days"), F.col("fiscal_week_days"))
    )
    inst_data = (
        lost_sales_weekly.filter(is_service_store(ctx))
        .select(
            "product_id",
            "store_id",
            "Year",
            "Week",
            "Year_Week",
            "Fiscal_Quarter",
            "Fiscal_Month",
            F.col("in_stock_days").alias("stocked_pairs"),
            available_days_expr.alias("available_days"),
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
        .select("product_id", "store_id", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Month", "lost_sales")
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

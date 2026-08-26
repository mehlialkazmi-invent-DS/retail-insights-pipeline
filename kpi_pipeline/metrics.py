"""KPI metric computation.

Sales metrics use all scoped stores; service metrics (WOS, instock, lost sales %, mean stock)
exclude EXCLUDED_STORE_IDS_FOR_SERVICE_METRICS via is_service_store().
"""

from __future__ import annotations

from typing import Dict, Sequence

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.pipeline import is_service_store


def filter_service_scope(ctx: KPIContext, df: DataFrame, scope_pair_weeks_in: DataFrame) -> DataFrame:
    out = df.filter(is_service_store(ctx))
    # Store-grain scoped_daily is already restricted to scope_pair_weeks; the semi-join is only
    # needed in the product-week (no store grain) fallback path.
    if "store_id" not in ctx.scope_keys:
        out = out.join(scope_pair_weeks_in, on=["product_id", "store_id", "Year", "Week"], how="left_semi")
    return out


def compute_kpis(
    ctx: KPIContext,
    scoped_daily_in: DataFrame,
    inst_in: DataFrame,
    scope_pair_weeks_in: DataFrame,
    period_col: str,
    group_keys: Sequence[str] = (),
    period_filter=F.lit(True),
) -> DataFrame:
    """Aggregate KPI columns for one period grain and optional slice group keys."""
    group_keys = list(group_keys)
    keys = [period_col] + group_keys
    daily_sales = scoped_daily_in.filter(period_filter)
    daily_service = filter_service_scope(ctx, scoped_daily_in, scope_pair_weeks_in).filter(period_filter)
    inst = inst_in.filter(period_filter)

    sales = (
        daily_sales.groupBy(*keys)
        .agg(
            F.sum("inventory").alias("total_inventory"),
            F.sum("sales_quantity").alias("total_sales_quantity"),
            F.sum("sales_revenue").alias("total_sales_revenue"),
            F.sum("sales_cost").alias("total_sales_cost"),
            F.countDistinct("product_id").alias("distinct_product_count"),
            F.countDistinct("store_id").alias("distinct_store_count"),
            F.countDistinct("product_id", "store_id").alias("distinct_pair_count"),
        )
        .withColumn(
            "AUR",
            F.when(F.col("total_sales_quantity") == 0, F.lit(None)).otherwise(
                F.col("total_sales_revenue") / F.col("total_sales_quantity")
            ),
        )
        .withColumn(
            "AUC",
            F.when(F.col("total_sales_quantity") == 0, F.lit(None)).otherwise(
                F.col("total_sales_cost") / F.col("total_sales_quantity")
            ),
        )
    )

    week_keys = ["product_id", "Year", "Week"] + group_keys
    period_extra = [period_col] if period_col not in week_keys else []
    daily_by_date = daily_service.groupBy(*week_keys, *period_extra, "date").agg(
        F.sum("inventory").alias("daily_total_inventory"),
        F.sum("sales_quantity").alias("daily_sales_units"),
        F.sum("inventory_retail").alias("daily_total_inventory_retail"),
        F.sum("sales_revenue").alias("daily_sales_revenue"),
        F.sum("inventory_cost").alias("daily_total_inventory_cost"),
        F.sum("sales_cost").alias("daily_sales_cost"),
    )
    daily_data_week = (
        daily_by_date.groupBy(*week_keys, *period_extra)
        .agg(
            F.avg("daily_total_inventory").alias("avg_daily_total_inventory"),
            F.sum("daily_sales_units").alias("weekly_sales_units"),
            F.avg("daily_total_inventory_retail").alias("avg_daily_inventory_retail"),
            F.sum("daily_sales_revenue").alias("weekly_sales_revenue"),
            F.avg("daily_total_inventory_cost").alias("avg_daily_inventory_cost"),
            F.sum("daily_sales_cost").alias("weekly_sales_cost"),
        )
        .withColumn(
            "wos_units",
            F.when(F.col("weekly_sales_units") > 0, F.col("avg_daily_total_inventory") / F.col("weekly_sales_units")).otherwise(
                F.lit(None)
            ),
        )
        .withColumn(
            "wos_revenue",
            F.when(F.col("weekly_sales_revenue") > 0, F.col("avg_daily_inventory_retail") / F.col("weekly_sales_revenue")).otherwise(
                F.lit(None)
            ),
        )
        .withColumn(
            "wos_cost",
            F.when(F.col("weekly_sales_cost") > 0, F.col("avg_daily_inventory_cost") / F.col("weekly_sales_cost")).otherwise(
                F.lit(None)
            ),
        )
    )
    wos = daily_data_week.groupBy(*keys).agg(
        (F.sum(F.col("wos_units") * F.col("weekly_sales_units")) / F.sum("weekly_sales_units")).alias("WOS"),
        (F.sum(F.col("wos_revenue") * F.col("weekly_sales_revenue")) / F.sum("weekly_sales_revenue")).alias("wos_revenue"),
        (F.sum(F.col("wos_cost") * F.col("weekly_sales_cost")) / F.sum("weekly_sales_cost")).alias("wos_cost"),
    )

    seg_day = daily_service.groupBy(*keys, "date").agg(
        F.sum("inventory").alias("daily_inv"),
        F.sum("inventory_retail").alias("daily_inv_retail"),
        F.sum("inventory_cost").alias("daily_inv_cost"),
    )
    mean_stock = seg_day.groupBy(*keys).agg(
        F.avg("daily_inv").alias("mean_stock"),
        F.avg("daily_inv_retail").alias("mean_stock_retail"),
        F.avg("daily_inv_cost").alias("mean_stock_cost"),
    )
    turnover = (
        daily_service.groupBy(*keys)
        .agg(F.sum("sales_quantity").alias("sales_units"))
        .join(mean_stock, on=keys, how="inner")
        .withColumn(
            "inventory_turnover_rate",
            F.when(F.col("mean_stock") == 0, F.lit(None)).otherwise(F.col("sales_units") / F.col("mean_stock")),
        )
        .select(*keys, "inventory_turnover_rate")
    )
    instock = inst.groupBy(*keys).agg(
        F.greatest(F.lit(0.0), F.sum("stocked_pairs") / F.sum("available_days")).alias("in_stock_rate")
    )

    # Sales-weighted in-stock rate: aggregate instock to Year×Week (+ slice group_keys), then
    # weight each week by its sales when rolling up to the reporting period.
    wi_week_keys = ["Year", "Week"] + group_keys
    wi_period_extra = [period_col] if period_col not in wi_week_keys else []

    weekly_pair_instock = inst.groupBy(*wi_week_keys, *wi_period_extra).agg(
        F.greatest(F.lit(0.0), F.sum("stocked_pairs") / F.sum("available_days")).alias("_pair_instock_rate"),
    )
    weekly_pair_sales = daily_service.groupBy(*wi_week_keys, *wi_period_extra).agg(
        F.sum("sales_quantity").alias("_pair_sales_qty")
    )
    weighted_instock = (
        weekly_pair_instock
        .join(weekly_pair_sales, on=wi_week_keys + wi_period_extra, how="left")
        .groupBy(*keys)
        .agg(
            (F.sum(F.col("_pair_instock_rate") * F.col("_pair_sales_qty")) / F.sum("_pair_sales_qty")).alias("weighted_instock_rate")
        )
    )

    return (
        sales.join(wos, on=keys, how="left")
        .join(mean_stock, on=keys, how="left")
        .join(turnover, on=keys, how="left")
        .join(instock, on=keys, how="left")
        .join(weighted_instock, on=keys, how="left")
    )


def build_kpi_table(
    ctx: KPIContext,
    frames: Dict[str, DataFrame],
    period_col: str,
    group_keys: Sequence[str] = (),
    period_filter=F.lit(True),
) -> pd.DataFrame:
    """Spark KPI aggregation joined with lost_sales_pct; returns a pandas table."""
    group_keys = list(group_keys)
    keys = [period_col] + group_keys
    kpis = compute_kpis(
        ctx, frames["scoped_daily"], frames["inst_data"], frames["scope_pair_weeks"], period_col, group_keys, period_filter
    )
    lost_pct = (
        frames["lost_base"]
        .filter(period_filter)
        .groupBy(*keys)
        .agg(
            F.sum("lost_sales").alias("_ls"),
            F.sum("TY_sales_quantity_weekly_corrected_lost_sales").alias("_den"),
        )
        .withColumn(
            "lost_sales_pct",
            F.when(F.col("_den") == 0, F.lit(None)).otherwise(F.col("_ls") / F.col("_den") * 100),
        )
        .drop("_ls", "_den")
    )
    return kpis.join(lost_pct, on=keys, how="left").toPandas().sort_values(keys).reset_index(drop=True)

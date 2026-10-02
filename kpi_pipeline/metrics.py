"""KPI metric computation."""

from __future__ import annotations

from typing import Dict, Sequence

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.filters import apply_group_population_filter


def _inventory_exprs(with_git: bool):
    """(units, retail, cost) store inventory expressions of a scoped_daily row: on-hand, or with
    ``with_git`` on-hand + goods in transit (retail = units x price_without_tax, cost = units x cogs,
    rounded like inventory_retail / inventory_cost)."""
    if not with_git:
        return F.col("inventory"), F.col("inventory_retail"), F.col("inventory_cost")
    units = F.col("inventory") + F.col("git_quantity")
    return units, F.round(units * F.col("price_without_tax"), 2), F.round(units * F.col("cogs"), 2)


def _dc_inventory_expr(with_git: bool):
    """DC inventory units of a dc_daily row: on-hand, or with ``with_git`` on-hand + goods in transit."""
    return F.col("inventory") + F.col("git_quantity") if with_git else F.col("inventory")


def _mean_stock_frame(daily_scoped: DataFrame, keys: Sequence[str], with_git: bool = False) -> DataFrame:
    """mean_stock/mean_stock_retail/mean_stock_cost from a (possibly population-filtered)
    daily_scoped frame, on-hand or (``with_git``) on-hand + goods in transit. Factored out so turnover
    can recompute its own mean-stock over its own population override instead of reusing the public
    mean_stock metric's frame verbatim."""
    units, retail, cost = _inventory_exprs(with_git)
    seg_day = daily_scoped.groupBy(*keys, "date").agg(
        F.sum(units).alias("daily_inv"),
        F.sum(retail).alias("daily_inv_retail"),
        F.sum(cost).alias("daily_inv_cost"),
    )
    return seg_day.groupBy(*keys).agg(
        F.avg("daily_inv").alias("mean_stock"),
        F.avg("daily_inv_retail").alias("mean_stock_retail"),
        F.avg("daily_inv_cost").alias("mean_stock_cost"),
    )


def compute_kpis(
    ctx: KPIContext,
    scoped_daily_in: DataFrame,
    inst_in: DataFrame,
    dc_daily_in: DataFrame,
    dc_inst_in: DataFrame,
    period_col: str,
    group_keys: Sequence[str] = (),
    period_filter=F.lit(True),
) -> DataFrame:
    """Aggregate KPI columns for one period grain and optional slice group keys.

    Each metric group below (see filters.METRIC_FILTER_GROUPS) applies its own
    metrics.population_filters override, on top of whatever root/cut restriction the caller
    already applied -- apply_group_population_filter is a no-op when a group has no configured
    override, so this changes nothing for configs that don't use the feature.

    inventory_git (config.py): scoped_daily / dc_daily also carry goods-in-transit-only days
    (has_daily_row / has_inventory_row False) and a git_quantity. Sales, weighted-instock's sales
    weights and every inventory metric NOT named in inventory_git.metrics read only the real rows with
    on-hand inventory, so they are exactly what they were without the feature. A metric that is named
    reads all rows with on-hand + git_quantity. Each group is computed on its own and joined as before.
    """
    group_keys = list(group_keys)
    keys = [period_col] + group_keys
    git_metrics = ctx.settings["INVENTORY_GIT"]["metrics"]
    # ONE daily-data population for every daily-data metric -- sales, inventory, WOS, turnover and
    # weighted-instock's sales weights all read the same rows, so a single output row can't
    # describe two different populations. Lost sales and in-stock come from their own source and
    # are restricted separately (inst/lost_base).
    daily_scoped = scoped_daily_in.filter(period_filter)
    inst = inst_in.filter(period_filter)
    dc_daily = dc_daily_in.filter(period_filter)
    dc_inst = dc_inst_in.filter(period_filter)
    # Real daily-data / inventory_warehouse rows: what a metric reads unless it is gated on GIT.
    real_daily = daily_scoped.filter(F.col("has_daily_row"))
    real_dc = dc_daily.filter(F.col("has_inventory_row"))

    def store_rows(metric: str) -> DataFrame:
        return daily_scoped if metric in git_metrics else real_daily

    def dc_rows(metric: str) -> DataFrame:
        return dc_daily if metric in git_metrics else real_dc

    sales_pop = apply_group_population_filter(real_daily, "sales", ctx.settings)
    sales = (
        sales_pop.groupBy(*keys)
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
    if "total_inventory" in git_metrics:
        total_inventory_with_git = (
            apply_group_population_filter(daily_scoped, "sales", ctx.settings)
            .groupBy(*keys)
            .agg(F.sum(F.col("inventory") + F.col("git_quantity")).alias("total_inventory"))
        )
        sales = sales.drop("total_inventory").join(total_inventory_with_git, on=keys, how="left")

    # WOS family: each product-week's average daily inventory is weighted by week_share = week_days / 7
    # (scoped_daily.week_days, calendar days of the week in the view). Always 1 for a whole week; it only
    # differs for the week report_end="latest_day" cuts mid-week (YTD's last week, 1-6 days), so that
    # part week's inventory is counted as the fraction of a week it covers against the sales of the
    # same days, not as a full week of inventory against a part week of sales.
    week_keys = ["product_id", "Year", "Week"] + group_keys
    period_extra = [period_col] if period_col not in week_keys else []
    wos_units, wos_retail, wos_cost_inventory = _inventory_exprs("wos" in git_metrics)
    wos_pop = apply_group_population_filter(store_rows("wos"), "wos", ctx.settings)
    daily_by_date = wos_pop.groupBy(*week_keys, *period_extra, "date").agg(
        F.max("week_days").alias("week_days"),
        F.sum(wos_units).alias("daily_total_inventory"),
        F.sum("sales_quantity").alias("daily_sales_units"),
        F.sum(wos_retail).alias("daily_total_inventory_retail"),
        F.sum("sales_revenue").alias("daily_sales_revenue"),
        F.sum(wos_cost_inventory).alias("daily_total_inventory_cost"),
        F.sum("sales_cost").alias("daily_sales_cost"),
    )
    daily_data_week = (
        daily_by_date.groupBy(*week_keys, *period_extra)
        .agg(
            F.max("week_days").alias("week_days"),
            F.avg("daily_total_inventory").alias("avg_daily_total_inventory"),
            F.sum("daily_sales_units").alias("weekly_sales_units"),
            F.avg("daily_total_inventory_retail").alias("avg_daily_inventory_retail"),
            F.sum("daily_sales_revenue").alias("weekly_sales_revenue"),
            F.avg("daily_total_inventory_cost").alias("avg_daily_inventory_cost"),
            F.sum("daily_sales_cost").alias("weekly_sales_cost"),
        )
        .withColumn("week_share", F.col("week_days") / F.lit(7.0))
        .withColumn(
            "wos_units",
            F.when(
                F.col("weekly_sales_units") > 0,
                F.col("avg_daily_total_inventory") * F.col("week_share") / F.col("weekly_sales_units"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "wos_revenue",
            F.when(
                F.col("weekly_sales_revenue") > 0,
                F.col("avg_daily_inventory_retail") * F.col("week_share") / F.col("weekly_sales_revenue"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "wos_cost",
            F.when(
                F.col("weekly_sales_cost") > 0,
                F.col("avg_daily_inventory_cost") * F.col("week_share") / F.col("weekly_sales_cost"),
            ).otherwise(F.lit(None)),
        )
    )
    # WOS_DC / WOS_TOTAL: twins of wos_units at the SAME product×fiscal-week grain -- reuse
    # daily_data_week (and its weekly_sales_units) rather than recomputing sales separately.
    # Left join a DC-inventory frame built at the identical week_keys+period_extra grain;
    # weeks with no DC record fill to 0 (the product simply had no warehouse inventory that
    # week), not dropped.
    dc_wos_pop = apply_group_population_filter(dc_rows("wos_dc"), "wos_dc_total", ctx.settings)
    dc_daily_week = (
        dc_wos_pop.groupBy(*week_keys, *period_extra, "date")
        .agg(F.sum(_dc_inventory_expr("wos_dc" in git_metrics)).alias("daily_dc_inventory"))
        .groupBy(*week_keys, *period_extra)
        .agg(F.avg("daily_dc_inventory").alias("avg_daily_dc_inventory"))
    )
    daily_data_week = (
        daily_data_week.join(dc_daily_week, on=week_keys + period_extra, how="left")
        .withColumn("avg_daily_dc_inventory", F.coalesce(F.col("avg_daily_dc_inventory"), F.lit(0.0)))
        .withColumn(
            "avg_daily_total_inventory_combined",
            F.col("avg_daily_total_inventory") + F.col("avg_daily_dc_inventory"),
        )
        .withColumn(
            "wos_dc",
            F.when(
                F.col("weekly_sales_units") > 0,
                F.col("avg_daily_dc_inventory") * F.col("week_share") / F.col("weekly_sales_units"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "wos_total",
            F.when(
                F.col("weekly_sales_units") > 0,
                F.col("avg_daily_total_inventory_combined") * F.col("week_share") / F.col("weekly_sales_units"),
            ).otherwise(F.lit(None)),
        )
    )
    wos = daily_data_week.groupBy(*keys).agg(
        (F.sum(F.col("wos_units") * F.col("weekly_sales_units")) / F.sum("weekly_sales_units")).alias("WOS"),
        (F.sum(F.col("wos_revenue") * F.col("weekly_sales_revenue")) / F.sum("weekly_sales_revenue")).alias("wos_revenue"),
        (F.sum(F.col("wos_cost") * F.col("weekly_sales_cost")) / F.sum("weekly_sales_cost")).alias("wos_cost"),
        (F.sum(F.col("wos_dc") * F.col("weekly_sales_units")) / F.sum("weekly_sales_units")).alias("WOS_DC"),
        (F.sum(F.col("wos_total") * F.col("weekly_sales_units")) / F.sum("weekly_sales_units")).alias("WOS_TOTAL"),
    )

    mean_stock_pop = apply_group_population_filter(store_rows("mean_stock"), "mean_stock", ctx.settings)
    mean_stock = _mean_stock_frame(mean_stock_pop, keys, "mean_stock" in git_metrics)

    # dc_mean_stock / total_mean_stock: plain per-day averages (not the WOS ratio), same shape
    # as _mean_stock_frame, at the same `keys` grain. dc_mean_stock gates the DC part of
    # total_mean_stock, mean_stock its store part.
    dc_mean_units = _dc_inventory_expr("dc_mean_stock" in git_metrics)
    dc_inv_pop = apply_group_population_filter(dc_rows("dc_mean_stock"), "dc_inventory", ctx.settings)
    dc_mean_stock = (
        dc_inv_pop.groupBy(*keys, "date")
        .agg(F.sum(dc_mean_units).alias("daily_dc_inv"))
        .groupBy(*keys)
        .agg(F.avg("daily_dc_inv").alias("dc_mean_stock"))
    )
    total_store_units, _, _ = _inventory_exprs("mean_stock" in git_metrics)
    total_store_pop = apply_group_population_filter(store_rows("mean_stock"), "dc_inventory", ctx.settings)
    total_store_day = total_store_pop.groupBy(*keys, "date").agg(F.sum(total_store_units).alias("store_daily_inv"))
    total_dc_day = dc_inv_pop.groupBy(*keys, "date").agg(F.sum(dc_mean_units).alias("dc_daily_inv"))
    total_mean_stock = (
        total_store_day.join(total_dc_day, on=[*keys, "date"], how="left")
        .withColumn("dc_daily_inv", F.coalesce(F.col("dc_daily_inv"), F.lit(0.0)))
        .withColumn("total_daily_inv", F.col("store_daily_inv") + F.col("dc_daily_inv"))
        .groupBy(*keys)
        .agg(F.avg("total_daily_inv").alias("total_mean_stock"))
    )

    turnover_pop = apply_group_population_filter(real_daily, "turnover", ctx.settings)
    turnover_stock_pop = apply_group_population_filter(store_rows("inventory_turnover_rate"), "turnover", ctx.settings)
    turnover_mean_stock = _mean_stock_frame(
        turnover_stock_pop, keys, "inventory_turnover_rate" in git_metrics
    ).select(*keys, "mean_stock")
    turnover = (
        turnover_pop.groupBy(*keys)
        .agg(F.sum("sales_quantity").alias("sales_units"))
        .join(turnover_mean_stock, on=keys, how="inner")
        .withColumn(
            "inventory_turnover_rate",
            F.when(F.col("mean_stock") == 0, F.lit(None)).otherwise(F.col("sales_units") / F.col("mean_stock")),
        )
        .select(*keys, "inventory_turnover_rate")
    )

    instock_pop = apply_group_population_filter(inst, "instock", ctx.settings)
    instock = instock_pop.groupBy(*keys).agg(
        F.greatest(F.lit(0.0), F.sum("stocked_pairs") / F.sum("available_days")).alias("in_stock_rate")
    )

    # Mirrors the "instock" block above, from dc_inst's grid instead. When disabled, dc_inst is
    # empty so this produces no rows and the later left join leaves dc_in_stock_rate null.
    dc_instock_pop = apply_group_population_filter(dc_inst, "dc_instock", ctx.settings)
    dc_instock = dc_instock_pop.groupBy(*keys).agg(
        F.greatest(F.lit(0.0), F.sum("dc_stocked_days") / F.sum("dc_available_days")).alias("dc_in_stock_rate")
    )

    # Sales-weighted in-stock rate: aggregate instock to Year×Week (+ slice group_keys), then
    # weight each week by its sales when rolling up to the reporting period. Both sides (instock
    # ratio and its sales weight) apply the SAME "weighted_instock" population override, so the
    # numerator/denominator population and the weighting population never diverge.
    wi_pop_inst = apply_group_population_filter(inst, "weighted_instock", ctx.settings)
    wi_pop_daily = apply_group_population_filter(real_daily, "weighted_instock", ctx.settings)
    wi_week_keys = ["Year", "Week"] + group_keys
    wi_period_extra = [period_col] if period_col not in wi_week_keys else []

    weekly_pair_instock = wi_pop_inst.groupBy(*wi_week_keys, *wi_period_extra).agg(
        F.greatest(F.lit(0.0), F.sum("stocked_pairs") / F.sum("available_days")).alias("_pair_instock_rate"),
    )
    weekly_pair_sales = wi_pop_daily.groupBy(*wi_week_keys, *wi_period_extra).agg(
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
        .join(dc_mean_stock, on=keys, how="left")
        .join(total_mean_stock, on=keys, how="left")
        .join(turnover, on=keys, how="left")
        .join(instock, on=keys, how="left")
        .join(weighted_instock, on=keys, how="left")
        .join(dc_instock, on=keys, how="left")
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
        ctx,
        frames["scoped_daily"],
        frames["inst_data"],
        frames["dc_daily"],
        frames["dc_inst"],
        period_col,
        group_keys,
        period_filter,
    )
    lost_base_pop = apply_group_population_filter(frames["lost_base"], "lost_sales", ctx.settings)
    lost_pct = (
        lost_base_pop
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

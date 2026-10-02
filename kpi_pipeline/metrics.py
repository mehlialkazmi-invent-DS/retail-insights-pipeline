"""KPI metric computation."""

from __future__ import annotations

from typing import Dict, Sequence

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.filters import apply_group_population_filter


def _day_sums(real_flag: str, units: bool = True, retail_cost: bool = False):
    """Per-day aggregates of one inventory frame for _day_avg: "<x>_oh" (on-hand) and "<x>_git" (on-hand
    + goods in transit) sums plus has_real, whether the day has a real (non GIT-only) row.

    A goods-in-transit-only row has on-hand 0, so the on-hand sums equal the real rows' own sums; only
    the set of days differs, which _day_avg handles."""
    with_git = F.col("inventory") + F.col("git_quantity")
    exprs = [F.max(real_flag).alias("has_real")]
    if units:
        exprs += [F.sum("inventory").alias("units_oh"), F.sum(with_git).alias("units_git")]
    if retail_cost:
        exprs += [
            F.sum("inventory_retail").alias("retail_oh"),
            F.sum(F.round(with_git * F.col("price_without_tax"), 2)).alias("retail_git"),
            F.sum("inventory_cost").alias("cost_oh"),
            F.sum(F.round(with_git * F.col("cogs"), 2)).alias("cost_git"),
        ]
    return exprs


def _day_avg(value: str, with_git: bool):
    """Average of a _day_sums column over days. With goods in transit: every day, on-hand + GIT. Without:
    only the days with a real row, on-hand only -- exactly the value without inventory_git."""
    if with_git:
        return F.avg(f"{value}_git")
    return F.avg(F.when(F.col("has_real"), F.col(f"{value}_oh")))


def _mean_stock_frame(daily_scoped: DataFrame, keys: Sequence[str], git_metrics: Sequence[str]) -> DataFrame:
    """mean_stock/mean_stock_retail/mean_stock_cost from a (possibly population-filtered)
    daily_scoped frame; each counts goods in transit only when named in ``git_metrics``. Factored out so
    turnover can recompute its own mean-stock over its own population override instead of reusing the
    public mean_stock metric's frame verbatim."""
    seg_day = daily_scoped.groupBy(*keys, "date").agg(*_day_sums("has_daily_row", retail_cost=True))
    return seg_day.groupBy(*keys).agg(
        _day_avg("units", "mean_stock" in git_metrics).alias("mean_stock"),
        _day_avg("retail", "mean_stock_retail" in git_metrics).alias("mean_stock_retail"),
        _day_avg("cost", "mean_stock_cost" in git_metrics).alias("mean_stock_cost"),
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
    (has_daily_row / has_inventory_row False) and a git_quantity. Sales and weighted-instock's sales
    weights read only the real rows. Every inventory metric reads on-hand on the days with a real row,
    exactly as without the feature, unless it is named in inventory_git.metrics: then it reads on-hand +
    git_quantity on every day (_day_sums / _day_avg). Each metric is gated on its own; each group is
    computed on its own and joined as before.
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
    # Real daily-data rows (no goods-in-transit-only days): the sales metrics' rows.
    real_daily = daily_scoped.filter(F.col("has_daily_row"))

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
    wos_pop = apply_group_population_filter(daily_scoped, "wos", ctx.settings)
    daily_by_date = wos_pop.groupBy(*week_keys, *period_extra, "date").agg(
        F.max("week_days").alias("week_days"),
        *_day_sums("has_daily_row", retail_cost=True),
        F.sum("sales_quantity").alias("daily_sales_units"),
        F.sum("sales_revenue").alias("daily_sales_revenue"),
        F.sum("sales_cost").alias("daily_sales_cost"),
    )
    daily_data_week = (
        daily_by_date.groupBy(*week_keys, *period_extra)
        .agg(
            F.max("week_days").alias("week_days"),
            _day_avg("units", "WOS" in git_metrics).alias("avg_daily_total_inventory"),
            # WOS_TOTAL's store part: on-hand + GIT only when WOS_TOTAL itself is named.
            _day_avg("units", "WOS_TOTAL" in git_metrics).alias("avg_daily_store_inventory_total"),
            F.sum("daily_sales_units").alias("weekly_sales_units"),
            _day_avg("retail", "wos_revenue" in git_metrics).alias("avg_daily_inventory_retail"),
            F.sum("daily_sales_revenue").alias("weekly_sales_revenue"),
            _day_avg("cost", "wos_cost" in git_metrics).alias("avg_daily_inventory_cost"),
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
    dc_wos_pop = apply_group_population_filter(dc_daily, "wos_dc_total", ctx.settings)
    dc_daily_week = (
        dc_wos_pop.groupBy(*week_keys, *period_extra, "date")
        .agg(*_day_sums("has_inventory_row"))
        .groupBy(*week_keys, *period_extra)
        .agg(
            _day_avg("units", "WOS_DC" in git_metrics).alias("avg_daily_dc_inventory"),
            _day_avg("units", "WOS_TOTAL" in git_metrics).alias("avg_daily_dc_inventory_total"),
        )
    )
    daily_data_week = (
        daily_data_week.join(dc_daily_week, on=week_keys + period_extra, how="left")
        .withColumn("avg_daily_dc_inventory", F.coalesce(F.col("avg_daily_dc_inventory"), F.lit(0.0)))
        .withColumn(
            "avg_daily_total_inventory_combined",
            F.col("avg_daily_store_inventory_total") + F.coalesce(F.col("avg_daily_dc_inventory_total"), F.lit(0.0)),
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

    mean_stock_pop = apply_group_population_filter(daily_scoped, "mean_stock", ctx.settings)
    mean_stock = _mean_stock_frame(mean_stock_pop, keys, git_metrics)

    # dc_mean_stock / total_mean_stock: plain per-day averages (not the WOS ratio), same shape
    # as _mean_stock_frame, at the same `keys` grain. total_mean_stock is averaged over the store
    # days; with goods in transit (total_mean_stock named) both its store and DC part count it.
    dc_inv_pop = apply_group_population_filter(dc_daily, "dc_inventory", ctx.settings)
    dc_day = dc_inv_pop.groupBy(*keys, "date").agg(*_day_sums("has_inventory_row"))
    dc_mean_stock = dc_day.groupBy(*keys).agg(
        _day_avg("units", "dc_mean_stock" in git_metrics).alias("dc_mean_stock")
    )
    total_git = "total_mean_stock" in git_metrics
    total_store_pop = apply_group_population_filter(daily_scoped, "dc_inventory", ctx.settings)
    total_store_day = total_store_pop.groupBy(*keys, "date").agg(*_day_sums("has_daily_row"))
    total_mean_stock = (
        total_store_day.join(
            dc_day.select(*keys, "date", F.col("units_oh").alias("dc_oh"), F.col("units_git").alias("dc_git")),
            on=[*keys, "date"],
            how="left",
        )
        .withColumn("units_oh", F.col("units_oh") + F.coalesce(F.col("dc_oh"), F.lit(0.0)))
        .withColumn("units_git", F.col("units_git") + F.coalesce(F.col("dc_git"), F.lit(0.0)))
        .groupBy(*keys)
        .agg(_day_avg("units", total_git).alias("total_mean_stock"))
    )

    turnover_pop = apply_group_population_filter(real_daily, "turnover", ctx.settings)
    turnover_stock_pop = apply_group_population_filter(daily_scoped, "turnover", ctx.settings)
    turnover_mean_stock = (
        turnover_stock_pop.groupBy(*keys, "date")
        .agg(*_day_sums("has_daily_row"))
        .groupBy(*keys)
        .agg(_day_avg("units", "inventory_turnover_rate" in git_metrics).alias("mean_stock"))
    )
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

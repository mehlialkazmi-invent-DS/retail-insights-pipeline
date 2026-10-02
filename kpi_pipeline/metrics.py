"""KPI metric computation."""

from __future__ import annotations

from typing import Dict, Sequence

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.filters import apply_group_population_filter

# One row's on-hand stock per measure, and the per-unit price column goods in transit is valued at.
_ON_HAND = {"units": "inventory", "retail": "inventory_retail", "cost": "inventory_cost"}
_UNIT_PRICE = {"retail": "price_without_tax", "cost": "cogs"}

# WOS family: (metric, measure of the store inventory behind it or None, sales column it divides by).
# WOS_DC and WOS_TOTAL also add the DC inventory (_WOS_DC_METRICS).
_WOS_METRICS = (
    ("WOS", "units", "sales_quantity"),
    ("wos_revenue", "retail", "sales_revenue"),
    ("wos_cost", "cost", "sales_cost"),
    ("WOS_DC", None, "sales_quantity"),
    ("WOS_TOTAL", "units", "sales_quantity"),
)
_WOS_DC_METRICS = ("WOS_DC", "WOS_TOTAL")


def _reads(ctx: KPIContext, metric: str):
    """The rows of a daily frame (scoped_daily / dc_daily) ``metric`` reads: only the unblocked ones when it
    is in blocked_scope.metrics, every row otherwise."""
    return ~F.col("is_blocked") if metric in ctx.settings["BLOCKED_SCOPE"]["metrics"] else F.lit(True)


def _read_only(ctx: KPIContext, metric: str, value):
    """``value`` on the rows ``metric`` reads, null on the others, so sum / avg / countDistinct skip them."""
    return F.when(_reads(ctx, metric), value)


def _per_unit(ctx: KPIContext, metric: str, numerator: str):
    """AUR / AUC: ``numerator`` per sales unit over the rows ``metric`` reads, null without units."""
    units = F.sum(_read_only(ctx, metric, F.col("sales_quantity")))
    return F.when(units == 0, F.lit(None)).otherwise(
        F.sum(_read_only(ctx, metric, F.col(numerator))) / units
    )


def _stock(measure: str, with_git: bool):
    """One row's stock of ``measure`` (units / retail / cost): on-hand, or on-hand + goods in transit."""
    if not with_git:
        return F.col(_ON_HAND[measure])
    units = F.col("inventory") + F.col("git_quantity")
    if measure == "units":
        return units
    return F.round(units * F.col(_UNIT_PRICE[measure]), 2)


def _day_stock(ctx: KPIContext, metric: str, measure: str, real_flag: str):
    """Per-day aggregates of one inventory metric, for _day_avg: "<metric>_day" is the day's stock summed over
    the rows the metric reads (on-hand, plus goods in transit when the metric is in
    goods_in_transit.inventory_metrics) and "<metric>_has" whether the day has a row it reads: any row with
    goods in transit, only a real (``real_flag``, not GIT-only) row without. A GIT-only row has on-hand 0, so
    it adds nothing to an on-hand sum and must not make a day of its own."""
    with_git = metric in ctx.settings["GOODS_IN_TRANSIT"]["inventory_metrics"]
    reads = _reads(ctx, metric)
    return [
        F.sum(F.when(reads, _stock(measure, with_git))).alias(f"{metric}_day"),
        F.max(reads if with_git else reads & F.col(real_flag)).alias(f"{metric}_has"),
    ]


def _day_avg(metric: str):
    """Average of a metric's _day_stock sums over the days it has a row on."""
    return F.avg(F.when(F.col(f"{metric}_has"), F.col(f"{metric}_day")))


def _mean_stock_frame(ctx: KPIContext, daily_scoped: DataFrame, keys: Sequence[str]) -> DataFrame:
    """mean_stock/mean_stock_retail/mean_stock_cost from a (possibly population-filtered) daily_scoped
    frame; each reads goods in transit and drops blocked days per its own config entries."""
    stock_day = daily_scoped.groupBy(*keys, "date").agg(
        *_day_stock(ctx, "mean_stock", "units", "has_daily_row"),
        *_day_stock(ctx, "mean_stock_retail", "retail", "has_daily_row"),
        *_day_stock(ctx, "mean_stock_cost", "cost", "has_daily_row"),
    )
    return stock_day.groupBy(*keys).agg(
        _day_avg("mean_stock").alias("mean_stock"),
        _day_avg("mean_stock_retail").alias("mean_stock_retail"),
        _day_avg("mean_stock_cost").alias("mean_stock_cost"),
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

    Blocked scope (blocked_scope.metrics): scoped_daily / dc_daily keep their blocked days, flagged
    is_blocked. Every metric reads only the unblocked rows when it is in the list and every row when not,
    by conditional aggregation (_read_only / _day_stock), so each frame is still aggregated once per
    family. The in-stock frames (inst / dc_inst) and lost sales's sales denominator have their blocked days
    removed when they are built (pipeline.build_instock_daily / build_dc_inst / build_pipeline_frames), only
    when in_stock_rate / dc_in_stock_rate / lost_sales_pct are in the list.

    goods_in_transit.inventory_metrics (config.py): scoped_daily / dc_daily also carry goods-in-transit-only
    days (has_daily_row / has_inventory_row False) and a git_quantity. Sales and weighted-instock's sales
    weights read only the real rows. Every inventory metric reads on-hand on the days with a real row,
    exactly as without the feature, unless it is named in goods_in_transit.inventory_metrics: then it reads
    on-hand + git_quantity on every day (_day_stock / _day_avg). Each metric is gated on its own; each
    group is computed on its own and joined as before.
    """
    group_keys = list(group_keys)
    keys = [period_col] + group_keys
    git_metrics = ctx.settings["GOODS_IN_TRANSIT"]["inventory_metrics"]
    # ONE daily-data population for every daily-data metric -- sales, inventory, WOS, turnover and
    # weighted-instock's sales weights all read the same rows -- with one deliberate exception:
    # blocked_scope.metrics, where a metric not in the list also reads the blocked days. Lost sales and
    # in-stock come from their own source and are restricted separately (inst/lost_base).
    daily_scoped = scoped_daily_in.filter(period_filter)
    inst = inst_in.filter(period_filter)
    dc_daily = dc_daily_in.filter(period_filter)
    dc_inst = dc_inst_in.filter(period_filter)
    # Real daily-data rows (no goods-in-transit-only days): the sales metrics' rows.
    real_daily = daily_scoped.filter(F.col("has_daily_row"))

    # The sales rows give every period x slice its output row (the other families are left-joined onto
    # it). Blocked days are left out of them only when every reported metric drops blocked days, so a
    # period x slice with blocked days only keeps its row while any reported metric still reads them.
    sales_rows = real_daily
    if set(ctx.settings["METRIC_COLS"]) <= set(ctx.settings["BLOCKED_SCOPE"]["metrics"]):
        sales_rows = real_daily.filter(~F.col("is_blocked"))
    sales_pop = apply_group_population_filter(sales_rows, "sales", ctx.settings)
    sales = sales_pop.groupBy(*keys).agg(
        F.sum(_read_only(ctx, "total_inventory", F.col("inventory"))).alias("total_inventory"),
        F.sum(_read_only(ctx, "total_sales_quantity", F.col("sales_quantity"))).alias("total_sales_quantity"),
        F.sum(_read_only(ctx, "total_sales_revenue", F.col("sales_revenue"))).alias("total_sales_revenue"),
        _per_unit(ctx, "AUR", "sales_revenue").alias("AUR"),
        _per_unit(ctx, "AUC", "sales_cost").alias("AUC"),
        F.countDistinct(_read_only(ctx, "distinct_product_count", F.col("product_id"))).alias(
            "distinct_product_count"
        ),
        F.countDistinct(_read_only(ctx, "distinct_store_count", F.col("store_id"))).alias("distinct_store_count"),
        F.countDistinct(
            _read_only(ctx, "distinct_pair_count", F.col("product_id")),
            _read_only(ctx, "distinct_pair_count", F.col("store_id")),
        ).alias("distinct_pair_count"),
    )
    if "total_inventory" in git_metrics:
        total_inventory_with_git = (
            apply_group_population_filter(daily_scoped, "sales", ctx.settings)
            .groupBy(*keys)
            .agg(
                F.sum(
                    _read_only(ctx, "total_inventory", F.col("inventory") + F.col("git_quantity"))
                ).alias("total_inventory")
            )
        )
        sales = sales.drop("total_inventory").join(total_inventory_with_git, on=keys, how="left")

    # WOS family: each product-week's average daily inventory is weighted by week_share = week_days / 7
    # (scoped_daily.week_days, calendar days of the week in the view). Always 1 for a whole week; it only
    # differs for the week report_end="latest_day" cuts mid-week (YTD's last week, 1-6 days), so that
    # part week's inventory is counted as the fraction of a week it covers against the sales of the
    # same days, not as a full week of inventory against a part week of sales.
    # Each metric of _WOS_METRICS carries its own columns, named "<metric>_...": its store inventory per
    # day and week, and the sales it divides by, all on the rows its own blocked_scope.metrics gate reads.
    week_keys = ["product_id", "Year", "Week"] + group_keys
    period_extra = [period_col] if period_col not in week_keys else []
    wos_pop = apply_group_population_filter(daily_scoped, "wos", ctx.settings)
    day_aggs = [F.max("week_days").alias("week_days")]
    week_aggs = [F.max("week_days").alias("week_days")]
    for metric, measure, sales_col in _WOS_METRICS:
        day_aggs.append(F.sum(_read_only(ctx, metric, F.col(sales_col))).alias(f"{metric}_sales_day"))
        week_aggs.append(F.sum(f"{metric}_sales_day").alias(f"{metric}_sales"))
        if measure is not None:
            day_aggs += _day_stock(ctx, metric, measure, "has_daily_row")
            week_aggs.append(_day_avg(metric).alias(f"{metric}_store_stock"))
    daily_data_week = (
        wos_pop.groupBy(*week_keys, *period_extra, "date")
        .agg(*day_aggs)
        .groupBy(*week_keys, *period_extra)
        .agg(*week_aggs)
        .withColumn("week_share", F.col("week_days") / F.lit(7.0))
    )
    # WOS_DC / WOS_TOTAL: twins of WOS at the SAME product×fiscal-week grain -- reuse daily_data_week's
    # store side rather than recomputing it. Left join a DC-inventory frame built at the identical
    # week_keys+period_extra grain; weeks with no DC record fill to 0 (the product simply had no
    # warehouse inventory that week), not dropped.
    dc_wos_pop = apply_group_population_filter(dc_daily, "wos_dc_total", ctx.settings)
    dc_daily_week = (
        dc_wos_pop.groupBy(*week_keys, *period_extra, "date")
        .agg(*[agg for m in _WOS_DC_METRICS for agg in _day_stock(ctx, m, "units", "has_inventory_row")])
        .groupBy(*week_keys, *period_extra)
        .agg(*[_day_avg(m).alias(f"{m}_dc_stock") for m in _WOS_DC_METRICS])
    )
    weekly_wos = []
    for metric, measure, _ in _WOS_METRICS:
        stock = F.col(f"{metric}_store_stock") if measure is not None else F.lit(0.0)
        if metric in _WOS_DC_METRICS:
            stock = stock + F.coalesce(F.col(f"{metric}_dc_stock"), F.lit(0.0))
        sales_units = F.col(f"{metric}_sales")
        weekly_wos.append(
            F.when(sales_units > 0, stock * F.col("week_share") / sales_units)
            .otherwise(F.lit(None))
            .alias(f"{metric}_weekly")
        )
    daily_data_week = daily_data_week.join(dc_daily_week, on=week_keys + period_extra, how="left").select(
        "*", *weekly_wos
    )
    wos = daily_data_week.groupBy(*keys).agg(
        *[
            (F.sum(F.col(f"{m}_weekly") * F.col(f"{m}_sales")) / F.sum(f"{m}_sales")).alias(m)
            for m, _, _ in _WOS_METRICS
        ]
    )

    mean_stock_pop = apply_group_population_filter(daily_scoped, "mean_stock", ctx.settings)
    mean_stock = _mean_stock_frame(ctx, mean_stock_pop, keys)

    # dc_mean_stock / total_mean_stock: plain per-day averages (not the WOS ratio), same shape
    # as _mean_stock_frame, at the same `keys` grain. total_mean_stock is averaged over the store
    # days; with goods in transit (total_mean_stock named) both its store and DC part count it.
    dc_inv_pop = apply_group_population_filter(dc_daily, "dc_inventory", ctx.settings)
    dc_day = dc_inv_pop.groupBy(*keys, "date").agg(
        *_day_stock(ctx, "dc_mean_stock", "units", "has_inventory_row"),
        *_day_stock(ctx, "total_mean_stock", "units", "has_inventory_row"),
    )
    dc_mean_stock = dc_day.groupBy(*keys).agg(_day_avg("dc_mean_stock").alias("dc_mean_stock"))
    total_store_pop = apply_group_population_filter(daily_scoped, "dc_inventory", ctx.settings)
    total_store_day = total_store_pop.groupBy(*keys, "date").agg(
        *_day_stock(ctx, "total_mean_stock", "units", "has_daily_row")
    )
    total_mean_stock = (
        total_store_day.join(
            dc_day.select(*keys, "date", F.col("total_mean_stock_day").alias("dc_day")),
            on=[*keys, "date"],
            how="left",
        )
        .withColumn("total_mean_stock_day", F.col("total_mean_stock_day") + F.coalesce(F.col("dc_day"), F.lit(0.0)))
        .groupBy(*keys)
        .agg(_day_avg("total_mean_stock").alias("total_mean_stock"))
    )

    turnover_pop = apply_group_population_filter(real_daily, "turnover", ctx.settings)
    turnover_stock_pop = apply_group_population_filter(daily_scoped, "turnover", ctx.settings)
    turnover_mean_stock = (
        turnover_stock_pop.groupBy(*keys, "date")
        .agg(*_day_stock(ctx, "inventory_turnover_rate", "units", "has_daily_row"))
        .groupBy(*keys)
        .agg(_day_avg("inventory_turnover_rate").alias("mean_stock"))
    )
    turnover = (
        turnover_pop.groupBy(*keys)
        .agg(F.sum(_read_only(ctx, "inventory_turnover_rate", F.col("sales_quantity"))).alias("sales_units"))
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
    # numerator/denominator population and the weighting population never diverge. The in-stock side is
    # the same frame as in_stock_rate (blocked days gated by in_stock_rate); the sales weights drop blocked
    # days when weighted_instock_rate is in blocked_scope.metrics.
    wi_pop_inst = apply_group_population_filter(inst, "weighted_instock", ctx.settings)
    wi_pop_daily = apply_group_population_filter(real_daily, "weighted_instock", ctx.settings)
    wi_week_keys = ["Year", "Week"] + group_keys
    wi_period_extra = [period_col] if period_col not in wi_week_keys else []

    weekly_pair_instock = wi_pop_inst.groupBy(*wi_week_keys, *wi_period_extra).agg(
        F.greatest(F.lit(0.0), F.sum("stocked_pairs") / F.sum("available_days")).alias("_pair_instock_rate"),
    )
    weekly_pair_sales = wi_pop_daily.groupBy(*wi_week_keys, *wi_period_extra).agg(
        F.sum(_read_only(ctx, "weighted_instock_rate", F.col("sales_quantity"))).alias("_pair_sales_qty")
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

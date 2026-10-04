"""KPI metric computation."""

from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.filters import apply_group_population_filter, resolve_group_population_filter

# One row's stock per measure: on-hand, and on-hand + goods in transit (derived per store row by
# collapse_frames before the store rows are summed, since the valued ones are rounded per row).
_ON_HAND = {"units": "inventory", "retail": "inventory_retail", "cost": "inventory_cost"}
_WITH_GIT = {"units": "inventory_git", "retail": "inventory_retail_git", "cost": "inventory_cost_git"}
_UNIT_PRICE = {"retail": "price_without_tax", "cost": "cogs"}

# The frames build_kpi_table reads: collapse_frames' output (for kpi_long, stacked per root x cut by
# kpi_long._stack_roots_and_cuts).
KPI_FRAMES = ("scoped_daily", "sales_pairs", "inst_data", "lost_base", "dc_daily", "dc_inst")

# collapse_frames, per metric frame: (the location column summed across, the additive measure columns).
# Every other column of a frame is a collapse group key, so it must be the same on every store / warehouse
# row of a product and day (product attributes, calendar, flags): a new additive per-store column must be
# listed here, or the collapse would merge it as a key. The measures are float (noob daily-data); a decimal
# measure would widen the sum of sums' precision.
_COLLAPSE = {
    "scoped_daily": (
        "store_id",
        (
            "sales_revenue", "sales_quantity", "sales_cost", "inventory", "inventory_retail", "inventory_cost",
            "git_quantity", "inventory_git", "inventory_retail_git", "inventory_cost_git",
        ),
    ),
    "inst_data": ("store_id", ("stocked_pairs", "available_days")),
    "lost_base": ("store_id", ("lost_sales", "sales_quantity_weekly", "TY_sales_quantity_weekly_corrected_lost_sales")),
    "dc_daily": ("warehouse_id", ("inventory", "git_quantity", "inventory_git")),
    "dc_inst": ("warehouse_id", ("dc_stocked_days", "dc_available_days", "dc_unblocked_days")),
}

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
    """``value`` on the rows ``metric`` reads, null on the others (skipped by sum / avg / countDistinct)."""
    return F.when(_reads(ctx, metric), value)


def _per_unit(ctx: KPIContext, metric: str, numerator: str):
    """AUR / AUC: ``numerator`` per sales unit over the rows ``metric`` reads, null without units."""
    units = F.sum(_read_only(ctx, metric, F.col("sales_quantity")))
    return F.when(units == 0, F.lit(None)).otherwise(
        F.sum(_read_only(ctx, metric, F.col(numerator))) / units
    )


def _stock(measure: str, with_git: bool):
    """One row's stock of ``measure`` (units / retail / cost): on-hand, or on-hand + goods in transit."""
    return F.col((_WITH_GIT if with_git else _ON_HAND)[measure])


def _day_stock(ctx: KPIContext, metric: str, measure: str, real_flag: str):
    """Per-day aggregates of one inventory metric for _day_avg: "<metric>_day", the day's stock over the rows
    the metric reads (+ goods in transit when named in goods_in_transit.inventory_metrics), and
    "<metric>_has", whether the day counts: any read row with goods in transit, only a real (``real_flag``)
    row without -- a GIT-only row has on-hand 0 and must not make a day of its own."""
    with_git = metric in ctx.settings["GOODS_IN_TRANSIT"]["inventory_metrics"]
    reads = _reads(ctx, metric)
    return [
        F.sum(F.when(reads, _stock(measure, with_git))).alias(f"{metric}_day"),
        F.max(reads if with_git else reads & F.col(real_flag)).alias(f"{metric}_has"),
    ]


def _day_avg(metric: str):
    """Average of a metric's _day_stock sums over the days it has a row on."""
    return F.avg(F.when(F.col(f"{metric}_has"), F.col(f"{metric}_day")))


# Store stock per day (_day_stock) of each population-filter group (filters.METRIC_FILTER_GROUPS) averaged
# over days: (metric, measure) pairs.
_STORE_DAY_STOCK = {
    "mean_stock": (("mean_stock", "units"), ("mean_stock_retail", "retail"), ("mean_stock_cost", "cost")),
    "dc_inventory": (("total_mean_stock", "units"),),
    "turnover": (("inventory_turnover_rate", "units"),),
}


def _store_day_stock(ctx: KPIContext, daily_scoped: DataFrame, keys: Sequence[str]) -> Dict[str, DataFrame]:
    """Per (keys, date) store stock of every _STORE_DAY_STOCK group, by group. Groups whose
    metrics.population_filters resolve to the same population share one groupBy, so one pass over
    scoped_daily serves all of them (each column is still aggregated over exactly its own group's rows)."""
    shared: List[Tuple[dict, List[str]]] = []
    for group in _STORE_DAY_STOCK:
        population = resolve_group_population_filter(group, ctx.settings)
        match = next((groups for p, groups in shared if p == population), None)
        if match is None:
            shared.append((population, [group]))
        else:
            match.append(group)
    frames: Dict[str, DataFrame] = {}
    for _, groups in shared:
        day = (
            apply_group_population_filter(daily_scoped, groups[0], ctx.settings)
            .groupBy(*keys, "date")
            .agg(
                *[
                    agg
                    for group in groups
                    for metric, measure in _STORE_DAY_STOCK[group]
                    for agg in _day_stock(ctx, metric, measure, "has_daily_row")
                ]
            )
        )
        frames.update({group: day for group in groups})
    return frames


def _filter_columns(ctx: KPIContext) -> set:
    """The columns a root, a cut (value filter) or any metrics.population_filters entry filters on."""
    population = ctx.settings.get("METRIC_POPULATION_FILTERS") or {}
    return (
        {root["dim_col"] for root in ctx.root_definitions}
        | set(ctx.cut_dimensions)
        | {dim for spec in population.values() for dim in (spec or {})}
    )


def _collapse(frame: DataFrame, location: str, measures: Sequence[str], filtered: set) -> DataFrame:
    """``frame`` summed across ``location``: one row per value combination of every other column, its
    ``measures`` summed (a sum of all-null stays null). Each collapsed row only merges rows identical in every
    column a filter, gate, group key or distinct count downstream reads, so filtering / grouping it and then
    summing equals doing so on the rows. Kept as is without the location column, or when a root / cut /
    population filter reads the location or a measure (that filter must see the rows)."""
    if location not in frame.columns or filtered & {location, *measures}:
        return frame
    keys = [c for c in frame.columns if c != location and c not in measures]
    return frame.groupBy(*keys).agg(*[F.sum(m).alias(m) for m in measures])


def _sales_rows(ctx: KPIContext, real_daily: DataFrame) -> DataFrame:
    """The rows of the sales metrics (sales / AUR / AUC / total_inventory / distinct counts), which give every
    period x slice its output row (the other families are left-joined onto it): the real daily rows, without
    the blocked days only when every reported metric drops them, so a row survives while any metric reads
    them."""
    if set(ctx.settings["METRIC_COLS"]) <= set(ctx.settings["BLOCKED_SCOPE"]["metrics"]):
        real_daily = real_daily.filter(~F.col("is_blocked"))
    return real_daily


def collapse_frames(
    ctx: KPIContext, frames: Dict[str, DataFrame], period_col: str, period_filter
) -> Dict[str, DataFrame]:
    """The KPI_FRAMES of one period-framed (and, for comparable pairs, pair-restricted) frame set, cached:
    every metric frame's ``period_filter`` rows summed across stores / warehouses to product level (_collapse),
    plus sales_pairs for the store and pair counts. Release with context.release_frames.

    Every row filter runs on the store rows first: period framing and the comparable restriction (the caller)
    and ``period_filter`` here. The root / cut filters (kpi_long._stack_roots_and_cuts) and population filters
    (compute_kpis) applied afterwards read product attributes, which the collapse keeps as group keys, so they
    select exactly the same rows; one that reads a store / warehouse or a measure leaves the frames it would
    see uncollapsed. Goods in transit valued at retail / cost is rounded per store row (as inventory_retail /
    inventory_cost / sales_cost are), so it is derived before the sum. A frame left uncollapsed is a filter
    of an already-cached pipeline frame and is not cached again.

    sales_pairs: the distinct (product_id, store_id, is_blocked, ``period_col``, filtered columns) of the real
    daily rows, from which distinct_store_count / distinct_pair_count count the same (product, store) set per
    group as on the store rows.
    """
    filtered = _filter_columns(ctx)
    units_git = F.col("inventory") + F.col("git_quantity")
    framed = {key: frames[key].filter(period_filter) for key in _COLLAPSE}
    framed["scoped_daily"] = (
        framed["scoped_daily"]
        .withColumn("inventory_git", units_git)
        .withColumn("inventory_retail_git", F.round(units_git * F.col(_UNIT_PRICE["retail"]), 2))
        .withColumn("inventory_cost_git", F.round(units_git * F.col(_UNIT_PRICE["cost"]), 2))
    )
    framed["dc_daily"] = framed["dc_daily"].withColumn("inventory_git", units_git)
    collapsed = {}
    for key in _COLLAPSE:
        frame = _collapse(framed[key], *_COLLAPSE[key], filtered)
        collapsed[key] = frame if frame is framed[key] else frame.cache()
    pair_cols = dict.fromkeys(
        ["product_id", "store_id", "is_blocked", period_col, *sorted(filtered & set(framed["scoped_daily"].columns))]
    )
    collapsed["sales_pairs"] = (
        framed["scoped_daily"].filter(F.col("has_daily_row")).select(*pair_cols).distinct().cache()
    )
    return collapsed


def _join_null_safe(left: DataFrame, right: DataFrame, keys: Sequence[str]) -> DataFrame:
    """``left`` left-joined with ``right`` on ``keys``, a null key (a cut value kept by keep_null) matching
    null, as when both sides were one aggregation."""
    right = right.select(
        *[F.col(k).alias(f"_r_{k}") for k in keys], *[c for c in right.columns if c not in keys]
    )
    on = [F.col(k).eqNullSafe(F.col(f"_r_{k}")) for k in keys]
    return left.join(right, on, how="left").drop(*[f"_r_{k}" for k in keys])


def compute_kpis(
    ctx: KPIContext, frames: Dict[str, DataFrame], period_col: str, group_keys: Sequence[str]
) -> DataFrame:
    """KPI columns per (period_col, *group_keys) of collapse_frames' frames.

    Each metric group (filters.METRIC_FILTER_GROUPS) applies its own metrics.population_filters on top of
    the caller's root / cut restriction (a no-op without an entry). kpi_long's group keys label each row's
    root x cut (kpi_long._stack_roots_and_cuts), so every group reads exactly one root x cut's rows; joins on
    them are plain equi-joins (a NULL cut value matches nothing) except the pair counts' (_join_null_safe).

    blocked_scope.metrics: scoped_daily / dc_daily keep blocked days, flagged is_blocked; a metric in the
    list reads only the unblocked rows (_read_only / _day_stock), the others every row. The in-stock frames
    and lost sales's sales denominator dropped them when built.

    goods_in_transit.inventory_metrics: scoped_daily / dc_daily also carry GIT-only days (has_daily_row /
    has_inventory_row False) and the on-hand + goods in transit stock (_WITH_GIT). Sales and
    weighted-instock's sales weights read the real rows only; an inventory metric reads on-hand on its
    real-row days, or on-hand + goods in transit on every day when it is named.

    The rows are product level (collapse_frames; store level for a frame whose filters read a store column),
    with is_blocked / has_daily_row / has_inventory_row and every other column kept, so each gate and filter
    reads them as it would the store rows.
    """
    group_keys = list(group_keys)
    keys = [period_col] + group_keys
    git_metrics = ctx.settings["GOODS_IN_TRANSIT"]["inventory_metrics"]
    daily_scoped = frames["scoped_daily"]
    inst = frames["inst_data"]
    dc_daily = frames["dc_daily"]
    dc_inst = frames["dc_inst"]
    # Real daily-data rows (no GIT-only days): the sales metrics' rows.
    real_daily = daily_scoped.filter(F.col("has_daily_row"))

    sales_pop = apply_group_population_filter(_sales_rows(ctx, real_daily), "sales", ctx.settings)
    sales = sales_pop.groupBy(*keys).agg(
        F.sum(_read_only(ctx, "total_inventory", F.col("inventory"))).alias("total_inventory"),
        F.sum(_read_only(ctx, "total_sales_quantity", F.col("sales_quantity"))).alias("total_sales_quantity"),
        F.sum(_read_only(ctx, "total_sales_revenue", F.col("sales_revenue"))).alias("total_sales_revenue"),
        _per_unit(ctx, "AUR", "sales_revenue").alias("AUR"),
        _per_unit(ctx, "AUC", "sales_cost").alias("AUC"),
        F.countDistinct(_read_only(ctx, "distinct_product_count", F.col("product_id"))).alias(
            "distinct_product_count"
        ),
    )
    # Store and pair counts over the same rows, from sales_pairs; every sales group has a row there.
    pairs_pop = apply_group_population_filter(_sales_rows(ctx, frames["sales_pairs"]), "sales", ctx.settings)
    pair_counts = pairs_pop.groupBy(*keys).agg(
        F.countDistinct(_read_only(ctx, "distinct_store_count", F.col("store_id"))).alias("distinct_store_count"),
        F.countDistinct(
            _read_only(ctx, "distinct_pair_count", F.col("product_id")),
            _read_only(ctx, "distinct_pair_count", F.col("store_id")),
        ).alias("distinct_pair_count"),
    )
    sales = _join_null_safe(sales, pair_counts, keys)
    if "total_inventory" in git_metrics:
        total_inventory_with_git = (
            apply_group_population_filter(daily_scoped, "sales", ctx.settings)
            .groupBy(*keys)
            .agg(
                F.sum(_read_only(ctx, "total_inventory", _stock("units", True))).alias("total_inventory")
            )
        )
        sales = sales.drop("total_inventory").join(total_inventory_with_git, on=keys, how="left")

    # WOS family: each product-week's average daily inventory is weighted by week_share = week_days / 7, which
    # is 1 except for a week report_end="latest_day" cuts mid-week, so a part week's inventory counts as the
    # fraction of a week it covers. Each _WOS_METRICS entry has its own "<metric>_..." columns, on the rows
    # its own blocked_scope.metrics gate reads.
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
    # WOS_DC / WOS_TOTAL: DC inventory at the same product-week grain, left-joined; a week without DC
    # inventory counts 0.
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

    store_day = _store_day_stock(ctx, daily_scoped, keys)
    mean_stock = store_day["mean_stock"].groupBy(*keys).agg(
        *[_day_avg(m).alias(m) for m, _ in _STORE_DAY_STOCK["mean_stock"]]
    )

    # dc_mean_stock / total_mean_stock: per-day averages like mean_stock (not the WOS ratio). total_mean_stock
    # is averaged over the store days; named in goods_in_transit.inventory_metrics, both its store and DC
    # part count goods in transit.
    dc_inv_pop = apply_group_population_filter(dc_daily, "dc_inventory", ctx.settings)
    dc_day = dc_inv_pop.groupBy(*keys, "date").agg(
        *_day_stock(ctx, "dc_mean_stock", "units", "has_inventory_row"),
        *_day_stock(ctx, "total_mean_stock", "units", "has_inventory_row"),
    )
    dc_mean_stock = dc_day.groupBy(*keys).agg(_day_avg("dc_mean_stock").alias("dc_mean_stock"))
    total_mean_stock = (
        store_day["dc_inventory"].join(
            dc_day.select(*keys, "date", F.col("total_mean_stock_day").alias("dc_day")),
            on=[*keys, "date"],
            how="left",
        )
        .withColumn("total_mean_stock_day", F.col("total_mean_stock_day") + F.coalesce(F.col("dc_day"), F.lit(0.0)))
        .groupBy(*keys)
        .agg(_day_avg("total_mean_stock").alias("total_mean_stock"))
    )

    turnover_pop = apply_group_population_filter(real_daily, "turnover", ctx.settings)
    turnover_mean_stock = (
        store_day["turnover"].groupBy(*keys).agg(_day_avg("inventory_turnover_rate").alias("mean_stock"))
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

    # dc_inst is empty when dc_instock is disabled, leaving dc_in_stock_rate null.
    dc_instock_pop = apply_group_population_filter(dc_inst, "dc_instock", ctx.settings)
    dc_instock = dc_instock_pop.groupBy(*keys).agg(
        F.greatest(F.lit(0.0), F.sum("dc_stocked_days") / F.sum("dc_available_days")).alias("dc_in_stock_rate")
    )

    # Sales-weighted in-stock rate: the week's in-stock rate weighted by its sales. Both sides apply the
    # "weighted_instock" population; the in-stock side is in_stock_rate's frame, and the sales weights drop
    # blocked days when weighted_instock_rate is in blocked_scope.metrics.
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
    group_keys: Sequence[str],
) -> pd.DataFrame:
    """Spark KPI aggregation of collapse_frames' frames joined with lost_sales_pct; returns a pandas table."""
    group_keys = list(group_keys)
    keys = [period_col] + group_keys
    kpis = compute_kpis(ctx, frames, period_col, group_keys)
    lost_base_pop = apply_group_population_filter(frames["lost_base"], "lost_sales", ctx.settings)
    lost_pct = (
        lost_base_pop
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

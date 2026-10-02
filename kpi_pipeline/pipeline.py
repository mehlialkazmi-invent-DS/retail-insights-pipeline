"""Pipeline input frames for a given scope."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import (
    DEFAULT_LOST_SALES_COLUMN_MAP,
    apply_input_filters,
    get_daily_data_raw,
    get_instock_daily_raw,
    get_inventory_warehouse_raw,
    get_item_family_raw,
    read_goods_in_transit_source,
    read_instock_source,
    read_lost_sales_source,
    read_speed_cluster_source,
    rename_column_or_fail,
)


def _enrich_weekly_with_time_grain(ctx: KPIContext, weekly_pair: DataFrame) -> DataFrame:
    fw_cols = ["week_start_date", "week_end_date", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month"]
    if ctx.settings["USE_FISCAL_CALENDAR"]:
        return weekly_pair.join(broadcast(ctx.fiscal_week.select(*fw_cols)), on="week_start_date", how="inner")
    if "_ls_year" in weekly_pair.columns and "_ls_week" in weekly_pair.columns:
        enriched = (
            weekly_pair.withColumn("Year", F.col("_ls_year"))
            .withColumn("Week", F.col("_ls_week"))
            .drop("_ls_year", "_ls_week")
        )
    else:
        enriched = weekly_pair.join(
            broadcast(ctx.fiscal_cal.select(F.col("date").alias("week_start_date"), "Year", "Week")),
            on="week_start_date",
            how="inner",
        )
    return enriched.drop("week_start_date").join(broadcast(ctx.fiscal_week.select(*fw_cols)), on=["Year", "Week"], how="inner")


def _aggregate_lost_sales_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """Aggregate ONE raw lost-sales model to (product_id, store_id, week_start_date) grain.

    lost_sales/in_stock/total_days source column names come from LOST_SALES_COLUMN_MAP
    (see config.py's lost_sales_source). in_stock/total_days are skipped here entirely
    when INSTOCK_SOURCE_ENABLED or INSTOCK_DAILY is enabled — they come from the separate
    instock_source table (see read_instock_weekly) or the daily builder (see build_instock_daily).

    Item-family-rolled to parent product_id right before aggregation when
    ITEM_FAMILY_ROLLUP["lost_sales"] is True (see config.py) — OFF by default, since
    report_dfu already does its own supersede substitution upstream and a second rollup here
    would likely be a no-op; available as an opt-in safety net. The groupBy below then combines
    any child+parent rows landing in the same (product_id[, store_id], week_start_date) bucket.
    """
    col_map = ctx.settings.get("LOST_SALES_COLUMN_MAP") or DEFAULT_LOST_SALES_COLUMN_MAP
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
    if ctx.settings["ITEM_FAMILY_ROLLUP"]["lost_sales"]:
        filtered = _roll_to_item_family_parent(filtered, ctx)
    agg_exprs = [F.sum(F.col(col_map["lost_sales_col"]).cast("double")).alias("lost_sales")]
    if not (ctx.settings.get("INSTOCK_SOURCE_ENABLED", False) or ctx.settings["INSTOCK_DAILY"]["enabled"]):
        agg_exprs.append(F.sum(F.col(col_map["in_stock_col"]).cast("double")).alias("in_stock_days"))
        agg_exprs.append(F.sum(F.col(col_map["total_days_col"]).cast("double")).alias("total_days"))
    ls_native_week = not ctx.settings["USE_FISCAL_CALENDAR"] and "week" in raw.columns
    if ls_native_week:
        # Keep the native fiscal week number, but derive Year from week_start_date
        # (calendar year) rather than the source 'year' column, which can carry the ISO
        # week-year (late-December weeks labelled as the next year). See fiscal.py.
        agg_exprs.append(F.first(F.col("week").cast("int"), ignorenulls=True).alias("_ls_week"))
    # Grouped without store_id when the source has none (store_col=None in
    # LOST_SALES_COLUMN_MAP) -- CAUTION (see config.py's lost_sales_source comment): lost_sales
    # is an absolute count, so a pair-week without store_id must never be broadcast across a
    # product's scoped stores, which would OVER-COUNT if later summed across stores -- see
    # build_pipeline_frames, which collapses scope to this grain instead.
    group_keys = ["product_id", "week_start_date"]
    if "store_id" in raw.columns:
        group_keys.insert(1, "store_id")
    return filtered.groupBy(*group_keys).agg(*agg_exprs)


def _aggregate_instock_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """Aggregate the standalone instock_source table to (product_id[, store_id], week_start_date).

    ``raw`` is read_instock_source's output, already on canonical in_stock/total_days columns
    (including any fallback_sources already appended -- see read_instock_source).

    Grouped without store_id when the source has none (store_col=None in
    INSTOCK_SOURCE_COLUMN_MAP, e.g. reporting_inv_fc_dfu/report_dfu) -- build_pipeline_frames
    then restricts it to scope at product x week, never fanning it out across stores.

    Item-family-rolled to parent product_id right before aggregation when
    ITEM_FAMILY_ROLLUP["lost_sales"] is True (see config.py) -- same toggle and reasoning as
    _aggregate_lost_sales_pairweek's own rollup, applied here for instock_source's own rows.
    """
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
    if ctx.settings["ITEM_FAMILY_ROLLUP"]["lost_sales"]:
        filtered = _roll_to_item_family_parent(filtered, ctx)
    agg_exprs = [
        F.sum(F.col("in_stock").cast("double")).alias("in_stock_days"),
        F.sum(F.col("total_days").cast("double")).alias("total_days"),
    ]
    group_keys = ["product_id", "week_start_date"]
    if "store_id" in raw.columns:
        group_keys.insert(1, "store_id")
    return filtered.groupBy(*group_keys).agg(*agg_exprs)


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

    instock_source.enabled=True: in_stock_days/total_days are not read here at all -- they come
    from read_instock_weekly, independently of the lost-sales rows. instock_daily.enabled=True skips
    them too -- in-stock then comes from build_instock_daily.
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
        # Both sides use the SAME LOST_SALES_COLUMN_MAP (store_col included), so fast/slow
        # store_id presence is expected to agree -- only fast_raw is checked, mirroring
        # native_week's own fast-only check just above.
        has_store = "store_id" in fast_raw.columns
        join_keys = ["product_id", "store_id", "week_start_date"] if has_store else ["product_id", "week_start_date"]

        fast_cols = [
            "product_id", "week_start_date",
            F.col("lost_sales").alias("lost_sales_fast"),
            F.col("in_stock_days").alias("in_stock_days_fast"),
            F.col("total_days").alias("total_days_fast"),
        ]
        slow_cols = [
            "product_id", "week_start_date",
            F.col("lost_sales").alias("lost_sales_slow"),
            F.col("in_stock_days").alias("in_stock_days_slow"),
            F.col("total_days").alias("total_days_slow"),
        ]
        if has_store:
            fast_cols.insert(1, "store_id")
            slow_cols.insert(1, "store_id")
        if native_week:
            fast_cols.append(F.col("_ls_week").alias("_ls_week_fast"))
            slow_cols.append(F.col("_ls_week").alias("_ls_week_slow"))
        fast = _aggregate_lost_sales_pairweek(ctx, fast_raw, start, end).select(*fast_cols)
        slow = _aggregate_lost_sales_pairweek(ctx, slow_raw, start, end).select(*slow_cols)

        cluster = read_speed_cluster_source(ctx.spark, s, quiet=True)
        merged = (
            fast.join(slow, on=join_keys, how="fullouter")
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
        select_cols = ["product_id", "week_start_date", "lost_sales", "in_stock_days", "total_days"]
        if has_store:
            select_cols.insert(1, "store_id")
        if native_week:
            select_cols.append("_ls_week")
        deduped = merged.select(*select_cols)
        if native_week:
            deduped = deduped.withColumn("_ls_year", F.year("week_start_date"))

    ctx.lost_sales_weekly_base = _enrich_weekly_with_time_grain(ctx, deduped).withColumn(
        "fiscal_week_days",
        F.datediff(F.col("week_end_date"), F.col("week_start_date")) + 1,
    ).cache()
    return ctx.lost_sales_weekly_base


def read_instock_weekly(ctx: KPIContext) -> DataFrame:
    """Weekly in-stock aggregates from instock_source for the report window (cached per run).

    Only called when INSTOCK_SOURCE_ENABLED. Independent of the lost-sales rows: every
    (product[, store], week) the in-stock table has is kept, whether or not lost_sales_source
    has a row for it. Scope restriction happens in build_pipeline_frames.
    """
    if ctx.instock_weekly_base is not None:
        return ctx.instock_weekly_base

    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    raw = read_instock_source(ctx.spark, s, quiet=True)
    ctx.instock_weekly_base = _enrich_weekly_with_time_grain(
        ctx, _aggregate_instock_pairweek(ctx, raw, start, end)
    ).cache()
    return ctx.instock_weekly_base


def build_scoped_daily(ctx: KPIContext, scope_core: DataFrame, scope_pairs_in: DataFrame, has_store: bool) -> DataFrame:
    """Daily sales/inventory for scoped pairs, with product cost/price and fiscal week attributes.

    Days removed by blocked scope (ctx.blocked_days) are dropped here, so every daily-data metric
    (sales, inventory, WOS, turnover, mean stock) excludes them.
    """
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
    )
    daily = rename_column_or_fail(daily, date_col, "date", "fiscal_calendar.daily_time_columns.date")
    daily = daily.filter(F.col("date").between(F.lit(start), F.lit(end)))
    if ctx.blocked_days is not None:
        daily = daily.join(ctx.blocked_days, on=["product_id", "store_id", "date"], how="left_anti")
    if has_store:
        daily = daily.join(scope_pairs_in, on=["product_id", "store_id"], how="left_semi")
    if not s["USE_FISCAL_CALENDAR"]:
        # Year = calendar year of `date`; Week = native fiscal week column. Avoids the
        # source 'year' column's ISO week-year mislabel (Dec -> next year). See fiscal.py.
        daily = daily.withColumn("Year", F.year(F.col("date")))
        daily = rename_column_or_fail(daily, week_col, "Week", "fiscal_calendar.daily_time_columns.week")
        daily = daily.withColumn("Week", F.col("Week").cast("int"))
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
            broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")),
            on=["Year", "Week"],
            how="inner",
        )
    )


def build_instock_daily(ctx: KPIContext, scope_core: DataFrame, scope_pairs: DataFrame) -> DataFrame:
    """Daily in-stock frame (instock_daily.enabled): same shape as the weekly inst_data, built from
    noob/daily-data instead of a weekly source.

    Per scope pair (after instock_daily.input_filters, which may reference product_id / store_id
    only), store-days run from the pair's count start to the report window's end. Every scoped pair
    counts, scope_adjustments additions included (they have no scope_start, so they count from their
    first daily row, and receive no blocks):
      * count start per instock_daily.count_start: "first_daily_row" (first daily-data row from
        history_start), "scope_start" (operation-scope start date), or "earliest" of the two;
        clipped to the window start. Pairs without a daily-data row are dropped when
        require_daily_data. Days without a daily-data row count as out of stock.
      * minus blocked days (ctx.blocked_days) and, when usable_only, days with usable != 1.
    An in-stock day is a usable day with inventory > 0 or, when git_date_shift_days is set, a
    day with store goods-in-transit quantity > 0 (rolled to the family main; snapshot D+1
    describes the end of day D, hence the shift). The two are united per day, never summed. Blocked and
    unusable days leave the in-stock days too.

    Counted per pair x fiscal week as stocked_pairs / available_days, so metrics.compute_kpis and
    population_filters work unchanged. Day counts per pair-week come from week bounds, not from
    exploding every pair-day.
    """
    s = ctx.settings
    cfg = s["INSTOCK_DAILY"]
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    pair_keys = ["product_id", "store_id"]
    day_keys = pair_keys + ["date"]
    week_keys = pair_keys + ["Year", "Week"]
    blocked = ctx.blocked_days
    cal = broadcast(ctx.fiscal_cal.select("date", "Year", "Week"))
    fw = broadcast(
        ctx.fiscal_week.select(
            "Year", "Week", "Year_Week", "week_start_date", "week_end_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month"
        )
    )

    pairs = apply_input_filters(scope_pairs, cfg["input_filters"], "instock_daily.input_filters")
    daily = get_instock_daily_raw(ctx).join(pairs, on=pair_keys, how="left_semi")
    if blocked is not None:
        daily = daily.join(blocked, on=day_keys, how="left_anti")
    daily = daily.cache()
    latest_daily_date = daily.agg(F.max("date")).first()[0]
    if latest_daily_date is None or latest_daily_date < end:
        raise ValueError(
            f"instock_daily: daily-data latest date {latest_daily_date} (scope pairs) is before the report "
            f"end {end}; days after it would count as out of stock. Wait for daily-data to reach the report "
            "end, or set reporting_window.as_of_date earlier."
        )

    first_day = daily.groupBy(*pair_keys).agg(F.min("date").alias("first_date"))
    pair_start = pairs.join(first_day, on=pair_keys, how="left")
    if ctx.operation_scope_pairs is not None:
        pair_start = pair_start.join(
            ctx.operation_scope_pairs.select(*pair_keys, "scope_start"), on=pair_keys, how="left"
        )
    else:
        pair_start = pair_start.withColumn("scope_start", F.lit(None).cast("date"))
    if cfg["require_daily_data"]:
        pair_start = pair_start.filter(F.col("first_date").isNotNull())
    if cfg["count_start"] == "first_daily_row":
        start_from = F.col("first_date")
    elif cfg["count_start"] == "scope_start":
        start_from = F.coalesce(F.col("scope_start"), F.col("first_date"))
    else:
        start_from = F.least(F.col("scope_start"), F.col("first_date"))
    pair_start = (
        pair_start.withColumn("start_from", start_from)
        .filter(F.col("start_from").isNotNull())
        .withColumn("count_from", F.greatest(F.col("start_from"), F.lit(start)))
        .filter(F.col("count_from") <= F.lit(end))
        .select(*pair_keys, "count_from")
        .cache()
    )

    def pair_week_count(days: DataFrame, name: str) -> DataFrame:
        return days.join(cal, on="date", how="inner").groupBy(*week_keys).agg(F.count(F.lit(1)).alias(name))

    # Only days from each pair's count_from on: the daily rows reach back to history_start, and a
    # pair's count_from can fall inside a week (window start, or a scope start before first stock).
    counted_daily = daily.join(pair_start, on=pair_keys, how="inner").filter(F.col("date") >= F.col("count_from"))

    unusable_days = None
    if cfg["usable_only"]:
        unusable_days = counted_daily.filter(~F.col("is_usable")).select(*day_keys).distinct()

    oh_days = counted_daily.filter(F.col("inventory") > 0)
    if cfg["usable_only"]:
        oh_days = oh_days.filter(F.col("is_usable"))
    in_stock_days = oh_days.select(*day_keys).distinct()
    shift = cfg["git_date_shift_days"]
    if shift is not None:
        git_days = (
            _goods_in_transit_days(ctx, 0, "store_id", shift)
            .join(pair_start, on=pair_keys, how="inner")
            .filter(F.col("date") >= F.col("count_from"))
            .select(*day_keys)
            .distinct()
        )
        if blocked is not None:
            git_days = git_days.join(blocked, on=day_keys, how="left_anti")
        if unusable_days is not None:
            git_days = git_days.join(unusable_days, on=day_keys, how="left_anti")
        in_stock_days = in_stock_days.unionByName(git_days).distinct()

    store_days = (
        pair_start.join(fw, F.col("week_end_date") >= F.col("count_from"))
        .withColumn(
            "counted_days",
            F.datediff(
                F.least(F.col("week_end_date"), F.lit(end)),
                F.greatest(F.col("week_start_date"), F.col("count_from"), F.lit(start)),
            )
            + 1,
        )
        .filter(F.col("counted_days") > 0)
    )
    removed_days = F.lit(0)
    if blocked is not None:
        blocked_in_count = blocked.join(pair_start, on=pair_keys, how="inner").filter(
            F.col("date") >= F.col("count_from")
        )
        store_days = store_days.join(
            pair_week_count(blocked_in_count.select(*day_keys), "n_blocked"), on=week_keys, how="left"
        )
        removed_days = removed_days + F.coalesce(F.col("n_blocked"), F.lit(0))
    if unusable_days is not None:
        store_days = store_days.join(pair_week_count(unusable_days, "n_unusable"), on=week_keys, how="left")
        removed_days = removed_days + F.coalesce(F.col("n_unusable"), F.lit(0))

    return (
        store_days.join(pair_week_count(in_stock_days, "n_in_stock"), on=week_keys, how="left")
        .withColumn("available_days", F.col("counted_days") - removed_days)
        .withColumn("stocked_pairs", F.coalesce(F.col("n_in_stock"), F.lit(0)))
        .filter(F.col("available_days") > 0)
        .join(scope_core, on=ctx.scope_keys, how="left_semi")
        .select(
            *pair_keys, "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month",
            "stocked_pairs", "available_days",
        )
        .join(ctx.product_dims, on="product_id", how="left")
    )


def _goods_in_transit_days(ctx: KPIContext, destination_type: int, location_col: str, shift: int) -> DataFrame:
    """Distinct (product_id, <location_col>, date) days with goods in transit (quantity > 0) to one
    destination type (0 = store, 1 = warehouse), rolled to the family main, for the report window.
    A snapshot dated D - shift describes the end of day D (shift -1: snapshot D+1 -> day D).
    """
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    git = (
        read_goods_in_transit_source(ctx.spark, ctx.settings)
        .filter(F.col("date").between(F.date_sub(F.lit(start), shift), F.date_sub(F.lit(end), shift)))
        .filter((F.col("destination_type") == destination_type) & (F.col("quantity") > 0))
        .select(
            "product_id",
            F.col("destination_id").alias(location_col),
            F.date_add(F.col("date"), shift).alias("date"),
        )
    )
    return _roll_to_item_family_parent(git, ctx).distinct()


def _roll_to_item_family_parent(df: DataFrame, ctx: KPIContext) -> DataFrame:
    """Maps product_id -> coalesce(parent_id, product_id). scope_core/defined_scope is already
    parent-rolled, so DC frames must be rolled too or child-id inventory is silently dropped.
    """
    child_to_parent = broadcast(
        get_item_family_raw(ctx).filter(~F.col("is_main")).select("product_id", "parent_id")
    )
    return (
        df.join(child_to_parent, on="product_id", how="left")
        .withColumn("product_id", F.coalesce(F.col("parent_id"), F.col("product_id")))
        .drop("parent_id")
    )


def _get_inventory_warehouse_parent_rolled(ctx: KPIContext) -> DataFrame:
    """inventory_warehouse for the report window, item-family-rolled to parent product_id
    (gated by ITEM_FAMILY_ROLLUP["inventory_warehouse"], default True -- preserves today's
    always-on behaviour) and re-aggregated so children sum rather than duplicate. Cached on ctx:
    scope-independent, so both DC frames and every scope variant share one materialization.
    """
    if ctx.inventory_warehouse_rolled is not None:
        return ctx.inventory_warehouse_rolled

    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    base = (
        get_inventory_warehouse_raw(ctx)
        .select("product_id", "warehouse_id", "date", "inventory")
        .withColumn("date", F.to_date(F.col("date")))
        .filter(F.col("date").between(F.lit(start), F.lit(end)))
    )
    if s["ITEM_FAMILY_ROLLUP"]["inventory_warehouse"]:
        base = _roll_to_item_family_parent(base, ctx)
    ctx.inventory_warehouse_rolled = (
        base.groupBy("product_id", "warehouse_id", "date")
        .agg(F.sum("inventory").alias("inventory"))
        .cache()
    )
    return ctx.inventory_warehouse_rolled


def build_dc_daily(ctx: KPIContext, scope_core: DataFrame) -> DataFrame:
    """Daily DC (warehouse) inventory for the in-scope product population.

    Unlike build_scoped_daily, there is no store join/semi-join here -- DC data has no
    store_id at all. Restriction is on (product_id, Year, Week): left-semi against
    scope_core's own in-scope product-weeks, the SAME population every other scoped frame
    for this scope/root is restricted to (not an independently-scoped universe).

    Year/Week must be attached to a DC row BEFORE the scope semi-join, not after -- scope_core
    always carries Year/Week (ctx.scope_keys includes it for every grain), and for
    product_store_week grain scope membership genuinely varies by week. Restricting on
    product_id alone (dropping Year/Week first) would keep a product's DC inventory for
    weeks it fell out of scope, since DC data itself has no notion of scope weeks.

    Reads inventory_warehouse item-family-rolled to parent product_id (matching scope_core's own
    id space), which changes dc_mean_stock/WOS_DC/WOS_TOTAL for families with inventory split
    across old and current item codes.
    """
    scope_product_weeks = scope_core.select("product_id", "Year", "Week").distinct()

    dc = (
        _get_inventory_warehouse_parent_rolled(ctx)
        .join(broadcast(ctx.fiscal_cal.select("date", "Year", "Week")), on="date", how="inner")
        .join(scope_product_weeks, on=["product_id", "Year", "Week"], how="left_semi")
    )
    return dc.join(ctx.product_dims, on="product_id", how="left").join(
        broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")),
        on=["Year", "Week"],
        how="inner",
    )


def build_dc_inst(ctx: KPIContext, scope_core: DataFrame) -> DataFrame:
    """DC in-stock rate frame: dc_stocked_days / dc_available_days over a per-pair daily grid.

    The grid is inventory-derived: each (product_id, warehouse_id) pair runs from its own first
    inventory_warehouse row to the report window's end, missing days 0-filled and counted as
    stockouts. That is the same bound the store-level in-stock denominator uses, so the two
    series stay on one definition (see README's "dc_instock" section). A day is stocked when
    inventory > stock_threshold or, with dc_instock.git_date_shift_days, goods are in transit to the
    DC. DC blocked days (ctx.dc_blocked_days) leave the grid.
    """
    s = ctx.settings
    if not s.get("DC_INSTOCK_ENABLED", False):
        # Disabled: empty, correctly-shaped frame so dc_in_stock_rate stays a literal-null column.
        empty_base = (
            scope_core.select("product_id", "Year", "Week")
            .limit(0)
            .withColumn("warehouse_id", F.lit(None).cast("int"))
            .withColumn("dc_stocked_days", F.lit(None).cast("long"))
            .withColumn("dc_available_days", F.lit(None).cast("long"))
        )
        return empty_base.join(ctx.product_dims, on="product_id", how="left").join(
            broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")),
            on=["Year", "Week"],
            how="inner",
        )

    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    threshold = s["DC_INSTOCK_STOCK_THRESHOLD"]

    # Each pair's window starts at its own first inventory_warehouse row. inventory_warehouse
    # carries a row whenever a pair holds stock, so that first row is the same "first day this
    # pair has any history" signal daily_data_expanded uses to bound the store-level in-stock
    # denominator. Trade-off: a pair ranged at a DC but never once stocked has no row anywhere,
    # so it is absent from the metric rather than reading 0%.
    inv = _get_inventory_warehouse_parent_rolled(ctx)
    pairs = inv.groupBy("product_id", "warehouse_id").agg(F.min("date").alias("first_stocked_date"))
    cal = broadcast(
        ctx.fiscal_cal.select("date", "Year", "Week").filter(F.col("date").between(F.lit(start), F.lit(end)))
    )
    scope_product_weeks = scope_core.select("product_id", "Year", "Week").distinct()

    # Mirrors daily_data_expanded's own F.explode(F.sequence(min_date, max_date)) per-pair grid
    # (customer-analysis-tbretail's 05_future_visibility_data_prep.py). inv is already filtered to
    # the report window, so first_stocked_date can never precede it and needs no further clamping.
    # Restrict to scope_core before joining inventory, so the join only runs over in-scope rows.
    grid = (
        pairs.withColumn(
            "date",
            F.explode(F.sequence(F.col("first_stocked_date"), F.lit(end), F.expr("interval 1 day"))),
        )
        .drop("first_stocked_date")
        .join(cal, on="date", how="inner")
        .join(scope_product_weeks, on=["product_id", "Year", "Week"], how="left_semi")
        .join(inv, on=["product_id", "warehouse_id", "date"], how="left")
        .withColumn("inventory", F.coalesce(F.col("inventory"), F.lit(0.0)))
    )
    day_keys = ["product_id", "warehouse_id", "date"]
    # DC blocked days (dc_instock.blocked_scope_solution_id) leave both stocked and available days.
    if ctx.dc_blocked_days is not None:
        grid = grid.join(ctx.dc_blocked_days, on=day_keys, how="left_anti")
    stocked = F.col("inventory") > F.lit(threshold)
    # dc_instock.git_date_shift_days: a day with goods in transit to the DC also counts as stocked.
    shift = s["DC_INSTOCK_GIT_DATE_SHIFT_DAYS"]
    if shift is not None:
        git_days = _goods_in_transit_days(ctx, 1, "warehouse_id", shift).withColumn("has_git", F.lit(True))
        grid = grid.join(git_days, on=day_keys, how="left")
        stocked = stocked | F.col("has_git").isNotNull()

    dc_inst = grid.groupBy("product_id", "warehouse_id", "Year", "Week").agg(
        F.sum(stocked.cast("int")).alias("dc_stocked_days"),
        F.count(F.lit(1)).alias("dc_available_days"),
    )
    return dc_inst.join(ctx.product_dims, on="product_id", how="left").join(
        broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")),
        on=["Year", "Week"],
        how="inner",
    )


def build_pipeline_frames(ctx: KPIContext, scope_in: DataFrame) -> Dict[str, DataFrame]:
    """Build scoped_daily, inst_data, lost_base, and scope helper frames for one scope variant.

    Four independent metric families, each restricted to scope on its own: daily-data metrics
    (scoped_daily), in-stock (inst_data), lost sales (lost_base), and DC (dc_daily/dc_inst).
    They only meet at the final per-period aggregate join (metrics.build_kpi_table).

    has_store is read from ctx.scope_keys (set once in scope.build_defined_scope from
    defined_scope.grain), not re-derived from scope_in.columns -- every scope variant this is
    called with (hybrid_scope_keys, defined_scope_keys, score_only_scope_keys) is already built
    to exactly ctx.scope_keys's columns, so ctx.scope_keys is the authoritative source. Failing
    loudly here on a genuine mismatch is far more useful than silently falling back to the
    store-less/product-week path and surfacing a confusing UNRESOLVED_COLUMN several calls later.
    """
    scope_keys = ctx.scope_keys
    has_store = "store_id" in scope_keys
    if has_store and "store_id" not in scope_in.columns:
        raise ValueError(
            "ctx.scope_keys expects store_id (defined_scope.grain is product_store or "
            "product_store_week) but the scope frame passed to build_pipeline_frames doesn't "
            f"have it -- columns: {sorted(scope_in.columns)}"
        )
    scope_core = scope_in.select(*scope_keys).distinct().cache()

    lost_sales_raw = read_lost_sales_weekly(ctx)
    # Collapse scope to the source's own grain, never fan the source out to scope's: drop store_id
    # from the join keys when lost_sales_raw has no per-store dimension (e.g. report_dfu,
    # store_col=None), then left_semi against the collapsed scope. Joining scope_core's stores onto
    # a store-less row instead would repeat lost_sales -- an ABSOLUTE count -- once per scoped
    # store, inflating every later sum across stores by the store-count factor.
    ls_keys = [k for k in scope_keys if k != "store_id" or "store_id" in lost_sales_raw.columns]
    lost_sales_weekly = lost_sales_raw.join(
        scope_core.select(*ls_keys).distinct(), on=ls_keys, how="left_semi"
    ).cache()

    # In-stock is its own metric family: with instock_daily enabled it is built from daily-data over
    # the scope pairs (see build_instock_daily, below, once scope_pairs exists). With instock_source
    # enabled it is read and scope-restricted on its own, at its own grain, exactly like lost-sales
    # above -- never joined onto the lost-sales rows. Otherwise it comes from lost_sales_source's
    # own in_stock/total_days columns, i.e. the same rows as lost_sales_weekly.
    instock_daily_enabled = ctx.settings["INSTOCK_DAILY"]["enabled"]
    instock_source_enabled = ctx.settings.get("INSTOCK_SOURCE_ENABLED", False)
    if instock_daily_enabled:
        instock_weekly = None
    elif instock_source_enabled:
        instock_raw = read_instock_weekly(ctx)
        inst_keys = [k for k in scope_keys if k != "store_id" or "store_id" in instock_raw.columns]
        instock_weekly = instock_raw.join(
            scope_core.select(*inst_keys).distinct(), on=inst_keys, how="left_semi"
        )
    else:
        instock_weekly = lost_sales_weekly

    # ls_has_store: whether lost_sales_weekly has its own store_id. Purely a property of
    # lost_sales_source.store_col -- the semi-join above only filters rows, it never attaches a
    # store dimension the source lacked -- so it is independent of scope's grain in BOTH
    # directions: a store-ful source under product grain keeps its own stores (store granularity
    # comes FROM lost-sales, scope itself has none), and a store-less source under product_store
    # grain stays store-less (see the else branches of scope_pair_weeks/weekly_sales_for_lost/
    # lost_base below). Neither is an error: nothing downstream reads a store dimension off these
    # frames -- distinct_store_count/distinct_pair_count come from daily-data, not from here.
    ls_has_store = "store_id" in lost_sales_weekly.columns
    if has_store:
        scope_pair_weeks = scope_core.select("product_id", "store_id", "Year", "Week").distinct().cache()
        scope_pairs = scope_core.select("product_id", "store_id").distinct().cache()
    elif ls_has_store:
        scope_pair_weeks = lost_sales_weekly.select("product_id", "store_id", "Year", "Week").distinct().cache()
        scope_pairs = lost_sales_weekly.select("product_id", "store_id").distinct().cache()
    else:
        scope_pair_weeks = lost_sales_weekly.select("product_id", "Year", "Week").distinct().cache()
        scope_pairs = lost_sales_weekly.select("product_id").distinct().cache()

    if instock_daily_enabled:
        inst_data = build_instock_daily(ctx, scope_core, scope_pairs).cache()
    else:
        # Fall back to the fiscal week's day-count only for lost_sales_source's own in_stock/total_days
        # (e.g. a null in the lost-sales table itself). instock_source's total_days is taken as-is --
        # padding a null to a full week would deflate in_stock_rate/weighted_instock_rate.
        available_days_expr = (
            F.col("total_days")
            if instock_source_enabled
            else F.coalesce(F.col("total_days"), F.col("fiscal_week_days"))
        )
        inst_select_cols = ["product_id", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month"]
        if "store_id" in instock_weekly.columns:
            inst_select_cols.insert(1, "store_id")
        inst_data = (
            instock_weekly.select(
                *inst_select_cols,
                F.col("in_stock_days").alias("stocked_pairs"),
                available_days_expr.alias("available_days"),
            )
            .filter(F.col("available_days") > 0)
            .join(ctx.product_dims, on="product_id", how="left")
        ).cache()

    scoped_daily = build_scoped_daily(ctx, scope_core, scope_pairs, has_store).cache()
    dc_daily = build_dc_daily(ctx, scope_core).cache()
    dc_inst = build_dc_inst(ctx, scope_core).cache()
    # lost_sales_source.sales_filter narrows ONLY the sales half of lost_sales_pct's denominator,
    # for when the lost-sales table covers a narrower population than daily_data (e.g. a model
    # that excludes e-commerce, whose numerator would otherwise be divided by a denominator that
    # still carries ecom sales). .get() so a customer config vendoring an older copy of
    # materialize() still loads -- same reason DEFAULT_LOST_SALES_COLUMN_MAP exists in inputs.py.
    ls_sales_filter = ctx.settings.get("LOST_SALES_SALES_FILTER") or []
    daily_for_lost = apply_input_filters(
        scoped_daily, ls_sales_filter, "lost_sales_source.sales_filter", quiet=not ls_sales_filter
    )
    weekly_pair = daily_for_lost.groupBy("product_id", "store_id", "Year", "Week").agg(
        F.sum("sales_quantity").alias("weekly_sales")
    )
    lost_base_keys = ["product_id", "Year", "Week"]
    lost_base_select_cols = ["product_id", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month", "lost_sales"]
    if ls_has_store:
        lost_base_keys.insert(1, "store_id")
        lost_base_select_cols.insert(1, "store_id")
        weekly_sales_for_lost = (
            weekly_pair.join(scope_pair_weeks, on=lost_base_keys, how="left_semi")
            .select(*lost_base_keys, F.col("weekly_sales").alias("sales_quantity_weekly"))
        )
    else:
        # No store dimension anywhere (scope AND lost_sales_source both store-less): roll
        # weekly_pair (still per-store, from daily_for_lost) UP to product-week first --
        # summed across every store selling the product, since there's no per-store lost_sales
        # figure to match against individually -- then restrict to the (product, week) combos
        # lost_sales_weekly actually covers, same as the has-store left_semi above.
        weekly_sales_for_lost = (
            weekly_pair.groupBy("product_id", "Year", "Week")
            .agg(F.sum("weekly_sales").alias("sales_quantity_weekly"))
            .join(scope_pair_weeks, on=lost_base_keys, how="left_semi")
        )
    lost_base = (
        lost_sales_weekly.select(*lost_base_select_cols)
        .join(weekly_sales_for_lost, on=lost_base_keys, how="left")
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
        "dc_daily": dc_daily,
        "dc_inst": dc_inst,
    }

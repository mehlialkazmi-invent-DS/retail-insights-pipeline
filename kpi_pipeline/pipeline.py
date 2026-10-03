"""Pipeline input frames for a given scope."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from kpi_pipeline.context import DC_GIT_METRICS, STORE_GIT_METRICS, KPIContext
from kpi_pipeline.fiscal import last_saturday_on_or_before, week_day_counts
from kpi_pipeline.inputs import (
    apply_input_filters,
    get_daily_data_excluded_days,
    get_daily_data_raw,
    get_instock_daily_raw,
    get_item_family_raw,
    read_goods_in_transit_source,
    read_instock_source,
    read_inventory_warehouse_source,
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


def _calendar_frame(ctx: KPIContext, *extra_cols: str) -> DataFrame:
    """(date, Year, Week) of the report window, plus ``extra_cols``. With report_end="latest_day" it is
    ctx.day_calendar, which also carries day_index and last_day_index (fiscal.build_latest_day_windows).
    """
    base = ctx.day_calendar if ctx.day_calendar is not None else ctx.fiscal_cal
    return base.select("date", "Year", "Week", *extra_cols)


def _fiscal_week_parts(ctx: KPIContext) -> DataFrame:
    """Fiscal weeks as rows with week_start_date / week_end_date, for the daily in-stock store-day count.

    Normally ctx.fiscal_week's own rows. With report_end="latest_day" the week containing day K (the
    YTD cut, see ctx.day_calendar) comes as two rows -- its days up to K and the days after -- each with
    its own bounds and last_day_index, so the in-stock frame can be cut at K without exploding other
    weeks. Every other week stays one row, carrying its last_day_index.
    """
    cols = ["Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month"]
    if ctx.settings["REPORT_END_MODE"] != "latest_day":
        return ctx.fiscal_week.select(*cols, "week_start_date", "week_end_date")
    parts = ctx.day_calendar.groupBy("Year", "Week", "last_day_index").agg(
        F.min("date").alias("week_start_date"),
        F.max("date").alias("week_end_date"),
    )
    return ctx.fiscal_week.select(*cols).join(parts, on=["Year", "Week"], how="inner")


def _with_week_days(ctx: KPIContext, daily: DataFrame) -> DataFrame:
    """Add week_days: the calendar days of the row's (Year, Week) inside the report window. A constant 7
    except with report_end="latest_day" (fiscal.week_day_counts), where a week can be shorter and
    metrics.compute_kpis weights WOS's weekly inventory by it."""
    if ctx.settings["REPORT_END_MODE"] != "latest_day":
        return daily.withColumn("week_days", F.lit(7))
    return daily.join(broadcast(week_day_counts(ctx).select("Year", "Week", "week_days")), on=["Year", "Week"], how="inner")


def _aggregate_lost_sales_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """One raw lost-sales model summed to (product_id[, store_id], week_start_date) for the window.

    Column names come from LOST_SALES_COLUMN_MAP. in_stock/total_days are read only with
    instock.method "lost_sales_source". Rolled to the family main first when
    ITEM_FAMILY_ROLLUP["lost_sales"] is True (child and parent rows then sum into one bucket).
    """
    col_map = ctx.settings["LOST_SALES_COLUMN_MAP"]
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
    if ctx.settings["ITEM_FAMILY_ROLLUP"]["lost_sales"]:
        filtered = _roll_to_item_family_parent(filtered, ctx)
    agg_exprs = [F.sum(F.col(col_map["lost_sales_col"]).cast("double")).alias("lost_sales")]
    if ctx.settings["INSTOCK_METHOD"] == "lost_sales_source":
        agg_exprs.append(F.sum(F.col(col_map["in_stock_col"]).cast("double")).alias("in_stock_days"))
        agg_exprs.append(F.sum(F.col(col_map["total_days_col"]).cast("double")).alias("total_days"))
    ls_native_week = not ctx.settings["USE_FISCAL_CALENDAR"] and "week" in raw.columns
    if ls_native_week:
        # Native week number; Year comes from week_start_date, not the source 'year' (ISO week-year).
        agg_exprs.append(F.first(F.col("week").cast("int"), ignorenulls=True).alias("_ls_week"))
    # No store_id when the source has none (store_col=None): lost_sales is an absolute count, so it must
    # never be fanned out across stores; build_pipeline_frames collapses scope to this grain instead.
    group_keys = ["product_id", "week_start_date"]
    if "store_id" in raw.columns:
        group_keys.insert(1, "store_id")
    return filtered.groupBy(*group_keys).agg(*agg_exprs)


def _aggregate_instock_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """read_instock_source's rows summed to (product_id[, store_id], week_start_date) for the window,
    store-less when the source has no store_col. Rolled to the family main like the lost sales
    (ITEM_FAMILY_ROLLUP["lost_sales"])."""
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


def read_lost_sales_weekly(ctx: KPIContext) -> DataFrame:
    """Weekly lost-sales aggregates for the report window, with fiscal week attributes (cached per run).

    lost_sales_ensemble.enabled=False: the single model at PATH_LOST_SALES. True: the fast (120-day) and slow
    (365-day) models blended by sales-speed cluster -- products whose cluster is in FAST_MOVER_CLUSTERS take
    the fast model, every other product (no cluster included) the slow one. One boolean picks all three
    fields of a pair-week, so they never mix models. in_stock_days/total_days are present only with
    instock.method "lost_sales_source".
    """
    if ctx.lost_sales_weekly_base is not None:
        return ctx.lost_sales_weekly_base

    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]

    if not s["LOST_SALES_ENSEMBLE_ENABLED"]:
        raw = read_lost_sales_source(ctx.spark, s, s["PATH_LOST_SALES"], quiet=True)
        deduped = _aggregate_lost_sales_pairweek(ctx, raw, start, end)
        if not s["USE_FISCAL_CALENDAR"] and "week" in raw.columns:
            deduped = deduped.withColumn("_ls_year", F.year("week_start_date"))
    else:
        fast_raw = read_lost_sales_source(ctx.spark, s, s["PATH_LOST_SALES"], quiet=True)
        slow_raw = read_lost_sales_source(ctx.spark, s, s["PATH_LOST_SALES_SLOW"], quiet=True)
        native_week = not s["USE_FISCAL_CALENDAR"] and "week" in fast_raw.columns
        # Both models share LOST_SALES_COLUMN_MAP, so checking fast_raw covers both.
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
            # A pair-week exists only when the chosen model has a row (not coalesced to 0).
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
    """Weekly in-stock aggregates of instock.weekly_source for the report window (cached per run), kept
    independently of the lost-sales rows; build_pipeline_frames restricts them to scope."""
    if ctx.instock_weekly_base is not None:
        return ctx.instock_weekly_base

    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    raw = read_instock_source(ctx.spark, s, quiet=True)
    ctx.instock_weekly_base = _enrich_weekly_with_time_grain(
        ctx, _aggregate_instock_pairweek(ctx, raw, start, end)
    ).cache()
    return ctx.instock_weekly_base


def _block_join(blocked: DataFrame, location_col: str):
    """``blocked``'s intervals with prefixed columns, and the condition matching a row of a frame keyed by
    (product_id, <location_col>, date) to the interval containing its day. The intervals of a pair are
    disjoint (scope._applied_block_intervals), so a row matches at most one."""
    intervals = blocked.select(
        F.col("product_id").alias("_block_product_id"),
        F.col(location_col).alias("_block_location"),
        F.col("first_day").alias("_block_first_day"),
        F.col("last_day").alias("_block_last_day"),
    )
    on = (
        (F.col("product_id") == F.col("_block_product_id"))
        & (F.col(location_col) == F.col("_block_location"))
        & F.col("date").between(F.col("_block_first_day"), F.col("_block_last_day"))
    )
    return intervals, on


def _flag_blocked_days(df: DataFrame, blocked: Optional[DataFrame], location_col: str) -> DataFrame:
    """Add is_blocked: whether the row's day lies in one of ``blocked``'s intervals (False everywhere when
    blocked scope is off). Blocked days stay in the frame: each metric decides in metrics.compute_kpis
    whether it reads them (blocked_scope.metrics)."""
    if blocked is None:
        return df.withColumn("is_blocked", F.lit(False))
    intervals, on = _block_join(blocked, location_col)
    return (
        df.join(intervals, on, how="left")
        .withColumn("is_blocked", F.col("_block_product_id").isNotNull())
        .drop(*intervals.columns)
    )


def _drop_blocked_days(df: DataFrame, blocked: DataFrame, location_col: str) -> DataFrame:
    """``df`` without the rows whose day lies in one of ``blocked``'s intervals."""
    intervals, on = _block_join(blocked, location_col)
    return df.join(intervals, on, how="left_anti")


def _join_store_goods_in_transit(ctx: KPIContext, daily: DataFrame, scope_pairs: Optional[DataFrame]) -> DataFrame:
    """Full outer join of store goods in transit (_goods_in_transit_quantity, destination_type 0) onto the
    window's daily rows on (product_id, store_id, date), adding has_daily_row and git_quantity; limited to
    the scoped pairs when the scope has stores.

    A GIT-only day gets sales / revenue / inventory 0 and has_daily_row False. GIT-only days whose daily row
    input_filters.daily_data removed (e.g. usable = 1) are dropped: those days are not reported. Daily rows
    are first summed to one row per pair-day (a child and its parent can share a date after the family
    roll-up), so the quantity attaches once per day; every sum-based metric is unchanged by that.
    """
    day_keys = ["product_id", "store_id", "date"]
    git = _goods_in_transit_quantity(ctx, 0, "store_id", ctx.settings["GOODS_IN_TRANSIT"]["date_shift_days"])
    if scope_pairs is not None:
        git = git.join(scope_pairs, on=["product_id", "store_id"], how="left_semi")
    git = git.join(get_daily_data_excluded_days(ctx).withColumn("_removed", F.lit(True)), on=day_keys, how="left")
    pair_days = (
        daily.groupBy(*day_keys)
        .agg(
            F.sum("sales_revenue").alias("sales_revenue"),
            F.sum("sales_quantity").alias("sales_quantity"),
            F.sum("inventory").alias("inventory"),
        )
        .withColumn("has_daily_row", F.lit(True))
    )
    joined = (
        pair_days.join(git, on=day_keys, how="full_outer")
        .filter(F.col("has_daily_row").isNotNull() | F.col("_removed").isNull())
        .drop("_removed")
    )
    for column in ("sales_revenue", "sales_quantity", "inventory"):
        joined = joined.withColumn(
            column, F.when(F.col("has_daily_row").isNull(), F.lit(0)).otherwise(F.col(column))
        )
    return (
        joined.withColumn("has_daily_row", F.coalesce(F.col("has_daily_row"), F.lit(False)))
        .withColumn("git_quantity", F.coalesce(F.col("git_quantity"), F.lit(0.0)))
    )


def build_scoped_daily(ctx: KPIContext, scope_core: DataFrame, scope_pairs_in: DataFrame, has_store: bool) -> DataFrame:
    """Daily sales / inventory of the scoped pairs, with product cost / price and fiscal week attributes.

    Order: daily data -> scoped-pair semi-join -> store goods in transit (when a store metric is in
    goods_in_transit.inventory_metrics; else has_daily_row True and git_quantity 0 on every row) ->
    blocked-day flag (is_blocked, kept for metrics.compute_kpis to gate per metric) -> calendar -> scope
    semi-join -> product attributes. week_days is the days of the row's fiscal week inside the window (7
    unless report_end="latest_day"); latest_day also adds day_index, which the YTD cut filters on.
    """
    s = ctx.settings
    time_cols = s["DAILY_TIME_COLUMNS"]
    date_col, week_col = time_cols["date"], time_cols["week"]
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    latest_day = s["REPORT_END_MODE"] == "latest_day"
    store_git = any(m in STORE_GIT_METRICS for m in s["GOODS_IN_TRANSIT"]["inventory_metrics"])
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
    if has_store:
        daily = daily.join(scope_pairs_in, on=["product_id", "store_id"], how="left_semi")
    if store_git:
        daily = _join_store_goods_in_transit(ctx, daily, scope_pairs_in if has_store else None)
    else:
        daily = daily.withColumn("has_daily_row", F.lit(True)).withColumn("git_quantity", F.lit(0.0))
    daily = _flag_blocked_days(daily, ctx.blocked_days, "store_id")
    if not s["USE_FISCAL_CALENDAR"]:
        # Year = calendar year of `date`, Week = the native week column (not the ISO week-year 'year').
        daily = daily.withColumn("Year", F.year(F.col("date")))
        daily = rename_column_or_fail(daily, week_col, "Week", "fiscal_calendar.daily_time_columns.week")
        daily = daily.withColumn("Week", F.col("Week").cast("int"))
        if latest_day:
            daily = daily.withColumn("day_index", F.dayofyear(F.col("date")))
    else:
        daily = daily.join(
            broadcast(_calendar_frame(ctx, *(["day_index"] if latest_day else []))), on="date", how="inner"
        )

    scope_keys = ["product_id", "store_id", "Year", "Week"] if has_store else ["product_id", "Year", "Week"]
    daily = daily.join(scope_core, on=scope_keys, how="left_semi")
    return (
        _with_week_days(ctx, daily)
        .join(ctx.products_attr, on="product_id", how="inner")
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
    """In-stock frame of instock.method="daily": per scope pair x fiscal week, stocked_pairs (in-stock days)
    and available_days (counted store-days), the shape of the weekly inst_data, from noob/daily-data.

    Pairs: the scope pairs after instock.daily.input_filters and the scope_source in-stock client rules
    (scope additions included; they have no scope_start and get no blocks). Each pair counts from its
    count start (instock.daily.count_start: "first_daily_row" from history_start, "scope_start", or the
    "earliest" of the two), clipped to the window start, to the window end; require_daily_data drops pairs
    without a daily row. A day without a daily row counts as out of stock.

    Removed from the store-days: blocked days when in_stock_rate is in blocked_scope.metrics, and days with
    usable != 1 when usable_only. An in-stock day is a usable day with inventory > 0 or, with
    goods_in_transit.store_instock, store goods in transit (united per day, never summed). Store-days per
    pair-week come from week bounds, not from exploding every pair-day. weighted_instock_rate reads this
    same frame.

    report_end="latest_day": the week containing the YTD cut day K comes as two rows per pair (days up to K
    and after, _fiscal_week_parts), each with last_day_index, so YTD keeps exactly days 1..K.
    """
    s = ctx.settings
    cfg = s["INSTOCK_DAILY"]
    goods_in_transit = s["GOODS_IN_TRANSIT"]
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    latest_day = s["REPORT_END_MODE"] == "latest_day"
    part_cols = ["last_day_index"] if latest_day else []
    pair_keys = ["product_id", "store_id"]
    day_keys = pair_keys + ["date"]
    week_keys = pair_keys + ["Year", "Week"] + part_cols
    blocked = ctx.blocked_days if "in_stock_rate" in s["BLOCKED_SCOPE"]["metrics"] else None
    cal = broadcast(_calendar_frame(ctx, *part_cols))
    fw = broadcast(_fiscal_week_parts(ctx))

    pairs = apply_input_filters(scope_pairs, cfg["input_filters"], "instock.daily.input_filters")
    if s["SCOPE_SOURCE"]["instock_main_eligible_only"]:
        # Client rule: stores where only a sub item (not the main) is eligible leave in-stock only.
        sub_only = ctx.operation_scope_pairs.filter(~F.col("main_eligible")).select(*pair_keys)
        pairs = pairs.join(sub_only, on=pair_keys, how="left_anti")
    if s["SCOPE_SOURCE"]["instock_exclude_unsuperseded_sizes"]:
        # Client rule: sizes of a superseded class color outside the supersession (likely NGF) leave in-stock only.
        pairs = pairs.join(broadcast(_unsuperseded_sizes(ctx)), on="product_id", how="left_anti")
    daily = get_instock_daily_raw(ctx).join(pairs, on=pair_keys, how="left_semi")
    if blocked is not None:
        daily = _drop_blocked_days(daily, blocked, "store_id")
    daily = daily.cache()
    latest_daily_date = daily.agg(F.max("date")).first()[0]
    if latest_daily_date is None or latest_daily_date < end:
        raise ValueError(
            f"instock.method='daily': daily-data latest date {latest_daily_date} (scope pairs) is before the report "
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

    # Only days from each pair's count_from on (the daily rows reach back to history_start).
    counted_daily = daily.join(pair_start, on=pair_keys, how="inner").filter(F.col("date") >= F.col("count_from"))

    unusable_days = None
    if cfg["usable_only"]:
        unusable_days = counted_daily.filter(~F.col("is_usable")).select(*day_keys).distinct()

    oh_days = counted_daily.filter(F.col("inventory") > 0)
    if cfg["usable_only"]:
        oh_days = oh_days.filter(F.col("is_usable"))
    in_stock_days = oh_days.select(*day_keys).distinct()
    if goods_in_transit["store_instock"]:
        git_days = (
            _goods_in_transit_days(ctx, 0, "store_id", goods_in_transit["date_shift_days"])
            .join(pair_start, on=pair_keys, how="inner")
            .filter(F.col("date") >= F.col("count_from"))
            .select(*day_keys)
            .distinct()
        )
        if blocked is not None:
            git_days = _drop_blocked_days(git_days, blocked, "store_id")
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
        # The blocked days from each pair's count_from on, one row each (a pair's intervals are disjoint).
        blocked_in_count = (
            blocked.join(pair_start, on=pair_keys, how="inner")
            .withColumn("first_day", F.greatest(F.col("first_day"), F.col("count_from")))
            .filter(F.col("first_day") <= F.col("last_day"))
            .select(*pair_keys, F.explode(F.sequence("first_day", "last_day")).alias("date"))
        )
        store_days = store_days.join(pair_week_count(blocked_in_count, "n_blocked"), on=week_keys, how="left")
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
            "stocked_pairs", "available_days", *part_cols,
        )
        .join(ctx.product_dims, on="product_id", how="left")
    )


def _unsuperseded_sizes(ctx: KPIContext) -> DataFrame:
    """product_ids in no item_family row whose class color (products.option_code) has a size in item_family:
    the sizes left out of a superseded class color's supersession (scope_source.instock_exclude_unsuperseded_sizes)."""
    family_ids = get_item_family_raw(ctx).select("product_id").distinct()
    products = ctx.spark.read.format("delta").load(ctx.settings["PATH_PRODUCTS"]).select("product_id", "option_code")
    superseded_class_colors = products.join(family_ids, on="product_id", how="inner").select("option_code").distinct()
    return (
        products.join(superseded_class_colors, on="option_code", how="inner")
        .join(family_ids, on="product_id", how="left_anti")
        .select("product_id")
        .distinct()
    )


def _goods_in_transit_quantity(ctx: KPIContext, destination_type: int, location_col: str, shift: int) -> DataFrame:
    """(product_id, <location_col>, date, git_quantity): goods in transit (quantity > 0) to one destination
    type (0 = store, 1 = warehouse), summed per day for the window, rolled to the family main when
    goods_in_transit.roll_to_family_main. A snapshot dated D - shift describes the end of day D.
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
            "quantity",
        )
    )
    if ctx.settings["GOODS_IN_TRANSIT"]["roll_to_family_main"]:
        git = _roll_to_item_family_parent(git, ctx)
    return (
        git.groupBy("product_id", location_col, "date")
        .agg(F.sum("quantity").alias("git_quantity"))
    )


def _goods_in_transit_days(ctx: KPIContext, destination_type: int, location_col: str, shift: int) -> DataFrame:
    """The (product_id, <location_col>, date) days of _goods_in_transit_quantity, for the in-stock metrics."""
    return _goods_in_transit_quantity(ctx, destination_type, location_col, shift).select(
        "product_id", location_col, "date"
    )


def _roll_to_item_family_parent(df: DataFrame, ctx: KPIContext) -> DataFrame:
    """product_id -> coalesce(parent_id, product_id) over item_family's non-main rows: rolls a frame onto the
    family-main id space scope_core is in, so child-id rows are not dropped by its joins."""
    child_to_parent = broadcast(
        get_item_family_raw(ctx).filter(~F.col("is_main")).select("product_id", "parent_id")
    )
    return (
        df.join(child_to_parent, on="product_id", how="left")
        .withColumn("product_id", F.coalesce(F.col("parent_id"), F.col("product_id")))
        .drop("parent_id")
    )


def _get_inventory_warehouse_parent_rolled(ctx: KPIContext) -> DataFrame:
    """inventory_warehouse for the window, rolled to the family main (ITEM_FAMILY_ROLLUP["inventory_warehouse"])
    and re-summed per (product_id, warehouse_id, date) so children add up instead of duplicating grid rows.
    Scope-independent, cached once per run."""
    if ctx.inventory_warehouse_rolled is not None:
        return ctx.inventory_warehouse_rolled

    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    base = (
        read_inventory_warehouse_source(ctx.spark, s, quiet=True)
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


def _dc_scope_keys(ctx: KPIContext) -> DataFrame:
    """(product_id, warehouse_id) of the DC (network) scope, scope_source.dc_solution_id."""
    return ctx.dc_scope_pairs.select("product_id", "warehouse_id")


def build_dc_daily(ctx: KPIContext, scope_core: DataFrame) -> DataFrame:
    """Daily DC (warehouse) inventory of the scope's product-weeks, rolled to the family main.

    DC data has no store: rows are restricted on (product_id, Year, Week) of scope_core, attached to the
    calendar first because scope membership can vary by week, then to ctx.dc_scope_pairs when set. With a
    DC metric in goods_in_transit.inventory_metrics (context.DC_GIT_METRICS), DC goods in transit
    (destination_type 1) is full-outer-joined on (product_id, warehouse_id, date): a GIT-only day has
    inventory 0 and has_inventory_row False. DC blocked days are flagged (is_blocked) on every row;
    report_end="latest_day" adds day_index.
    """
    s = ctx.settings
    latest_day = s["REPORT_END_MODE"] == "latest_day"
    scope_product_weeks = scope_core.select("product_id", "Year", "Week").distinct()

    inventory = _get_inventory_warehouse_parent_rolled(ctx).withColumn("has_inventory_row", F.lit(True))
    if any(m in DC_GIT_METRICS for m in s["GOODS_IN_TRANSIT"]["inventory_metrics"]):
        git = _goods_in_transit_quantity(ctx, 1, "warehouse_id", s["GOODS_IN_TRANSIT"]["date_shift_days"])
        inventory = (
            inventory.join(git, on=["product_id", "warehouse_id", "date"], how="full_outer")
            .withColumn(
                "inventory", F.when(F.col("has_inventory_row").isNull(), F.lit(0)).otherwise(F.col("inventory"))
            )
            .withColumn("has_inventory_row", F.coalesce(F.col("has_inventory_row"), F.lit(False)))
            .withColumn("git_quantity", F.coalesce(F.col("git_quantity"), F.lit(0.0)))
        )
    else:
        inventory = inventory.withColumn("git_quantity", F.lit(0.0))
    inventory = _flag_blocked_days(inventory, ctx.dc_blocked_days, "warehouse_id")

    dc = (
        inventory.join(
            broadcast(_calendar_frame(ctx, *(["day_index"] if latest_day else []))), on="date", how="inner"
        )
        .join(scope_product_weeks, on=["product_id", "Year", "Week"], how="left_semi")
    )
    if ctx.dc_scope_pairs is not None:
        # DC (network) scope: only its product x warehouse pairs, among the store scope's products.
        dc = dc.join(_dc_scope_keys(ctx), on=["product_id", "warehouse_id"], how="left_semi")
    return dc.join(ctx.product_dims, on="product_id", how="left").join(
        broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")),
        on=["Year", "Week"],
        how="inner",
    )


def build_dc_inst(ctx: KPIContext, scope_core: DataFrame) -> DataFrame:
    """DC in-stock frame: dc_stocked_days / dc_available_days per (product_id, warehouse_id, fiscal week).

    Each pair's daily grid runs from its first inventory_warehouse row to the window end, missing days
    0-filled (stockouts) -- the same bound as the store in-stock denominator. A pair never stocked has no
    row and is absent rather than 0%. A day is stocked when inventory > stock_threshold or, with
    goods_in_transit.dc_instock, goods are in transit to the DC. DC blocked days leave both counts only
    when dc_in_stock_rate is in blocked_scope.metrics; dc_unblocked_days counts the unblocked grid days
    either way (comparable.py's pair universe). report_end="latest_day": grouped per week part with
    last_day_index, like build_instock_daily.
    """
    s = ctx.settings
    latest_day = s["REPORT_END_MODE"] == "latest_day"
    part_cols = ["last_day_index"] if latest_day else []
    if not s["DC_INSTOCK_ENABLED"]:
        # Disabled: an empty frame of the right shape, so dc_in_stock_rate stays null.
        empty_base = (
            scope_core.select("product_id", "Year", "Week")
            .limit(0)
            .withColumn("warehouse_id", F.lit(None).cast("int"))
            .withColumn("dc_stocked_days", F.lit(None).cast("long"))
            .withColumn("dc_available_days", F.lit(None).cast("long"))
            .withColumn("dc_unblocked_days", F.lit(None).cast("long"))
        )
        if latest_day:
            empty_base = empty_base.withColumn("last_day_index", F.lit(None).cast("int"))
        return empty_base.join(ctx.product_dims, on="product_id", how="left").join(
            broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")),
            on=["Year", "Week"],
            how="inner",
        )

    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    threshold = s["DC_INSTOCK_STOCK_THRESHOLD"]

    inv = _get_inventory_warehouse_parent_rolled(ctx)
    pairs = inv.groupBy("product_id", "warehouse_id").agg(F.min("date").alias("first_stocked_date"))
    if ctx.dc_scope_pairs is not None:
        # DC (network) scope pairs only, before the per-day expansion.
        pairs = pairs.join(_dc_scope_keys(ctx), on=["product_id", "warehouse_id"], how="left_semi")
    cal = broadcast(
        _calendar_frame(ctx, *part_cols).filter(F.col("date").between(F.lit(start), F.lit(end)))
    )
    scope_product_weeks = scope_core.select("product_id", "Year", "Week").distinct()

    # inv is window-filtered, so first_stocked_date needs no clamping. The scope restriction runs before the
    # inventory join so that join only sees in-scope rows.
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
    grid = _flag_blocked_days(grid, ctx.dc_blocked_days, "warehouse_id")
    stocked = F.col("inventory") > F.lit(threshold)
    if s["GOODS_IN_TRANSIT"]["dc_instock"]:
        git_days = _goods_in_transit_days(
            ctx, 1, "warehouse_id", s["GOODS_IN_TRANSIT"]["date_shift_days"]
        ).withColumn("has_git", F.lit(True))
        grid = grid.join(git_days, on=day_keys, how="left")
        stocked = stocked | F.col("has_git").isNotNull()
    counted = ~F.col("is_blocked") if "dc_in_stock_rate" in s["BLOCKED_SCOPE"]["metrics"] else F.lit(True)

    dc_inst = (
        grid.groupBy("product_id", "warehouse_id", "Year", "Week", *part_cols)
        .agg(
            F.sum(F.when(counted, stocked.cast("int"))).alias("dc_stocked_days"),
            F.sum(F.when(counted, F.lit(1))).alias("dc_available_days"),
            F.sum(F.when(~F.col("is_blocked"), F.lit(1))).alias("dc_unblocked_days"),
        )
        .filter(F.col("dc_available_days") > 0)
    )
    return dc_inst.join(ctx.product_dims, on="product_id", how="left").join(
        broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Half", "Fiscal_Month")),
        on=["Year", "Week"],
        how="inner",
    )


def build_pipeline_frames(ctx: KPIContext, scope_in: DataFrame) -> Dict[str, DataFrame]:
    """The metric frames of one scope variant: scoped_daily, inst_data, lost_base, dc_daily, dc_inst, plus
    scope_pairs / scope_pair_weeks / lost_sales_weekly.

    Each metric family is restricted to scope on its own and they only meet in the per-period join
    (metrics.build_kpi_table). Goods in transit only reaches scoped_daily / dc_daily
    (inventory_metrics) and the two in-stock frames (store_instock / dc_instock), never lost sales. Blocked
    days are flagged on scoped_daily / dc_daily and gated per metric in metrics.compute_kpis; the in-stock
    frames and lost sales's sales denominator drop them at build time when in_stock_rate /
    dc_in_stock_rate / lost_sales_pct are in blocked_scope.metrics.

    has_store comes from ctx.scope_keys (defined_scope.grain); a scope frame without the store_id it
    expects fails here rather than as an unresolved column later.
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
    # Collapse scope to the source's grain (no store_id for a store-less source such as report_dfu), never
    # fan the source out to scope's: lost_sales is an absolute count and would repeat once per store.
    ls_keys = [k for k in scope_keys if k != "store_id" or "store_id" in lost_sales_raw.columns]
    lost_sales_weekly = lost_sales_raw.join(
        scope_core.select(*ls_keys).distinct(), on=ls_keys, how="left_semi"
    ).cache()

    # In-stock: "daily" is built below from the scope pairs; "weekly_source" is scope-restricted at its own
    # grain like lost sales; "lost_sales_source" is lost_sales_weekly's own in_stock/total_days.
    instock_method = ctx.settings["INSTOCK_METHOD"]
    instock_source_enabled = instock_method == "weekly_source"
    if instock_method == "daily":
        instock_weekly = None
    elif instock_source_enabled:
        instock_raw = read_instock_weekly(ctx)
        inst_keys = [k for k in scope_keys if k != "store_id" or "store_id" in instock_raw.columns]
        instock_weekly = instock_raw.join(
            scope_core.select(*inst_keys).distinct(), on=inst_keys, how="left_semi"
        )
    else:
        instock_weekly = lost_sales_weekly

    # Whether lost sales has its own store_id: a property of lost_sales_source.store_col only, independent
    # of the scope grain (store and pair counts come from daily-data, not from these frames).
    ls_has_store = "store_id" in lost_sales_weekly.columns
    if has_store:
        # scope_core is already the distinct (product_id, store_id, Year, Week) keys.
        scope_pair_weeks = scope_core
        scope_pairs = scope_core.select("product_id", "store_id").distinct().cache()
    elif ls_has_store:
        scope_pair_weeks = lost_sales_weekly.select("product_id", "store_id", "Year", "Week").distinct().cache()
        scope_pairs = lost_sales_weekly.select("product_id", "store_id").distinct().cache()
    else:
        scope_pair_weeks = lost_sales_weekly.select("product_id", "Year", "Week").distinct().cache()
        scope_pairs = lost_sales_weekly.select("product_id").distinct().cache()

    if instock_method == "daily":
        inst_data = build_instock_daily(ctx, scope_core, scope_pairs).cache()
    else:
        # A null total_days falls back to the week's day count for lost_sales_source only; weekly_source's is
        # taken as-is (padding a null to a full week would deflate the in-stock rates).
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
    # lost_sales_source.sales_filter narrows only the sales half of lost_sales_pct's denominator, for a
    # lost-sales table covering a narrower population than daily-data (e.g. no e-commerce).
    ls_sales_filter = ctx.settings["LOST_SALES_SALES_FILTER"]
    daily_for_lost = scoped_daily.filter(F.col("has_daily_row"))
    if "lost_sales_pct" in ctx.settings["BLOCKED_SCOPE"]["metrics"]:
        daily_for_lost = daily_for_lost.filter(~F.col("is_blocked"))
    daily_for_lost = apply_input_filters(
        daily_for_lost,
        ls_sales_filter,
        "lost_sales_source.sales_filter",
        quiet=not ls_sales_filter,
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
        # Store-less lost sales: sum the sales across stores to product-week, then keep the covered weeks.
        weekly_sales_for_lost = (
            weekly_pair.groupBy("product_id", "Year", "Week")
            .agg(F.sum("weekly_sales").alias("sales_quantity_weekly"))
            .join(scope_pair_weeks, on=lost_base_keys, how="left_semi")
        )
    # report_end="latest_day": lost sales only reaches the last Saturday on or before REPORT_END_DATE, so a
    # later partial week would divide missing lost sales by real sales; every view stops at that Saturday.
    lost_weeks = lost_sales_weekly
    if ctx.settings["REPORT_END_MODE"] == "latest_day":
        lost_weeks = lost_weeks.filter(
            F.col("week_end_date") <= F.lit(last_saturday_on_or_before(ctx.settings["REPORT_END_DATE"]))
        )
    lost_base = (
        lost_weeks.select(*lost_base_select_cols)
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

"""Scope table, score-based scope and hybrid union."""

from __future__ import annotations

import datetime
from functools import reduce
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import (
    blocked_scope_keys,
    get_daily_data_raw,
    read_active_product_ids,
    read_blocked_scope_source,
    read_scope_source,
    rename_column_or_fail,
)


def _window_weeks(ctx: KPIContext) -> DataFrame:
    """Fiscal weeks overlapping [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]."""
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    return ctx.fiscal_week.filter(
        (F.col("week_start_date") <= F.lit(end)) & (F.col("week_end_date") >= F.lit(start))
    )


def _scope_pairs(ctx: KPIContext, solution_ids: Optional[List[int]], location_col: str) -> DataFrame:
    """Distinct scope pairs (product_id[, <location_col>]) of the scope table's rows for solution_ids, without
    a time column. With columns.start they also carry scope_start and main_eligible and are cached.

    roll_to_family_main rolls every row to its family main (a product without a main keeps its own id), so a
    location where only a sub-item is in scope gets the main. A pair keeps the EARLIEST start of its rows as
    scope_start; main_eligible says whether the main itself (not only a sub-item) is in scope there
    (scope.instock_main_eligible_only). active_only drops pairs of inactive products.
    """
    cfg = ctx.settings["SCOPE"]
    has_start = cfg["columns"]["start"] is not None
    keys = ["product_id", location_col] if cfg["columns"]["store"] else ["product_id"]
    rows = read_scope_source(ctx.spark, ctx.settings, solution_ids, location_col)
    rows = rows.withColumn("scope_product_id", F.col("product_id")) if has_start else rows.distinct()
    if cfg["roll_to_family_main"]:
        from kpi_pipeline.pipeline import _roll_to_item_family_parent

        rows = _roll_to_item_family_parent(rows, ctx)
    if has_start:
        pairs = rows.groupBy(*keys).agg(
            F.min("start_date").alias("scope_start"),
            F.max(F.col("product_id") == F.col("scope_product_id")).alias("main_eligible"),
        )
    else:
        pairs = rows.distinct()
    if cfg["active_only"]:
        pairs = pairs.join(read_active_product_ids(ctx.spark, ctx.settings), on="product_id", how="inner")
    if not has_start:
        return pairs
    pairs = pairs.cache()
    if pairs.filter(F.col("scope_start").isNull()).limit(1).count() > 0:
        raise ValueError(f"scope rows without a start date (solution_id in {solution_ids})")
    return pairs


def _applied_block_intervals(
    ctx: KPIContext, pairs: DataFrame, location_col: str, folder: str, solution_ids: List[int], kinds: List[str]
) -> DataFrame:
    """(product_id, <location_col>, first_day, last_day): the days one UI blocked-scope snapshot folder blocks,
    as disjoint per-pair date intervals inside the report window.

    ``kinds`` are the folder's kinds to read (product, product_destination, destination), each with
    start_date / end_date (null = open-ended). Blocks match `pairs` (with scope_start) on the main's own
    product_id: block product_ids are NOT rolled to the family main. Rule "after_scope_start" applies a block
    only when block.start_date >= the pair's scope_start (same day applies; an earlier block was superseded by
    the pair being set up again); "all" applies every matched block. An applied block covers start_date ..
    end_date, clipped to the report window.

    Overlapping or adjacent blocks of a pair are merged (gaps and islands), so every blocked day lies in
    exactly one interval: a range join on (pair, first_day <= date <= last_day) matches a day at most once,
    and the intervals hold exactly the days an explode of every block would give, without materializing
    one row per pair-day.
    """
    rule = ctx.settings["BLOCKED_SCOPE"]["rule"]
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    matched = reduce(
        DataFrame.unionByName,
        [
            pairs.join(
                read_blocked_scope_source(ctx.spark, folder, solution_ids, kind, location_col),
                on=blocked_scope_keys(kind, location_col),
                how="inner",
            ).select("product_id", location_col, "scope_start", "block_start", "block_end")
            for kind in kinds
        ],
    )
    applies = (F.col("block_start") >= F.col("scope_start")) if rule == "after_scope_start" else F.lit(True)
    clipped = (
        matched.filter(applies)
        .select(
            "product_id",
            location_col,
            F.greatest(F.col("block_start"), F.lit(start)).alias("first_day"),
            F.least(F.coalesce(F.col("block_end"), F.lit(end)), F.lit(end)).alias("last_day"),
        )
        .filter(F.col("first_day") <= F.col("last_day"))
    )
    # An interval starts a new island when it begins after every earlier interval of the pair has ended (a
    # gap of at least one day); otherwise it extends the current island.
    order = Window.partitionBy("product_id", location_col).orderBy("first_day", "last_day")
    reached = F.max("last_day").over(order.rowsBetween(Window.unboundedPreceding, -1))
    return (
        clipped.withColumn("_starts", (reached.isNull() | (F.col("first_day") > F.date_add(reached, 1))).cast("int"))
        .withColumn("_island", F.sum("_starts").over(order.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .groupBy("product_id", location_col, "_island")
        .agg(F.min("first_day").alias("first_day"), F.max("last_day").alias("last_day"))
        .drop("_island")
    )


def _blocked_pair_days(intervals: DataFrame) -> int:
    """Number of blocked pair-days in disjoint block intervals."""
    days = F.datediff(F.col("last_day"), F.col("first_day")) + 1
    return intervals.agg(F.coalesce(F.sum(days), F.lit(0))).first()[0]


def build_blocked_days(ctx: KPIContext) -> None:
    """Build ctx.blocked_days, the cached (product_id, store_id, first_day, last_day) block intervals of
    {ui_parameters_path}/blocked_scope for blocked_scope.solution_id, matched to the scope pairs
    (None when blocked_scope.ui_parameters_path is None). The metrics named in blocked_scope.metrics drop the
    blocked days; the daily frames only flag them (pipeline._flag_blocked_days).
    """
    if ctx.blocked_days is not None:
        ctx.blocked_days.unpersist()
    ctx.blocked_days = None
    cfg = ctx.settings["BLOCKED_SCOPE"]
    if cfg["path"] is None:
        return
    ctx.blocked_days = _applied_block_intervals(
        ctx, ctx.scope_pairs, "store_id", cfg["path"], cfg["solution_id"], cfg["kinds"]
    ).cache()
    print(f"blocked scope rule={cfg['rule']} | blocked pair-days in window: {_blocked_pair_days(ctx.blocked_days):,}")


def build_dc_scope(ctx: KPIContext) -> None:
    """Build ctx.dc_scope_pairs: the DC (network) scope's (product_id, warehouse_id) pairs from the scope table
    rows of scope.dc_solution_id, built like the store scope (None when dc_solution_id is None). The store
    scope leads: every DC metric reads only these pairs whose product is in the store scope, and the DC
    blocked days take their start dates from them."""
    if ctx.dc_scope_pairs is not None:
        ctx.dc_scope_pairs.unpersist()
    ctx.dc_scope_pairs = None
    solution_ids = ctx.settings["SCOPE"]["dc_solution_id"]
    if solution_ids is None:
        return
    ctx.dc_scope_pairs = _scope_pairs(ctx, solution_ids, "warehouse_id")
    print(f"DC scope pairs (solution_id in {solution_ids}): {ctx.dc_scope_pairs.count():,}")


def build_dc_blocked_days(ctx: KPIContext) -> None:
    """Build ctx.dc_blocked_days, the cached (product_id, warehouse_id, first_day, last_day) DC block intervals
    of {ui_parameters_path}/dc_blocked_scope for blocked_scope.dc_solution_id, matched to ctx.dc_scope_pairs by
    blocked_scope.rule (None unless scope.dc_solution_id, blocked_scope.ui_parameters_path and
    blocked_scope.dc_solution_id are set). The DC metrics named in blocked_scope.metrics drop those days.
    """
    if ctx.dc_blocked_days is not None:
        ctx.dc_blocked_days.unpersist()
    ctx.dc_blocked_days = None
    s = ctx.settings
    if ctx.dc_scope_pairs is None or s["BLOCKED_SCOPE"]["path"] is None or s["BLOCKED_SCOPE"]["dc_solution_id"] is None:
        return
    solution_ids = s["BLOCKED_SCOPE"]["dc_solution_id"]
    ctx.dc_blocked_days = _applied_block_intervals(
        ctx, ctx.dc_scope_pairs, "warehouse_id", s["BLOCKED_SCOPE"]["dc_path"], solution_ids, s["BLOCKED_SCOPE"]["dc_kinds"]
    ).cache()
    print(
        f"DC blocked scope rule={s['BLOCKED_SCOPE']['rule']} | blocked DC pair-days in window: "
        f"{_blocked_pair_days(ctx.dc_blocked_days):,}"
    )


def _weekly_scope_keys(ctx: KPIContext, raw: DataFrame) -> DataFrame:
    """The scope table's own (product[, store], Year, Week) rows inside the report window, for time="weekly",
    with an optional leading-gap backfill.

    Weeks come from ``columns.date`` via fiscal_cal, or from native ``columns.year`` / ``columns.week`` on the
    civil calendar only: with USE_FISCAL_CALENDAR a native week numbering (e.g. ISO week-year) could mismatch
    every other frame's Year/Week, so columns.date is required there. Rolled to the family main when
    roll_to_family_main, and limited to active products when active_only.

    backfill_leading_gap: when the source's earliest week (across every pair) starts after the window start,
    the pairs whose first week IS that earliest week are assumed in scope back to the window start -- the gap
    is the feed's own availability limit. A pair first seen later is a real new pair and keeps its own start.
    Only the leading gap is filled; every other week is used as recorded.
    """
    cfg = ctx.settings["SCOPE"]
    pair_keys = [k for k in ctx.scope_keys if k not in ("Year", "Week")]
    if cfg["columns"]["date"] is not None:
        keyed = (
            raw.select(*pair_keys, "scope_date")
            .distinct()
            .join(
                broadcast(ctx.fiscal_cal.select(F.col("date").alias("scope_date"), "Year", "Week")),
                on="scope_date",
                how="inner",
            )
            .drop("scope_date")
        )
    elif ctx.settings["USE_FISCAL_CALENDAR"]:
        raise ValueError(
            "scope.time='weekly' with USE_FISCAL_CALENDAR=True requires scope.columns.date -- "
            "columns.year / columns.week (the NATIVE path) cannot be trusted to "
            "match fiscal_cal/fiscal_week's own Year/Week numbering."
        )
    else:
        keyed = raw.select(*pair_keys, "Year", "Week").distinct()

    if cfg["roll_to_family_main"]:
        from kpi_pipeline.pipeline import _roll_to_item_family_parent

        keyed = _roll_to_item_family_parent(keyed, ctx).distinct()
    if cfg["active_only"]:
        keyed = keyed.join(read_active_product_ids(ctx.spark, ctx.settings), on="product_id", how="inner")

    window_weeks = _window_weeks(ctx).select("Year", "Week", "week_start_date").distinct()
    keyed = keyed.join(broadcast(window_weeks.select("Year", "Week")), on=["Year", "Week"], how="inner").cache()

    if not cfg["backfill_leading_gap"]:
        return keyed.select(*ctx.scope_keys).distinct()

    window_start = ctx.settings["EFFECTIVE_REPORT_START_DATE"]
    pair_first_week = (
        keyed.join(broadcast(window_weeks), on=["Year", "Week"], how="inner")
        .groupBy(*pair_keys)
        .agg(F.min("week_start_date").alias("first_scope_week_start"))
    )
    scope_earliest_start = pair_first_week.agg(F.min("first_scope_week_start")).collect()[0][0]
    pairs_with_gap = pair_first_week.filter(
        (F.col("first_scope_week_start") == F.lit(scope_earliest_start))
        & (F.col("first_scope_week_start") > F.lit(window_start))
    )
    backfill = (
        pairs_with_gap.crossJoin(broadcast(window_weeks))
        .filter(F.col("week_start_date") < F.col("first_scope_week_start"))
        .select(*pair_keys, "Year", "Week")
    )

    return keyed.unionByName(backfill).select(*ctx.scope_keys).distinct()


def build_scope(ctx: KPIContext) -> None:
    """Read the scope table into ctx.scope_table_keys, (product[, store], Year, Week) keys at ``scope.grain``.

    time="daily": the pairs (_scope_pairs) apply to EVERY window week; with columns.start the full pair frame
    stays on ctx.scope_pairs for the blocked-scope rule and the daily in-stock count start.
    time="weekly": the table keeps its own weeks (_weekly_scope_keys).
    """
    cfg = ctx.settings["SCOPE"]
    has_store = cfg["grain"] == "product_store"
    ctx.scope_keys = (
        ["product_id", "store_id", "Year", "Week"] if has_store else ["product_id", "Year", "Week"]
    )

    if cfg["time"] == "weekly":
        raw = read_scope_source(ctx.spark, ctx.settings, cfg["solution_id"], "store_id", quiet=True)
        ctx.scope_table_keys = _weekly_scope_keys(ctx, raw).cache()
    else:
        if ctx.scope_pairs is not None:
            ctx.scope_pairs.unpersist()
        ctx.scope_pairs = None
        pairs = _scope_pairs(ctx, cfg["solution_id"], "store_id")
        if cfg["columns"]["start"] is not None:
            ctx.scope_pairs = pairs
            print(f"scope pairs: {ctx.scope_pairs.count():,}")
        pairs = pairs.select(*[c for c in ("product_id", "store_id") if c in ctx.scope_keys]).distinct()
        # Distinct pairs x distinct weeks: already distinct, no distinct() pass needed.
        window_yw = _window_weeks(ctx).select("Year", "Week").distinct()
        ctx.scope_table_keys = pairs.crossJoin(broadcast(window_yw)).select(*ctx.scope_keys).cache()

    print(f"scope time: {cfg['time']} | grain: {cfg['grain']} | keys: {ctx.scope_keys} | count: {ctx.scope_table_keys.count()}")


def read_daily_for_scope(ctx: KPIContext, start_date: datetime.date, end_date: datetime.date) -> DataFrame:
    """Daily sales / inventory per (product_id, store_id, date) for score scope, every store included
    (exclude a store with input_filters.daily_data)."""
    time_cols = ctx.settings["DAILY_TIME_COLUMNS"]
    date_col = time_cols["date"]
    daily = (
        get_daily_data_raw(ctx)
        .select("product_id", "store_id", date_col, "sales_revenue", "sales_quantity", "inventory")
        .withColumn(date_col, F.to_date(F.col(date_col)))
    )
    daily = rename_column_or_fail(daily, date_col, "date", "fiscal_calendar.daily_time_columns.date")
    return (
        daily
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .groupBy("product_id", "store_id", "date")
        .agg(
            F.sum("sales_quantity").alias("sales_quantity"),
            F.sum("sales_revenue").alias("sales_revenue"),
            F.sum("inventory").alias("inventory"),
        )
    )


def build_weekly_scope(
    daily: DataFrame,
    fiscal_cal: DataFrame,
    fiscal_week: DataFrame,
    min_percentile: float,
    min_weeks_for_filter: int,
) -> DataFrame:
    """Flag each pair-week in scope ("yes") when its sales and weekly inventory both reach the pair's
    ``min_percentile``, or when the pair has at most ``min_weeks_for_filter`` weeks. Weekly inventory is
    the week's last available daily snapshot (max_by on date), not Saturday's only."""
    weekly = (
        daily.join(
            broadcast(fiscal_cal.select("date", "Year", "Week")),
            on="date",
            how="inner",
        )
        .join(
            broadcast(fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "week_end_date")),
            on=["Year", "Week"],
            how="inner",
        )
        .groupBy("product_id", "store_id", "Year", "Week", "Year_Week", "week_start_date", "week_end_date")
        .agg(
            F.sum("sales_quantity").alias("weekly_sales"),
            F.max_by(F.col("inventory"), F.col("date")).alias("weekly_inventory"),
        )
        .fillna(0.0, subset=["weekly_sales", "weekly_inventory"])
    )
    w = Window.partitionBy("product_id", "store_id")
    weekly = (
        weekly
        .withColumn("sales_pct_thr", F.percentile_approx("weekly_sales", min_percentile).over(w))
        .withColumn("inv_pct_thr", F.percentile_approx("weekly_inventory", min_percentile).over(w))
        .withColumn("pair_week_count", F.count(F.lit(1)).over(w))
    )
    skip_filter = F.col("pair_week_count") <= min_weeks_for_filter
    passes_filter = (F.col("weekly_sales") >= F.col("sales_pct_thr")) & (F.col("weekly_inventory") >= F.col("inv_pct_thr"))
    return (
        weekly
        .withColumn("in_scope", F.when(skip_filter | passes_filter, F.lit("yes")).otherwise(F.lit("no")))
        .select("product_id", "store_id", "week_start_date", "in_scope")
    )


def build_score_scope_keys(ctx: KPIContext, daily_in: DataFrame) -> DataFrame:
    """Scope key columns (product×[store×]Year×Week) for pairs passing the score filter (in_scope='yes')."""
    ws = build_weekly_scope(
        daily_in,
        ctx.fiscal_cal,
        ctx.fiscal_week,
        ctx.settings["SCOPE_MIN_PERCENTILE"],
        ctx.settings["SCOPE_MIN_WEEKS_FOR_FILTER"],
    )
    return (
        ws.filter(F.col("in_scope") == "yes")
        .select("product_id", "store_id", "week_start_date")
        .join(broadcast(ctx.fiscal_week.select("Year", "Week", "week_start_date")), on="week_start_date", how="inner")
        .select("product_id", "store_id", "Year", "Week")
        .distinct()
    )


def build_hybrid_scope(ctx: KPIContext) -> None:
    """Set ctx.hybrid_scope_keys (with scope_origin): scope_table_keys, plus with use_hybrid_scope the
    score-scope keys of the window weeks the scope table does not cover. Only time="weekly" can leave weeks
    uncovered; time="daily" covers every week, so the backfill adds nothing there.
    Score scope is computed only for hybrid or run_scope_diff.
    """
    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    use_hybrid = s["SCOPE"]["use_hybrid_scope"]
    run_scope_diff = s["SCOPE"]["run_scope_diff"]
    need_score = use_hybrid or run_scope_diff

    ctx.score_only_scope_keys = None
    if need_score:
        daily_all = read_daily_for_scope(ctx, start, end).cache()
        ctx.score_only_scope_keys = (
            build_score_scope_keys(ctx, daily_all).select(*ctx.scope_keys).distinct().cache()
        )

    if not use_hybrid:
        ctx.hybrid_scope_keys = ctx.scope_table_keys.withColumn("scope_origin", F.lit("scope")).cache()
        print("scope mode: scope table only (hybrid disabled)")
        print("grain:", ctx.scope_keys, "| final_scope:", ctx.hybrid_scope_keys.count())
        return

    window_weeks = _window_weeks(ctx).select("Year", "Week").distinct()
    covered_weeks = ctx.scope_table_keys.select("Year", "Week").distinct()
    missing_weeks = window_weeks.join(broadcast(covered_weeks), on=["Year", "Week"], how="left_anti").cache()

    scope_part = ctx.scope_table_keys.withColumn("scope_origin", F.lit("scope"))
    score_backfill = ctx.score_only_scope_keys.join(
        broadcast(missing_weeks), on=["Year", "Week"], how="left_semi"
    ).withColumn("scope_origin", F.lit("score"))
    ctx.hybrid_scope_keys = scope_part.unionByName(score_backfill).cache()

    print("scope mode: hybrid (scope table + score backfill on missing weeks)")
    print("grain:", ctx.scope_keys)
    print("missing_weeks:", missing_weeks.count(), "| final_scope:", ctx.hybrid_scope_keys.count())


def _fully_blocked_pairs(ctx: KPIContext) -> DataFrame:
    """(product_id, store_id) of ctx.hybrid_scope_keys whose every in-window scope day is a blocked day: a
    pair-week is fully blocked when its blocked days reach its days inside the window. The intervals of a pair
    are disjoint, so summing their overlaps with a week counts each day once."""
    pair_keys = ["product_id", "store_id"]
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    weeks = ctx.fiscal_week.select(
        "Year",
        "Week",
        F.greatest(F.col("week_start_date"), F.lit(start)).alias("_from"),
        F.least(F.col("week_end_date"), F.lit(end)).alias("_to"),
    )
    overlap = F.datediff(F.least(F.col("last_day"), F.col("_to")), F.greatest(F.col("first_day"), F.col("_from"))) + 1
    return (
        ctx.hybrid_scope_keys.select(*pair_keys, "Year", "Week")
        .distinct()
        .join(ctx.blocked_days.select(*pair_keys).distinct(), on=pair_keys, how="left_semi")
        .join(broadcast(weeks), on=["Year", "Week"], how="inner")
        .join(ctx.blocked_days, on=pair_keys, how="inner")
        .groupBy(*pair_keys, "Year", "Week", "_from", "_to")
        .agg(F.sum(F.greatest(F.lit(0), overlap)).alias("_blocked_days"))
        .groupBy(*pair_keys)
        .agg(F.min(F.col("_blocked_days") >= F.datediff(F.col("_to"), F.col("_from")) + 1).alias("_all_blocked"))
        .filter(F.col("_all_blocked"))
        .select(*pair_keys)
    )


def build_scope_removals(ctx: KPIContext) -> None:
    """Build the cached removal sets once per scope build, read by the Scope debug summary and the pipeline:

    * ctx.fully_blocked_pairs (blocked scope on, store-level scope): scope pairs blocked on every in-window
      scope day.
    * ctx.instock_sub_only_pairs (instock.method "daily" + scope.instock_main_eligible_only): scope pairs
      where only a sub item, not the main, is eligible.
    * ctx.instock_unsuperseded_products (instock.method "daily" + scope.instock_exclude_unsuperseded_sizes):
      sizes of a superseded class color outside the supersession.
    """
    from kpi_pipeline.pipeline import _unsuperseded_sizes

    s = ctx.settings
    for name in ("fully_blocked_pairs", "instock_sub_only_pairs", "instock_unsuperseded_products"):
        if getattr(ctx, name) is not None:
            getattr(ctx, name).unpersist()
        setattr(ctx, name, None)

    # Pair counts need a store-level scope; grain "product" can still carry blocks (flagged on the daily rows).
    if ctx.blocked_days is not None and "store_id" in ctx.scope_keys:
        ctx.fully_blocked_pairs = _fully_blocked_pairs(ctx).cache()
        print(f"scope pairs blocked on every window day: {ctx.fully_blocked_pairs.count():,}")
    if s["INSTOCK_METHOD"] != "daily":
        return
    if s["SCOPE"]["instock_main_eligible_only"]:
        ctx.instock_sub_only_pairs = (
            ctx.scope_pairs.filter(~F.col("main_eligible")).select("product_id", "store_id").cache()
        )
        print(f"in-stock removal, sub-item-only pairs: {ctx.instock_sub_only_pairs.count():,}")
    if s["SCOPE"]["instock_exclude_unsuperseded_sizes"]:
        ctx.instock_unsuperseded_products = _unsuperseded_sizes(ctx).cache()
        print(f"in-stock removal, unsuperseded sizes: {ctx.instock_unsuperseded_products.count():,}")


def scope_summary_by_origin(scope: DataFrame):
    """Row counts by scope_origin, plus a TOTAL row."""
    spark = scope.sparkSession
    if "scope_origin" not in scope.columns:
        total = scope.count()
        return spark.createDataFrame([("TOTAL", total)], ["scope_origin", "scope_rows"])
    by_origin = scope.groupBy("scope_origin").agg(F.count(F.lit(1)).alias("scope_rows"))
    origin_rows = by_origin.collect()
    total = sum(int(row["scope_rows"]) for row in origin_rows)
    total_row = spark.createDataFrame([("TOTAL", total)], ["scope_origin", "scope_rows"])
    return by_origin.unionByName(total_row).orderBy(F.desc(F.col("scope_origin") == "TOTAL"), "scope_origin")

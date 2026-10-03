"""Defined scope, score-based scope, hybrid union, and manual scope adjustments."""

from __future__ import annotations

import datetime
from functools import reduce
from typing import Any, Callable, Dict, List, Optional

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
    read_csv_source,
    read_defined_scope_source,
    read_operation_scope_source,
    rename_column_or_fail,
)


def _window_weeks(ctx: KPIContext) -> DataFrame:
    """Fiscal weeks overlapping [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]."""
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    return ctx.fiscal_week.filter(
        (F.col("week_start_date") <= F.lit(end)) & (F.col("week_end_date") >= F.lit(start))
    )


def _defined_scope_pairs(ctx: KPIContext, raw: DataFrame) -> DataFrame:
    """Distinct scope pairs (or products) of the defined_scope table at the configured grain, without a time
    column; rolled to the family main when ITEM_FAMILY_ROLLUP["defined_scope"] is True (off by default:
    a client's scope source may already be rolled, as tbretail's is)."""
    config = ctx.settings["DEFINED_SCOPE"]
    sel = [F.col(config["product_col"]).alias("product_id")]
    if "store_id" in ctx.scope_keys:
        sel.append(F.col(config["store_col"]).alias("store_id"))
    pairs = raw.select(*sel).distinct()
    if ctx.settings["ITEM_FAMILY_ROLLUP"]["defined_scope"]:
        from kpi_pipeline.pipeline import _roll_to_item_family_parent

        pairs = _roll_to_item_family_parent(pairs, ctx).distinct()
    return pairs


def _scope_run_date(ctx: KPIContext) -> datetime.date:
    """scope_source.run_date, or the latest Sunday on or before today when unset."""
    configured = ctx.settings["SCOPE_SOURCE"]["run_date"]
    if configured is not None:
        return configured
    today = datetime.date.today()
    return today - datetime.timedelta(days=(today.weekday() + 1) % 7)


def _scope_start_pairs(ctx: KPIContext, solution_ids: List[int], location_col: str) -> DataFrame:
    """Cached (product_id, <location_col>, scope_start, main_eligible) from one solution's operation/scope run.

    roll_to_family_main rolls every row to its family main (a product without a main keeps its own id), so a
    location where only a sub-item is in scope gets the main. A pair keeps the EARLIEST start_date of its
    rows as scope_start; main_eligible says whether the main itself (not only a sub-item) is in scope there
    (scope_source.instock_main_eligible_only). active_only drops pairs of inactive products.
    """
    cfg = ctx.settings["SCOPE_SOURCE"]
    rows = read_operation_scope_source(ctx.spark, ctx.settings, _scope_run_date(ctx), solution_ids, location_col)
    rows = rows.withColumn("scope_product_id", F.col("product_id"))
    if cfg["roll_to_family_main"]:
        from kpi_pipeline.pipeline import _roll_to_item_family_parent

        rows = _roll_to_item_family_parent(rows, ctx)
    pairs = rows.groupBy("product_id", location_col).agg(
        F.min("start_date").alias("scope_start"),
        F.max(F.col("product_id") == F.col("scope_product_id")).alias("main_eligible"),
    )
    if cfg["active_only"]:
        pairs = pairs.join(read_active_product_ids(ctx.spark, ctx.settings), on="product_id", how="inner")
    pairs = pairs.cache()
    if pairs.filter(F.col("scope_start").isNull()).limit(1).count() > 0:
        raise ValueError(f"operation scope rows without start_date (solution_id in {solution_ids})")
    return pairs


def _operation_scope_pairs(ctx: KPIContext) -> DataFrame:
    """Scope universe of scope_source.mode="operation_scope": the distinct pairs of _scope_start_pairs at the
    configured grain, without a time column. The full frame stays on ctx.operation_scope_pairs for the
    blocked-scope rule and the daily in-stock count start."""
    if ctx.operation_scope_pairs is not None:
        ctx.operation_scope_pairs.unpersist()
    ctx.operation_scope_pairs = _scope_start_pairs(ctx, ctx.settings["SCOPE_SOURCE"]["solution_id"], "store_id")
    print(f"operation scope pairs: {ctx.operation_scope_pairs.count():,}")
    return ctx.operation_scope_pairs.select(
        *[c for c in ("product_id", "store_id") if c in ctx.scope_keys]
    ).distinct()


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
    {ui_parameters_path}/blocked_scope for scope_source.solution_id, matched to the operation-scope pairs
    (None when blocked_scope.ui_parameters_path is None). Pairs added by scope_adjustments are not
    operation-scope pairs, so no block reaches them. The metrics named in blocked_scope.metrics drop the
    blocked days; the daily frames only flag them (pipeline._flag_blocked_days).
    """
    if ctx.blocked_days is not None:
        ctx.blocked_days.unpersist()
    ctx.blocked_days = None
    cfg = ctx.settings["BLOCKED_SCOPE"]
    if cfg["path"] is None:
        return
    ctx.blocked_days = _applied_block_intervals(
        ctx, ctx.operation_scope_pairs, "store_id", cfg["path"], cfg["solution_id"], cfg["kinds"]
    ).cache()
    print(f"blocked scope rule={cfg['rule']} | blocked pair-days in window: {_blocked_pair_days(ctx.blocked_days):,}")


def build_dc_scope(ctx: KPIContext) -> None:
    """Build ctx.dc_scope_pairs: the DC (network) scope's (product_id, warehouse_id) pairs from operation/scope
    of scope_source.dc_solution_id, built like the store scope (None when dc_solution_id is None). The store
    scope leads: every DC metric reads only these pairs whose product is in the store scope, and the DC
    blocked days take their start dates from them."""
    if ctx.dc_scope_pairs is not None:
        ctx.dc_scope_pairs.unpersist()
    ctx.dc_scope_pairs = None
    solution_ids = ctx.settings["SCOPE_SOURCE"]["dc_solution_id"]
    if solution_ids is None:
        return
    ctx.dc_scope_pairs = _scope_start_pairs(ctx, solution_ids, "warehouse_id")
    print(f"DC scope pairs (solution_id in {solution_ids}): {ctx.dc_scope_pairs.count():,}")


def build_dc_blocked_days(ctx: KPIContext) -> None:
    """Build ctx.dc_blocked_days, the cached (product_id, warehouse_id, first_day, last_day) DC block intervals
    of {ui_parameters_path}/dc_blocked_scope for blocked_scope.dc_solution_id, matched to ctx.dc_scope_pairs by
    blocked_scope.rule (None unless scope_source.dc_solution_id, blocked_scope.ui_parameters_path and
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


def _defined_scope_weekly(ctx: KPIContext, raw: DataFrame) -> DataFrame:
    """The scope table's own (product, store, Year, Week) rows inside the report window, for the
    ``product_store_week`` grain, with an optional leading-gap backfill.

    Weeks come from ``date_col`` via fiscal_cal, or from native ``year_col``/``week_col`` on the civil
    calendar only: with USE_FISCAL_CALENDAR a native week numbering (e.g. ISO week-year) could mismatch
    every other frame's Year/Week, so date_col is required there. Rolled to the family main when
    ITEM_FAMILY_ROLLUP["defined_scope"] is True.

    backfill_leading_gap (default True): when the source's earliest week (across every pair) starts after
    the window start, the pairs whose first week IS that earliest week are assumed in scope back to the
    window start -- the gap is the feed's own availability limit. A pair first seen later is a real new
    pair and keeps its own start. Only the leading gap is filled; every other week is used as recorded.
    """
    config = ctx.settings["DEFINED_SCOPE"]
    sel = [
        F.col(config["product_col"]).alias("product_id"),
        F.col(config["store_col"]).alias("store_id"),
    ]
    date_col = config.get("date_col")
    if date_col is not None:
        keyed = (
            raw.select(*sel, F.to_date(F.col(date_col)).alias("scope_date"))
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
            "defined_scope.grain='product_store_week' with USE_FISCAL_CALENDAR=True requires "
            "defined_scope.date_col -- year_col/week_col (the NATIVE path) cannot be trusted to "
            "match fiscal_cal/fiscal_week's own Year/Week numbering."
        )
    else:
        year_col, week_col = config.get("year_col"), config.get("week_col")
        keyed = raw.select(
            *sel,
            F.col(year_col).cast("int").alias("Year"),
            F.col(week_col).cast("int").alias("Week"),
        ).distinct()

    if ctx.settings["ITEM_FAMILY_ROLLUP"]["defined_scope"]:
        from kpi_pipeline.pipeline import _roll_to_item_family_parent

        keyed = _roll_to_item_family_parent(keyed, ctx).distinct()

    window_weeks = _window_weeks(ctx).select("Year", "Week", "week_start_date").distinct()
    keyed = keyed.join(broadcast(window_weeks.select("Year", "Week")), on=["Year", "Week"], how="inner").cache()

    if not config.get("backfill_leading_gap", True):
        return keyed.select(*ctx.scope_keys).distinct()

    window_start = ctx.settings["EFFECTIVE_REPORT_START_DATE"]
    pair_first_week = (
        keyed.join(broadcast(window_weeks), on=["Year", "Week"], how="inner")
        .groupBy("product_id", "store_id")
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
        .select("product_id", "store_id", "Year", "Week")
    )

    return keyed.unionByName(backfill).select(*ctx.scope_keys).distinct()


def build_defined_scope(ctx: KPIContext) -> None:
    """Read the scope table into ctx.defined_scope_keys, (product[, store], Year, Week) keys at
    ``defined_scope.grain``: "product" and "product_store" pairs apply to EVERY window week;
    "product_store_week" keeps the table's own weeks (not supported with operation_scope).

    The table is ``defined_scope`` (scope_source.mode="defined_scope") or the platform operation/scope
    (mode="operation_scope", _operation_scope_pairs).
    """
    config = ctx.settings["DEFINED_SCOPE"]
    grain = config["grain"]
    has_store = grain in ("product_store", "product_store_week")
    ctx.scope_keys = (
        ["product_id", "store_id", "Year", "Week"] if has_store else ["product_id", "Year", "Week"]
    )

    operation_scope = ctx.settings["SCOPE_SOURCE"]["mode"] == "operation_scope"

    if grain == "product_store_week":
        raw = read_defined_scope_source(ctx.spark, ctx.settings, quiet=True)
        ctx.defined_scope_keys = _defined_scope_weekly(ctx, raw).cache()
    else:
        if operation_scope:
            pairs = _operation_scope_pairs(ctx)
        else:
            pairs = _defined_scope_pairs(ctx, read_defined_scope_source(ctx.spark, ctx.settings, quiet=True))
        # Distinct pairs x distinct weeks: already distinct, no distinct() pass needed.
        window_yw = _window_weeks(ctx).select("Year", "Week").distinct()
        ctx.defined_scope_keys = pairs.crossJoin(broadcast(window_yw)).select(*ctx.scope_keys).cache()

    print(f"defined scope grain: {grain} | keys: {ctx.scope_keys} | count: {ctx.defined_scope_keys.count()}")


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
    """Set ctx.hybrid_scope_keys (with scope_origin): defined_scope_keys, plus with use_hybrid_scope the
    score-scope keys of the window weeks the defined scope does not cover. Only product_store_week can
    leave weeks uncovered; the week-agnostic grains cover every week, so the backfill adds nothing there.
    Score scope is computed only for hybrid or run_scope_diff.
    """
    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    use_hybrid = s["USE_HYBRID_SCOPE"]
    run_scope_diff = s.get("RUN_SCOPE_DIFF", False)
    need_score = use_hybrid or run_scope_diff

    ctx.score_only_scope_keys = None
    if need_score:
        daily_all = read_daily_for_scope(ctx, start, end).cache()
        ctx.score_only_scope_keys = (
            build_score_scope_keys(ctx, daily_all).select(*ctx.scope_keys).distinct().cache()
        )

    if not use_hybrid:
        ctx.hybrid_scope_keys = ctx.defined_scope_keys.withColumn("scope_origin", F.lit("defined")).cache()
        print("scope mode: defined only (hybrid disabled)")
        print("grain:", ctx.scope_keys, "| final_scope:", ctx.hybrid_scope_keys.count())
        return

    window_weeks = _window_weeks(ctx).select("Year", "Week").distinct()
    covered_weeks = ctx.defined_scope_keys.select("Year", "Week").distinct()
    missing_weeks = window_weeks.join(broadcast(covered_weeks), on=["Year", "Week"], how="left_anti").cache()

    defined_part = ctx.defined_scope_keys.withColumn("scope_origin", F.lit("defined"))
    score_backfill = ctx.score_only_scope_keys.join(
        broadcast(missing_weeks), on=["Year", "Week"], how="left_semi"
    ).withColumn("scope_origin", F.lit("score"))
    ctx.hybrid_scope_keys = defined_part.unionByName(score_backfill).cache()

    print("scope mode: hybrid (defined scope + score backfill on missing weeks)")
    print("grain:", ctx.scope_keys)
    print("missing_weeks:", missing_weeks.count(), "| final_scope:", ctx.hybrid_scope_keys.count())


def _resolve_adjustment_path(
    ctx: KPIContext, adj_cfg: Dict[str, Any], fund_paste: Optional[Callable[..., str]] = None
) -> str:
    if adj_cfg.get("path"):
        return adj_cfg["path"]
    segments = adj_cfg.get("path_segments")
    if segments:
        if fund_paste is None:
            raise ValueError(
                "scope adjustment uses path_segments but fund_paste was not provided; "
                "set an absolute 'path' instead."
            )
        return fund_paste(ctx.settings["BUCKET"], *segments)
    raise ValueError("scope adjustment requires 'path' or 'path_segments'")


def _adjustment_source(adj_cfg: Dict[str, Any], path: str) -> str:
    source = (adj_cfg.get("source") or "").strip().lower()
    if source in {"delta", "csv"}:
        return source
    return "csv" if path.lower().endswith(".csv") else "delta"


def _read_adjustment_raw(ctx: KPIContext, adj_cfg: Dict[str, Any], path: str) -> DataFrame:
    """Load an adjustment table from Delta or CSV (``"location": "workspace"`` reads a /Workspace/... CSV)."""
    source = _adjustment_source(adj_cfg, path)
    if source == "csv":
        print(f"scope adjustment source: csv ({path})")
        return read_csv_source(
            ctx.spark,
            path,
            csv_options=adj_cfg.get("csv_options") or {},
            location=adj_cfg.get("location", "datastore"),
        )
    print(f"scope adjustment source: delta ({path})")
    return ctx.spark.read.format("delta").load(path)


def _enabled_adjustments(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    steps = []
    for adj in cfg.get("additions", []):
        if adj.get("enabled"):
            steps.append({**adj, "action": "addition"})
    for adj in cfg.get("removals", []):
        if adj.get("enabled"):
            steps.append({**adj, "action": "removal"})
    return steps


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


def print_scope_summary(title: str, scope: DataFrame, scope_keys: List[str]) -> None:
    """Print a human-readable scope snapshot."""
    print(title)
    print(f"  grain: {scope_keys}")
    if "scope_origin" not in scope.columns:
        print(f"  total rows: {scope.count():,}")
        return
    origin_rows = scope.groupBy("scope_origin").agg(F.count(F.lit(1)).alias("scope_rows")).collect()
    total = sum(int(row["scope_rows"]) for row in origin_rows)
    print(f"  total rows: {total:,}")
    print("  by scope_origin:")
    for row in sorted(origin_rows, key=lambda r: r["scope_origin"]):
        print(f"    {row['scope_origin']}: {row['scope_rows']:,}")


def _adjustment_select_cols(adj_cfg: Dict[str, Any], join_keys: List[str]) -> List:
    cols = []
    product_col = adj_cfg.get("product_col", "product_id")
    store_col = adj_cfg.get("store_col", "store_id")
    if "product_id" in join_keys:
        cols.append(F.col(product_col).alias("product_id"))
    if "store_id" in join_keys:
        cols.append(F.col(store_col).alias("store_id"))
    return cols


def _read_adjustment_keys(
    ctx: KPIContext,
    adj_cfg: Dict[str, Any],
    path: str,
    scope_pairs: Optional[DataFrame] = None,
) -> DataFrame:
    """An adjustment table's keys at the scope_keys grain for the report window. ``scope_pairs`` (distinct
    daily-data pairs) expands a single-key adjustment to the other key."""
    join_keys = adj_cfg["join_keys"]
    raw = _read_adjustment_raw(ctx, adj_cfg, path)
    window_weeks = _window_weeks(ctx).select("Year", "Week", "week_start_date", "week_end_date")

    date_col = adj_cfg.get("date_col")
    year_col = adj_cfg.get("year_col")
    week_col = adj_cfg.get("week_col")

    if date_col:
        sel = _adjustment_select_cols(adj_cfg, join_keys) + [F.to_date(F.col(date_col)).alias("scope_date")]
        keyed = raw.select(*sel).distinct()
        keyed = keyed.join(
            broadcast(ctx.fiscal_cal.select(F.col("date").alias("scope_date"), "Year", "Week")),
            on="scope_date",
            how="inner",
        ).drop("scope_date")
        keyed = keyed.join(window_weeks.select("Year", "Week"), on=["Year", "Week"], how="inner")
    elif year_col and week_col:
        sel = _adjustment_select_cols(adj_cfg, join_keys) + [
            F.col(year_col).cast("int").alias("Year"),
            F.col(week_col).cast("int").alias("Week"),
        ]
        keyed = raw.select(*sel).distinct()
        keyed = keyed.join(window_weeks.select("Year", "Week"), on=["Year", "Week"], how="inner")
    else:
        base = raw.select(*_adjustment_select_cols(adj_cfg, join_keys)).distinct()
        keyed = base.crossJoin(window_weeks.select("Year", "Week"))

    if "store_id" in ctx.scope_keys and "store_id" not in join_keys and "product_id" in join_keys:
        keyed = keyed.join(scope_pairs, on="product_id", how="inner")

    if "product_id" in ctx.scope_keys and "product_id" not in join_keys and "store_id" in join_keys:
        keyed = keyed.join(scope_pairs, on="store_id", how="inner")

    output_cols = [c for c in ctx.scope_keys if c in keyed.columns]
    return keyed.select(*output_cols).distinct()


def _anti_join_by_keys(scope: DataFrame, removal_keys: DataFrame, join_keys: List[str]) -> DataFrame:
    """Anti-join ``scope`` against ``removal_keys`` on the adjustment's ``join_keys`` the scope grain carries,
    plus Year/Week. A product-only removal under a product_store scope therefore removes every store of the
    product. Fails when no non-time join key is left (a Year/Week-only match would strike whole weeks).
    """
    key_cols = set(scope.columns) & set(removal_keys.columns)
    join_on = [k for k in join_keys if k in key_cols and k not in ("Year", "Week")]
    join_on += [k for k in ("Year", "Week") if k in key_cols]
    if all(k in ("Year", "Week") for k in join_on):
        raise ValueError(
            f"scope adjustment removal join_keys={join_keys} share no usable non-time column "
            f"with the resolved scope grain {sorted(scope.columns)}; check defined_scope.grain "
            "is compatible with this adjustment's join_keys/store_col."
        )
    return scope.join(removal_keys.select(*join_on).distinct(), on=join_on, how="left_anti")


def apply_scope_adjustments(ctx: KPIContext, fund_paste: Optional[Callable[..., str]] = None) -> None:
    """Apply the enabled scope_adjustments additions and removals, in order, to ctx.hybrid_scope_keys.

    Each step's scope is cached, so its row counts, its summary and the next step read it once instead of
    recomputing every earlier step.
    """
    cfg = ctx.settings.get("SCOPE_ADJUSTMENTS", {})
    enabled = _enabled_adjustments(cfg)

    ctx.scope_adjustments_applied = False
    ctx.scope_before_adjustments = None
    ctx.scope_adjustment_steps = []

    if not enabled:
        return

    scope = ctx.hybrid_scope_keys
    ctx.scope_before_adjustments = scope.cache()
    ctx.scope_adjustments_applied = True

    # Distinct (product, store) pairs with daily data in the window, for single-key adjustments: a
    # join_keys=["product_id"] addition expands to every store selling the product.
    needs_pairs = any(
        ("store_id" in ctx.scope_keys and "store_id" not in adj["join_keys"] and "product_id" in adj["join_keys"])
        or ("product_id" in ctx.scope_keys and "product_id" not in adj["join_keys"] and "store_id" in adj["join_keys"])
        for adj in enabled
    )
    scope_pairs: Optional[DataFrame] = None
    if needs_pairs:
        scope_pairs = get_daily_data_raw(ctx).select("product_id", "store_id").distinct().cache()

    print("=" * 72)
    print("SCOPE ADJUSTMENTS — scope BEFORE additions/removals")
    print_scope_summary("Base scope (hybrid or defined-only)", ctx.scope_before_adjustments, ctx.scope_keys)

    # Counted on scope_keys only (not scope_origin): a key already in scope must not read as new coverage
    # because a later step claims it under another origin.
    before_count = scope.select(*ctx.scope_keys).distinct().count()
    for adj in enabled:
        path = _resolve_adjustment_path(ctx, adj, fund_paste)
        source = _adjustment_source(adj, path)
        action = adj["action"]
        previous = scope

        if action == "addition":
            label = adj.get("label", "manual_add")
            add_keys = _read_adjustment_keys(ctx, adj, path, scope_pairs).withColumn("scope_origin", F.lit(label))
            # A key keeps the FIRST origin it was claimed under: keys already in scope are anti-joined out
            # before the union, or the key would get a second row under this label.
            new_keys = add_keys.join(scope.select(*ctx.scope_keys).distinct(), on=ctx.scope_keys, how="left_anti")
            scope = scope.unionByName(new_keys.select(*scope.columns)).distinct().cache()
        else:
            removal_keys = _read_adjustment_keys(ctx, adj, path, scope_pairs)
            scope = _anti_join_by_keys(scope, removal_keys, adj["join_keys"]).distinct().cache()
        after_count = scope.select(*ctx.scope_keys).distinct().count()
        if previous is not ctx.scope_before_adjustments:
            previous.unpersist()

        step = {"action": action}
        if action == "addition":
            step["label"] = label
        step.update(
            {
                "source": source,
                "path": path,
                "join_keys": adj["join_keys"],
                "rows_before": before_count,
                "rows_after": after_count,
                "rows_delta": after_count - before_count,
            }
        )
        print("-" * 72)
        if action == "addition":
            print(f"ADDITION '{label}' from {source}: {path}")
            print(f"  join_keys: {adj['join_keys']} | rows added (net): {step['rows_delta']:,}")
        else:
            print(f"REMOVAL from {source}: {path}")
            print(f"  join_keys: {adj['join_keys']} | rows removed: {before_count - after_count:,}")

        ctx.scope_adjustment_steps.append(step)
        print_scope_summary(f"Scope AFTER {action}", scope, ctx.scope_keys)
        before_count = after_count

    if scope_pairs is not None:
        scope_pairs.unpersist()
    # Every step ends with distinct(), so the last step's cached frame is the final scope as is.
    ctx.hybrid_scope_keys = scope
    print("=" * 72)
    print("SCOPE ADJUSTMENTS — FINAL scope used for KPIs")
    print_scope_summary("Final scope", ctx.hybrid_scope_keys, ctx.scope_keys)
    print("=" * 72)

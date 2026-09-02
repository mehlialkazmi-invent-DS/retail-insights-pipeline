"""Defined scope, score-based scope, hybrid union, and manual scope adjustments."""

from __future__ import annotations

import datetime
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import (
    get_daily_data_raw,
    read_csv_source,
    read_defined_scope_source,
    rename_column_or_fail,
)


def _window_weeks(ctx: KPIContext) -> DataFrame:
    """Fiscal weeks overlapping [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]."""
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    return ctx.fiscal_week.filter(
        (F.col("week_start_date") <= F.lit(end)) & (F.col("week_end_date") >= F.lit(start))
    )


def _defined_scope_pairs(ctx: KPIContext, raw: DataFrame) -> DataFrame:
    """Distinct scope universe at the configured grain, without any time column."""
    config = ctx.settings["DEFINED_SCOPE"]
    sel = [F.col(config["product_col"]).alias("product_id")]
    if "store_id" in ctx.scope_keys:
        sel.append(F.col(config["store_col"]).alias("store_id"))
    return raw.select(*sel).distinct()


def _defined_scope_weekly(ctx: KPIContext, raw: DataFrame) -> DataFrame:
    """Honour the scope table's own (product, store, Year, Week) rows, window-filtered.

    Used for the ``product_store_week`` grain. Weeks are resolved from ``date_col`` via
    fiscal_cal, or from native ``year_col``/``week_col``.
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
    else:
        year_col, week_col = config.get("year_col"), config.get("week_col")
        keyed = raw.select(
            *sel,
            F.col(year_col).cast("int").alias("Year"),
            F.col(week_col).cast("int").alias("Week"),
        ).distinct()
    window_yw = _window_weeks(ctx).select("Year", "Week").distinct()
    return keyed.join(broadcast(window_yw), on=["Year", "Week"], how="inner").select(*ctx.scope_keys).distinct()


def build_defined_scope(ctx: KPIContext) -> None:
    """Read the scope table into defined_scope_keys at the configured grain.

    ``defined_scope.grain`` controls how the scope table defines membership:
      * ``"product"``            -> distinct product_id; store- and week-agnostic.
      * ``"product_store"``      -> distinct (product_id, store_id); week-agnostic.
      * ``"product_store_week"`` -> the scope table's own (product, store, week) rows, honoured.

    For the week-agnostic grains (product, product_store) the pairs are applied to EVERY week
    in the report window, so a pair scoped in any period counts for the whole window. For
    product_store_week the scope table's own weeks are honoured.
    Downstream always consumes (product[, store], Year, Week) keys.
    """
    config = ctx.settings["DEFINED_SCOPE"]
    grain = config["grain"]
    has_store = grain in ("product_store", "product_store_week")
    ctx.scope_keys = (
        ["product_id", "store_id", "Year", "Week"] if has_store else ["product_id", "Year", "Week"]
    )

    raw = read_defined_scope_source(ctx.spark, ctx.settings, quiet=True)

    if grain == "product_store_week":
        ctx.defined_scope_keys = _defined_scope_weekly(ctx, raw).cache()
    else:
        pairs = _defined_scope_pairs(ctx, raw)
        window_yw = _window_weeks(ctx).select("Year", "Week").distinct()
        ctx.defined_scope_keys = (
            pairs.crossJoin(broadcast(window_yw)).select(*ctx.scope_keys).distinct().cache()
        )

    print(f"defined scope grain: {grain} | keys: {ctx.scope_keys} | count: {ctx.defined_scope_keys.count()}")


def read_daily_for_scope(ctx: KPIContext, start_date: datetime.date, end_date: datetime.date) -> DataFrame:
    """Daily sales/inventory for scope building (score scope and adjustment pair expansion).

    Includes ALL stores — scope membership is store-agnostic so that total sales/revenue/
    inventory cover every store. Any store you want excluded from specific metrics should be
    filtered via input_filters.daily_data (see config.py) rather than baked in here.
    """
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
    """Flag in-scope weeks per pair using percentile thresholds on sales and weekly inventory.

    weekly_inventory uses the last available daily snapshot in the fiscal week
    (max_by on date), not Saturday-only — avoids false zero when week_end_date is missing.
    """
    # Equi-join on date via fiscal_cal (point lookup) — avoids the range join on week bounds.
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
    # Window functions compute per-pair thresholds in one pass — no separate groupBy + join.
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
    """Finalise scope keys, optionally backfilling weeks the defined scope does not cover.

    Not hybrid (default): final scope = ``defined_scope_keys`` as built at the configured grain.

    Hybrid: window weeks the defined scope does NOT cover (missing weeks) are backfilled from
    score scope (per-(product, store) sales/inventory activity percentile, computed once over the
    full window). For the week-agnostic grains (product, product_store) the defined scope already
    covers every window week, so there are no missing weeks and the backfill is a no-op; it is
    meaningful for the product_store_week grain, whose covered weeks are only those present in the
    scope table.
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
    """Load an adjustment table from Delta or CSV.

    CSV files may live in the datastore (default) or a Databricks workspace path —
    set ``"location": "workspace"`` on the adjustment to read a /Workspace/... CSV.
    """
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
    """Resolve an adjustment table to scope_keys grain for the current report window.

    scope_pairs is a cached distinct (product_id, store_id) frame used for single-key
    expansions — computed once in apply_scope_adjustments and reused across all steps.
    """
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
    """Anti-join ``scope`` against ``removal_keys`` on the adjustment's own ``join_keys``,
    restricted to whichever of those columns the resolved scope grain actually carries, plus
    Year/Week (present on both once resolved). ``_read_adjustment_keys`` has already
    cascaded/collapsed ``removal_keys`` to the scope's own grain -- e.g. a product-only removal
    (``join_keys=["product_id"]``) under a product_store-grain scope deliberately drops the
    store_id that removal_keys picked up while cascading via scope_pairs, so the anti-join matches
    (and removes) every store of that product, not just the ones with recorded daily activity in
    scope_pairs (see apply_scope_adjustments's scope_pairs comment for the addition-side analog).

    A join_keys column the scope grain lacks entirely (e.g. store_id under
    defined_scope.grain="product") has no sound anti-join translation here -- silently falling
    back to a Year/Week-only match would strike every row in those weeks regardless of
    product/store. Fails loudly instead.
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
    """Apply configured manual scope additions and removals to the final scope."""
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

    # Compute the distinct active pairs once; reused by any single-key adjustment expansions.
    # read_daily_for_scope includes ALL stores, so join_keys=["product_id"] additions expand to
    # every store selling the product -- every metric uses all scoped stores (see config.py),
    # so there's no separate downstream exclusion for these pairs to fall under.
    needs_pairs = any(
        ("store_id" in ctx.scope_keys and "store_id" not in adj["join_keys"] and "product_id" in adj["join_keys"])
        or ("product_id" in ctx.scope_keys and "product_id" not in adj["join_keys"] and "store_id" in adj["join_keys"])
        for adj in enabled
    )
    scope_pairs: Optional[DataFrame] = None
    if needs_pairs:
        start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
        scope_pairs = read_daily_for_scope(ctx, start, end).select("product_id", "store_id").distinct().cache()

    print("=" * 72)
    print("SCOPE ADJUSTMENTS — scope BEFORE additions/removals")
    print_scope_summary("Base scope (hybrid or defined-only)", ctx.scope_before_adjustments, ctx.scope_keys)

    for adj in enabled:
        path = _resolve_adjustment_path(ctx, adj, fund_paste)
        source = _adjustment_source(adj, path)
        action = adj["action"]
        # Counted on scope_keys only (not the full row incl. scope_origin) — a key already in
        # scope under one origin must not read as "new coverage" just because a later step
        # claims it under a different origin label.
        before_count = scope.select(*ctx.scope_keys).distinct().count()

        if action == "addition":
            label = adj.get("label", "manual_add")
            add_keys = _read_adjustment_keys(ctx, adj, path, scope_pairs).withColumn("scope_origin", F.lit(label))
            # Keep the FIRST origin a key was claimed under — anti-join out keys already in
            # scope before unioning, so a key present under e.g. "defined" doesn't also get a
            # second physical row under this addition's label (scope.distinct() dedupes whole
            # rows, so two different scope_origin values for the same key would otherwise both
            # survive, double-counting that key in scope_summary_by_origin and in rows_delta).
            new_keys = add_keys.join(scope.select(*ctx.scope_keys).distinct(), on=ctx.scope_keys, how="left_anti")
            scope = scope.unionByName(new_keys.select(*scope.columns)).distinct()
            after_count = scope.select(*ctx.scope_keys).distinct().count()
            step = {
                "action": action,
                "label": label,
                "source": source,
                "path": path,
                "join_keys": adj["join_keys"],
                "rows_before": before_count,
                "rows_after": after_count,
                "rows_delta": after_count - before_count,
            }
            print("-" * 72)
            print(f"ADDITION '{label}' from {source}: {path}")
            print(f"  join_keys: {adj['join_keys']} | rows added (net): {step['rows_delta']:,}")
        else:
            removal_keys = _read_adjustment_keys(ctx, adj, path, scope_pairs)
            scope = _anti_join_by_keys(scope, removal_keys, adj["join_keys"]).distinct()
            after_count = scope.select(*ctx.scope_keys).distinct().count()
            step = {
                "action": action,
                "source": source,
                "path": path,
                "join_keys": adj["join_keys"],
                "rows_before": before_count,
                "rows_after": after_count,
                "rows_delta": after_count - before_count,
            }
            print("-" * 72)
            print(f"REMOVAL from {source}: {path}")
            print(f"  join_keys: {adj['join_keys']} | rows removed: {before_count - after_count:,}")

        ctx.scope_adjustment_steps.append(step)
        print_scope_summary(f"Scope AFTER {action}", scope, ctx.scope_keys)

    ctx.hybrid_scope_keys = scope.distinct().cache()
    print("=" * 72)
    print("SCOPE ADJUSTMENTS — FINAL scope used for KPIs")
    print_scope_summary("Final scope", ctx.hybrid_scope_keys, ctx.scope_keys)
    print("=" * 72)

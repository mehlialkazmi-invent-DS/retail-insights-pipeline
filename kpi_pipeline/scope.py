"""Defined scope, score-based scope, hybrid union, and manual scope adjustments."""

from __future__ import annotations

import datetime
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import get_daily_data_raw, read_defined_scope_source


def _window_weeks(ctx: KPIContext) -> DataFrame:
    """Fiscal weeks overlapping [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]."""
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    return ctx.fiscal_week.filter(
        (F.col("week_start_date") <= F.lit(end)) & (F.col("week_end_date") >= F.lit(start))
    )


def _filter_scope_to_window(scope: DataFrame, ctx: KPIContext) -> DataFrame:
    start, end = ctx.settings["EFFECTIVE_REPORT_START_DATE"], ctx.settings["REPORT_END_DATE"]
    if "week_start_date" in scope.columns and "week_end_date" in scope.columns:
        return scope.filter(
            (F.col("week_start_date") <= F.lit(end)) & (F.col("week_end_date") >= F.lit(start))
        )
    return scope.join(
        broadcast(_window_weeks(ctx).select("Year", "Week")),
        on=["Year", "Week"],
        how="inner",
    )


def read_defined_scope(ctx: KPIContext) -> DataFrame:
    """Load defined scope and attach Year/Week via date_col or native year/week columns."""
    config = ctx.settings["DEFINED_SCOPE"]
    fiscal_cal_in = ctx.fiscal_cal
    fiscal_week_in = ctx.fiscal_week

    date_col = config.get("date_col")
    if date_col is None:
        year_col, week_col = config.get("year_col"), config.get("week_col")
        if not year_col or not week_col:
            raise ValueError(
                "defined_scope requires date_col OR both year_col and week_col; "
                f"got date_col=None with year_col={year_col!r}, week_col={week_col!r}."
            )

    raw = read_defined_scope_source(ctx.spark, ctx.settings, quiet=True)
    sel = [F.col(config["product_col"]).alias("product_id")]
    store_col = config.get("store_col")
    if store_col is not None:
        sel.append(F.col(store_col).alias("store_id"))

    fw_cols = ["Year", "Week", "Year_Week", "week_start_date", "week_end_date"]

    if date_col is not None:
        print(f"defined-scope time resolution: via date->fiscal_cal (date_col={date_col})")
        sel.append(F.to_date(F.col(date_col)).alias("scope_date"))
        scope = raw.select(*sel).distinct()
        scope = scope.join(
            broadcast(fiscal_cal_in.select(F.col("date").alias("scope_date"), "Year", "Week")),
            on="scope_date",
            how="inner",
        )
        scope = scope.drop("scope_date").join(broadcast(fiscal_week_in.select(*fw_cols)), on=["Year", "Week"], how="inner")
    else:
        year_col, week_col = config["year_col"], config["week_col"]
        print(f"defined-scope time resolution: native year/week (Year<-{year_col}, Week<-{week_col})")
        sel += [F.col(year_col).cast("int").alias("Year"), F.col(week_col).cast("int").alias("Week")]
        scope = raw.select(*sel).distinct()
        scope = scope.join(broadcast(fiscal_week_in.select(*fw_cols)), on=["Year", "Week"], how="inner")

    return _filter_scope_to_window(scope, ctx)


def build_defined_scope(ctx: KPIContext) -> None:
    ctx.defined_scope = read_defined_scope(ctx).cache()
    scope_has_store = "store_id" in ctx.defined_scope.columns
    ctx.scope_keys = (
        ["product_id", "store_id", "Year", "Week"]
        if scope_has_store
        else ["product_id", "Year", "Week"]
    )
    if not scope_has_store:
        print("NOTE: defined scope has no store grain -> falling back to product-week scoping downstream.")

    ctx.defined_scope_psw = ctx.defined_scope.select(*ctx.scope_keys).distinct().cache()
    print("defined_scope_psw grain:", ctx.scope_keys, "| count:", ctx.defined_scope_psw.count())


def read_daily_for_scope(ctx: KPIContext, start_date: datetime.date, end_date: datetime.date) -> DataFrame:
    """Daily sales/inventory for score scope; excludes e-com stores."""
    excluded = ctx.settings["EXCLUDED_STORE_IDS_FOR_SERVICE_METRICS"]
    time_cols = ctx.settings["DAILY_TIME_COLUMNS"]
    date_col = time_cols["date"]
    return (
        get_daily_data_raw(ctx)
        .select("product_id", "store_id", date_col, "sales_revenue", "sales_quantity", "inventory")
        .withColumn(date_col, F.to_date(F.col(date_col)))
        .withColumnRenamed(date_col, "date")
        .filter(F.col("date").between(F.lit(start_date), F.lit(end_date)))
        .filter(~F.col("store_id").isin(excluded))
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


def score_scope_psw(ctx: KPIContext, daily_in: DataFrame) -> DataFrame:
    """Product-store-week keys that pass the score filter (in_scope='yes')."""
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
    """Build final scope: defined-only, or defined + score backfill when hybrid is enabled.

    Score scope is computed ONCE over the full report window — per-pair percentile thresholds
    use all available weeks. The hybrid backfill is that same full-window score scope restricted
    to the fiscal weeks missing from defined scope, so the defined-vs-score diff exactly mirrors
    what the backfill contributes.
    """
    s = ctx.settings
    start, end = s["EFFECTIVE_REPORT_START_DATE"], s["REPORT_END_DATE"]
    use_hybrid = s["USE_HYBRID_SCOPE"]
    run_scope_diff = s.get("RUN_SCOPE_DIFF", False)
    need_score = use_hybrid or run_scope_diff

    ctx.score_only_psw = None
    if need_score:
        daily_all = read_daily_for_scope(ctx, start, end).cache()
        ctx.score_only_psw = score_scope_psw(ctx, daily_all).select(*ctx.scope_keys).distinct().cache()

    defined_tagged = ctx.defined_scope_psw.withColumn("scope_origin", F.lit("defined"))

    if not use_hybrid:
        ctx.hybrid_scope_psw = defined_tagged.cache()
        print("scope mode: defined only (hybrid disabled)")
        print("grain:", ctx.scope_keys, "| final_scope:", ctx.hybrid_scope_psw.count())
        return

    defined_weeks = ctx.defined_scope_psw.select("Year", "Week").distinct()
    window_weeks = _window_weeks(ctx).select("Year", "Week").distinct()
    missing_weeks = window_weeks.join(broadcast(defined_weeks), on=["Year", "Week"], how="left_anti").cache()

    # Backfill = full-window score scope (thresholds over all weeks) restricted to the missing weeks.
    score_backfill = ctx.score_only_psw.join(
        broadcast(missing_weeks), on=["Year", "Week"], how="left_semi"
    ).withColumn("scope_origin", F.lit("score"))
    ctx.hybrid_scope_psw = defined_tagged.unionByName(score_backfill).cache()

    print("scope mode: hybrid (defined + score backfill)")
    print("grain:", ctx.scope_keys)
    print("defined_weeks:", defined_weeks.count(), "| missing_weeks:", missing_weeks.count())
    print("final_scope:", ctx.hybrid_scope_psw.count())


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
    """Load an adjustment table from Delta or CSV."""
    source = _adjustment_source(adj_cfg, path)
    if source == "csv":
        csv_opts = adj_cfg.get("csv_options") or {}
        reader = ctx.spark.read.option("header", str(csv_opts.get("header", True)).lower())
        if csv_opts.get("inferSchema", True):
            reader = reader.option("inferSchema", "true")
        for key, value in csv_opts.items():
            if key not in {"header", "inferSchema"}:
                reader = reader.option(key, value)
        print(f"scope adjustment source: csv ({path})")
        return reader.csv(path)
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
    has_time = "Year" in removal_keys.columns and "Week" in removal_keys.columns

    if has_time and join_keys == ["product_id"]:
        return scope.join(removal_keys.select("product_id", "Year", "Week").distinct(), on=["product_id", "Year", "Week"], how="left_anti")
    if has_time and join_keys == ["store_id"]:
        return scope.join(removal_keys.select("store_id", "Year", "Week").distinct(), on=["store_id", "Year", "Week"], how="left_anti")
    if has_time and join_keys == ["product_id", "store_id"]:
        return scope.join(
            removal_keys.select("product_id", "store_id", "Year", "Week").distinct(),
            on=["product_id", "store_id", "Year", "Week"],
            how="left_anti",
        )

    if join_keys == ["product_id"]:
        return scope.join(removal_keys.select("product_id").distinct(), on="product_id", how="left_anti")
    if join_keys == ["store_id"]:
        return scope.join(removal_keys.select("store_id").distinct(), on="store_id", how="left_anti")
    if join_keys == ["product_id", "store_id"]:
        return scope.join(removal_keys.select("product_id", "store_id").distinct(), on=["product_id", "store_id"], how="left_anti")
    if set(join_keys) == set(["product_id", "store_id", "Year", "Week"]):
        return scope.join(removal_keys.distinct(), on=join_keys, how="left_anti")
    raise ValueError(f"Unsupported removal join_keys: {join_keys}")


def apply_scope_adjustments(ctx: KPIContext, fund_paste: Optional[Callable[..., str]] = None) -> None:
    """Apply configured manual scope additions and removals to the final scope."""
    cfg = ctx.settings.get("SCOPE_ADJUSTMENTS", {})
    enabled = _enabled_adjustments(cfg)

    ctx.scope_adjustments_applied = False
    ctx.scope_before_adjustments = None
    ctx.scope_adjustment_steps = []

    if not enabled:
        return

    scope = ctx.hybrid_scope_psw
    ctx.scope_before_adjustments = scope.cache()
    ctx.scope_adjustments_applied = True

    # Compute the distinct active pairs once; reused by any single-key adjustment expansions.
    # NOTE: read_daily_for_scope excludes e-com stores (excluded_store_ids). Adjustments using
    # join_keys=["product_id"] will therefore not expand to e-com stores even if those stores
    # sell the product. This is intentional — e-com stores are excluded from service metrics scope.
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
        before_count = scope.distinct().count()

        if action == "addition":
            label = adj.get("label", "manual_add")
            add_keys = _read_adjustment_keys(ctx, adj, path, scope_pairs).withColumn("scope_origin", F.lit(label))
            scope = scope.unionByName(add_keys.select(*scope.columns)).distinct()
            after_count = scope.count()
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
            after_count = scope.count()
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

    ctx.hybrid_scope_psw = scope.distinct().cache()
    print("=" * 72)
    print("SCOPE ADJUSTMENTS — FINAL scope used for KPIs")
    print_scope_summary("Final scope", ctx.hybrid_scope_psw, ctx.scope_keys)
    print("=" * 72)

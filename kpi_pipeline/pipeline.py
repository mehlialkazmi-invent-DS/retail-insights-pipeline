"""Pipeline input frames for a given scope."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from kpi_pipeline.context import KPIContext
from kpi_pipeline.inputs import (
    DEFAULT_LOST_SALES_COLUMN_MAP,
    get_daily_data_raw,
    read_instock_source,
    read_lost_sales_source,
    read_speed_cluster_source,
    rename_column_or_fail,
)


def _enrich_lost_sales_with_time_grain(ctx: KPIContext, lost_sales_pair: DataFrame) -> DataFrame:
    fw_cols = ["week_start_date", "week_end_date", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Month"]
    if ctx.settings["USE_FISCAL_CALENDAR"]:
        return lost_sales_pair.join(broadcast(ctx.fiscal_week.select(*fw_cols)), on="week_start_date", how="inner")
    if "_ls_year" in lost_sales_pair.columns and "_ls_week" in lost_sales_pair.columns:
        enriched = (
            lost_sales_pair.withColumn("Year", F.col("_ls_year"))
            .withColumn("Week", F.col("_ls_week"))
            .drop("_ls_year", "_ls_week")
        )
    else:
        enriched = lost_sales_pair.join(
            broadcast(ctx.fiscal_cal.select(F.col("date").alias("week_start_date"), "Year", "Week")),
            on="week_start_date",
            how="inner",
        )
    return enriched.drop("week_start_date").join(broadcast(ctx.fiscal_week.select(*fw_cols)), on=["Year", "Week"], how="inner")


def _aggregate_lost_sales_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """Aggregate ONE raw lost-sales model to (product_id, store_id, week_start_date) grain.

    lost_sales/in_stock/total_days source column names come from LOST_SALES_COLUMN_MAP
    (see config.py's lost_sales_source). in_stock/total_days are skipped here entirely
    when INSTOCK_SOURCE_ENABLED — they come from the separate instock_source table
    instead (see read_lost_sales_weekly).
    """
    col_map = ctx.settings.get("LOST_SALES_COLUMN_MAP") or DEFAULT_LOST_SALES_COLUMN_MAP
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
    agg_exprs = [F.sum(F.col(col_map["lost_sales_col"]).cast("double")).alias("lost_sales")]
    if not ctx.settings.get("INSTOCK_SOURCE_ENABLED", False):
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
    # is an absolute count, so a pair-week without store_id gets broadcast across every scoped
    # store of that product downstream (read_lost_sales_weekly's join), which OVER-COUNTS if
    # later summed across stores. Only safe for in_stock/total_days (ratios), not lost_sales.
    group_keys = ["product_id", "week_start_date"]
    if "store_id" in raw.columns:
        group_keys.insert(1, "store_id")
    return filtered.groupBy(*group_keys).agg(*agg_exprs)


def _aggregate_instock_pairweek(ctx: KPIContext, raw: DataFrame, start, end) -> DataFrame:
    """Aggregate the standalone instock_source table to (product_id[, store_id], week_start_date).

    ``raw`` is read_instock_source's output, already on canonical in_stock/total_days columns
    (including any fallback_sources already appended -- see read_instock_source).

    Grouped without store_id when the source has none (store_col=None in
    INSTOCK_SOURCE_COLUMN_MAP, e.g. reporting_inv_fc_dfu/report_dfu) -- read_lost_sales_weekly's
    join then broadcasts this pair-week's value across every scoped store of that product
    instead of requiring an exact (product, store, week) match. Safe here: in_stock_days/
    total_days are a ratio, and summing the same broadcast value across a product's stores then
    dividing reproduces the original ratio (numerator and denominator scale identically).
    """
    filtered = (
        raw.withColumn("week_start_date", F.to_date("week_start_date"))
        .filter(F.col("week_start_date").between(F.lit(start), F.lit(end)))
    )
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

    instock_source.enabled=True (mutually exclusive with the ensemble above): in_stock_days
    and total_days are read from a separate table instead of PATH_LOST_SALES, left-joined
    onto the lost-sales pair-weeks by (product_id, store_id, week_start_date).
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

    if s.get("INSTOCK_SOURCE_ENABLED", False):
        instock_raw = read_instock_source(ctx.spark, s, quiet=True)
        instock_agg = _aggregate_instock_pairweek(ctx, instock_raw, start, end)
        # store_id only goes in the join key when BOTH sides actually have it -- a real
        # per-store match. If only one side has it (either direction: instock_agg store-less
        # while deduped isn't, or vice versa), dropping store_id from the join key lets the
        # regular (non-semi) join fan out/broadcast using whichever side does have it: that
        # side's store_id survives as a plain pass-through column on the result, same
        # mechanism as the same-direction broadcast documented in _aggregate_instock_pairweek.
        # If NEITHER side has it, deduped simply stays store-less (handled downstream in
        # build_pipeline_frames). Checking only instock_agg's side here would crash the join
        # outright when instock_agg has store_id but deduped doesn't (on= requires the column
        # to exist on both sides).
        instock_join_keys = ["product_id", "week_start_date"]
        if "store_id" in instock_agg.columns and "store_id" in deduped.columns:
            instock_join_keys.insert(1, "store_id")
        deduped = deduped.join(instock_agg, on=instock_join_keys, how="left")

    ctx.lost_sales_weekly_base = _enrich_lost_sales_with_time_grain(ctx, deduped).withColumn(
        "fiscal_week_days",
        F.datediff(F.col("week_end_date"), F.col("week_start_date")) + 1,
    ).cache()
    return ctx.lost_sales_weekly_base


def build_scoped_daily(ctx: KPIContext, scope_core: DataFrame, scope_pairs_in: DataFrame, has_store: bool) -> DataFrame:
    """Daily sales/inventory for scoped pairs, with product cost/price and fiscal week attributes."""
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
            broadcast(ctx.fiscal_week.select("Year", "Week", "Year_Week", "week_start_date", "Fiscal_Quarter", "Fiscal_Month")),
            on=["Year", "Week"],
            how="inner",
        )
    )


def build_pipeline_frames(ctx: KPIContext, scope_in: DataFrame) -> Dict[str, DataFrame]:
    """Build scoped_daily, inst_data, lost_base, and scope helper frames for one scope variant.

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
    if has_store and "store_id" not in lost_sales_raw.columns:
        # lost_sales_source has no per-store dimension (e.g. report_dfu, store_col=None) --
        # left_semi can't attach a store_id column that was never on lost_sales_raw, so this
        # broadcasts its product-week value across every scoped store instead: a regular join
        # on the non-store keys fans lost_sales_raw's single row out to one row per matching
        # (product, store, week) in scope_core, picking up store_id from scope_core's side.
        broadcast_keys = [k for k in scope_keys if k != "store_id"]
        lost_sales_weekly = lost_sales_raw.join(scope_core, on=broadcast_keys, how="inner").cache()
    else:
        lost_sales_weekly = lost_sales_raw.join(scope_core, on=scope_keys, how="left_semi").cache()

    # ls_has_store: whether lost_sales_weekly ended up with its own store_id -- independent of
    # has_store (scope's own grain). Under has_store=True this is always True (native, or
    # broadcast-attached from scope_core just above). Under has_store=False (product grain) it
    # reflects lost_sales_source.store_col directly: True if lost_sales_source is genuinely
    # per-store (store granularity comes FROM lost-sales, scope itself has none), False if
    # lost_sales_source is ALSO store-less -- a legitimate, fully-supported pure product-grain
    # combination (see the else branches of scope_pair_weeks/weekly_sales_for_lost/lost_base
    # below), not an error: nothing downstream needs a store dimension when neither scope nor
    # lost-sales has one.
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

    # Fall back to the fiscal week's day-count only when total_days can legitimately be missing
    # for reasons unrelated to instock_source (e.g. a null in the lost-sales table itself). Under
    # instock_source.enabled=True, a null total_days specifically means "no matching row in the
    # separate instock table" -- coalescing it to a full week would silently pad available_days
    # for an unmatched pair-week instead of excluding it via the filter below, deflating
    # in_stock_rate/weighted_instock_rate for exactly the coverage gaps instock_source expects.
    available_days_expr = (
        F.col("total_days")
        if ctx.settings.get("INSTOCK_SOURCE_ENABLED", False)
        else F.coalesce(F.col("total_days"), F.col("fiscal_week_days"))
    )
    inst_select_cols = ["product_id", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Month"]
    if ls_has_store:
        inst_select_cols.insert(1, "store_id")
    inst_data = (
        lost_sales_weekly.select(
            *inst_select_cols,
            F.col("in_stock_days").alias("stocked_pairs"),
            available_days_expr.alias("available_days"),
        )
        .filter(F.col("available_days") > 0)
        .join(ctx.product_dims, on="product_id", how="left")
    ).cache()

    scoped_daily = build_scoped_daily(ctx, scope_core, scope_pairs, has_store).cache()
    weekly_pair = scoped_daily.groupBy("product_id", "store_id", "Year", "Week").agg(
        F.sum("sales_quantity").alias("weekly_sales")
    )
    lost_base_keys = ["product_id", "Year", "Week"]
    lost_base_select_cols = ["product_id", "Year", "Week", "Year_Week", "Fiscal_Quarter", "Fiscal_Month", "lost_sales"]
    if ls_has_store:
        lost_base_keys.insert(1, "store_id")
        lost_base_select_cols.insert(1, "store_id")
        weekly_sales_for_lost = (
            weekly_pair.join(scope_pair_weeks, on=lost_base_keys, how="left_semi")
            .select(*lost_base_keys, F.col("weekly_sales").alias("sales_quantity_weekly"))
        )
    else:
        # No store dimension anywhere (scope AND lost_sales_source both store-less): roll
        # weekly_pair (still per-store, straight from daily_data) UP to product-week first --
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
    }

"""Read and preview pipeline input tables with optional config filters."""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def resolve_csv_path(path: str, location: str = "datastore") -> str:
    """Resolve a CSV path for Spark based on where the file physically lives.

    location:
      - "datastore" (default): a cloud / DBFS path under the datastore mount
        (e.g. /mnt/invent-{customer}-datastore/...). Used as-is.
      - "workspace": a Databricks **workspace** file (e.g. /Workspace/Users/...).
        Spark reads workspace files through the ``file:`` scheme, so the prefix is
        added when missing.
    """
    loc = (location or "datastore").strip().lower()
    if loc not in {"datastore", "workspace"}:
        raise ValueError(f"csv location must be 'datastore' or 'workspace'; got {location!r}")
    if loc == "workspace" and not path.startswith("file:"):
        return "file:" + path if path.startswith("/") else "file:/" + path
    return path


def read_csv_source(
    spark: SparkSession,
    path: str,
    csv_options: Optional[Dict[str, Any]] = None,
    location: str = "datastore",
) -> DataFrame:
    """Read a CSV from either the datastore or a Databricks workspace path.

    ``csv_options`` mirrors Spark CSV reader options. ``header`` (default True) and
    ``inferSchema`` (default True) are applied as booleans; any other keys are passed
    through verbatim. Use ``location`` to read workspace-resident CSVs (see
    :func:`resolve_csv_path`).
    """
    opts = dict(csv_options or {})
    resolved = resolve_csv_path(path, location)
    reader = spark.read.option("header", str(opts.get("header", True)).lower())
    if opts.get("inferSchema", True):
        reader = reader.option("inferSchema", "true")
    for key, value in opts.items():
        if key not in {"header", "inferSchema"}:
            reader = reader.option(key, value)
    print(f"  csv source ({location}): {resolved}")
    return reader.csv(resolved)


def _input_filters(settings: Dict[str, Any], source: str) -> List[str]:
    return list(settings.get("INPUT_FILTERS", {}).get(source, []) or [])


def apply_input_filters(df: DataFrame, expressions: List[str], source_name: str, quiet: bool = False) -> DataFrame:
    for expr in expressions:
        expr = expr.strip()
        if not expr:
            continue
        df = df.filter(expr)
        if not quiet:
            print(f"  applied {source_name} filter: {expr}")
    return df


def read_defined_scope_source(
    spark: SparkSession, settings: Dict[str, Any], quiet: bool = False
) -> DataFrame:
    path = settings["DEFINED_SCOPE"]["path"]
    filters = _input_filters(settings, "defined_scope")
    if not quiet:
        print(f"reading defined_scope: {path}")
    raw = spark.read.format("delta").load(path)
    if filters and not quiet:
        print(f"defined_scope filters ({len(filters)}):")
    return apply_input_filters(raw, filters, "defined_scope", quiet=quiet)


def _print_date_range(df: DataFrame, date_col: str, label: str) -> None:
    """Always printed (independent of the read's own ``quiet`` logging flag) — the date span
    actually present in a main input source is worth surfacing on every run, not just verbose ones."""
    if date_col not in df.columns:
        return
    parsed = F.to_date(F.col(date_col))
    bounds = df.agg(F.min(parsed).alias("min"), F.max(parsed).alias("max")).collect()[0]
    print(f"  {label} date range in source ({date_col}): {bounds['min']} to {bounds['max']}")


def rename_column_or_fail(df: DataFrame, source_col: str, canonical: str, config_key: str) -> DataFrame:
    """Rename source_col -> canonical, failing loudly if source_col isn't actually a column.

    Plain ``withColumnRenamed`` silently no-ops when the source column doesn't exist --
    instead of erroring at the point of misconfiguration, it hides the problem until some
    later, unrelated operation (e.g. a groupBy several calls downstream) finally references
    the canonical name and fails with a confusing "column not found" pointing at the wrong
    line. Every config-driven rename in this pipeline should go through this helper instead.

    Verifies the column exists even when source_col already equals canonical (no rename
    needed) -- "already correctly named" is still a claim about the source's actual schema,
    not something to assume without checking.
    """
    if source_col not in df.columns:
        raise ValueError(
            f"{config_key}={source_col!r} not found on the source table; "
            f"available columns: {sorted(df.columns)}"
        )
    return df.withColumnRenamed(source_col, canonical)


def _rename_join_keys_to_canonical(df: DataFrame, col_map: Dict[str, Any]) -> DataFrame:
    """Rename a source's own product/store/week columns to the pipeline's canonical
    names (product_id, store_id, week_start_date) so every downstream reader can keep
    assuming those names regardless of what a client's raw table calls them.

    product_col and store_col are each optional: a source with no per-store dimension sets
    store_col to None and the resulting frame simply has no store_id column at all; a source
    with no native product-id-level column at all sets product_col to None, relying entirely
    on product_agg_level_col's join (_map_product_agg_level_to_product_id, called before this)
    to have already produced product_id.

    Configure exactly one of product_col / product_agg_level_col per source, never both: if
    product_col is set AND actually present on the source, it always takes precedence over
    product_agg_level_col (see _map_product_agg_level_to_product_id's own no-op check) --
    leaving both set is misleading, not additive, since the agg-level join would silently
    never fire.
    """
    product_col = col_map.get("product_col")
    if product_col:
        df = rename_column_or_fail(df, product_col, "product_id", "product_col")
    elif "product_id" not in df.columns:
        raise ValueError(
            "product_col is not set and no product_id column was produced from "
            "product_agg_level_col -- configure exactly one of product_col / "
            "product_agg_level_col."
        )
    df = rename_column_or_fail(df, col_map["week_col"], "week_start_date", "week_col")
    store_col = col_map.get("store_col")
    if store_col:
        df = rename_column_or_fail(df, store_col, "store_id", "store_col")
    return df


def _map_product_agg_level_to_product_id(
    spark: SparkSession,
    df: DataFrame,
    col_map: Dict[str, Any],
    settings: Dict[str, Any],
    quiet: bool = False,
) -> DataFrame:
    """Backfill product_id from product_agg_level when a source is keyed by planning/DFU level
    instead of product_id (e.g. a table where that key is genuinely one-to-many with
    product_id) -- mirrors kpi-skill-toolkit's own product_agg_level fallback (same
    product_planning_level table, same planning_level_id rename, same inner join).

    product_col is a precedence flag as much as a rename target: this is a no-op whenever
    product_col is configured (non-None) AND actually present on the source -- product_col
    always wins, even if product_agg_level_col also happens to be set. It also no-ops when
    product_agg_level_col isn't configured at all (nothing to fall back to). It only actually
    fires -- and does the join -- when product_col is None/absent-from-source AND
    product_agg_level_col is configured. Configure exactly one of the two per source; if
    product_agg_level_col IS explicitly configured (in the firing branch), the configured
    column must actually exist -- unlike "not configured", a wrong explicit value is a real
    misconfiguration and should fail loudly here rather than silently no-op and surface as a
    confusing missing-product_id error several calls downstream.
    """
    product_col = col_map.get("product_col")
    agg_col = col_map.get("product_agg_level_col")
    if (product_col and product_col in df.columns) or not agg_col:
        return df
    if agg_col not in df.columns:
        raise ValueError(
            f"product_agg_level_col={agg_col!r} not found on the source table; "
            f"available columns: {sorted(df.columns)}"
        )
    if not quiet:
        print(f"  mapping {agg_col!r} -> product_id via product_planning_level")
    mapping = (
        spark.read.format("delta")
        .load(settings["PATH_PRODUCT_PLANNING_LEVEL"])
        .select(F.col("planning_level_id").alias(agg_col), "product_id")
        .distinct()
    )
    return df.join(mapping, on=agg_col, how="inner")


def read_lost_sales_source(
    spark: SparkSession, settings: Dict[str, Any], path: Optional[str] = None, quiet: bool = False
) -> DataFrame:
    path = path or settings["PATH_LOST_SALES"]
    col_map = settings["LOST_SALES_COLUMN_MAP"]
    filters = _input_filters(settings, "lost_sales")
    if not quiet:
        print(f"reading lost_sales: {path}")
    raw = spark.read.format("delta").load(path)
    raw = _map_product_agg_level_to_product_id(spark, raw, col_map, settings, quiet=quiet)
    raw = _rename_join_keys_to_canonical(raw, col_map)
    if filters and not quiet:
        print(f"lost_sales filters ({len(filters)}):")
    out = apply_input_filters(raw, filters, "lost_sales", quiet=quiet)
    _print_date_range(out, "week_start_date", "lost_sales")
    return out


def read_instock_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    """In-stock table of instock.weekly_source (only read when instock.method is "weekly_source").

    Renamed to canonical product_id/[store_id/]week_start_date/in_stock/total_days columns,
    regardless of what the source calls them (see INSTOCK_SOURCE_COLUMN_MAP).

    fallback_sources (optional): additional column-sets read from the SAME table -- e.g.
    report_dfu's LY_/LLY_ columns, which carry the same in_stock/total_days formula for the
    calendar week exactly 52/104 weeks before each row's own TY_ week (see README) -- appended
    in listed order to fill in weeks the primary column-set doesn't have. A fallback never
    overrides a (product[, store], week) the primary (or an earlier fallback) already covered;
    it only fills genuinely missing weeks. Safe because in_stock/total_days is a ratio and both
    sources compute it the same way for any real week they both happen to cover.
    """
    path = settings["PATH_INSTOCK_SOURCE"]
    col_map = settings["INSTOCK_SOURCE_COLUMN_MAP"]
    if not quiet:
        print(f"reading instock.weekly_source: {path}")
    raw = spark.read.format("delta").load(path)

    def _build(cm: Dict[str, Any]) -> DataFrame:
        df = _map_product_agg_level_to_product_id(spark, raw, cm, settings, quiet=quiet)
        df = _rename_join_keys_to_canonical(df, cm)
        select_cols = ["product_id", "week_start_date"]
        if "store_id" in df.columns:
            select_cols.insert(1, "store_id")
        return df.select(
            *select_cols,
            F.col(cm["in_stock_col"]).alias("in_stock"),
            F.col(cm["total_days_col"]).alias("total_days"),
        )

    combined = _build(col_map)
    key_cols = [c for c in ("product_id", "store_id", "week_start_date") if c in combined.columns]
    for fallback_map in col_map.get("fallback_sources", []) or []:
        if not quiet:
            print(f"  appending instock fallback source (week_col={fallback_map['week_col']!r})")
        fallback = _build(fallback_map)
        combined = combined.unionByName(fallback.join(combined.select(*key_cols), on=key_cols, how="left_anti"))
    return combined


def read_speed_cluster_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    """One row per product_id with its numeric sales-speed cluster.

    Supports two source table shapes via SPEED_CLUSTER_FORMAT:
      "long" (default) - a long-format attributes table (one row per product_id x
          attribute_name); filtered to SPEED_CLUSTER_ATTRIBUTE_NAME, attribute_value is
          the cluster. This is the platform's noob/product-cluster-attributes-snapshot shape.
      "wide" - the cluster is already its own column (SPEED_CLUSTER_VALUE_COL) on a
          table with one row per product_id.
    """
    path = settings["PATH_SPEED_CLUSTER"]
    fmt = settings.get("SPEED_CLUSTER_FORMAT", "long")
    raw = spark.read.format("delta").load(path)
    if fmt == "wide":
        value_col = settings["SPEED_CLUSTER_VALUE_COL"]
        if not quiet:
            print(f"reading speed_cluster: {path} (wide, value_col == {value_col!r})")
        return (
            raw.select("product_id", F.col(value_col).cast("int").alias("sales_speed_cluster"))
            .dropDuplicates(["product_id"])
        )
    attr = settings["SPEED_CLUSTER_ATTRIBUTE_NAME"]
    if not quiet:
        print(f"reading speed_cluster: {path} (long, attribute_name == {attr!r})")
    return (
        raw.filter(F.col("attribute_name") == attr)
        .select("product_id", F.col("attribute_value").cast("int").alias("sales_speed_cluster"))
        .dropDuplicates(["product_id"])
    )


def read_daily_data_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    path = settings["PATH_DAILY_DATA"]
    filters = _input_filters(settings, "daily_data")
    if not quiet:
        print(f"reading daily_data: {path}")
    raw = spark.read.format("delta").load(path)
    if filters and not quiet:
        print(f"daily_data filters ({len(filters)}):")
    out = apply_input_filters(raw, filters, "daily_data", quiet=quiet)
    _print_date_range(out, settings["DAILY_TIME_COLUMNS"]["date"], "daily_data")
    return out


def get_daily_data_raw(ctx) -> DataFrame:
    """Cached daily-data read (config filters applied once per run), item-family-rolled to
    parent product_id when ITEM_FAMILY_ROLLUP["daily_data"] is True (default -- see config.py).

    Without this, build_scoped_daily's own join to already-parent-rolled scope_core/
    ctx.products_attr (pipeline.py) silently DROPS any daily-data row still carrying a
    child/superseded product_id, since the join has no matching parent-only key for it -- a
    pre-existing bug this default-on rollup fixes. Applied once here (not at each of this
    function's call sites) since every consumer (build_scoped_daily, scope.py's
    read_daily_for_scope, fiscal.py) should see the same parent-rolled id space scope_core
    itself is already in.

    Deliberately does NOT re-aggregate after the mapping, unlike
    pipeline._get_inventory_warehouse_parent_rolled: a child and its parent both having a row on
    one date is harmless here, since every consumer either sums those rows (sales, inventory
    totals) or groups by date before averaging (metrics._mean_stock_frame) or uses countDistinct.
    inventory_warehouse must re-aggregate because build_dc_inst joins it onto a per-pair date grid,
    where a duplicate key would fan out grid rows and inflate dc_available_days.
    """
    if ctx.daily_data_raw is None:
        raw = read_daily_data_source(ctx.spark, ctx.settings, quiet=True)
        if ctx.settings["ITEM_FAMILY_ROLLUP"]["daily_data"]:
            from kpi_pipeline.pipeline import _roll_to_item_family_parent

            raw = _roll_to_item_family_parent(raw, ctx)
        ctx.daily_data_raw = raw.cache()
    return ctx.daily_data_raw


def get_daily_data_excluded_days(ctx) -> DataFrame:
    """Cached distinct (product_id, store_id, date) days in the report window on which
    input_filters.daily_data removes a daily-data row (a row failing, or null on, any filter
    expression), rolled to the family main like get_daily_data_raw. Empty when daily_data has no filters.

    build_scoped_daily uses it to drop goods-in-transit-only days whose daily row was filtered out
    (e.g. unusable days, usable = 1) instead of letting them re-enter as zero-sales inventory days.
    Scope-independent, so it is built once per run and shared by every scope variant. The expressions
    run on the raw product_id before the roll-up, as in read_daily_data_source.
    """
    if ctx.daily_data_excluded_days is None:
        s = ctx.settings
        date_col = s["DAILY_TIME_COLUMNS"]["date"]
        start = s["EFFECTIVE_REPORT_START_DATE"]
        day_after_end = s["REPORT_END_DATE"] + datetime.timedelta(days=1)
        kept = F.lit(True)
        for expr in _input_filters(s, "daily_data"):
            if expr.strip():
                kept = kept & F.expr(expr)
        removed = (
            ctx.spark.read.format("delta")
            .load(s["PATH_DAILY_DATA"])
            .filter(
                (F.col(date_col) >= F.lit(start.isoformat())) & (F.col(date_col) < F.lit(day_after_end.isoformat()))
            )
            .filter(~F.coalesce(kept, F.lit(False)))
            .select("product_id", "store_id", F.to_date(F.col(date_col)).alias("date"))
        )
        if s["ITEM_FAMILY_ROLLUP"]["daily_data"]:
            from kpi_pipeline.pipeline import _roll_to_item_family_parent

            removed = _roll_to_item_family_parent(removed, ctx)
        ctx.daily_data_excluded_days = removed.distinct().cache()
    return ctx.daily_data_excluded_days


def read_inventory_warehouse_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    """DC/warehouse daily inventory table -- plain product_id/warehouse_id/date/inventory
    columns, no column-mapping needed (unlike lost_sales_source/instock.weekly_source)."""
    path = settings["PATH_INVENTORY_WAREHOUSE"]
    filters = _input_filters(settings, "inventory_warehouse")
    if not quiet:
        print(f"reading inventory_warehouse: {path}")
    raw = spark.read.format("delta").load(path)
    if filters and not quiet:
        print(f"inventory_warehouse filters ({len(filters)}):")
    out = apply_input_filters(raw, filters, "inventory_warehouse", quiet=quiet)
    _print_date_range(out, "date", "inventory_warehouse")
    return out


def get_inventory_warehouse_raw(ctx) -> DataFrame:
    """Cached inventory_warehouse read (config filters applied once per run)."""
    if ctx.inventory_warehouse_raw is None:
        ctx.inventory_warehouse_raw = read_inventory_warehouse_source(ctx.spark, ctx.settings, quiet=True).cache()
    return ctx.inventory_warehouse_raw


def read_item_family_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    """Parent/child item-family map -- rolls superseded child products (is_main=false) onto
    their parent product_id (see README's "dc_instock" section). Read unconditionally whenever
    inventory_warehouse is configured; is_main=false filtering happens downstream in pipeline.py."""
    path = settings["PATH_ITEM_FAMILY"]
    col_map = settings["ITEM_FAMILY_COLUMN_MAP"]
    filters = _input_filters(settings, "item_family")
    if not quiet:
        print(f"reading item_family: {path}")
    raw = spark.read.format("delta").load(path)
    if filters and not quiet:
        print(f"item_family filters ({len(filters)}):")
    filtered = apply_input_filters(raw, filters, "item_family", quiet=quiet)
    renamed = rename_column_or_fail(filtered, col_map["product_col"], "product_id", "item_family_source.product_col")
    renamed = rename_column_or_fail(renamed, col_map["parent_col"], "parent_id", "item_family_source.parent_col")
    renamed = rename_column_or_fail(renamed, col_map["is_main_col"], "is_main", "item_family_source.is_main_col")
    return renamed.select("product_id", "parent_id", "is_main")


def get_item_family_raw(ctx) -> DataFrame:
    """Cached item_family read (config filters applied once per run)."""
    if ctx.item_family_raw is None:
        ctx.item_family_raw = read_item_family_source(ctx.spark, ctx.settings, quiet=True).cache()
    return ctx.item_family_raw


def read_operation_scope_source(
    spark: SparkSession, settings: Dict[str, Any], run_date, solution_ids: List[int], location_col: str
) -> DataFrame:
    """Platform scope table (operation/scope): one solution's rows for one run_date that are still
    open on it (end_date null or >= run_date), as (product_id, <location_col>, start_date).
    location_id is the store for a store solution and the warehouse for a DC solution.

    Fails loudly when the solution has no rows for run_date -- a wrong solution_id or run_date must
    not silently produce an empty report.
    """
    path = settings["PATH_SCOPE"]
    print(f"reading operation scope: {path} (solution_id in {solution_ids}, run_date={run_date})")
    rows = (
        spark.read.format("delta").load(path)
        .filter(F.col("solution_id").isin(solution_ids) & (F.col("run_date") == F.lit(run_date)))
        .filter(F.col("end_date").isNull() | (F.col("end_date") >= F.lit(run_date)))
        .select("product_id", F.col("location_id").alias(location_col), F.to_date("start_date").alias("start_date"))
    )
    if rows.limit(1).count() == 0:
        raise ValueError(
            f"No operation scope rows for solution_id in {solution_ids} run_date={run_date} at {path}; "
            "check the solution_id setting and scope_source.run_date."
        )
    return rows


def read_active_product_ids(spark: SparkSession, settings: Dict[str, Any]) -> DataFrame:
    return (
        spark.read.format("delta").load(settings["PATH_PRODUCTS"])
        .filter(F.col("is_active") == True)  # noqa: E712
        .select("product_id")
        .distinct()
    )


# Source key columns of each UI blocked-scope kind. destination_id is the location: the store in
# blocked_scope, the warehouse in dc_blocked_scope (renamed per caller, see blocked_scope_keys).
BLOCKED_SCOPE_KINDS = {
    "product": ["product_id"],
    "product_destination": ["product_id", "destination_id"],
    "destination": ["destination_id"],
}


def blocked_scope_keys(kind: str, location_col: str) -> List[str]:
    """Join keys of one blocked-scope kind, destination_id named location_col (store_id / warehouse_id)."""
    return [location_col if c == "destination_id" else c for c in BLOCKED_SCOPE_KINDS[kind]]


def read_blocked_scope_source(
    spark: SparkSession, folder: str, solution_ids: List[int], kind: str, location_col: str
) -> DataFrame:
    """One kind of a UI blocked-scope snapshot folder ({ui_parameters_path}/blocked_scope or
    {ui_parameters_path}/dc_blocked_scope, then /{kind}) for one solution, as
    (blocked_scope_keys(kind, location_col), block_start, block_end). Fails loudly (Spark read error)
    when the folder is missing.
    """
    path = f"{folder}/{kind}"
    print(f"reading blocked scope {kind} (solution_id in {solution_ids}): {path}")
    blocks = (
        spark.read.parquet(path)
        .filter(F.col("solution_id").isin(solution_ids))
        .select(
            *[
                F.col(source).cast("int").alias(key)
                for source, key in zip(BLOCKED_SCOPE_KINDS[kind], blocked_scope_keys(kind, location_col))
            ],
            F.to_date("start_date").alias("block_start"),
            F.to_date("end_date").alias("block_end"),
        )
    )
    if blocks.filter(F.col("block_start").isNull()).limit(1).count() > 0:
        raise ValueError(f"blocked scope {kind} has rows without start_date at {path}")
    return blocks


def read_goods_in_transit_source(spark: SparkSession, settings: Dict[str, Any]) -> DataFrame:
    """Raw operation/goods_in_transit (snapshot per date; destination_type 0 = store, 1 = warehouse)."""
    path = settings["PATH_GOODS_IN_TRANSIT"]
    print(f"reading goods_in_transit: {path}")
    return spark.read.format("delta").load(path)


def get_instock_daily_raw(ctx) -> DataFrame:
    """Daily-data read for the daily in-stock metric: product_id, store_id, date, inventory and
    an is_usable flag (usable == 1, null counts as unusable), limited to history_start..report end.

    The range is filtered on the raw date column, before to_date, so Delta file pruning applies.
    Not cached here: build_instock_daily caches the frame after its scope join.

    Deliberately NOT the input_filters.daily_data read that get_daily_data_raw uses: that filter
    typically drops unusable days (usable = 1), but the daily in-stock method needs to see them so
    they can leave the store-day denominator. Not item-family-rolled here either: noob/daily-data is
    already rolled to the family main upstream, the same id space as the rolled scope pairs.
    """
    s = ctx.settings
    date_col = s["DAILY_TIME_COLUMNS"]["date"]
    history_start = s["INSTOCK_DAILY"]["history_start"] or s["EFFECTIVE_REPORT_START_DATE"]
    day_after_end = s["REPORT_END_DATE"] + datetime.timedelta(days=1)
    raw = ctx.spark.read.format("delta").load(s["PATH_DAILY_DATA"])
    return (
        raw.filter((F.col(date_col) >= F.lit(history_start.isoformat())) & (F.col(date_col) < F.lit(day_after_end.isoformat())))
        .select(
            "product_id",
            "store_id",
            F.to_date(F.col(date_col)).alias("date"),
            "inventory",
            (F.coalesce(F.col("usable"), F.lit(0)) == 1).alias("is_usable"),
        )
    )


def preview_input_table(
    df: DataFrame,
    settings: Dict[str, Any],
    name: str,
    limit: int = 20,
    date_col: Optional[str] = None,
) -> DataFrame:
    """Print counts and return a sample restricted to the report date window when possible."""
    sample = df
    if date_col and date_col in df.columns:
        start, end = settings["EFFECTIVE_REPORT_START_DATE"], settings["REPORT_END_DATE"]
        sample = df.withColumn(date_col, F.to_date(F.col(date_col))).filter(
            F.col(date_col).between(F.lit(start), F.lit(end))
        )
        print(f"{name}: {sample.count():,} rows in report window ({start} -> {end}) on `{date_col}`")
    else:
        print(f"{name}: {df.count():,} rows after config filters")
    print(f"  showing up to {limit} rows:")
    return sample.limit(limit)

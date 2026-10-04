"""Read and preview pipeline input tables with optional config filters."""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

def resolve_csv_path(path: str, location: str = "datastore") -> str:
    """CSV path for Spark: "datastore" (default) paths are used as-is; "workspace" paths (a Databricks
    /Workspace/... file) get the ``file:`` scheme Spark needs to read them."""
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
    """Read a CSV from the datastore or a workspace path (see resolve_csv_path). ``header`` and
    ``inferSchema`` default to True; other ``csv_options`` pass through to the Spark reader."""
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
    return list(settings["INPUT_FILTERS"].get(source) or [])


def apply_input_filters(df: DataFrame, expressions: List[str], source_name: str, quiet: bool = False) -> DataFrame:
    for expr in expressions:
        expr = expr.strip()
        if not expr:
            continue
        df = df.filter(expr)
        if not quiet:
            print(f"  applied {source_name} filter: {expr}")
    return df


def _print_date_range(df: DataFrame, date_col: str, label: str) -> None:
    """Print the date span present in a main input source (on every run, whatever ``quiet`` says)."""
    parsed = F.to_date(F.col(date_col))
    bounds = df.agg(F.min(parsed).alias("min"), F.max(parsed).alias("max")).collect()[0]
    print(f"  {label} date range in source ({date_col}): {bounds['min']} to {bounds['max']}")


def rename_column_or_fail(df: DataFrame, source_col: str, canonical: str, config_key: str) -> DataFrame:
    """Rename source_col -> canonical, failing when source_col is not a column (withColumnRenamed silently
    no-ops, which surfaces later as a confusing missing column). Checked even when no rename is needed."""
    if source_col not in df.columns:
        raise ValueError(
            f"{config_key}={source_col!r} not found on the source table; "
            f"available columns: {sorted(df.columns)}"
        )
    return df.withColumnRenamed(source_col, canonical)


def _rename_join_keys_to_canonical(df: DataFrame, col_map: Dict[str, Any]) -> DataFrame:
    """Rename a source's product / store / week columns to product_id / store_id / week_start_date.

    store_col None: the frame has no store_id. product_col None: product_id must already come from
    product_agg_level_col (_map_product_agg_level_to_product_id). Configure exactly one of product_col /
    product_agg_level_col: a present product_col always wins, so setting both is misleading.
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
    """Map a source keyed by planning / DFU level to product_id via product_planning_level (inner join on
    planning_level_id). No-op when product_col is set and present on the source, or product_agg_level_col
    is unset; a configured product_agg_level_col missing from the source fails.
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
    """instock.weekly_source's table as product_id[, store_id], week_start_date, in_stock, total_days.

    fallback_sources: further column sets of the same table (e.g. report_dfu's LY_ / LLY_ columns, the same
    formula for the week 52 / 104 weeks earlier), appended in order to fill only the (product[, store],
    week) keys no earlier set has; a covered week is never overridden.
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
    """One row per product_id with its integer sales_speed_cluster. SPEED_CLUSTER_FORMAT "long" (default): an
    attributes table filtered to SPEED_CLUSTER_ATTRIBUTE_NAME (attribute_value is the cluster); "wide": the
    cluster is the SPEED_CLUSTER_VALUE_COL column."""
    path = settings["PATH_SPEED_CLUSTER"]
    fmt = settings["SPEED_CLUSTER_FORMAT"]
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


def _gross_sales_by_day(ctx) -> DataFrame:
    """operation/transactional_sales as (product_id, store_id, _gross_date, gross_revenue, gross_quantity):
    every row of the report window that is not a return (sales_type != "return"; return rows are stored
    positive), rolled to the family main like daily-data and summed per product x store x day.

    The window filter is on the raw date column, so Delta file pruning applies; only the five columns it
    needs are read. Rolled before the sum because daily-data is in the family-main id space.
    """
    s = ctx.settings
    day_after_end = s["REPORT_END_DATE"] + datetime.timedelta(days=1)
    gross = (
        ctx.spark.read.format("delta")
        .load(s["PATH_TRANSACTIONAL_SALES"])
        .filter(
            (F.col("date") >= F.lit(s["EFFECTIVE_REPORT_START_DATE"].isoformat()))
            & (F.col("date") < F.lit(day_after_end.isoformat()))
        )
        .filter(F.col("sales_type") != "return")
        .select("product_id", "store_id", F.to_date(F.col("date")).alias("_gross_date"), "sales_revenue", "sales_quantity")
    )
    if s["ITEM_FAMILY_ROLLUP"]["daily_data"]:
        gross = roll_to_item_family_parent(gross, ctx)
    return gross.groupBy("product_id", "store_id", "_gross_date").agg(
        F.sum(F.col("sales_revenue").cast("double")).alias("gross_revenue"),
        F.sum(F.col("sales_quantity").cast("double")).alias("gross_quantity"),
    )


def _with_gross_sales(daily: DataFrame, ctx) -> DataFrame:
    """``daily`` with sales_revenue / sales_quantity replaced by the gross (non-return) transactional sales of
    the same product x store x day, 0 where the day has none. Daily rows stay as they are, and a transactional
    day with no daily row is not added.

    With the daily-data family roll-up, a child and its parent on the same date are summed into one row first,
    so the gross of that day attaches once instead of once per row.
    """
    s = ctx.settings
    date_col = s["DAILY_TIME_COLUMNS"]["date"]
    if s["ITEM_FAMILY_ROLLUP"]["daily_data"]:
        measures = ("sales_revenue", "sales_quantity", "inventory")
        daily = daily.groupBy(*[c for c in daily.columns if c not in measures]).agg(
            *[F.sum(m).alias(m) for m in measures]
        )
    return (
        daily.withColumn("_gross_date", F.to_date(F.col(date_col)))
        .join(_gross_sales_by_day(ctx), on=["product_id", "store_id", "_gross_date"], how="left")
        .withColumn("sales_revenue", F.coalesce(F.col("gross_revenue"), F.lit(0.0)))
        .withColumn("sales_quantity", F.coalesce(F.col("gross_quantity"), F.lit(0.0)))
        .drop("_gross_date", "gross_revenue", "gross_quantity")
    )


def get_daily_data_raw(ctx) -> DataFrame:
    """Cached daily-data for the report window: input_filters.daily_data applied, rows dated outside
    [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE] dropped, rolled to the family main when
    ITEM_FAMILY_ROLLUP["daily_data"] is True, and only the columns its readers use. With sales_basis "gross"
    its sales_revenue / sales_quantity are the non-return transactional sales (_with_gross_sales), so every
    reader follows the basis.

    Every reader (build_scoped_daily, scope.read_daily_for_scope,
    fiscal.build_time_grain_from_daily_data) keeps only window dates itself, so the window filter and the
    column selection drop nothing they read; they only keep the cache small.

    The roll-up matters because scope_core and products_attr are already in the family-main id space: a
    child product_id would drop out of build_scoped_daily's joins. Under "net" a child and its parent on the
    same date are not re-aggregated here; every reader sums those rows, groups by date first or counts distinct.
    """
    if ctx.daily_data_raw is None:
        s = ctx.settings
        time_cols = s["DAILY_TIME_COLUMNS"]
        columns = ["product_id", "store_id", time_cols["date"], "sales_revenue", "sales_quantity", "inventory"]
        if not s["USE_FISCAL_CALENDAR"]:
            columns.append(time_cols["week"])
        raw = read_daily_data_source(ctx.spark, s, quiet=True).filter(
            F.to_date(F.col(time_cols["date"])).between(
                F.lit(s["EFFECTIVE_REPORT_START_DATE"]), F.lit(s["REPORT_END_DATE"])
            )
        )
        if s["ITEM_FAMILY_ROLLUP"]["daily_data"]:
            raw = roll_to_item_family_parent(raw, ctx)
        raw = raw.select(*columns)
        if s["SALES_BASIS"] == "gross":
            raw = _with_gross_sales(raw, ctx)
        ctx.daily_data_raw = raw.cache()
    return ctx.daily_data_raw


def get_daily_data_excluded_days(ctx) -> DataFrame:
    """Cached distinct (product_id, store_id, date) window days whose daily-data row input_filters.daily_data
    removes (fails, or is null on, a filter), rolled to the family main like get_daily_data_raw; empty
    without filters. build_scoped_daily drops goods-in-transit-only days on them, so a filtered-out day
    (e.g. usable = 1) does not re-enter as a zero-sales inventory day. Scope-independent, built once per run.
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
            removed = roll_to_item_family_parent(removed, ctx)
        ctx.daily_data_excluded_days = removed.distinct().cache()
    return ctx.daily_data_excluded_days


def read_inventory_warehouse_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    """DC (warehouse) daily inventory: product_id, warehouse_id, date, inventory."""
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


def read_item_family_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    """Item-family map (product_id, parent_id, is_main): superseded children roll onto their parent."""
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


def roll_to_item_family_parent(df: DataFrame, ctx) -> DataFrame:
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


def scope_run_date(settings: Dict[str, Any]) -> datetime.date:
    """scope.run_date, or the latest Sunday on or before today when unset."""
    configured = settings["SCOPE"]["run_date"]
    if configured is not None:
        return configured
    today = datetime.date.today()
    return today - datetime.timedelta(days=(today.weekday() + 1) % 7)


def read_scope_source(
    spark: SparkSession,
    settings: Dict[str, Any],
    solution_ids: Optional[List[int]],
    location_col: str,
    quiet: bool = False,
) -> DataFrame:
    """The scope table's rows, renamed to product_id, <location_col> (the store or the warehouse), start_date
    (columns.start, as a date), and for time="weekly" scope_date (columns.date, as a date) or Year / Week
    (columns.year / columns.week, as ints); only the configured columns are kept.

    Rows are limited to solution_ids (columns.solution), to the scope run_date (columns.run_date), and to
    those still open on it (columns.end null or >= run_date); input_filters.scope applies on top. Fails when
    a solution / run_date filter leaves no row: a wrong solution_id or run_date must not give an empty report.
    """
    cfg = settings["SCOPE"]
    cols = cfg["columns"]
    path = cfg["path"]
    filters = _input_filters(settings, "scope")
    run_date = scope_run_date(settings)
    if not quiet:
        detail = f" (solution_id in {solution_ids}, run_date={run_date})" if cols["solution"] or cols["run_date"] else ""
        print(f"reading scope: {path}{detail}")
        if filters:
            print(f"scope filters ({len(filters)}):")
    rows = apply_input_filters(spark.read.format("delta").load(path), filters, "scope", quiet=quiet)
    if cols["solution"]:
        rows = rows.filter(F.col(cols["solution"]).isin(solution_ids))
    if cols["run_date"]:
        rows = rows.filter(F.col(cols["run_date"]) == F.lit(run_date))
    if cols["end"]:
        rows = rows.filter(F.col(cols["end"]).isNull() | (F.col(cols["end"]) >= F.lit(run_date)))
    select = [F.col(cols["product"]).alias("product_id")]
    if cols["store"]:
        select.append(F.col(cols["store"]).alias(location_col))
    if cols["start"]:
        select.append(F.to_date(cols["start"]).alias("start_date"))
    if cfg["time"] == "weekly":
        if cols["date"]:
            select.append(F.to_date(cols["date"]).alias("scope_date"))
        else:
            select += [F.col(cols["year"]).cast("int").alias("Year"), F.col(cols["week"]).cast("int").alias("Week")]
    rows = rows.select(*select)
    if (cols["solution"] or cols["run_date"]) and rows.limit(1).count() == 0:
        raise ValueError(
            f"No scope rows for solution_id in {solution_ids} run_date={run_date} at {path}; "
            "check scope.solution_id and scope.run_date."
        )
    return rows


def read_active_product_ids(spark: SparkSession, settings: Dict[str, Any]) -> DataFrame:
    return (
        spark.read.format("delta").load(settings["PATH_PRODUCTS"])
        .filter(F.col("is_active") == True)  # noqa: E712
        .select("product_id")
        .distinct()
    )


# Source key columns of each UI blocked-scope kind; destination_id is the store (blocked_scope) or the
# warehouse (dc_blocked_scope).
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
    """{folder}/{kind} of a UI blocked-scope snapshot for the solution(s), as (blocked_scope_keys(kind,
    location_col), block_start, block_end). A missing folder fails the Spark read; a row without
    start_date fails here."""
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
    """noob/daily-data for the daily in-stock metric: product_id, store_id, date, inventory and is_usable
    (usable == 1; null is unusable), from instock.daily.history_start to the report end, filtered on the
    raw date column so Delta file pruning applies. Not cached here (build_instock_daily caches it after
    its scope join).

    Not get_daily_data_raw: input_filters.daily_data typically drops unusable days, which the daily
    in-stock method must see to take them out of the store-days. Not rolled to the family main:
    noob/daily-data is already in the family-main id space.
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

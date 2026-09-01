"""Read and preview pipeline input tables with optional config filters."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Fallback defaults when a settings dict predates lost_sales_source/instock_source (e.g. a
# customer config that duplicates config.py's schema instead of importing it) and so lacks
# these resolved keys entirely — reproduces the schema every pipeline read has always assumed.
DEFAULT_LOST_SALES_COLUMN_MAP: Dict[str, Any] = {
    "week_col": "week_start_date",
    "product_col": "product_id",
    "store_col": "store_id",
    "lost_sales_col": "lost_sales",
    "in_stock_col": "in_stock",
    "total_days_col": "details.total_days",
    "product_agg_level_col": None,
}
DEFAULT_INSTOCK_SOURCE_COLUMN_MAP: Dict[str, Any] = {
    "week_col": "week_start_date",
    "product_col": "product_id",
    "store_col": "store_id",
    "in_stock_col": "in_stock",
    "total_days_col": "total_days",
    "product_agg_level_col": None,
    "fallback_sources": [],
}


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


def _rename_join_keys_to_canonical(df: DataFrame, col_map: Dict[str, Any]) -> DataFrame:
    """Rename a source's own product/store/week columns to the pipeline's canonical
    names (product_id, store_id, week_start_date) so every downstream reader can keep
    assuming those names regardless of what a client's raw table calls them.

    store_col is optional: a source with no per-store dimension (e.g. reporting_inv_fc_dfu/
    report_dfu's lost_sales/instock columns, aggregated to product_agg_level x week only) sets
    it to None, and the resulting frame simply has no store_id column at all.
    """
    renames = {
        col_map["product_col"]: "product_id",
        col_map["week_col"]: "week_start_date",
    }
    store_col = col_map.get("store_col")
    if store_col:
        renames[store_col] = "store_id"
    for source_col, canonical in renames.items():
        if source_col != canonical:
            df = df.withColumnRenamed(source_col, canonical)
    return df


def _map_product_agg_level_to_product_id(
    spark: SparkSession,
    df: DataFrame,
    col_map: Dict[str, Any],
    settings: Dict[str, Any],
    quiet: bool = False,
) -> DataFrame:
    """Backfill product_id from product_agg_level when a source is keyed by planning/DFU level
    instead of product_id (e.g. reporting_inv_fc_dfu/report_dfu) -- mirrors kpi-skill-toolkit's
    own product_agg_level fallback. A no-op when product_col is already on the source (even if
    product_agg_level_col happens to be configured) or when product_agg_level_col isn't
    configured/present -- so this only ever fires when actually needed.
    """
    product_col = col_map["product_col"]
    agg_col = col_map.get("product_agg_level_col")
    if product_col in df.columns or not agg_col or agg_col not in df.columns:
        return df
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
    col_map = settings.get("LOST_SALES_COLUMN_MAP") or DEFAULT_LOST_SALES_COLUMN_MAP
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
    """In-stock table for the instock_source override (only read when enabled).

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
    col_map = settings.get("INSTOCK_SOURCE_COLUMN_MAP") or DEFAULT_INSTOCK_SOURCE_COLUMN_MAP
    if not quiet:
        print(f"reading instock_source: {path}")
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
    """Cached daily-data read (config filters applied once per run)."""
    if ctx.daily_data_raw is None:
        ctx.daily_data_raw = read_daily_data_source(ctx.spark, ctx.settings, quiet=True).cache()
    return ctx.daily_data_raw


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

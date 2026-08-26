"""Read and preview pipeline input tables with optional config filters."""

from __future__ import annotations

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


def read_lost_sales_source(
    spark: SparkSession, settings: Dict[str, Any], path: Optional[str] = None, quiet: bool = False
) -> DataFrame:
    path = path or settings["PATH_LOST_SALES"]
    filters = _input_filters(settings, "lost_sales")
    if not quiet:
        print(f"reading lost_sales: {path}")
    raw = spark.read.format("delta").load(path)
    if filters and not quiet:
        print(f"lost_sales filters ({len(filters)}):")
    return apply_input_filters(raw, filters, "lost_sales", quiet=quiet)


def read_speed_cluster_source(spark: SparkSession, settings: Dict[str, Any], quiet: bool = False) -> DataFrame:
    """One row per product_id with its numeric sales-speed cluster (long-format attrs table)."""
    path = settings["PATH_SPEED_CLUSTER"]
    attr = settings["SPEED_CLUSTER_ATTRIBUTE_NAME"]
    if not quiet:
        print(f"reading speed_cluster: {path} (attribute_name == {attr!r})")
    raw = spark.read.format("delta").load(path)
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
    return apply_input_filters(raw, filters, "daily_data", quiet=quiet)


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

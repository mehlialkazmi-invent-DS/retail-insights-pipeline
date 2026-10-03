"""Pre-flight scope check: distinct product / store counts, overall and per slice."""

from __future__ import annotations

import pandas as pd
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.kpi_long import _apply_value_filter


def scope_universe_counts(ctx: KPIContext) -> pd.DataFrame:
    """Distinct product, store and pair counts of ctx.hybrid_scope_keys: one "overall" row, then one row per
    (active slice dimension, value) with SLICE_VALUE_FILTERS applied as in kpi_long. NULL values show as
    "NULL". Store and pair counts only with a store grain. Raises before build_scopes / build_dimensions.
    """
    if ctx.hybrid_scope_keys is None:
        raise RuntimeError(
            "scope_universe_counts needs ctx.hybrid_scope_keys — call "
            "runner.build_scopes() (or runner.run()) first."
        )
    if ctx.product_dims is None:
        raise RuntimeError(
            "scope_universe_counts needs ctx.product_dims — call "
            "runner.build_dimensions() (or runner.run()) first."
        )

    has_store = "store_id" in ctx.scope_keys
    key_cols = ["product_id", "store_id"] if has_store else ["product_id"]
    pairs = ctx.hybrid_scope_keys.select(*key_cols).distinct()

    def count_exprs():
        exprs = [F.countDistinct("product_id").alias("distinct_product_count")]
        if has_store:
            exprs.append(F.countDistinct("store_id").alias("distinct_store_count"))
            exprs.append(F.countDistinct("product_id", "store_id").alias("distinct_pair_count"))
        return exprs

    overall = (
        pairs.agg(*count_exprs())
        .withColumn("dimension", F.lit("overall"))
        .withColumn("dimension_value", F.lit("ALL"))
    )

    value_filters = ctx.settings.get("SLICE_VALUE_FILTERS", {}) or {}
    enriched = pairs.join(ctx.product_dims, on="product_id", how="left")

    combined = overall
    for dim in ctx.active_slice_dimensions:
        df = enriched
        if dim in value_filters:
            df = _apply_value_filter(df, dim, value_filters[dim])
        by_value = (
            df.groupBy(F.coalesce(F.col(dim).cast("string"), F.lit("NULL")).alias("dimension_value"))
            .agg(*count_exprs())
            .withColumn("dimension", F.lit(dim))
        )
        combined = combined.unionByName(by_value)

    out_cols = ["dimension", "dimension_value", "distinct_product_count"]
    if has_store:
        out_cols += ["distinct_store_count", "distinct_pair_count"]

    pdf = combined.select(*out_cols).toPandas()
    pdf["_overall_first"] = (pdf["dimension"] != "overall").astype(int)
    pdf = (
        pdf.sort_values(["_overall_first", "dimension", "dimension_value"])
        .drop(columns="_overall_first")
        .reset_index(drop=True)
    )
    return pdf

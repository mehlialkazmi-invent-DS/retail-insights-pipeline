"""Pre-flight scope debug: distinct product/store counts overall and per slice.

Lightweight sanity check to run after build_scopes and before the expensive
build_kpis. Reuses the kpi_long value filter so per-slice counts match what the
KPI step reports for the same slice.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.kpi_long import _apply_value_filter


def scope_universe_counts(ctx: KPIContext) -> pd.DataFrame:
    """Distinct product/store/pair counts for the final scope, overall and per slice.

    Counts are taken from ``ctx.hybrid_scope_keys`` (the final scope after hybrid
    backfill and manual adjustments). Per-slice rows join the distinct scope pairs
    to ``ctx.product_dims`` and apply the same ``SLICE_VALUE_FILTERS`` the KPI step
    applies, so the debug counts match what ``build_kpi_long`` reports for that slice.

    NULL slice values are shown as the string ``"NULL"`` for readability; the same
    rows carry an empty/None ``dimension_value`` in ``kpi_long``.

    :param ctx: pipeline context with ``hybrid_scope_keys`` (from build_scopes) and
        ``product_dims`` (from build_dimensions) populated.
    :return: one ``overall`` row plus one row per (active slice dimension, value),
        sorted with ``overall`` first then by dimension and value. Columns:
        ``dimension``, ``dimension_value``, ``distinct_product_count`` and — only when
        the scope has store grain — ``distinct_store_count``, ``distinct_pair_count``.
    :rtype: pandas.DataFrame
    :raises RuntimeError: if scope or product dimensions have not been built yet.
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

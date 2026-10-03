"""Pre-flight scope check: distinct product / store counts, overall and per slice, before and after removals."""

from __future__ import annotations

from functools import reduce

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.kpi_long import _apply_value_filter
from kpi_pipeline.pipeline import instock_daily_pairs

_PAIR_KEYS = ["product_id", "store_id"]


def _stage_pairs(ctx: KPIContext, has_store: bool) -> dict:
    """The counted pairs of each stage, in order, from the removal sets scope.build_scope_removals built once
    (the same sets the pipeline reads). Each stage is the population of the metrics it names, not of all:

    * ``scope``: ctx.hybrid_scope_keys (scope source + score backfill) -- every metric outside the two below.
    * ``unblocked`` (blocked scope on): scope pairs minus ctx.fully_blocked_pairs -- the metrics in
      blocked_scope.metrics, which drop blocked days (a pair blocked every day is left with none).
    * ``instock`` (instock.method "daily"): the pairs in_stock_rate and weighted_instock_rate count, after
      instock.daily.input_filters and the in-stock client rules, from ``unblocked`` when in_stock_rate is in
      blocked_scope.metrics. require_daily_data is not applied (it needs the daily-data scan of the run).
    """
    key_cols = _PAIR_KEYS if has_store else ["product_id"]
    pairs = ctx.hybrid_scope_keys.select(*key_cols).distinct()
    stages = {"scope": pairs}
    if not has_store:
        return stages
    instock_base = pairs
    blocked_metrics = ctx.settings["BLOCKED_SCOPE"]["metrics"]
    if ctx.fully_blocked_pairs is not None:
        stages["unblocked"] = pairs.join(ctx.fully_blocked_pairs, on=_PAIR_KEYS, how="left_anti")
        print(f"unblocked_*: population of blocked_scope.metrics {blocked_metrics}")
        if "in_stock_rate" in blocked_metrics:
            instock_base = stages["unblocked"]
    if ctx.settings["INSTOCK_METHOD"] == "daily":
        stages["instock"] = instock_daily_pairs(ctx, instock_base).select(*_PAIR_KEYS)
        print("instock_*: population of in_stock_rate and weighted_instock_rate")
    return stages


def scope_universe_counts(ctx: KPIContext) -> pd.DataFrame:
    """Distinct product, store and pair counts of the scope at each stage of removal (see _stage_pairs), one
    column set per stage ({stage}_product_count, {stage}_store_count, {stage}_pair_count): one "overall" row,
    then one row per (active slice dimension, value) with SLICE_VALUE_FILTERS applied as in kpi_long. NULL
    values show as "NULL". Store and pair counts, and the stages after ``scope``, only with a store grain.
    Raises before build_scopes / build_dimensions.
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
    stages = _stage_pairs(ctx, has_store)
    tagged = reduce(
        DataFrame.unionByName, [df.withColumn("stage", F.lit(name)) for name, df in stages.items()]
    ).cache()

    count_cols = ["product_count"]
    if has_store:
        count_cols += ["store_count", "pair_count"]

    def count_exprs():
        exprs = [F.countDistinct("product_id").alias("product_count")]
        if has_store:
            exprs.append(F.countDistinct("store_id").alias("store_count"))
            exprs.append(F.countDistinct("product_id", "store_id").alias("pair_count"))
        return exprs

    combined = (
        tagged.groupBy("stage")
        .agg(*count_exprs())
        .withColumn("dimension", F.lit("overall"))
        .withColumn("dimension_value", F.lit("ALL"))
    )

    value_filters = ctx.settings.get("SLICE_VALUE_FILTERS", {}) or {}
    enriched = tagged.join(ctx.product_dims, on="product_id", how="left")
    for dim in ctx.active_slice_dimensions:
        df = enriched
        if dim in value_filters:
            df = _apply_value_filter(df, dim, value_filters[dim])
        by_value = (
            df.groupBy("stage", F.coalesce(F.col(dim).cast("string"), F.lit("NULL")).alias("dimension_value"))
            .agg(*count_exprs())
            .withColumn("dimension", F.lit(dim))
        )
        combined = combined.unionByName(by_value)

    pdf = combined.select("stage", "dimension", "dimension_value", *count_cols).toPandas()
    tagged.unpersist()

    # A slice value (or a whole stage) with no pairs left after a removal has no row for that stage: 0.
    wide = pdf.set_index(["dimension", "dimension_value", "stage"])[count_cols].unstack("stage", fill_value=0)
    wide = wide.reindex(
        columns=pd.MultiIndex.from_tuples([(col, stage) for stage in stages for col in count_cols]), fill_value=0
    )
    wide.columns = [f"{stage}_{col}" for col, stage in wide.columns]
    wide = wide.reset_index()
    wide["_overall_first"] = (wide["dimension"] != "overall").astype(int)
    return (
        wide.sort_values(["_overall_first", "dimension", "dimension_value"])
        .drop(columns="_overall_first")
        .reset_index(drop=True)
    )

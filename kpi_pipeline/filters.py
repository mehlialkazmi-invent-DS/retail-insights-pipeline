"""Shared value-filter mechanics: slice-dimension filtering (kpi_long.py) and per-metric
population overrides (metrics.py) both restrict a frame to rows matching a dimension's values,
using the exact same include/exclude/keep_null shape -- kept in one place so neither module
reimplements it.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_value_filter(spec) -> Dict[str, Any]:
    """Normalise one ``value_filters``-shaped entry into a canonical include/exclude/keep_null form.

    Accepts two shapes:

    LIST form (include-only)::

        []           -> keep all NON-NULL values (drop the NULL bucket)
        ["A", "B"]   -> keep ONLY 'A' and 'B' (NULL and anything else dropped)

    DICT form (include and/or exclude, NULL-aware)::

        {"include": ["A", "B"]}              -> keep ONLY 'A' and 'B'          (NULL dropped)
        {"exclude": ["X", "Y"]}              -> keep EVERYTHING EXCEPT 'X'/'Y' (NULL KEPT)
        {"include": [...], "exclude": [...]} -> keep the include set, then remove the excludes
        {..., "keep_null": True/False}       -> optional override of the NULL bucket

    Default NULL handling (when ``keep_null`` is not given):

      * ``include`` present -> NULL is DROPPED (you asked for a specific set of values)
      * ``include`` absent  -> NULL is KEPT   (``exclude`` keeps the whole complement)

    :param spec: the raw list-or-dict value filter spec.
    :return: ``{"include": Optional[list], "exclude": list, "keep_null": bool}``.
    :rtype: Dict[str, Any]
    :raises ValueError: on an unrecognised shape or an unknown dict key.
    """
    if isinstance(spec, (list, tuple, set)):
        values = list(spec)
        if not values:  # [] -> keep all non-null
            return {"include": None, "exclude": [], "keep_null": False}
        return {"include": values, "exclude": [], "keep_null": False}

    if isinstance(spec, dict):
        allowed_keys = {"include", "exclude", "keep_null"}
        unknown = set(spec) - allowed_keys
        if unknown:
            raise ValueError(
                f"value filter entry has unknown key(s) {sorted(unknown)}; "
                f"allowed keys: {sorted(allowed_keys)}"
            )
        include = spec.get("include")
        include = None if include is None else list(include)
        exclude = list(spec.get("exclude", []) or [])
        # include present -> default drop NULL; include absent -> default keep NULL.
        keep_null = bool(spec.get("keep_null", include is None))
        return {"include": include, "exclude": exclude, "keep_null": keep_null}

    raise ValueError(
        "value filter entry must be a list (include-only) or a dict with "
        f"include/exclude/keep_null keys; got {type(spec).__name__}: {spec!r}"
    )


def apply_value_filter(df: DataFrame, dim: str, spec) -> DataFrame:
    """Filter ``df`` on one dimension column per a normalised value-filter spec.

    A row is kept when EITHER

      * its value is NON-NULL and satisfies the include/exclude rules, OR
      * its value is NULL and ``keep_null`` is True.

    Spark ``isin`` / ``NOT isin`` both evaluate to NULL for a NULL cell, so the NULL
    bucket is handled explicitly here rather than left to SQL three-valued logic.

    :param df: frame carrying the ``dim`` column.
    :param dim: dimension column name.
    :param spec: raw list-or-dict value filter spec.
    :return: the filtered frame.
    :rtype: DataFrame
    """
    norm = normalize_value_filter(spec)
    col = F.col(dim)

    value_match = F.lit(True)
    if norm["include"] is not None:
        value_match = value_match & col.isin(norm["include"])
    if norm["exclude"]:
        value_match = value_match & ~col.isin(norm["exclude"])

    keep = col.isNotNull() & value_match
    if norm["keep_null"]:
        keep = keep | col.isNull()
    return df.filter(keep)


# Metric columns that share ONE aggregation pass in metrics.py's compute_kpis/build_kpi_table --
# a metrics.population_filters entry on any one of a group's columns applies to the WHOLE group,
# since splitting them into independently-filtered passes would mean recomputing the same
# aggregation multiple times over. Keep in sync with metrics.py's actual grouping.
METRIC_FILTER_GROUPS: Dict[str, Tuple[str, ...]] = {
    "sales": (
        "total_sales_quantity", "total_sales_revenue", "total_inventory", "AUR", "AUC",
        "distinct_product_count", "distinct_store_count", "distinct_pair_count",
    ),
    "wos": ("WOS", "wos_revenue", "wos_cost"),
    "mean_stock": ("mean_stock", "mean_stock_retail", "mean_stock_cost"),
    "turnover": ("inventory_turnover_rate",),
    "instock": ("in_stock_rate",),
    "weighted_instock": ("weighted_instock_rate",),
    "lost_sales": ("lost_sales_pct",),
}


def resolve_group_population_filter(group: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Union metrics.population_filters entries across one METRIC_FILTER_GROUPS group.

    The group's columns share a single aggregation pass, so they can only be filtered together --
    fails loudly if two columns in the same group configure different specs for the same dim_col,
    since silently picking one would look like the other column's setting was silently ignored.
    """
    cols = METRIC_FILTER_GROUPS[group]
    all_filters = settings.get("METRIC_POPULATION_FILTERS") or {}
    merged: Dict[str, Any] = {}
    owner: Dict[str, str] = {}
    for col in cols:
        spec = all_filters.get(col)
        if not spec:
            continue
        for dim_col, value_spec in spec.items():
            if dim_col in merged and merged[dim_col] != value_spec:
                raise ValueError(
                    f"metrics.population_filters has conflicting specs for dim {dim_col!r} "
                    f"between {owner[dim_col]!r} and {col!r} -- both belong to the same "
                    f"computation group {cols} and must agree."
                )
            merged[dim_col] = value_spec
            owner[dim_col] = col
    return merged


def apply_group_population_filter(df: DataFrame, group: str, settings: Dict[str, Any]) -> DataFrame:
    """Restrict df to one METRIC_FILTER_GROUPS group's configured population override.

    No-op when nothing in the group has a metrics.population_filters entry -- callers can apply
    this unconditionally without changing behaviour for configs that don't use the feature.
    """
    for dim_col, spec in resolve_group_population_filter(group, settings).items():
        df = apply_value_filter(df, dim_col, spec)
    return df

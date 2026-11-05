"""Value filters shared by the slice cuts (kpi_long) and the per-metric population filters (metrics)."""

from __future__ import annotations

from typing import Any, Dict, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def normalize_value_filter(spec) -> Dict[str, Any]:
    """One value filter entry as ``{"include": Optional[list], "exclude": list, "keep_null": bool}``.

    List form (include-only): ``[]`` keeps every non-null value; ``["A", "B"]`` keeps only A and B.
    Dict form: ``include`` and / or ``exclude`` lists, and optional ``keep_null``. NULL is dropped by
    default when ``include`` is given and kept when it is not. Raises on another shape or an unknown key.
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
        keep_null = bool(spec.get("keep_null", include is None))
        return {"include": include, "exclude": exclude, "keep_null": keep_null}

    raise ValueError(
        "value filter entry must be a list (include-only) or a dict with "
        f"include/exclude/keep_null keys; got {type(spec).__name__}: {spec!r}"
    )


def value_filter_condition(dim: str, spec) -> Column:
    """The rows a value filter keeps on column ``dim``: a non-null value must pass include / exclude, a NULL
    is kept only with keep_null (handled explicitly: ``isin`` / ``NOT isin`` are NULL on NULL)."""
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
    return keep


def apply_value_filter(df: DataFrame, dim: str, spec) -> DataFrame:
    """``df`` restricted to the rows a value filter keeps on column ``dim`` (value_filter_condition)."""
    return df.filter(value_filter_condition(dim, spec))


# Metric columns computed in one aggregation in metrics.compute_kpis: a metrics.population_filters entry
# on any column applies to its whole group. Keep in sync with compute_kpis.
METRIC_FILTER_GROUPS: Dict[str, Tuple[str, ...]] = {
    "sales": (
        "total_sales_quantity", "total_sales_revenue", "total_inventory", "AUR", "AUC",
        "distinct_product_count", "distinct_store_count", "distinct_pair_count",
    ),
    "wos": ("WOS", "wos_revenue", "wos_cost"),
    "wos_dc_total": ("WOS_DC", "WOS_TOTAL"),
    "mean_stock": ("mean_stock", "mean_stock_retail", "mean_stock_cost"),
    "dc_inventory": ("dc_mean_stock", "total_mean_stock"),
    "turnover": ("inventory_turnover_rate",),
    "instock": ("in_stock_rate",),
    "weighted_instock": ("weighted_instock_rate",),
    "dc_instock": ("dc_in_stock_rate",),
    "lost_sales": ("lost_sales_pct",),
}


def resolve_group_population_filter(group: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """The metrics.population_filters entries of one METRIC_FILTER_GROUPS group, merged; fails when two of
    its columns set different specs for the same dimension."""
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
    """``df`` restricted to one METRIC_FILTER_GROUPS group's population (unchanged without an entry)."""
    for dim_col, spec in resolve_group_population_filter(group, settings).items():
        df = apply_value_filter(df, dim_col, spec)
    return df

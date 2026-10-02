# Generic reference config for the retail-insights-pipeline: a template for a NEW customer, not any
# customer's deployed values. Copy it, replace the placeholders; README.md documents every key.
# Usage (Databricks, next to main.ipynb): %run ./config
#                                          settings = materialize(fund.paste)
#
# First edits: customer, path_segments (defined_scope, daily_data, products, lost_sales at minimum),
# defined_scope column names, reporting_window.as_of_date.
# Every section and key stays in the file, switched off when unused. Off in this template:
# scope_adjustments, dimension_sources, lost_sales_ensemble, comparable_pairs, dc_instock,
# blocked_scope, goods_in_transit, and instock.method "daily" / "weekly_source".
# To keep a store out of every metric, filter it in input_filters.daily_data and, for the daily in-stock, in
# instock.daily.input_filters (README: input_filters).

import copy
import datetime
import os
from typing import Any, Callable, Dict, List, Optional

# Period-over-period comparison kinds, in canonical order.
COMPARISON_KINDS_ALL = ("yoy", "ytd")

# Comparable-pairs (like-for-like) kinds, in canonical order (README: Comparable pairs).
COMPARABLE_KINDS_ALL = ("ytd", "yoy", "quarter", "half")

# Every metric the pipeline reports, in canonical order. metrics.metric_cols, scope_diff_metrics and
# blocked_scope.metrics pick from this list.
METRICS_ALL = (
    "total_sales_quantity", "total_sales_revenue", "AUR", "AUC", "total_inventory",
    "distinct_product_count", "distinct_store_count", "distinct_pair_count",
    "mean_stock", "mean_stock_retail", "mean_stock_cost", "dc_mean_stock", "total_mean_stock",
    "WOS", "wos_revenue", "wos_cost", "WOS_DC", "WOS_TOTAL", "inventory_turnover_rate",
    "in_stock_rate", "weighted_instock_rate", "dc_in_stock_rate", "lost_sales_pct",
)

# The inventory metrics that can add goods in transit to on-hand: goods_in_transit.inventory_metrics.
INVENTORY_GIT_METRICS_ALL = (
    "total_inventory", "mean_stock", "mean_stock_retail", "mean_stock_cost", "dc_mean_stock",
    "total_mean_stock", "WOS", "wos_revenue", "wos_cost", "WOS_DC", "WOS_TOTAL", "inventory_turnover_rate",
)

# Where in_stock_rate comes from: instock.method.
INSTOCK_METHODS = ("daily", "weekly_source", "lost_sales_source")

CONFIG: Dict[str, Any] = {
    # =============================================================================
    # IDENTITY & RUN WINDOW
    # =============================================================================
    "customer": "your_customer",  # datastore bucket: /mnt/invent-{customer}-datastore
    "run": {
        "mode": "full",  # "full" computes from the source tables; "html_only" renders saved outputs
    },
    "reporting_window": {  # README: Reporting window
        "as_of_date": "2026-09-01",  # update before each run
        "run_min_date": None,  # None = YTD from Jan 1; "2024-01-01" for multi-year
        "report_end": "as_of",  # "as_of" | "complete_month" | "latest_day" ("latest_day" needs instock.method "daily")
    },
    # =============================================================================
    # CALENDAR
    # =============================================================================
    "fiscal_calendar": {
        "use_fiscal_calendar": True,
        "half_periods": False,  # True adds the Half tab and the "half" comparable kind
        "column_map": {  # fiscal_cal upload columns; set month_name_col when the fiscal year is offset
            "quarter_col": "Quarter",
            "month_col": "Month",
            "month_name_col": "month_name",
        },
        "daily_time_columns": {  # raw daily-data columns; Year always comes from date, week is civil path only
            "date": "date",
            "week": "week",
        },
    },
    # =============================================================================
    # SCOPE & POPULATION
    # =============================================================================
    "score_scope": {  # used by use_hybrid_scope and run_scope_diff (README: Scope modes)
        "min_percentile": 0.2,
        "min_weeks_for_filter": 2,
    },
    "scope": {
        "use_hybrid_scope": False,  # True also backfills the weeks the defined scope leaves uncovered
        "run_scope_diff": False,
    },
    "defined_scope": {  # README: defined_scope
        "grain": "product_store",  # "product" | "product_store" | "product_store_week"
        "product_col": "product_id",
        "store_col": "store_id",  # required for the store grains
        "date_col": "week_start_date",  # date_col / year_col / week_col: product_store_week grain only
        "year_col": None,
        "week_col": None,
        "backfill_leading_gap": True,  # product_store_week only
    },
    "scope_adjustments": {  # manual product additions / removals (README: Manual scope adjustments)
        "additions": [
            {
                "enabled": False,  # flip on once path points at a real table / CSV
                "label": "example_addition",
                "source": "csv",
                "path": "/Workspace/Shared/your_project/data/addition_product_ids.csv",
                "location": "workspace",
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,  # store_col and the time columns None = every store, every week
                "date_col": None,
                "year_col": None,
                "week_col": None,
            },
        ],
        "removals": [
            {
                "enabled": False,  # flip on once path points at a real table / CSV
                "label": "example_removal",
                "source": "csv",
                "path": "/Workspace/Shared/your_project/data/removal_product_ids.csv",
                "location": "workspace",
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,
                "date_col": None,
                "year_col": None,
                "week_col": None,
            },
        ],
    },
    # =============================================================================
    # DATA SOURCES
    # =============================================================================
    "path_segments": {
        "fiscal": ["one_time_uploads", "fiscal_cal"],
        "daily_data": ["noob", "daily-data"],
        "inventory_warehouse": ["operation", "inventory_warehouse"],  # DC daily inventory
        "item_family": ["operation", "item_family"],  # parent / child product map
        "scope": ["operation", "scope"],  # platform scope table (scope_source.mode "operation_scope")
        "goods_in_transit": ["operation", "goods_in_transit"],  # destination_type 0 = store, 1 = warehouse
        "products": ["master-data", "products"],
        "lost_sales": ["noob", "lost-sales"],  # add a model_id=... segment if partitioned by model
        "defined_scope": ["analysis", "instock_rate", "instock_rate_scope"],
        "product_planning_level": ["operation", "product_planning_level"],  # product_agg_level -> product_id map
    },
    "input_filters": {  # Spark SQL expressions applied when reading each source (README: input_filters)
        "defined_scope": [],
        "lost_sales": [],  # also applied to lost_sales_ensemble.slow_path_segments
        "daily_data": ["usable = 1"],
        "inventory_warehouse": [],
        "item_family": [],
    },
    # ---------------------------------------------------------------------------
    # LOST SALES SOURCE -- column mapping of the lost-sales table (README: lost_sales_source)
    # ---------------------------------------------------------------------------
    # Configure exactly one of product_col / product_agg_level_col.
    "lost_sales_source": {
        "week_col": "week_start_date",
        "product_col": "product_id",
        "store_col": "store_id",
        "lost_sales_col": "lost_sales",
        "in_stock_col": "in_stock",  # read only when instock.method is "lost_sales_source"
        "total_days_col": "details.total_days",  # a dotted nested-struct path works
        "product_agg_level_col": None,
        "sales_filter": [],  # Spark SQL narrowing the daily-data sales in lost_sales_pct's denominator
    },
    # ---------------------------------------------------------------------------
    # IN-STOCK -- where in_stock_rate comes from (README: instock)
    # ---------------------------------------------------------------------------
    #   method "daily"             built from daily-data over the scope pairs (store-level scope grain)
    #   method "weekly_source"     read from a separate weekly table (weekly_source below)
    #   method "lost_sales_source" read from lost_sales_source's in_stock_col / total_days_col
    # Both sub-sections stay in the file; only the one the method names is used.
    "instock": {
        "method": "lost_sales_source",
        "daily": {
            "count_start": "first_daily_row",  # or "scope_start" / "earliest" (need scope_source "operation_scope")
            "require_daily_data": True,  # drop pairs with no daily-data row at all
            "history_start": None,  # None = window start; earlier counts pairs stocked before the window
            "usable_only": True,  # usable != 1 days leave the store-days and the in-stock days
            "input_filters": [],  # Spark SQL on product_id / store_id, applied to the pair universe
        },
        "weekly_source": {
            "path_segments": None,  # required when method is "weekly_source"
            "week_col": "week_start_date",
            "product_col": "product_id",
            "store_col": "store_id",
            "in_stock_col": "in_stock",
            "total_days_col": "total_days",
            "product_agg_level_col": None,
            "fallback_sources": [],  # extra column-sets of the same table filling weeks the main set lacks
        },
    },
    # ---------------------------------------------------------------------------
    # ITEM FAMILY -- parent / child product roll-up (README: item_family)
    # ---------------------------------------------------------------------------
    "item_family_source": {  # column names of path_segments.item_family
        "product_col": "product_id",
        "parent_col": "parent_id",
        "is_main_col": "is_main",
    },
    "item_family_rollup": {  # roll child / superseded products onto their family main, per source
        "daily_data": True,
        "lost_sales": False,  # opt-in safety net
        "inventory_warehouse": True,
        "defined_scope": False,  # opt-in safety net
    },
    # ---------------------------------------------------------------------------
    # DC IN-STOCK -- dc_in_stock_rate from an expanded inventory_warehouse grid (README: dc_instock)
    # ---------------------------------------------------------------------------
    # DC metrics (dc_mean_stock, WOS_DC, WOS_TOTAL's DC part, dc_in_stock_rate) read
    # path_segments.inventory_warehouse for the STORE scope's products only (the product-weeks of
    # scope_source), at every warehouse; there is no separate DC product scope. The DC scope
    # (blocked_scope.dc_solution_id) only gives each DC pair its start date for the DC blocked days.
    "dc_instock": {
        "enabled": False,
        "stock_threshold": 0,  # a day is stocked when inventory > stock_threshold
    },
    # ---------------------------------------------------------------------------
    # SCOPE SOURCE -- which table defines the scope universe (README: scope_source)
    # ---------------------------------------------------------------------------
    "scope_source": {
        "mode": "defined_scope",  # "defined_scope" | "operation_scope"
        "solution_id": 21,  # operation scope solution(s): an int or a list; also the blocked_scope solution(s)
        "run_date": None,  # "YYYY-MM-DD" Sunday; None = latest Sunday on or before today
        "roll_to_family_main": True,
        "active_only": True,
        # In-stock only at stores where the main item itself is eligible (sub-only stores stay in every
        # other metric). Needs roll_to_family_main and instock.method "daily".
        "instock_main_eligible_only": False,
        # In-stock leaves out sizes not in a supersession whose class color is (likely NGF); they stay
        # in every other metric. Needs instock.method "daily".
        "instock_exclude_unsuperseded_sizes": False,
    },
    # ---------------------------------------------------------------------------
    # BLOCKED SCOPE -- UI blocked days (README: blocked_scope). Needs scope_source "operation_scope".
    # ---------------------------------------------------------------------------
    "blocked_scope": {
        "ui_parameters_path": None,  # path under the datastore root; None = blocked scope off
        "rule": "after_scope_start",  # or "all"
        "dc_solution_id": None,  # None = no DC blocks; an int or a list of DC solutions
        "kinds": ["product", "product_destination", "destination"],  # store block folders read
        "dc_kinds": ["product", "product_destination"],  # DC block folders read (no destination folder)
        "metrics": "all",  # metrics that drop blocked days: "all" or a list from METRICS_ALL
    },
    # ---------------------------------------------------------------------------
    # GOODS IN TRANSIT (GIT) -- (README: goods_in_transit)
    # ---------------------------------------------------------------------------
    "goods_in_transit": {
        "date_shift_days": None,  # None = GIT off everywhere; -1 = snapshot dated D+1 is the end of day D
        "roll_to_family_main": True,
        "store_instock": False,  # a daily in-stock day also counts store GIT (needs instock.method "daily")
        "dc_instock": False,  # a DC in-stock day also counts DC GIT
        "inventory_metrics": [],  # INVENTORY_GIT_METRICS_ALL names that add GIT to on-hand
    },
    # ---------------------------------------------------------------------------
    # LOST SALES ENSEMBLE -- blend a fast and a slow lost-sales model (README: lost_sales_ensemble)
    # ---------------------------------------------------------------------------
    "lost_sales_ensemble": {
        "enabled": False,
        "slow_path_segments": ["noob", "lost-sales"],  # e.g. a longer-lookback model variant
        "speed_cluster_path_segments": ["noob", "product-cluster-attributes-snapshot"],
        "speed_cluster_format": "long",  # "long" = one row per product x attribute; "wide" = own column
        "speed_cluster_attribute_name": "sales_speed",
        "speed_cluster_value_col": "product_speed_cluster",  # "wide" format only
        "fast_mover_clusters": [1, 2, 3],  # these clusters take the fast model, everyone else the slow one
    },
    # =============================================================================
    # CUTS & DIMENSIONS
    # =============================================================================
    "slices": {  # breakdown dimensions from the products table
        "dimensions": ["brand"],
        "derived_dimensions": {"example_derived": "CASE WHEN brand = 'A' THEN 'Group A' ELSE 'Other' END"},
        "value_filters": {},  # per-dimension include / exclude of values (README: Value filters)
    },
    "dimension_sources": [  # external tables that add slice dimensions and root tabs (README: Dimension sources)
        {
            "enabled": False,
            "label": "example_dimension_source",
            "source": "delta",
            "path_segments": ["operation", "some_attribute_table"],
            "join_key": "product_id",
            "columns": [],
            "derived": {"IS_EXAMPLE_FLAG": "CASE WHEN some_column = 'X' THEN 'yes' ELSE 'no' END"},
            "fillna": {"IS_EXAMPLE_FLAG": "no"},  # products absent from the source get NULL, not the ELSE branch
            "root_values": {"IS_EXAMPLE_FLAG": {"yes": "example_root"}},  # root "example_root" = flag "yes"
        },
    ],
    # =============================================================================
    # COMPARISONS
    # =============================================================================
    "comparisons": {
        "enabled": ["yoy", "ytd"],  # any of COMPARISON_KINDS_ALL
    },
    "comparable_pairs": {  # like-for-like; needs run_min_date spanning 2+ years (README: Comparable pairs)
        "enabled": False,
        "kinds": ["ytd"],  # any of COMPARABLE_KINDS_ALL
        "grain": "product_store",  # or "product"
        "pair_days": "unblocked",  # pairs present on "unblocked" days in every year, or on "all" (incl. blocked)
    },
    # =============================================================================
    # METRICS
    # =============================================================================
    "metrics": {
        "metric_cols": [
            "total_sales_quantity",
            "total_sales_revenue",
            "AUR",
            "AUC",
            "total_inventory",
            "distinct_product_count",
            "distinct_store_count",
            "distinct_pair_count",
            "mean_stock",
            "mean_stock_retail",
            "mean_stock_cost",
            "dc_mean_stock",
            "total_mean_stock",
            "WOS",
            "wos_revenue",
            "wos_cost",
            "WOS_DC",
            "WOS_TOTAL",
            "inventory_turnover_rate",
            "in_stock_rate",
            "weighted_instock_rate",
            "dc_in_stock_rate",
            "lost_sales_pct",
        ],
        "scope_diff_metrics": [
            "total_sales_quantity",
            "total_sales_revenue",
            "total_inventory",
            "distinct_product_count",
            "distinct_pair_count",
            "WOS",
            "WOS_DC",
            "WOS_TOTAL",
            "in_stock_rate",
            "dc_in_stock_rate",
            "lost_sales_pct",
        ],
        "labels": {
            "total_sales_revenue": "Sales Revenue",
            "total_sales_quantity": "Sales Units",
            "AUR": "AUR",
            "AUC": "AUC",
            "total_inventory": "Total Inventory",
            "mean_stock": "Daily stock avg (M units)",
            "mean_stock_retail": "Daily stock avg retail (M $)",
            "mean_stock_cost": "Daily stock avg cost (M $)",
            "dc_mean_stock": "Daily DC stock avg (M units)",
            "total_mean_stock": "Daily total stock avg (M units)",
            "WOS": "WOS (units)",
            "wos_revenue": "WOS revenue",
            "wos_cost": "WOS cost",
            "WOS_DC": "WOS (DC)",
            "WOS_TOTAL": "WOS (Total)",
            "inventory_turnover_rate": "Inventory Turnover Rate",
            "in_stock_rate": "In-Stock Rate",
            "weighted_instock_rate": "Weighted In-Stock Rate",
            "dc_in_stock_rate": "DC In-Stock Rate",
            "lost_sales_pct": "Lost Sales %",
            "distinct_product_count": "Distinct products",
            "distinct_store_count": "Distinct stores",
            "distinct_pair_count": "Distinct pairs",
        },
        "pp_change_metrics": ["in_stock_rate", "weighted_instock_rate", "dc_in_stock_rate", "lost_sales_pct"],
        "population_filters": {},  # {metric: {dim_col: value filter}} narrows one metric (README: Population filters)
    },
    # =============================================================================
    # OUTPUT & REPORTING
    # =============================================================================
    "output": {
        "save_outputs": True,
        "path_segments": ["analysis", "kpi_reports", "outputs"],
        "run_date": None,
        "save_mode": "initial",  # "initial" | "incremental" | "full_refresh"
        "allow_overwrite_existing": True,
        "recompute_comparisons_from_history": True,
    },
    "html_report": {
        "enabled": True,
        "filename": "kpi_report_{customer}_{report_end}.html",
        "report_title": "KPI Report",
        "output_path_segments": None,
        "metric_definitions": {},
        "weekly_display_weeks": 5,  # periods shown per tab; None = all
        "monthly_display_months": 5,
        "quarterly_display_quarters": 5,
        "half_display_halves": 4,
        "yearly_display_years": None,
        "root_labels": {},  # root id -> tab label
        "dimension_labels": {},  # slice dimension -> tab label, e.g. {"brand": "Banner"}
    },
}


def _solution_ids(raw: Any, key: str) -> List[int]:
    """One solution id or a non-empty list of them, as a list of ints (bool rejected)."""
    ids = list(raw) if isinstance(raw, (list, tuple)) else [raw]
    if not ids or any(type(i) is not int for i in ids):
        raise ValueError(f"{key} must be an integer or a non-empty list of integers; got {raw!r}")
    return ids


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in ("1", "true", "yes")


def _validate_value_filters(value_filters: Dict[str, Any]) -> None:
    """Fail loudly on a malformed value_filters entry.

    Each entry is either a LIST (include-only) or a DICT with any of the keys
    ``include`` / ``exclude`` / ``keep_null``. Anything else is a config error.
    """
    allowed_keys = {"include", "exclude", "keep_null"}
    for dim, spec in value_filters.items():
        if isinstance(spec, (list, tuple)):
            continue
        if isinstance(spec, dict):
            unknown = set(spec) - allowed_keys
            if unknown:
                raise ValueError(
                    f"value_filters[{dim!r}] has unknown key(s) {sorted(unknown)}; "
                    f"allowed keys: {sorted(allowed_keys)}"
                )
            if "keep_null" in spec and not isinstance(spec["keep_null"], bool):
                raise ValueError(f"value_filters[{dim!r}]['keep_null'] must be a boolean.")
            for k in ("include", "exclude"):
                if k in spec and spec[k] is not None and not isinstance(spec[k], (list, tuple)):
                    raise ValueError(f"value_filters[{dim!r}][{k!r}] must be a list.")
            continue
        raise ValueError(
            f"value_filters[{dim!r}] must be a list or a dict with include/exclude/keep_null; "
            f"got {type(spec).__name__}."
        )


def _validate_population_filters(population_filters: Dict[str, Any], metric_cols: list) -> None:
    """Fail loudly on a malformed metrics.population_filters entry.

    Each key must be a real metric_col; each value must be a dict of {dim_col: value_filter_spec},
    where value_filter_spec has the same shape _validate_value_filters already accepts (a list or
    a dict with include/exclude/keep_null).
    """
    known = set(metric_cols)
    for metric_col, dim_spec in population_filters.items():
        if metric_col not in known:
            raise ValueError(
                f"metrics.population_filters has an entry for {metric_col!r}, which is not in "
                f"metrics.metric_cols {sorted(known)}."
            )
        if not isinstance(dim_spec, dict):
            raise ValueError(
                f"metrics.population_filters[{metric_col!r}] must be a dict of "
                f"{{dim_col: value_filter_spec}}; got {type(dim_spec).__name__}."
            )
        _validate_value_filters(dim_spec)


def _validate_scope_diff_metrics(scope_diff_metrics: list, metric_cols: list) -> None:
    """Fail loudly if metrics.scope_diff_metrics names anything outside metrics.metric_cols.

    Unlike population_filters, nothing else validates this list -- build_scope_diff indexes
    a DataFrame with it directly (kpi_pipeline/comparisons.py), so a stale/typo'd name here
    would otherwise surface as a raw KeyError deep inside a scope.run_scope_diff=True run.
    """
    known = set(metric_cols)
    unknown = [m for m in scope_diff_metrics if m not in known]
    if unknown:
        raise ValueError(
            f"metrics.scope_diff_metrics has entr{'y' if len(unknown) == 1 else 'ies'} "
            f"{unknown} not in metrics.metric_cols {sorted(known)}."
        )


def _validate_scope_adjustments(scope_adjustments_cfg: Dict[str, Any]) -> None:
    """Fail loudly on an enabled scope_adjustments entry missing required fields.

    Every other field on an addition/removal entry (product_col, store_col, date_col, ...)
    is read via .get() with a default; join_keys and a source location are not -- they're
    indexed directly in kpi_pipeline/scope.py, so a copy-pasted entry that forgot one of them
    would otherwise KeyError deep inside scope building instead of failing at config load.
    """
    for section in ("additions", "removals"):
        for entry in scope_adjustments_cfg.get(section, []) or []:
            if not entry.get("enabled"):
                continue
            label = entry.get("label", f"scope_adjustments.{section}[unlabeled]")
            join_keys = entry.get("join_keys")
            if not join_keys or not isinstance(join_keys, (list, tuple)):
                raise ValueError(
                    f"scope_adjustments.{section} entry {label!r} is enabled but has no "
                    f"non-empty join_keys list."
                )
            if not entry.get("path") and not entry.get("path_segments"):
                raise ValueError(
                    f"scope_adjustments.{section} entry {label!r} is enabled but has neither "
                    f"'path' nor 'path_segments' set."
                )


def _parse_percentile(raw: str) -> float:
    value = float(raw)
    return value / 100.0 if value > 1 else value


def _apply_env_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Apply optional KPI_* environment variables over CONFIG (see README)."""
    out = copy.deepcopy(cfg)

    if "KPI_CUSTOMER" in os.environ:
        out["customer"] = os.environ["KPI_CUSTOMER"]

    rn = out.setdefault("run", {})
    if "KPI_RUN_MODE" in os.environ:
        rn["mode"] = os.environ["KPI_RUN_MODE"].strip().lower()

    rw = out.setdefault("reporting_window", {})
    if "KPI_AS_OF_DATE" in os.environ:
        rw["as_of_date"] = os.environ["KPI_AS_OF_DATE"]
    if "KPI_RUN_MIN_DATE" in os.environ:
        rw["run_min_date"] = os.environ["KPI_RUN_MIN_DATE"].strip() or None
    if "KPI_REPORT_END" in os.environ:
        rw["report_end"] = os.environ["KPI_REPORT_END"].strip().lower()

    fc = out.setdefault("fiscal_calendar", {})
    if "KPI_USE_FISCAL_CALENDAR" in os.environ:
        fc["use_fiscal_calendar"] = _parse_bool(os.environ["KPI_USE_FISCAL_CALENDAR"])
    if "KPI_HALF_PERIODS" in os.environ:
        fc["half_periods"] = _parse_bool(os.environ["KPI_HALF_PERIODS"])

    ss = out.setdefault("score_scope", {})
    if "KPI_SCOPE_MIN_PERCENTILE" in os.environ:
        ss["min_percentile"] = _parse_percentile(os.environ["KPI_SCOPE_MIN_PERCENTILE"])
    if "KPI_SCOPE_MIN_WEEKS_FOR_FILTER" in os.environ:
        ss["min_weeks_for_filter"] = int(os.environ["KPI_SCOPE_MIN_WEEKS_FOR_FILTER"])

    sc = out.setdefault("scope", {})
    if "KPI_USE_HYBRID_SCOPE" in os.environ:
        sc["use_hybrid_scope"] = _parse_bool(os.environ["KPI_USE_HYBRID_SCOPE"])
    if "KPI_RUN_SCOPE_DIFF" in os.environ:
        sc["run_scope_diff"] = _parse_bool(os.environ["KPI_RUN_SCOPE_DIFF"])

    cp = out.setdefault("comparable_pairs", {})
    if "KPI_COMPARABLE_PAIRS" in os.environ:
        cp["enabled"] = _parse_bool(os.environ["KPI_COMPARABLE_PAIRS"])

    cmp_cfg = out.setdefault("comparisons", {})
    if "KPI_COMPARISONS" in os.environ:
        cmp_cfg["enabled"] = [
            c.strip().lower() for c in os.environ["KPI_COMPARISONS"].split(",") if c.strip()
        ]

    lse = out.setdefault("lost_sales_ensemble", {})
    if "KPI_LOST_SALES_ENSEMBLE" in os.environ:
        lse["enabled"] = _parse_bool(os.environ["KPI_LOST_SALES_ENSEMBLE"])
    if "KPI_LOST_SALES_SLOW_PATH" in os.environ:
        lse["slow_path_segments"] = [
            s.strip() for s in os.environ["KPI_LOST_SALES_SLOW_PATH"].split(",") if s.strip()
        ]
    if "KPI_SPEED_CLUSTER_PATH" in os.environ:
        lse["speed_cluster_path_segments"] = [
            s.strip() for s in os.environ["KPI_SPEED_CLUSTER_PATH"].split(",") if s.strip()
        ]
    if "KPI_SPEED_CLUSTER_FORMAT" in os.environ:
        lse["speed_cluster_format"] = os.environ["KPI_SPEED_CLUSTER_FORMAT"].strip().lower()
    if "KPI_SPEED_CLUSTER_ATTRIBUTE" in os.environ:
        lse["speed_cluster_attribute_name"] = os.environ["KPI_SPEED_CLUSTER_ATTRIBUTE"].strip()
    if "KPI_SPEED_CLUSTER_VALUE_COL" in os.environ:
        lse["speed_cluster_value_col"] = os.environ["KPI_SPEED_CLUSTER_VALUE_COL"].strip()
    if "KPI_FAST_MOVER_CLUSTERS" in os.environ:
        lse["fast_mover_clusters"] = [
            int(c.strip()) for c in os.environ["KPI_FAST_MOVER_CLUSTERS"].split(",") if c.strip()
        ]

    lss = out.setdefault("lost_sales_source", {})
    if "KPI_LOST_SALES_WEEK_COL" in os.environ:
        lss["week_col"] = os.environ["KPI_LOST_SALES_WEEK_COL"].strip()
    if "KPI_LOST_SALES_PRODUCT_COL" in os.environ:
        lss["product_col"] = os.environ["KPI_LOST_SALES_PRODUCT_COL"].strip()
    if "KPI_LOST_SALES_STORE_COL" in os.environ:
        lss["store_col"] = os.environ["KPI_LOST_SALES_STORE_COL"].strip()
    if "KPI_LOST_SALES_COL" in os.environ:
        lss["lost_sales_col"] = os.environ["KPI_LOST_SALES_COL"].strip()
    if "KPI_LOST_SALES_IN_STOCK_COL" in os.environ:
        lss["in_stock_col"] = os.environ["KPI_LOST_SALES_IN_STOCK_COL"].strip()
    if "KPI_LOST_SALES_TOTAL_DAYS_COL" in os.environ:
        lss["total_days_col"] = os.environ["KPI_LOST_SALES_TOTAL_DAYS_COL"].strip()

    ins_cfg = out.setdefault("instock", {})
    if "KPI_INSTOCK_METHOD" in os.environ:
        ins_cfg["method"] = os.environ["KPI_INSTOCK_METHOD"].strip().lower()
    ins = ins_cfg.setdefault("weekly_source", {})
    if "KPI_INSTOCK_SOURCE_PATH" in os.environ:
        ins["path_segments"] = [
            s.strip() for s in os.environ["KPI_INSTOCK_SOURCE_PATH"].split(",") if s.strip()
        ]
    if "KPI_INSTOCK_WEEK_COL" in os.environ:
        ins["week_col"] = os.environ["KPI_INSTOCK_WEEK_COL"].strip()
    if "KPI_INSTOCK_PRODUCT_COL" in os.environ:
        ins["product_col"] = os.environ["KPI_INSTOCK_PRODUCT_COL"].strip()
    if "KPI_INSTOCK_STORE_COL" in os.environ:
        ins["store_col"] = os.environ["KPI_INSTOCK_STORE_COL"].strip()
    if "KPI_INSTOCK_IN_STOCK_COL" in os.environ:
        ins["in_stock_col"] = os.environ["KPI_INSTOCK_IN_STOCK_COL"].strip()
    if "KPI_INSTOCK_TOTAL_DAYS_COL" in os.environ:
        ins["total_days_col"] = os.environ["KPI_INSTOCK_TOTAL_DAYS_COL"].strip()

    ifr = out.setdefault("item_family_rollup", {})
    if "KPI_ITEM_FAMILY_ROLLUP_DAILY_DATA" in os.environ:
        ifr["daily_data"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_DAILY_DATA"])
    if "KPI_ITEM_FAMILY_ROLLUP_LOST_SALES" in os.environ:
        ifr["lost_sales"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_LOST_SALES"])
    if "KPI_ITEM_FAMILY_ROLLUP_INVENTORY_WAREHOUSE" in os.environ:
        ifr["inventory_warehouse"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_INVENTORY_WAREHOUSE"])
    if "KPI_ITEM_FAMILY_ROLLUP_DEFINED_SCOPE" in os.environ:
        ifr["defined_scope"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_DEFINED_SCOPE"])

    op = out.setdefault("output", {})
    if "KPI_SAVE_OUTPUTS" in os.environ:
        op["save_outputs"] = _parse_bool(os.environ["KPI_SAVE_OUTPUTS"])
    if "KPI_OUTPUT_PATH" in os.environ:
        op["path_segments"] = [s.strip() for s in os.environ["KPI_OUTPUT_PATH"].split(",") if s.strip()]
    if "KPI_OUTPUT_RUN_DATE" in os.environ:
        op["run_date"] = os.environ["KPI_OUTPUT_RUN_DATE"].strip() or None
    if "KPI_OUTPUT_SAVE_MODE" in os.environ:
        op["save_mode"] = os.environ["KPI_OUTPUT_SAVE_MODE"].strip().lower()
    if "KPI_ALLOW_OVERWRITE_EXISTING" in os.environ:
        op["allow_overwrite_existing"] = _parse_bool(os.environ["KPI_ALLOW_OVERWRITE_EXISTING"])
    if "KPI_RECOMPUTE_COMPARISONS" in os.environ:
        op["recompute_comparisons_from_history"] = _parse_bool(os.environ["KPI_RECOMPUTE_COMPARISONS"])

    sl = out.setdefault("slices", {})
    if "KPI_SLICE_DIMENSIONS" in os.environ:
        sl["dimensions"] = [c.strip() for c in os.environ["KPI_SLICE_DIMENSIONS"].split(",") if c.strip()]

    hr = out.setdefault("html_report", {})
    if "KPI_HTML_ENABLED" in os.environ:
        hr["enabled"] = _parse_bool(os.environ["KPI_HTML_ENABLED"])
    if "KPI_HTML_FILENAME" in os.environ:
        hr["filename"] = os.environ["KPI_HTML_FILENAME"].strip()
    if "KPI_HTML_TITLE" in os.environ:
        hr["report_title"] = os.environ["KPI_HTML_TITLE"].strip()
    if "KPI_HTML_OUTPUT_PATH" in os.environ:
        hr["output_path_segments"] = [
            s.strip() for s in os.environ["KPI_HTML_OUTPUT_PATH"].split(",") if s.strip()
        ]
    if "KPI_HTML_WEEKLY_WEEKS" in os.environ:
        raw = os.environ["KPI_HTML_WEEKLY_WEEKS"].strip()
        hr["weekly_display_weeks"] = int(raw) if raw else None
    if "KPI_HTML_MONTHLY_MONTHS" in os.environ:
        raw = os.environ["KPI_HTML_MONTHLY_MONTHS"].strip()
        hr["monthly_display_months"] = int(raw) if raw else None
    if "KPI_HTML_QUARTERLY_QUARTERS" in os.environ:
        raw = os.environ["KPI_HTML_QUARTERLY_QUARTERS"].strip()
        hr["quarterly_display_quarters"] = int(raw) if raw else None
    if "KPI_HTML_HALF_HALVES" in os.environ:
        raw = os.environ["KPI_HTML_HALF_HALVES"].strip()
        hr["half_display_halves"] = int(raw) if raw else None
    if "KPI_HTML_YEARLY_YEARS" in os.environ:
        raw = os.environ["KPI_HTML_YEARLY_YEARS"].strip()
        hr["yearly_display_years"] = int(raw) if raw else None

    return out


def _sunday_of_week(d: datetime.date) -> datetime.date:
    """Sunday that starts the Sun–Sat week containing d (Python weekday: Sun=6)."""
    return d if d.weekday() == 6 else d - datetime.timedelta(days=d.weekday() + 1)


def _saturday_of_week(d: datetime.date) -> datetime.date:
    return _sunday_of_week(d) + datetime.timedelta(days=6)


def _last_completed_saturday(as_of: datetime.date) -> datetime.date:
    """Saturday ending the last full Sun–Sat week on or before as_of."""
    week_end = _saturday_of_week(as_of)
    if as_of >= week_end:
        return week_end
    return week_end - datetime.timedelta(days=7)


def _resolve_report_window(
    as_of: datetime.date,
    run_min: Optional[str],
    report_end_mode: str,
) -> Dict[str, Any]:
    report_start = _sunday_of_week(datetime.date(as_of.year, 1, 1))
    report_end = as_of if report_end_mode == "latest_day" else _last_completed_saturday(as_of)
    run_week_start = _sunday_of_week(as_of)
    run_week_end = _saturday_of_week(as_of)

    run_min_resolved = _sunday_of_week(datetime.date.fromisoformat(run_min)) if run_min else None
    if run_min_resolved is not None:
        if run_min_resolved > report_end:
            raise ValueError(
                f"run_min_date Sunday ({run_min_resolved}) must be <= REPORT_END_DATE ({report_end})."
            )
        effective_start = run_min_resolved
    else:
        effective_start = report_start

    return {
        "AS_OF_DATE": as_of,
        "RUN_WEEK_START_DATE": run_week_start,
        "RUN_WEEK_END_DATE": run_week_end,
        "REPORT_START_DATE": report_start,
        "REPORT_END_DATE": report_end,
        "RUN_MIN_DATE": run_min_resolved,
        "EFFECTIVE_REPORT_START_DATE": effective_start,
    }


def materialize(fund_paste: Callable[..., str], cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Resolve CONFIG into flat settings dict for KPIRunner (paths, dates, metrics, slices)."""
    cfg = _apply_env_overrides(cfg or CONFIG)

    customer = cfg["customer"]
    rw = cfg["reporting_window"]
    run_min_raw = rw.get("run_min_date")
    run_min = run_min_raw.strip() if isinstance(run_min_raw, str) and run_min_raw.strip() else None

    bucket = os.environ.get("KPI_BUCKET", f"/mnt/invent-{customer}-datastore")
    path_segments = cfg["path_segments"]
    paths = {
        "PATH_FISCAL": fund_paste(bucket, *path_segments["fiscal"]),
        "PATH_DAILY_DATA": fund_paste(bucket, *path_segments["daily_data"]),
        "PATH_INVENTORY_WAREHOUSE": fund_paste(bucket, *path_segments["inventory_warehouse"]),
        "PATH_ITEM_FAMILY": fund_paste(bucket, *path_segments["item_family"]),
        "PATH_PRODUCTS": fund_paste(bucket, *path_segments["products"]),
        "PATH_LOST_SALES": fund_paste(bucket, *path_segments["lost_sales"]),
        "PATH_DEFINED_SCOPE": fund_paste(bucket, *path_segments["defined_scope"]),
        "PATH_LOST_SALES_SLOW": fund_paste(bucket, *cfg["lost_sales_ensemble"]["slow_path_segments"]),
        "PATH_SPEED_CLUSTER": fund_paste(bucket, *cfg["lost_sales_ensemble"]["speed_cluster_path_segments"]),
        "PATH_PRODUCT_PLANNING_LEVEL": fund_paste(bucket, *path_segments["product_planning_level"]),
        "PATH_SCOPE": fund_paste(bucket, *path_segments["scope"]),
        "PATH_GOODS_IN_TRANSIT": fund_paste(bucket, *path_segments["goods_in_transit"]),
    }

    lost_sales_source_cfg = cfg.get("lost_sales_source", {}) or {}
    lost_sales_column_map = {
        "week_col": lost_sales_source_cfg.get("week_col", "week_start_date"),
        "product_col": lost_sales_source_cfg.get("product_col", "product_id"),
        "store_col": lost_sales_source_cfg.get("store_col", "store_id"),
        "lost_sales_col": lost_sales_source_cfg.get("lost_sales_col", "lost_sales"),
        "in_stock_col": lost_sales_source_cfg.get("in_stock_col", "in_stock"),
        "total_days_col": lost_sales_source_cfg.get("total_days_col", "details.total_days"),
        "product_agg_level_col": lost_sales_source_cfg.get("product_agg_level_col"),
    }
    lost_sales_sales_filter = list(lost_sales_source_cfg.get("sales_filter") or [])

    instock_cfg = cfg["instock"]
    instock_method = instock_cfg["method"]
    if instock_method not in INSTOCK_METHODS:
        raise ValueError(f"instock.method must be one of {list(INSTOCK_METHODS)}; got {instock_method!r}")
    instock_source_enabled = instock_method == "weekly_source"
    instock_source_cfg = instock_cfg["weekly_source"]
    if instock_source_enabled and not instock_source_cfg["path_segments"]:
        raise ValueError("instock.weekly_source.path_segments is required when instock.method='weekly_source'")
    paths["PATH_INSTOCK_SOURCE"] = (
        fund_paste(bucket, *instock_source_cfg["path_segments"]) if instock_source_enabled else None
    )
    instock_source_column_map = {
        "week_col": instock_source_cfg["week_col"],
        "product_col": instock_source_cfg["product_col"],
        "store_col": instock_source_cfg["store_col"],
        "in_stock_col": instock_source_cfg["in_stock_col"],
        "total_days_col": instock_source_cfg["total_days_col"],
        "product_agg_level_col": instock_source_cfg["product_agg_level_col"],
        "fallback_sources": [
            {
                "week_col": fb["week_col"],
                "in_stock_col": fb["in_stock_col"],
                "total_days_col": fb["total_days_col"],
                "product_col": fb.get("product_col", instock_source_cfg["product_col"]),
                "store_col": fb.get("store_col", instock_source_cfg["store_col"]),
                "product_agg_level_col": fb.get("product_agg_level_col", instock_source_cfg["product_agg_level_col"]),
            }
            for fb in instock_source_cfg["fallback_sources"]
        ],
    }

    item_family_source_cfg = cfg.get("item_family_source", {}) or {}
    item_family_column_map = {
        "product_col": item_family_source_cfg.get("product_col", "product_id"),
        "parent_col": item_family_source_cfg.get("parent_col", "parent_id"),
        "is_main_col": item_family_source_cfg.get("is_main_col", "is_main"),
    }

    item_family_rollup_cfg = cfg.get("item_family_rollup", {}) or {}
    item_family_rollup = {
        "daily_data": bool(item_family_rollup_cfg.get("daily_data", True)),
        "lost_sales": bool(item_family_rollup_cfg.get("lost_sales", False)),
        "inventory_warehouse": bool(item_family_rollup_cfg.get("inventory_warehouse", True)),
        "defined_scope": bool(item_family_rollup_cfg.get("defined_scope", False)),
    }

    dc_instock_cfg = cfg["dc_instock"]
    dc_instock_enabled = bool(dc_instock_cfg["enabled"])
    dc_instock_stock_threshold = dc_instock_cfg["stock_threshold"]

    report_end_mode = rw["report_end"]
    if report_end_mode not in ("as_of", "complete_month", "latest_day"):
        raise ValueError(
            f"reporting_window.report_end must be 'as_of', 'complete_month' or 'latest_day'; got {report_end_mode!r}"
        )
    window = _resolve_report_window(
        datetime.date.fromisoformat(rw["as_of_date"]),
        run_min,
        report_end_mode,
    )

    defined_scope = {**cfg["defined_scope"], "path": paths["PATH_DEFINED_SCOPE"]}

    grain = defined_scope.get("grain", "product_store")
    valid_grains = {"product", "product_store", "product_store_week"}
    if grain not in valid_grains:
        raise ValueError(f"defined_scope.grain must be one of {sorted(valid_grains)}; got {grain!r}")
    if grain in ("product_store", "product_store_week") and not defined_scope.get("store_col"):
        raise ValueError(f"defined_scope.grain={grain!r} requires defined_scope.store_col")
    if grain == "product_store_week" and not (
        defined_scope.get("date_col") or (defined_scope.get("year_col") and defined_scope.get("week_col"))
    ):
        raise ValueError(
            "defined_scope.grain='product_store_week' requires date_col OR both year_col and week_col"
        )
    defined_scope["grain"] = grain

    lse = cfg["lost_sales_ensemble"]
    if lse.get("enabled") and instock_source_enabled:
        raise ValueError(
            "lost_sales_ensemble.enabled and instock.method='weekly_source' cannot be combined: "
            "the fast/slow blend selects in_stock/total_days from the chosen model and uses its "
            "total_days to decide row existence, neither of which is computed when "
            "instock.weekly_source is used. "
            "Use at most one of the two."
        )
    if lse.get("enabled"):
        fast_clusters = lse.get("fast_mover_clusters")
        if not isinstance(fast_clusters, (list, tuple)) or not fast_clusters:
            raise ValueError("lost_sales_ensemble.fast_mover_clusters must be a non-empty list when enabled")
        if not all(isinstance(c, int) for c in fast_clusters):
            raise ValueError("lost_sales_ensemble.fast_mover_clusters must be integers (e.g. [1, 2, 3])")
        speed_cluster_format = str(lse.get("speed_cluster_format", "long")).strip().lower()
        if speed_cluster_format not in ("long", "wide"):
            raise ValueError(
                f"lost_sales_ensemble.speed_cluster_format={speed_cluster_format!r} must be 'long' or 'wide'"
            )
        lse["speed_cluster_format"] = speed_cluster_format
        if speed_cluster_format == "long" and not lse.get("speed_cluster_attribute_name"):
            raise ValueError(
                "lost_sales_ensemble.speed_cluster_attribute_name is required when "
                "speed_cluster_format='long' (default) and enabled=True"
            )
        if speed_cluster_format == "wide" and not lse.get("speed_cluster_value_col"):
            raise ValueError(
                "lost_sales_ensemble.speed_cluster_value_col is required when "
                "speed_cluster_format='wide' and enabled=True"
            )

    scope_source_cfg = cfg["scope_source"]
    scope_source_run_date = scope_source_cfg["run_date"]
    scope_source = {
        "mode": scope_source_cfg["mode"],
        "solution_id": _solution_ids(scope_source_cfg["solution_id"], "scope_source.solution_id"),
        "run_date": (
            datetime.date.fromisoformat(scope_source_run_date) if scope_source_run_date else None
        ),
        "roll_to_family_main": bool(scope_source_cfg["roll_to_family_main"]),
        "active_only": bool(scope_source_cfg["active_only"]),
        "instock_main_eligible_only": bool(scope_source_cfg["instock_main_eligible_only"]),
        "instock_exclude_unsuperseded_sizes": bool(scope_source_cfg["instock_exclude_unsuperseded_sizes"]),
    }
    if scope_source["mode"] not in ("defined_scope", "operation_scope"):
        raise ValueError(
            f"scope_source.mode must be 'defined_scope' or 'operation_scope'; got {scope_source['mode']!r}"
        )
    if scope_source["run_date"] is not None and scope_source["run_date"].weekday() != 6:
        raise ValueError(f"scope_source.run_date must be a Sunday; got {scope_source['run_date']}")
    operation_scope_mode = scope_source["mode"] == "operation_scope"
    if operation_scope_mode and grain == "product_store_week":
        raise ValueError("scope_source.mode='operation_scope' does not support defined_scope.grain='product_store_week'")

    blocked_scope_cfg = cfg["blocked_scope"]
    ui_parameters_path = blocked_scope_cfg["ui_parameters_path"]
    blocked_scope = {
        "rule": blocked_scope_cfg["rule"],
        "path": (
            fund_paste(bucket, *ui_parameters_path.strip("/").split("/"), "blocked_scope")
            if ui_parameters_path is not None
            else None
        ),
        # DC blocks of the same snapshot, read only when dc_solution_id is set.
        "dc_path": (
            fund_paste(bucket, *ui_parameters_path.strip("/").split("/"), "dc_blocked_scope")
            if ui_parameters_path is not None
            else None
        ),
    }
    if blocked_scope["rule"] not in ("after_scope_start", "all"):
        raise ValueError(f"blocked_scope.rule must be 'after_scope_start' or 'all'; got {blocked_scope['rule']!r}")
    if blocked_scope["path"] is not None and not operation_scope_mode:
        raise ValueError("blocked_scope.ui_parameters_path requires scope_source.mode='operation_scope'")

    blocked_metrics_cfg = blocked_scope_cfg["metrics"]
    if blocked_metrics_cfg != "all" and not isinstance(blocked_metrics_cfg, (list, tuple)):
        raise ValueError("blocked_scope.metrics must be 'all' or a list of metric names")
    blocked_requested = set(METRICS_ALL) if blocked_metrics_cfg == "all" else set(blocked_metrics_cfg)
    unknown_blocked_metrics = sorted(blocked_requested - set(METRICS_ALL))
    if unknown_blocked_metrics:
        raise ValueError(
            f"blocked_scope.metrics has unknown names {unknown_blocked_metrics}; "
            f"allowed: {list(METRICS_ALL)} or 'all'"
        )
    blocked_scope["metrics"] = [m for m in METRICS_ALL if m in blocked_requested]
    blocked_scope["dc_solution_id"] = (
        _solution_ids(blocked_scope_cfg["dc_solution_id"], "blocked_scope.dc_solution_id")
        if blocked_scope_cfg["dc_solution_id"] is not None
        else None
    )
    for key in ("kinds", "dc_kinds"):
        kinds = list(blocked_scope_cfg[key])
        unknown_kinds = sorted(set(kinds) - {"product", "product_destination", "destination"})
        if unknown_kinds or not kinds:
            raise ValueError(
                f"blocked_scope.{key} must be a non-empty subset of product / product_destination / destination; "
                f"got {kinds}"
            )
        blocked_scope[key] = kinds
    if blocked_scope["dc_solution_id"] is not None:
        if blocked_scope["path"] is None:
            raise ValueError("blocked_scope.dc_solution_id requires blocked_scope.ui_parameters_path")

    instock_daily_cfg = instock_cfg["daily"]
    history_start_raw = instock_daily_cfg["history_start"]
    daily_instock = {
        "count_start": instock_daily_cfg["count_start"],
        "require_daily_data": bool(instock_daily_cfg["require_daily_data"]),
        "history_start": datetime.date.fromisoformat(history_start_raw) if history_start_raw else None,
        "usable_only": bool(instock_daily_cfg["usable_only"]),
        "input_filters": list(instock_daily_cfg["input_filters"]),
    }
    if daily_instock["count_start"] not in ("first_daily_row", "scope_start", "earliest"):
        raise ValueError(
            "instock.daily.count_start must be 'first_daily_row', 'scope_start' or 'earliest'; "
            f"got {daily_instock['count_start']!r}"
        )
    if daily_instock["history_start"] is not None and daily_instock["history_start"] > window["EFFECTIVE_REPORT_START_DATE"]:
        raise ValueError(
            f"instock.daily.history_start {daily_instock['history_start']} is after the report window "
            f"start {window['EFFECTIVE_REPORT_START_DATE']}"
        )
    if instock_method == "daily":
        if grain == "product":
            raise ValueError("instock.method='daily' requires a store-level defined_scope.grain (product_store)")
        if cfg["lost_sales_ensemble"].get("enabled"):
            raise ValueError("instock.method='daily' and lost_sales_ensemble.enabled cannot both be True")
        if daily_instock["count_start"] != "first_daily_row" and not operation_scope_mode:
            raise ValueError(
                f"instock.daily.count_start={daily_instock['count_start']!r} needs the operation scope start "
                "date: set scope_source.mode='operation_scope' or count_start='first_daily_row'."
            )
        if daily_instock["count_start"] == "first_daily_row" and not daily_instock["require_daily_data"]:
            raise ValueError("instock.daily.count_start='first_daily_row' requires require_daily_data=True")
    if scope_source["instock_main_eligible_only"] and not (
        operation_scope_mode and scope_source["roll_to_family_main"] and instock_method == "daily"
    ):
        raise ValueError(
            "scope_source.instock_main_eligible_only requires scope_source.mode='operation_scope', "
            "roll_to_family_main=True and instock.method='daily'"
        )
    if scope_source["instock_exclude_unsuperseded_sizes"] and instock_method != "daily":
        raise ValueError("scope_source.instock_exclude_unsuperseded_sizes requires instock.method='daily'")
    if report_end_mode == "latest_day" and instock_method != "daily":
        raise ValueError(
            "reporting_window.report_end='latest_day' requires instock.method='daily': the YTD cut at the "
            "latest day splits the daily in-stock frame's fiscal week, which weekly in-stock sources cannot do"
        )

    git_cfg = cfg["goods_in_transit"]
    git_shift_days = git_cfg["date_shift_days"]
    if git_shift_days is not None and type(git_shift_days) is not int:
        raise ValueError("goods_in_transit.date_shift_days must be an integer or None")
    git_requested = set(git_cfg["inventory_metrics"])
    unknown_git_metrics = sorted(git_requested - set(INVENTORY_GIT_METRICS_ALL))
    if unknown_git_metrics:
        raise ValueError(
            f"goods_in_transit.inventory_metrics has unknown names {unknown_git_metrics}; "
            f"allowed: {list(INVENTORY_GIT_METRICS_ALL)}"
        )
    goods_in_transit = {
        "date_shift_days": git_shift_days,
        "roll_to_family_main": bool(git_cfg["roll_to_family_main"]),
        "store_instock": bool(git_cfg["store_instock"]),
        "dc_instock": bool(git_cfg["dc_instock"]),
        "inventory_metrics": [m for m in INVENTORY_GIT_METRICS_ALL if m in git_requested],
    }
    if git_shift_days is None and (
        goods_in_transit["store_instock"] or goods_in_transit["dc_instock"] or goods_in_transit["inventory_metrics"]
    ):
        raise ValueError(
            "goods_in_transit.date_shift_days is None (goods in transit off) but store_instock / dc_instock / "
            "inventory_metrics use it: set an integer, or turn them off"
        )
    if goods_in_transit["store_instock"] and instock_method != "daily":
        raise ValueError("goods_in_transit.store_instock=True requires instock.method='daily'")
    if goods_in_transit["inventory_metrics"] and not cfg["fiscal_calendar"]["use_fiscal_calendar"]:
        raise ValueError("goods_in_transit.inventory_metrics requires fiscal_calendar.use_fiscal_calendar=True")

    dimension_sources = []
    for src in cfg.get("dimension_sources", []) or []:
        resolved = dict(src)
        if not resolved.get("path") and resolved.get("path_segments"):
            resolved["path"] = fund_paste(bucket, *resolved["path_segments"])
        dimension_sources.append(resolved)

    # Root specs: EVERY column an enabled dimension_source contributes (columns + derived) is
    # root-defining -- never a cut (see README's "Dimension sources -> roots" section: "every one
    # of its columns becomes a root ... not a flat cut", and the mutual-exclusivity design
    # constraint). root_values supplies an explicit value->root-name mapping for a given dim_col;
    # a dim_col with no entry in root_values (or a source with root_values omitted entirely)
    # auto-discovers one root per distinct value instead (fiscal._resolve_root_definitions, since
    # that needs real data). Resolved here (not left for fiscal.py to re-derive) so root_values
    # stays the single place a root-defining column is declared, and so a root_values key that
    # doesn't match any of the source's own columns/derived fails loudly now instead of silently
    # never applying (that dim_col would otherwise fall through and become an ordinary cut).
    root_specs = []
    for src in dimension_sources:
        if not src.get("enabled"):
            continue
        contributed = list(
            dict.fromkeys(list(src.get("columns") or []) + list((src.get("derived") or {}).keys()))
        )
        root_values_cfg = dict(src.get("root_values") or {})
        unknown_root_value_cols = set(root_values_cfg) - set(contributed)
        if unknown_root_value_cols:
            raise ValueError(
                f"dimension_source {src.get('label', 'dimension_source')!r} root_values has "
                f"column(s) {sorted(unknown_root_value_cols)} not among its own columns/derived "
                f"{contributed}."
            )
        for dim_col in contributed:
            root_specs.append({"dim_col": dim_col, "root_values": root_values_cfg.get(dim_col) or {}})

    # Per-dimension value filters, from slices only. Applied to a slice's own breakdown
    # (see kpi_pipeline/kpi_long._filter_frames_for_dimension).
    slice_value_filters = dict(cfg["slices"].get("value_filters", {}) or {})
    _validate_value_filters(slice_value_filters)

    # Selected comparison kinds — validated and normalised to canonical order.
    comparisons_cfg = cfg.get("comparisons", {}) or {}
    requested_kinds = comparisons_cfg.get("enabled")
    if requested_kinds is None:
        requested_kinds = list(COMPARISON_KINDS_ALL)
    requested_set = {str(k).strip().lower() for k in requested_kinds}
    invalid_kinds = sorted(requested_set - set(COMPARISON_KINDS_ALL))
    if invalid_kinds:
        raise ValueError(
            f"comparisons.enabled has invalid kinds {invalid_kinds}; "
            f"allowed: {list(COMPARISON_KINDS_ALL)}"
        )
    comparison_kinds = [k for k in COMPARISON_KINDS_ALL if k in requested_set]
    if not comparison_kinds:
        raise ValueError(
            "comparisons.enabled resolved to an empty list; "
            f"choose at least one of {list(COMPARISON_KINDS_ALL)}"
        )

    # Selected comparable-pairs kinds — validated and normalised to canonical order. Only
    # meaningful when comparable_pairs.enabled=True; resolves to an empty list when disabled (no
    # need to force a non-empty kinds list on an off feature).
    comparable_pairs_cfg = cfg.get("comparable_pairs", {}) or {}
    comparable_pairs_enabled = bool(comparable_pairs_cfg.get("enabled", False))
    requested_comparable_kinds = comparable_pairs_cfg.get("kinds")
    if requested_comparable_kinds is None:
        requested_comparable_kinds = ["ytd"]
    comparable_requested_set = {str(k).strip().lower() for k in requested_comparable_kinds}
    invalid_comparable_kinds = sorted(comparable_requested_set - set(COMPARABLE_KINDS_ALL))
    if invalid_comparable_kinds:
        raise ValueError(
            f"comparable_pairs.kinds has invalid kinds {invalid_comparable_kinds}; "
            f"allowed: {list(COMPARABLE_KINDS_ALL)}"
        )
    comparable_kinds = (
        [k for k in COMPARABLE_KINDS_ALL if k in comparable_requested_set] if comparable_pairs_enabled else []
    )
    if comparable_pairs_enabled and not comparable_kinds:
        raise ValueError(
            "comparable_pairs.kinds resolved to an empty list while comparable_pairs.enabled=True; "
            f"choose at least one of {list(COMPARABLE_KINDS_ALL)}"
        )

    # Grain of the comparable same-pairs population itself (NOT defined_scope.grain -- see the
    # comparable_pairs config block). Validated the same way defined_scope.grain is, above.
    comparable_pairs_grain = comparable_pairs_cfg.get("grain", "product_store")
    comparable_pairs_pair_days = comparable_pairs_cfg["pair_days"]
    if comparable_pairs_pair_days not in ("unblocked", "all"):
        raise ValueError(f"comparable_pairs.pair_days must be 'unblocked' or 'all'; got {comparable_pairs_pair_days!r}")
    valid_comparable_grains = {"product", "product_store"}
    if comparable_pairs_grain not in valid_comparable_grains:
        raise ValueError(
            f"comparable_pairs.grain must be one of {sorted(valid_comparable_grains)}; "
            f"got {comparable_pairs_grain!r}"
        )

    half_periods = bool(cfg["fiscal_calendar"]["half_periods"])
    if "half" in comparable_kinds and not half_periods:
        raise ValueError("comparable_pairs.kinds 'half' needs fiscal_calendar.half_periods=True")

    output_cfg = cfg["output"]
    output_root = fund_paste(bucket, *output_cfg["path_segments"])
    run_date_raw = output_cfg.get("run_date")
    output_run_date = (
        run_date_raw.strip()
        if isinstance(run_date_raw, str) and run_date_raw.strip()
        else str(window["AS_OF_DATE"])
    )
    save_mode = output_cfg.get("save_mode", "incremental").lower()
    if save_mode not in {"initial", "incremental", "full_refresh"}:
        raise ValueError("output.save_mode must be one of: initial, incremental, full_refresh")

    metrics = cfg["metrics"]
    population_filters = dict(metrics.get("population_filters", {}) or {})
    unknown_metric_cols = sorted(set(metrics["metric_cols"]) - set(METRICS_ALL))
    if unknown_metric_cols:
        raise ValueError(f"metrics.metric_cols has unknown metrics {unknown_metric_cols}; allowed: {list(METRICS_ALL)}")
    _validate_population_filters(population_filters, metrics["metric_cols"])
    _validate_scope_diff_metrics(metrics["scope_diff_metrics"], metrics["metric_cols"])

    _validate_scope_adjustments(cfg.get("scope_adjustments", {}) or {})

    score_scope = cfg["score_scope"]
    min_pct = score_scope["min_percentile"]
    if min_pct > 1:
        min_pct = min_pct / 100.0

    html_cfg = cfg["html_report"]
    # The filename stays a template ({customer}, {report_end}): the report end is only final once
    # fiscal.apply_report_end_mode has cut it, so KPIRunner formats the filename when it writes.
    html_filename_template = html_cfg["filename"]
    try:
        html_filename_template.format(customer=customer, report_end="")
    except (KeyError, IndexError) as exc:
        raise ValueError(
            f"html_report.filename {html_filename_template!r} may only use the placeholders "
            "{customer} and {report_end}"
        ) from exc
    dimension_labels = html_cfg["dimension_labels"]
    if not isinstance(dimension_labels, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in dimension_labels.items()
    ):
        raise ValueError("html_report.dimension_labels must be a dict of slice dimension name -> tab label (str -> str)")
    html_output_segs = html_cfg["output_path_segments"]
    html_output_dir = fund_paste(bucket, *html_output_segs) if html_output_segs else None

    run_mode = cfg.get("run", {}).get("mode", "full").lower()
    if run_mode not in {"full", "html_only"}:
        raise ValueError("run.mode must be one of: full, html_only")

    return {
        "CONFIG": cfg,
        "CUSTOMER": customer,
        "RUN_MODE": run_mode,
        "BUCKET": bucket,
        **window,
        "REPORT_END_MODE": report_end_mode,
        "USE_FISCAL_CALENDAR": cfg["fiscal_calendar"]["use_fiscal_calendar"],
        "HALF_PERIODS": half_periods,
        "FISCAL_QUARTER_COL": cfg["fiscal_calendar"].get("column_map", {}).get("quarter_col"),
        "FISCAL_MONTH_COL": cfg["fiscal_calendar"].get("column_map", {}).get("month_col"),
        "FISCAL_MONTH_NAME_COL": cfg["fiscal_calendar"].get("column_map", {}).get("month_name_col"),
        "DAILY_TIME_COLUMNS": cfg["fiscal_calendar"]["daily_time_columns"],
        "SCOPE_MIN_PERCENTILE": min_pct,
        "SCOPE_MIN_WEEKS_FOR_FILTER": score_scope["min_weeks_for_filter"],
        "USE_HYBRID_SCOPE": cfg["scope"]["use_hybrid_scope"],
        "RUN_SCOPE_DIFF": cfg["scope"].get("run_scope_diff", False),
        "COMPARABLE_PAIRS_ENABLED": comparable_pairs_enabled,
        "COMPARABLE_KINDS": comparable_kinds,
        "COMPARABLE_PAIRS_GRAIN": comparable_pairs_grain,
        "COMPARABLE_PAIRS_PAIR_DAYS": comparable_pairs_pair_days,
        "COMPARISON_KINDS": comparison_kinds,
        "SCOPE_ADJUSTMENTS": cfg.get("scope_adjustments", {}),
        # .get() throughout, matching what the validators above actually require: they only
        # demand fast_mover_clusters when enabled=True, and speed_cluster_attribute_name when
        # format="long". Indexing these directly raised KeyError on configs the validators had
        # deliberately just accepted -- e.g. any format="wide" config omitting the unused
        # attribute-name key.
        "LOST_SALES_ENSEMBLE_ENABLED": lse.get("enabled", False),
        "FAST_MOVER_CLUSTERS": list(lse.get("fast_mover_clusters") or [1, 2, 3]),
        "SPEED_CLUSTER_FORMAT": lse.get("speed_cluster_format", "long"),
        "SPEED_CLUSTER_ATTRIBUTE_NAME": lse.get("speed_cluster_attribute_name"),
        "SPEED_CLUSTER_VALUE_COL": lse.get("speed_cluster_value_col", "product_speed_cluster"),
        "LOST_SALES_COLUMN_MAP": lost_sales_column_map,
        "LOST_SALES_SALES_FILTER": lost_sales_sales_filter,
        "INSTOCK_METHOD": instock_method,
        "INSTOCK_SOURCE_COLUMN_MAP": instock_source_column_map,
        "ITEM_FAMILY_COLUMN_MAP": item_family_column_map,
        "ITEM_FAMILY_ROLLUP": item_family_rollup,
        "SCOPE_SOURCE": scope_source,
        "BLOCKED_SCOPE": blocked_scope,
        "INSTOCK_DAILY": daily_instock,
        "GOODS_IN_TRANSIT": goods_in_transit,
        "DC_INSTOCK_ENABLED": dc_instock_enabled,
        "DC_INSTOCK_STOCK_THRESHOLD": dc_instock_stock_threshold,
        **paths,
        "DEFINED_SCOPE": defined_scope,
        "INPUT_FILTERS": cfg.get("input_filters", {}),
        "SLICE_DIMENSIONS": cfg["slices"]["dimensions"],
        "DERIVED_SLICE_DIMENSIONS": cfg["slices"]["derived_dimensions"],
        "DIMENSION_SOURCES": dimension_sources,
        "ROOT_SPECS": root_specs,
        "SLICE_VALUE_FILTERS": slice_value_filters,
        "METRIC_COLS": metrics["metric_cols"],
        "SCOPE_DIFF_METRICS": metrics["scope_diff_metrics"],
        "METRIC_LABELS": metrics["labels"],
        "PP_CHANGE_METRICS": frozenset(metrics["pp_change_metrics"]),
        "METRIC_POPULATION_FILTERS": population_filters,
        "SAVE_OUTPUTS": output_cfg["save_outputs"],
        "OUTPUT_SAVE_MODE": save_mode,
        "ALLOW_OVERWRITE_EXISTING": output_cfg.get("allow_overwrite_existing", False),
        "RECOMPUTE_COMPARISONS_FROM_HISTORY": output_cfg.get("recompute_comparisons_from_history", True),
        "PATH_OUTPUT_ROOT": output_root,
        "OUTPUT_RUN_DATE": output_run_date,
        # HTML report
        "HTML_REPORT_ENABLED": html_cfg.get("enabled", True),
        "HTML_REPORT_FILENAME_TEMPLATE": html_filename_template,
        "HTML_REPORT_TITLE": html_cfg.get("report_title") or None,
        "HTML_REPORT_METRIC_DEFS": html_cfg.get("metric_definitions") or {},
        "HTML_REPORT_OUTPUT_DIR": html_output_dir,
        "HTML_REPORT_WEEKLY_DISPLAY_WEEKS": html_cfg.get("weekly_display_weeks"),
        "HTML_REPORT_MONTHLY_DISPLAY_MONTHS": html_cfg.get("monthly_display_months"),
        "HTML_REPORT_QUARTERLY_DISPLAY_QUARTERS": html_cfg.get("quarterly_display_quarters"),
        "HTML_REPORT_HALF_DISPLAY_HALVES": html_cfg["half_display_halves"],
        "HTML_REPORT_YEARLY_DISPLAY_YEARS": html_cfg.get("yearly_display_years"),
        "HTML_REPORT_ROOT_LABELS": html_cfg["root_labels"],
        "HTML_REPORT_DIMENSION_LABELS": dimension_labels,
    }

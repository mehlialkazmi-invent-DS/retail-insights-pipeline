# Generic reference config for the retail-insights-pipeline toolkit -- a starting point for a
# NEW customer, not any specific customer's own deployed values. Copy this file, then replace
# every placeholder below with your own paths/business rules; see README.md for full docs on
# every CONFIG key.
# Usage (Databricks, same folder as main.ipynb): %run ./config
#                                                 settings = materialize(fund.paste)
#
# GETTING STARTED — minimum edits before your first run:
#   1. customer: your customer's slug (used to resolve the datastore bucket by default).
#   2. path_segments: point every entry at your own tables (defined_scope, daily_data,
#      products, lost_sales at minimum).
#   3. defined_scope: column names on YOUR scope table (product_col/store_col/date_col/etc).
#   4. reporting_window.as_of_date: today, or whatever date you want the report to run through.
#   5. service_metrics.excluded_store_ids: any e-com/non-physical "stores" to exclude from
#      WOS/turnover/mean_stock/instock/lost sales (leave [] if none apply).
#
# OPTIONAL, gated features (all OFF/empty by default in this template — turn on as needed):
#   scope_adjustments   — manual scope additions/removals from a CSV/Delta source, e.g. a
#                         business-curated product list that should always be in (or out of)
#                         scope regardless of what the scope table itself says. See the
#                         disabled example below for the pattern (store_col=None + date_col/
#                         year_col/week_col=None = "every store, every week" for that source's
#                         product_ids).
#   dimension_sources   — join an external table onto products to add a new slice dimension
#                         that can also become its OWN root population tab (see README's
#                         "Dimension sources -> roots" section) — e.g. a channel or program
#                         flag from a table the products master doesn't itself carry.
#   lost_sales_ensemble — blend a fast/slow lost-sales model pair by product sales-speed
#                         cluster instead of reading a single lost-sales source as-is.
#   instock_source      — read in-stock rate from a SEPARATE table instead of lost_sales_
#                         source's own in_stock/total_days columns (e.g. when a different
#                         pipeline computes instock than the one computing lost sales).
#                         Supports fallback_sources to backfill weeks a rolling-window
#                         source's primary column-set doesn't reach (see README).
#   comparable_pairs    — like-for-like YTD over pairs present in both years of each link;
#                         requires 2+ years in the report window (run_min_date spanning that
#                         far back) to have anything to compute.

import copy
import datetime
import os
from typing import Any, Callable, Dict, Optional

# Period-over-period comparison kinds the pipeline can produce, in canonical order.
# QoQ/MoM/WoW comparison tables were dropped for simplicity — the Quarter/Monthly/Weekly period
# tabs already show recent-period value trends, covering "recent quarters/months/weeks" without
# a separate delta table.
COMPARISON_KINDS_ALL = ("yoy", "ytd")

CONFIG: Dict[str, Any] = {
    # =============================================================================
    # IDENTITY & RUN WINDOW
    # =============================================================================
    "customer": "your_customer",  # datastore bucket defaults to /mnt/invent-{customer}-datastore
    "run": {
        # full       = compute KPIs from source Delta tables (default)
        # html_only  = load saved outputs from output.path_segments and render HTML only
        "mode": "full",
    },
    "reporting_window": {
        # Update as_of_date before each run. REPORT_END_DATE resolves to the last
        # completed Saturday on or before this date.
        "as_of_date": "2026-09-01",
        "run_min_date": None,  # None = YTD from Jan 1; e.g. "2024-01-01" for multi-year
    },
    # =============================================================================
    # CALENDAR
    # =============================================================================
    "fiscal_calendar": {
        "use_fiscal_calendar": True,
        # Your fiscal_cal upload's column names (used since use_fiscal_calendar=True above).
        # Each is auto-detected/optional -- read when present, derived when absent. If your
        # fiscal year is calendar-offset (e.g. doesn't start in January), set month_name_col so
        # the Monthly tab reads a real display label verbatim instead of deriving one from the
        # (fiscal, not calendar) month number -- a fiscal month number doesn't reliably map to
        # a real calendar month once the fiscal year itself is offset.
        "column_map": {
            "quarter_col": "Quarter",
            "month_col": "Month",
            "month_name_col": "month_name",
        },
        # Column-name map for the RAW daily-data table -- only consulted on the CIVIL path
        # (use_fiscal_calendar=False). "year" is unused dead config: Year always comes from
        # `date`, never a raw 'year' column (ISO week-year risk).
        "daily_time_columns": {
            "date": "date",
            "year": "year",
            "week": "week",
        },
    },
    # =============================================================================
    # SCOPE & POPULATION
    # =============================================================================
    "score_scope": {
        # Only consulted for the MISSING weeks under hybrid scope (see "scope" below).
        "min_percentile": 0.2,
        "min_weeks_for_filter": 2,
    },
    "scope": {
        # Defined-scope grain (product / product_store / product_store_week) is set in
        # "defined_scope" below. Hybrid only adds a backfill on top of that:
        #   False (default) = final scope is the defined scope as-is.
        #   True (hybrid)   = also backfill (from score scope) the window weeks the defined
        #                     scope leaves uncovered. No-op for the week-agnostic grains
        #                     (product, product_store); meaningful for product_store_week.
        "use_hybrid_scope": False,
        "run_scope_diff": False,
    },
    "defined_scope": {
        # grain — how the scope table defines membership:
        #   "product"            -> distinct product_id (store- and week-agnostic: every store,
        #                           every window week, for each in-scope product).
        #   "product_store"      -> distinct (product_id, store_id); week-agnostic (default).
        #   "product_store_week" -> the scope table's own (product, store, week) rows are honoured
        #                           (strict); weeks come from date_col (or year_col/week_col).
        # Week-agnostic grains span the whole report window; product_store_week is the only grain
        # whose hybrid backfill fills weeks the scope table does not cover.
        "grain": "product_store",
        "product_col": "product_id",
        "store_col": "store_id",        # required for product_store / product_store_week grains
        # date/year/week: read ONLY for product_store_week grain (to resolve the scope's weeks).
        #   DATE path:   date_col -> fiscal_cal -> Year/Week (preferred).
        #   NATIVE path: date_col=None, set year_col/week_col (year must be a true calendar year).
        "date_col": "week_start_date",
        "year_col": None,
        "week_col": None,
    },
    "scope_adjustments": {
        # ---------------------------------------------------------------------------
        # ADDITIONS: force specific products into scope regardless of the defined scope
        # table (e.g. a business-curated "always report on these" list). Disabled example
        # below shows the pattern -- store_col=None + date_col/year_col/week_col=None means
        # "every store, every week in the report window" for this source's product_ids.
        # ---------------------------------------------------------------------------
        "additions": [
            {
                "enabled": False,  # flip on once path points at a real table/CSV
                "label": "example_addition",
                "source": "csv",
                "path": "/Workspace/Shared/your_project/data/addition_product_ids.csv",
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
        # ---------------------------------------------------------------------------
        # REMOVALS: exclude specific products from scope (e.g. a business-curated
        # "never report on these" list). Same shape/mechanics as additions above.
        # ---------------------------------------------------------------------------
        "removals": [
            {
                "enabled": False,  # flip on once path points at a real table/CSV
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
        "products": ["master-data", "products"],
        # Add a model_id=... path segment here if your lost-sales table is partitioned by model.
        "lost_sales": ["noob", "lost-sales"],
        "defined_scope": ["analysis", "instock_rate", "instock_rate_scope"],
        # product_agg_level -> product_id map for lost_sales_source/instock_source's
        # product_agg_level_col (see README) -- only needed if either source is keyed by a
        # planning/DFU level instead of product_id.
        "product_planning_level": ["operation", "product_planning_level"],
    },
    "input_filters": {
        # Optional Spark SQL expressions applied when reading each source.
        # NOTE: when lost_sales_ensemble.enabled=True, this "lost_sales" filter list is
        # applied to BOTH the fast (path_segments.lost_sales) and slow
        # (lost_sales_ensemble.slow_path_segments) sources — same schema, same filters.
        "defined_scope": [],
        "lost_sales": [],
        "daily_data": ["usable = 1"],
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES SOURCE — column mapping for the raw lost-sales table
    # ---------------------------------------------------------------------------
    # Downstream code always sees canonical column names regardless of this mapping (see
    # README). Defaults below match a typical noob/lost-sales schema -- override only the
    # columns your own table names differently.
    "lost_sales_source": {
        "week_col": "week_start_date",
        "product_col": "product_id",
        "store_col": "store_id",
        "lost_sales_col": "lost_sales",
        "in_stock_col": "in_stock",
        "total_days_col": "details.total_days",  # supports a dotted nested-struct path
        "product_agg_level_col": None,  # set if source has product_agg_level, not product_id (see README)
    },
    # ---------------------------------------------------------------------------
    # INSTOCK SOURCE — optional override to read in-stock days from a DIFFERENT table
    # ---------------------------------------------------------------------------
    # OFF by default: in-stock/total-days come from lost_sales_source above. Turn this on
    # only when a DIFFERENT pipeline computes instock than the one computing lost sales.
    # Mutually exclusive with lost_sales_ensemble below.
    #
    # Real-world example (one deployment's actual config, kept here to illustrate
    # fallback_sources -- adapt the path/columns for your own source, don't copy verbatim):
    #   "enabled": True,
    #   "path_segments": ["reporting", "future_visibility", "some_report_table"],
    #   "week_col": "TY_week_start_date",
    #   "in_stock_col": "TY_total_days_instock",
    #   "total_days_col": "TY_total_day",
    #   "product_agg_level_col": "product_agg_level",
    #   "store_col": None,
    #   # fallback_sources: additional column-sets from the SAME table, appended in order to
    #   # fill weeks the primary column-set doesn't have (e.g. a rolling-window source whose
    #   # TY_ only reaches back so far, backfilled by LY_/LLY_ columns carrying the identical
    #   # formula for the calendar week 52/104 weeks earlier -- see README). Each fallback
    #   # entry inherits product_col/store_col/product_agg_level_col from above unless
    #   # overridden.
    #   "fallback_sources": [
    #       {"week_col": "LY_week_start_date", "in_stock_col": "LY_total_days_instock", "total_days_col": "LY_total_day"},
    #       {"week_col": "LLY_week_start_date", "in_stock_col": "LLY_total_days_instock", "total_days_col": "LLY_total_day"},
    #   ],
    "instock_source": {
        "enabled": False,
        "path_segments": None,  # required when enabled=True, e.g. ["some", "instock", "table"]
        "week_col": "week_start_date",
        "product_col": "product_id",
        "store_col": "store_id",
        "in_stock_col": "in_stock",
        "total_days_col": "total_days",
        "product_agg_level_col": None,
        "fallback_sources": [],  # optional additional column-sets from the same table (see above)
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES ENSEMBLE — blend two lost-sales models by product sales speed
    # ---------------------------------------------------------------------------
    # OFF by default: lost_sales_source above is read as a single source, as-is. Turn this on
    # to blend a fast-mover model (path_segments.lost_sales) with a slow-mover model (below) by
    # each product's own sales-speed cluster instead -- fast movers (cluster in
    # fast_mover_clusters) take the fast model; everyone else (other clusters AND products with
    # no/NULL cluster) takes the slow model. All three aggregate fields (lost_sales, in_stock,
    # total_days) for a given product/store/week always come from the SAME chosen model.
    "lost_sales_ensemble": {
        "enabled": False,
        "slow_path_segments": ["noob", "lost-sales"],  # e.g. a longer-lookback model variant
        # Product sales-speed source. "long" = a long-format attributes table (one row per
        # product_id x attribute_name); "wide" = the cluster is already its own column.
        "speed_cluster_path_segments": ["noob", "product-cluster-attributes-snapshot"],
        "speed_cluster_format": "long",
        "speed_cluster_attribute_name": "sales_speed",
        "speed_cluster_value_col": "product_speed_cluster",  # unused under format="long"
        "fast_mover_clusters": [1, 2, 3],
    },
    # =============================================================================
    # CUTS & DIMENSIONS
    # =============================================================================
    # ---------------------------------------------------------------------------
    # SLICES — dimensions sourced from master-data/products
    # ---------------------------------------------------------------------------
    "slices": {
        "dimensions": ["brand"],  # any column(s) on your products table
        "derived_dimensions": {
            # Example: a SQL CASE expression evaluated against the products table, producing
            # a new cut dimension not present as a raw column. Replace with your own, or
            # remove this entry if you don't need any derived dimensions.
            "example_derived": "CASE WHEN brand = 'A' THEN 'Group A' ELSE 'Other' END",
        },
        # Restrict which values of a slice dimension appear in the breakdown (that
        # dimension only; Overall and other slices are unaffected). Two shapes:
        #   LIST (include-only): omit -> all incl NULL | [] -> all non-null | ["A","B"] -> only those
        #   DICT (include/exclude): {"include": ["A"]} keep only A | {"exclude": ["A"]} keep the
        #       rest incl NULL | add "keep_null": True/False to force the NULL bucket.
        "value_filters": {},
    },
    # ---------------------------------------------------------------------------
    # DIMENSION SOURCES — optional external tables that become named ROOT populations
    # ---------------------------------------------------------------------------
    # Gated feature: disabled example below shows the pattern. Each enabled source is
    # left-joined onto products by join_key, contributes new slice dimension(s), and — via
    # root_values — can also become its own root population tab (see README's "Dimension
    # sources -> roots" section) instead of an ordinary flat cut.
    "dimension_sources": [
        {
            "enabled": False,
            "label": "example_dimension_source",
            "source": "delta",
            "path_segments": ["operation", "some_attribute_table"],
            "join_key": "product_id",
            "columns": [],
            "derived": {
                # Products absent from the source get NULL, not the ELSE branch -- fillna
                # (below) imputes that if you need a clean two-value split with no NULLs.
                "IS_EXAMPLE_FLAG": "CASE WHEN some_column = 'X' THEN 'yes' ELSE 'no' END",
            },
            "fillna": {"IS_EXAMPLE_FLAG": "no"},
            # Root "example_root" = IS_EXAMPLE_FLAG=='yes' only ('no'/NULL aren't their own root).
            "root_values": {"IS_EXAMPLE_FLAG": {"yes": "example_root"}},
        },
    ],
    # =============================================================================
    # COMPARISONS
    # =============================================================================
    "comparisons": {
        # Which period-over-period comparisons to compute, print, save, and render.
        # Choose any subset of "yoy", "ytd". kpi_long (the raw per-period metrics, including
        # quarter/monthly/weekly value trends) is always produced in full regardless of this
        # setting — there is no separate QoQ/MoM/WoW comparison table.
        "enabled": ["yoy", "ytd"],
    },
    "comparable_pairs": {
        # OFF by default -- requires run_min_date to span at least 2 years (e.g. "2024-01-01"
        # for a comparable 2024-vs-2025 link) to have anything to compute. Like-for-like YTD:
        # recomputes YTD metrics over only the (product_id, store_id) pairs present in BOTH
        # years of each consecutive-year link.
        "enabled": False,
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
            "WOS",
            "wos_revenue",
            "wos_cost",
            "inventory_turnover_rate",
            "in_stock_rate",
            "weighted_instock_rate",
            "lost_sales_pct",
        ],
        "scope_diff_metrics": [
            "total_sales_quantity",
            "total_sales_revenue",
            "total_inventory",
            "distinct_product_count",
            "distinct_pair_count",
            "WOS",
            "in_stock_rate",
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
            "WOS": "WOS (units)",
            "wos_revenue": "WOS revenue",
            "wos_cost": "WOS cost",
            "inventory_turnover_rate": "Inventory Turnover Rate",
            "in_stock_rate": "In-Stock Rate",
            "weighted_instock_rate": "Weighted In-Stock Rate",
            "lost_sales_pct": "Lost Sales %",
            "distinct_product_count": "Distinct products",
            "distinct_store_count": "Distinct stores",
            "distinct_pair_count": "Distinct pairs",
        },
        "pp_change_metrics": ["in_stock_rate", "weighted_instock_rate", "lost_sales_pct"],
    },
    "service_metrics": {
        # Store IDs to exclude from WOS/turnover/mean_stock/instock/lost-sales specifically
        # (e.g. e-com fulfillment "stores" with no real shelf inventory) -- sales/AUR/AUC still
        # include all scoped stores regardless. Empty by default; add your own if applicable.
        "excluded_store_ids": [],
    },
    # =============================================================================
    # OUTPUT & REPORTING
    # =============================================================================
    "output": {
        "save_outputs": True,
        "path_segments": ["analysis", "kpi_reports", "outputs"],
        "run_date": None,
        "save_mode": "initial",
        "allow_overwrite_existing": True,
        "recompute_comparisons_from_history": True,
    },
    "html_report": {
        "enabled": True,
        "filename": "kpi_report_{customer}_{report_end}.html",
        "report_title": "KPI Report",  # customize per client, e.g. "Acme Corp KPI Report"
        "output_path_segments": None,
        "metric_definitions": {},
        # Set to 3 or 4 to cap if the report gets too wide with many years in view.
        "weekly_display_weeks": 5,
        "monthly_display_months": 5,
        "quarterly_display_quarters": 5,
        "yearly_display_years": None,
    },
}


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

    fc = out.setdefault("fiscal_calendar", {})
    if "KPI_USE_FISCAL_CALENDAR" in os.environ:
        fc["use_fiscal_calendar"] = _parse_bool(os.environ["KPI_USE_FISCAL_CALENDAR"])

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

    ins = out.setdefault("instock_source", {})
    if "KPI_INSTOCK_SOURCE_ENABLED" in os.environ:
        ins["enabled"] = _parse_bool(os.environ["KPI_INSTOCK_SOURCE_ENABLED"])
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
) -> Dict[str, Any]:
    report_start = _sunday_of_week(datetime.date(as_of.year, 1, 1))
    report_end = _last_completed_saturday(as_of)
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
        "PATH_PRODUCTS": fund_paste(bucket, *path_segments["products"]),
        "PATH_LOST_SALES": fund_paste(bucket, *path_segments["lost_sales"]),
        "PATH_DEFINED_SCOPE": fund_paste(bucket, *path_segments["defined_scope"]),
        "PATH_LOST_SALES_SLOW": fund_paste(bucket, *cfg["lost_sales_ensemble"]["slow_path_segments"]),
        "PATH_SPEED_CLUSTER": fund_paste(bucket, *cfg["lost_sales_ensemble"]["speed_cluster_path_segments"]),
        "PATH_PRODUCT_PLANNING_LEVEL": fund_paste(bucket, *path_segments["product_planning_level"]),
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

    instock_source_cfg = cfg.get("instock_source", {}) or {}
    instock_source_enabled = bool(instock_source_cfg.get("enabled", False))
    if instock_source_enabled and not instock_source_cfg.get("path_segments"):
        raise ValueError("instock_source.path_segments is required when instock_source.enabled=True")
    paths["PATH_INSTOCK_SOURCE"] = (
        fund_paste(bucket, *instock_source_cfg["path_segments"]) if instock_source_enabled else None
    )
    instock_source_column_map = {
        "week_col": instock_source_cfg.get("week_col", "week_start_date"),
        "product_col": instock_source_cfg.get("product_col", "product_id"),
        "store_col": instock_source_cfg.get("store_col", "store_id"),
        "in_stock_col": instock_source_cfg.get("in_stock_col", "in_stock"),
        "total_days_col": instock_source_cfg.get("total_days_col", "total_days"),
        "product_agg_level_col": instock_source_cfg.get("product_agg_level_col"),
        "fallback_sources": [
            {
                "week_col": fb["week_col"],
                "in_stock_col": fb["in_stock_col"],
                "total_days_col": fb["total_days_col"],
                "product_col": fb.get("product_col", instock_source_cfg.get("product_col", "product_id")),
                "store_col": fb.get("store_col", instock_source_cfg.get("store_col", "store_id")),
                "product_agg_level_col": fb.get("product_agg_level_col", instock_source_cfg.get("product_agg_level_col")),
            }
            for fb in instock_source_cfg.get("fallback_sources", []) or []
        ],
    }

    window = _resolve_report_window(
        datetime.date.fromisoformat(rw["as_of_date"]),
        run_min,
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
            "lost_sales_ensemble.enabled and instock_source.enabled cannot both be True: "
            "the fast/slow blend picks in_stock/total_days per-row from whichever model was "
            "chosen, which instock_source's separate-table override does not compose with. "
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

    dimension_sources = []
    for src in cfg.get("dimension_sources", []) or []:
        resolved = dict(src)
        if not resolved.get("path") and resolved.get("path_segments"):
            resolved["path"] = fund_paste(bucket, *resolved["path_segments"])
        dimension_sources.append(resolved)

    # Root specs: one entry per (dim_col, root_values) pair declared on an enabled
    # dimension_source -- resolved here (not left for fiscal.py to re-derive) so root_values
    # stays the single place a dimension_source's root-defining column is declared. Consumed by
    # fiscal.py's _resolve_root_definitions to build ctx.root_definitions and exclude these
    # columns from ctx.cut_dimensions (see README's "Dimension sources -> roots" section).
    root_specs = [
        {"dim_col": dim_col, "root_values": mapping}
        for src in dimension_sources
        if src.get("enabled")
        for dim_col, mapping in (src.get("root_values") or {}).items()
    ]

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
    score_scope = cfg["score_scope"]
    min_pct = score_scope["min_percentile"]
    if min_pct > 1:
        min_pct = min_pct / 100.0

    html_cfg = cfg.get("html_report", {})
    html_filename_tpl = html_cfg.get("filename", "kpi_report_{customer}_{report_end}.html")
    html_filename = html_filename_tpl.format(
        customer=customer,
        report_end=str(window["REPORT_END_DATE"]),
    )
    html_output_segs = html_cfg.get("output_path_segments")
    html_output_path = (
        fund_paste(bucket, *html_output_segs, html_filename)
        if html_output_segs
        else None
    )

    run_mode = cfg.get("run", {}).get("mode", "full").lower()
    if run_mode not in {"full", "html_only"}:
        raise ValueError("run.mode must be one of: full, html_only")

    return {
        "CONFIG": cfg,
        "CUSTOMER": customer,
        "RUN_MODE": run_mode,
        "BUCKET": bucket,
        **window,
        "EXCLUDED_STORE_IDS_FOR_SERVICE_METRICS": cfg["service_metrics"]["excluded_store_ids"],
        "USE_FISCAL_CALENDAR": cfg["fiscal_calendar"]["use_fiscal_calendar"],
        "FISCAL_QUARTER_COL": cfg["fiscal_calendar"].get("column_map", {}).get("quarter_col"),
        "FISCAL_MONTH_COL": cfg["fiscal_calendar"].get("column_map", {}).get("month_col"),
        "FISCAL_MONTH_NAME_COL": cfg["fiscal_calendar"].get("column_map", {}).get("month_name_col"),
        "DAILY_TIME_COLUMNS": cfg["fiscal_calendar"]["daily_time_columns"],
        "SCOPE_MIN_PERCENTILE": min_pct,
        "SCOPE_MIN_WEEKS_FOR_FILTER": score_scope["min_weeks_for_filter"],
        "USE_HYBRID_SCOPE": cfg["scope"]["use_hybrid_scope"],
        "RUN_SCOPE_DIFF": cfg["scope"].get("run_scope_diff", False),
        "COMPARABLE_PAIRS_ENABLED": cfg.get("comparable_pairs", {}).get("enabled", False),
        "COMPARISON_KINDS": comparison_kinds,
        "SCOPE_ADJUSTMENTS": cfg.get("scope_adjustments", {}),
        "LOST_SALES_ENSEMBLE_ENABLED": lse["enabled"],
        "FAST_MOVER_CLUSTERS": list(lse["fast_mover_clusters"]),
        "SPEED_CLUSTER_FORMAT": lse.get("speed_cluster_format", "long"),
        "SPEED_CLUSTER_ATTRIBUTE_NAME": lse["speed_cluster_attribute_name"],
        "SPEED_CLUSTER_VALUE_COL": lse.get("speed_cluster_value_col", "product_speed_cluster"),
        "LOST_SALES_COLUMN_MAP": lost_sales_column_map,
        "INSTOCK_SOURCE_ENABLED": instock_source_enabled,
        "INSTOCK_SOURCE_COLUMN_MAP": instock_source_column_map,
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
        "SAVE_OUTPUTS": output_cfg["save_outputs"],
        "OUTPUT_SAVE_MODE": save_mode,
        "ALLOW_OVERWRITE_EXISTING": output_cfg.get("allow_overwrite_existing", False),
        "RECOMPUTE_COMPARISONS_FROM_HISTORY": output_cfg.get("recompute_comparisons_from_history", True),
        "PATH_OUTPUT_ROOT": output_root,
        "OUTPUT_RUN_DATE": output_run_date,
        # HTML report
        "HTML_REPORT_ENABLED": html_cfg.get("enabled", True),
        "HTML_REPORT_FILENAME": html_filename,
        "HTML_REPORT_TITLE": html_cfg.get("report_title") or None,
        "HTML_REPORT_METRIC_DEFS": html_cfg.get("metric_definitions") or {},
        "HTML_REPORT_OUTPUT_PATH": html_output_path,
        "HTML_REPORT_WEEKLY_DISPLAY_WEEKS": html_cfg.get("weekly_display_weeks"),
        "HTML_REPORT_MONTHLY_DISPLAY_MONTHS": html_cfg.get("monthly_display_months"),
        "HTML_REPORT_QUARTERLY_DISPLAY_QUARTERS": html_cfg.get("quarterly_display_quarters"),
        "HTML_REPORT_YEARLY_DISPLAY_YEARS": html_cfg.get("yearly_display_years"),
    }

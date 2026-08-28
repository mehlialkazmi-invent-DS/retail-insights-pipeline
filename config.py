# Edit CONFIG below, then in main.ipynb:  %run ./config  ->  settings = materialize(fund.paste)

import copy
import datetime
import os
from typing import Any, Callable, Dict, Optional

# Period-over-period comparison kinds the pipeline can produce, in canonical order.
# QoQ/MoM/WoW comparison tables were dropped for simplicity — the Quarter/Monthly/Weekly period
# tabs already show recent-period value trends (see html_report's *_display_* settings), which
# covers the same "recent quarters/months/weeks" need without a separate delta table.
COMPARISON_KINDS_ALL = ("yoy", "ytd")

CONFIG: Dict[str, Any] = {
    "customer": "your_client",
    "run": {
        # full       = compute KPIs from source Delta tables (default)
        # html_only  = load saved outputs from output.path_segments and render HTML only
        "mode": "full",
    },
    "reporting_window": {
        # as_of_date: end anchor for the run.
        # run_min_date: optional narrow start (null/"" = YTD from Jan 1). Both align to Sun-Sat weeks.
        # REPORT_END_DATE resolves to the last completed Saturday on or before as_of_date.
        "as_of_date": "2026-06-15",
        "run_min_date": None,
    },
    "service_metrics": {
        # E-com / non-service stores excluded from instock, WOS, lost sales %, mean stock.
        # Sales totals still include all scoped stores.
        "excluded_store_ids": [],
    },
    "fiscal_calendar": {
        "use_fiscal_calendar": True,
        "daily_time_columns": {
            "date": "date",
            "year": "year",
            "week": "week",
        },
    },
    "score_scope": {
        # Score backfill keeps a week when weekly_sales >= p(min_percentile) AND
        # weekly_inventory >= p(min_percentile), per (product_id, store_id).
        # weekly_inventory = last available daily snapshot in the fiscal week (not Saturday-only).
        # Only consulted for the MISSING weeks under hybrid scope (see "scope" below).
        "min_percentile": 0.2,
        "min_weeks_for_filter": 2,  # skip filter when pair has this many weeks or fewer
    },
    "scope": {
        # The defined-scope grain (product / product_store / product_store_week) is set in
        # "defined_scope" below. Hybrid only layers a backfill on top of the defined scope:
        #
        # False (default) = final scope is the defined scope exactly as built at its grain.
        # True (hybrid)   = also backfill (from score scope, the activity percentile above)
        #                   the window weeks the defined scope leaves uncovered. This is a
        #                   no-op for the week-agnostic grains (product, product_store), which
        #                   already cover every window week; it is meaningful only for the
        #                   product_store_week grain, whose covered weeks are those in the table.
        "use_hybrid_scope": False,
        # True  = compute score scope and annual defined-vs-score KPI diff (scope_diff)
        # False = skip score scope unless required for hybrid backfill (default)
        "run_scope_diff": False,
    },
    "comparable_pairs": {
        # GATED, opt-in like-for-like view, YTD-only (no comparable YoY). When enabled, YTD
        # metrics are recomputed over only the (product_id, store_id) pairs present in BOTH years
        # of each consecutive-year link, then compared — isolating like-for-like change from mix
        # shifts (new/closed pairs). Each link gets its OWN pair universe: comparing 2025 YTD vs
        # 2026 YTD uses pairs present in both 2025 and 2026; if 2024 is also in the window, the
        # 2024-vs-2025 link independently uses pairs present in both 2024 and 2025 — a pair need
        # not also be present in 2026 to count for that link.
        # Pair-level data exists only for the current run window, so a comparable comparison
        # appears only when the run window spans at least 2 years.
        # Output:
        #   * comparable_kpi_long Delta table (per-link YTD metrics + comparable_pair_count)
        #   * comparable_comparison_ytd Delta table
        #   * a second "Comparable pairs" comparison table on the YTD panel in the HTML report
        "enabled": False,
    },
    "comparisons": {
        # Which period-over-period comparisons to compute, print, save, and render.
        # Choose any subset of:
        #   "yoy" (year-over-year, full calendar/fiscal year vs the prior full year)
        #   "ytd" (each year's elapsed window — only the fiscal quarters fully closed as of
        #       as_of_date for the latest year, that SAME quarter-set applied to every year —
        #       vs the prior year's same window, chained across consecutive years, e.g. 2026 YTD
        #       vs 2025 YTD and 2025 YTD vs 2024 YTD. Use this instead of "yoy" once the current
        #       year is only partially reported — "yoy" would otherwise compare a partial current
        #       year against a full prior year.)
        # There is no separate QoQ/MoM/WoW comparison table — the Quarter/Monthly/Weekly period
        # tabs (kpi_long, always produced in full) already show the N most recent quarters/
        # months/weeks as plain value trends (see html_report's *_display_* settings), which
        # covers "recent period" reporting without a delta table.
        # Only the selected kinds are computed, saved as comparison_{kind} Delta tables,
        # and shown in the HTML report; the others are skipped entirely. kpi_long (the raw
        # per-period metrics, including the "ytd" period type) is always produced in full
        # regardless of this setting.
        #
        # Reading a comparison from saved history: a single latest-week run can still
        # produce e.g. YoY. With output.save_mode="incremental" and
        # output.recompute_comparisons_from_history=True, the selected comparisons are
        # rebuilt from the FULL merged kpi_long (this run's window unioned onto prior saved
        # runs), so prior years come from the saved data — no need to recompute them this run.
        # (Same applies to comparable_pairs comparisons.)
        "enabled": ["yoy", "ytd"],
    },
    "scope_adjustments": {
        # Optional manual adds/removes applied after hybrid/defined scope is built.
        # MULTIPLE entries: both "additions" and "removals" are lists — add as many
        # objects as you need, one per source table/CSV.  Each runs in order.
        # source: "delta" (default) or "csv" — auto-detected when path ends with .csv
        "additions": [
            {
                "enabled": False,
                "label": "manual_add",
                "source": "delta",
                "path_segments": ["analysis", "kpi_reports", "manual_scope_additions"],
                # CSV example (source auto-detected from .csv extension):
                # "source": "csv",
                # "path": "/mnt/invent-{customer}-datastore/analysis/kpi_reports/manual_additions.csv",
                # "csv_options": {"header": True, "inferSchema": True},
                # "location": "datastore",  # "datastore" (default) or "workspace" (/Workspace/... CSV via file: scheme)
                # Expected logical keys: product_id, store_id, and a date (via date_col).
                "join_keys": ["product_id", "store_id"],
                "product_col": "product_id",
                "store_col": "store_id",
                "date_col": "week_start_date",
                "year_col": None,
                "week_col": None,
            }
        ],
        "removals": [
            {
                "enabled": False,
                "source": "delta",
                "path_segments": ["analysis", "kpi_reports", "manual_scope_removals"],
                # "location": "datastore",  # "datastore" (default) or "workspace" for /Workspace/... CSVs
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": "store_id",
                "date_col": None,
                "year_col": None,
                "week_col": None,
            }
        ],
    },
    # ---------------------------------------------------------------------------
    # DIMENSION SOURCES — extra slices from tables other than master-data/products
    # ---------------------------------------------------------------------------
    # Use dimension_sources ONLY when a breakdown column does NOT live on the products
    # table. If the column already exists on (or is derivable from) products, use
    # "slices" instead (see below) — it is simpler and has no join overhead.
    #
    # Classic use-case: NVROUT flag lives on operation/extended_product, not products.
    #
    # HOW IT WORKS
    # Each enabled entry is left-joined onto the product attribute projection by
    # `join_key` (must be a products column, typically product_id). Its raw `columns`
    # and `derived` SQL expressions become slice dimensions automatically — do NOT also
    # list them in slices.dimensions.  The source is deduplicated to ONE row per
    # join_key before joining — pre-aggregate it yourself when the raw table has
    # multiple rows per product.
    #
    # MULTIPLE SOURCES: add as many objects to this list as you need — one per
    # external table.  Each enabled source fails loudly on a bad path / missing column
    # / bad expression; set enabled=False to skip without removing it.
    #
    # OVERLAPPING SEGMENTS: use independent boolean dimensions (is_nvrout, is_comp)
    # rather than a single mutually-exclusive column — a product can be "yes" for both.
    #
    # NULL behaviour: products absent from the source get NULL for the new dimension
    # (left join) — a "derived" CASE...ELSE never fires for them, since they have no row
    # in the source to evaluate it against. Two ways to fix this:
    #   1. fillna (below): coalesce those NULLs to a literal default AFTER the join.
    #      Use this when the source is intentionally partial (e.g. a list of only the
    #      "special" items) and everything else should read as one fixed complement value.
    #   2. Make the source cover the full product universe (a pre-agg table with the
    #      flag already computed for every product) so "derived" itself yields a clean
    #      yes/no split with no NULLs to begin with.
    "dimension_sources": [
        {
            "enabled": False,
            "label": "extended_product",
            "source": "delta",  # "delta" | "csv"
            "path_segments": ["operation", "extended_product"],
            # CSV options (source auto-detected from .csv extension):
            # "source": "csv",
            # "path": "/Workspace/Users/you@invent.ai/lists/nvrout.csv",
            # "location": "workspace",  # "datastore" (default) or "workspace" (/Workspace/...)
            # "csv_options": {"header": True, "inferSchema": True},
            "join_key": "product_id",
            "columns": [],  # raw source columns to carry over as slice dimensions
            "derived": {
                # Spark SQL expressions evaluated against the SOURCE table's columns.
                # Each key becomes a new slice dimension column.
                "is_nvrout": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END",
                # Add more dimensions from this source here, e.g.:
                # "is_comp": "CASE WHEN program LIKE '%COMP%' THEN 'yes' ELSE 'no' END",
            },
            # fillna: {dim_name: default_value} — coalesces NULLs left by the join to a
            # literal, for products that have no row in this source at all. Keys must be
            # among this source's own "columns"/"derived" dimensions. Example: a source
            # listing only NON-COMP product_ids, with derived {"is_comp": "'no'"} — every
            # other product would otherwise be NULL; fillna makes them read as 'yes':
            #   "fillna": {"is_comp": "yes"},
            #
            # value_filters: restrict which values of a dimension appear in the report
            # breakdown (applied to that dimension's OWN slice only — never to Overall or
            # any other slice). Two shapes are accepted:
            #
            #   LIST form (include-only):
            #     omit a dim        -> keep ALL values, including NULL (default)
            #     [] (empty list)   -> keep all NON-NULL values (drop the NULL bucket)
            #     ["yes"]           -> keep ONLY 'yes' (drops 'no' and NULL)
            #
            #   DICT form (include and/or exclude, NULL-aware):
            #     {"include": ["yes"]}           -> keep ONLY 'yes'             (NULL dropped)
            #     {"exclude": ["no"]}            -> keep EVERYTHING except 'no'  (NULL KEPT)
            #     {"include": [...], "exclude": [...]} -> include set, then drop the excludes
            #     add "keep_null": True/False    -> force-keep or force-drop the NULL bucket
            #   Default NULL rule: include present -> NULL dropped; include absent -> NULL kept.
            #
            # Products missing from this source get NULL. ["yes"] restricts the breakdown to
            # the NVROUT universe (nvrout=yes only). To EXCLUDE a set but keep the whole rest
            # (including the NULL bucket), use exclude, e.g. {"exclude": ["nfg"]}.
            "value_filters": {"is_nvrout": ["yes"]},
        },
        # Add more external sources here if needed, e.g.:
        # {
        #     "enabled": False,
        #     "label": "another_table",
        #     "source": "delta",
        #     "path_segments": ["operation", "another_table"],
        #     "join_key": "product_id",
        #     "columns": ["some_flag"],
        #     "derived": {},
        # },
    ],
    "path_segments": {
        "fiscal": ["one_time_uploads", "fiscal_cal"],
        "daily_data": ["noob", "daily-data"],
        "products": ["master-data", "products"],
        "lost_sales": ["noob", "lost-sales", "model_id=top_down_excluding_ecom"],
        # Delta path segments (under bucket) for the defined scope table.
        "defined_scope": ["analysis", "instock_rate", "instock_rate_scope"],
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
    "input_filters": {
        # Optional Spark SQL expressions applied when reading each source.
        # Used by the pipeline and the notebook input-preview cells.
        # Example: "brand = 'NIKE'" or "store_id NOT IN (829, 639, 917)"
        # NOTE: when lost_sales_ensemble.enabled=True, this "lost_sales" filter list is
        # applied to BOTH the fast (path_segments.lost_sales) and slow
        # (lost_sales_ensemble.slow_path_segments) sources — same schema, same filters.
        "defined_scope": [],
        "lost_sales": [],
        "daily_data": [],
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES SOURCE — column mapping for the raw lost-sales table
    # ---------------------------------------------------------------------------
    # Only the COLUMN NAMES on path_segments.lost_sales (and, when
    # lost_sales_ensemble.enabled=True, the same-schema slow_path_segments table) are
    # configurable here — downstream code always sees the canonical names
    # (week_start_date, lost_sales, in_stock, total_days) regardless of what's set below.
    # Change these ONLY if a client's lost-sales table uses different column names than
    # tbretail's; the defaults below reproduce tbretail's current schema exactly, so
    # other customers are unaffected.
    # total_days_col supports a dotted nested-struct path (e.g. "details.total_days").
    # in_stock_col / total_days_col are ignored when instock_source.enabled=True below.
    "lost_sales_source": {
        "week_col": "week_start_date",
        "product_col": "product_id",
        "store_col": "store_id",
        "lost_sales_col": "lost_sales",
        "in_stock_col": "in_stock",
        "total_days_col": "details.total_days",
    },
    # ---------------------------------------------------------------------------
    # INSTOCK SOURCE — optional override to read in-stock days from a DIFFERENT table
    # ---------------------------------------------------------------------------
    # OFF by default: in-stock days / total days come from lost_sales_source above (the
    # SAME table/row as lost_sales), exactly as before. Set enabled=True when a client
    # calculates in-stock rate from a table other than its lost-sales model — the
    # pipeline then reads THIS table instead and joins it onto the lost-sales pair-weeks
    # by (product_col, store_col, week_col), overriding in_stock/total_days there.
    # Applies uniformly regardless of lost_sales_ensemble (fast/slow) selection.
    "instock_source": {
        "enabled": False,
        "path_segments": None,  # required when enabled=True, e.g. ["some", "instock", "table"]
        "week_col": "week_start_date",
        "product_col": "product_id",
        "store_col": "store_id",
        "in_stock_col": "in_stock",
        "total_days_col": "total_days",
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES ENSEMBLE — blend two lost-sales models by product sales speed
    # ---------------------------------------------------------------------------
    # OFF by default: the pipeline reads the SINGLE fast-mover model at
    # path_segments.lost_sales (the 120-day model) exactly as before, so other
    # customers are unaffected. Set enabled=True to blend two models:
    # fast-movers (speed cluster in fast_mover_clusters) take the 120-day model;
    # everyone else — slower clusters AND products with no/NULL cluster — takes
    # the 365-day model. All three aggregate fields (lost_sales, in_stock,
    # details.total_days) for a given product/store/week come from ONE model
    # (never mixed), so downstream in-stock-rate math stays internally consistent.
    "lost_sales_ensemble": {
        "enabled": False,
        # Slow-mover (365-day lookback) model. SAME schema as the fast model
        # (path_segments.lost_sales), different model_id partition. Read only when
        # enabled=True.
        "slow_path_segments": ["noob", "lost-sales", "model_id=top_down_excluding_ecom_365days"],
        # Product sales-speed source. Read only when enabled=True. Two table shapes
        # are supported via speed_cluster_format:
        #   "long" (default) - a long-format attributes table with one row per
        #       (product_id, attribute_name); filter to speed_cluster_attribute_name,
        #       the numeric attribute_value (1=fastest .. 5=slowest) is the cluster.
        #       This is the platform's noob/product-cluster-attributes-snapshot shape.
        #   "wide" - the cluster is already its own column (speed_cluster_value_col)
        #       on a table with one row per product_id. speed_cluster_attribute_name
        #       is ignored in this mode.
        "speed_cluster_path_segments": ["noob", "product-cluster-attributes-snapshot"],
        "speed_cluster_format": "long",
        "speed_cluster_attribute_name": "sales_speed",
        "speed_cluster_value_col": "product_speed_cluster",
        # Clusters whose products use the FAST (120-day) model. Everything else
        # (other clusters AND products with no/NULL cluster row) uses the SLOW model.
        "fast_mover_clusters": [1, 2, 3],
    },
    # ---------------------------------------------------------------------------
    # SLICES — breakdown dimensions sourced from master-data/products
    # ---------------------------------------------------------------------------
    # Use "slices" for columns that already live on (or are computable from) the
    # products table.  For attributes on OTHER tables (e.g. NVROUT from
    # operation/extended_product), use "dimension_sources" (see above) instead.
    #
    # dimensions:         list of existing column names in master-data/products.
    # derived_dimensions: dict of {new_col_name: "Spark SQL expression"} evaluated
    #                     against the products schema at runtime; failures are skipped
    #                     with a warning (unlike dimension_sources which fail loudly).
    #
    # Multiple dimensions: add as many column names as you need, e.g.:
    #   "dimensions": ["brand", "category", "price_tier"]
    "slices": {
        "dimensions": ["brand"],
        "derived_dimensions": {
            # Example:
            # "price_tier": "CASE WHEN price_without_tax < 50 THEN 'budget' ELSE 'premium' END",
        },
        # Restrict which values of a slice dimension appear in the breakdown (that
        # dimension only; Overall and other slices are unaffected). Two shapes:
        #   LIST (include-only): omit -> all incl NULL | [] -> all non-null | ["A","B"] -> only those
        #   DICT (include/exclude): {"include": ["A"]} keep only A | {"exclude": ["A"]} keep the
        #       rest incl NULL | add "keep_null": True/False to force the NULL bucket.
        # e.g. {"brand": ["NIKE","ADIDAS"]}, {"brand": []} to drop NULL, or
        #      {"brand": {"exclude": ["OUTLET"]}} to drop one brand but keep everything else.
        "value_filters": {},
    },
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
    "output": {
        "save_outputs": False,
        "path_segments": ["analysis", "kpi_reports", "outputs"],
        # Delta path per table: .../outputs/{table_name}/run_date={run_date}/
        # run_date defaults to reporting_window.as_of_date; set explicitly to target another partition.
        "run_date": None,
        # initial       = first-time setup; fails if output tables already exist
        # incremental   = append rows with new merge keys; skip overlaps unless allow_overwrite_existing=True
        # full_refresh  = overwrite each table with this run's output only (no merge with prior saved history)
        # See README "Output saves" for merge keys, workflows, and caveats.
        "save_mode": "incremental",
        "allow_overwrite_existing": False,
        # Incremental only: after merging kpi_long onto the latest prior run_date partition,
        # recompute YoY/YTD from the full merged history (not just this run's window) and
        # overwrite the comparison tables in this partition. Lets a single-week refresh still
        # produce a YoY vs last year. Set False to keep comparisons scoped to the current run.
        "recompute_comparisons_from_history": True,
    },
    "html_report": {
        # Set enabled=False to skip HTML generation entirely.
        "enabled": True,
        # Filename written next to the notebook. {customer} and {report_end} are interpolated.
        "filename": "kpi_report_{customer}_{report_end}.html",
        # Optional custom title (defaults to "<CUSTOMER> KPI Report").
        "report_title": None,
        # Optional: also save under the datastore bucket at this path.
        # Set to None (default) to write only locally next to the notebook.
        "output_path_segments": None,
        # Override or extend DEFAULT_METRIC_DEFINITIONS from kpi_pipeline/html_report.py.
        # Keys are metric column names; values are dicts with keys:
        #   label, definition, store_scope, formula
        # Example:
        #   "metric_definitions": {
        #       "total_sales_revenue": {"definition": "Net sales excluding returns."},
        #   }
        "metric_definitions": {},
        # Period display limits: trim kpi_long and Delta saves to the N most recent periods.
        # Set to null to keep all periods present in the data.
        "weekly_display_weeks": 5,
        "monthly_display_months": 5,
        "quarterly_display_quarters": 5,
        "yearly_display_years": 5,
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
    }

    lost_sales_source_cfg = cfg.get("lost_sales_source", {}) or {}
    lost_sales_column_map = {
        "week_col": lost_sales_source_cfg.get("week_col", "week_start_date"),
        "product_col": lost_sales_source_cfg.get("product_col", "product_id"),
        "store_col": lost_sales_source_cfg.get("store_col", "store_id"),
        "lost_sales_col": lost_sales_source_cfg.get("lost_sales_col", "lost_sales"),
        "in_stock_col": lost_sales_source_cfg.get("in_stock_col", "in_stock"),
        "total_days_col": lost_sales_source_cfg.get("total_days_col", "details.total_days"),
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

    # Resolve optional dimension-source paths up front (same pattern as PATH_* above) so
    # fiscal.py reads absolute paths without needing fund_paste. path_segments are joined
    # under the bucket; an explicit "path" (e.g. a /Workspace CSV) is passed through as-is.
    dimension_sources = []
    for src in cfg.get("dimension_sources", []) or []:
        resolved = dict(src)
        if not resolved.get("path") and resolved.get("path_segments"):
            resolved["path"] = fund_paste(bucket, *resolved["path_segments"])
        dimension_sources.append(resolved)

    # Per-dimension value filters, merged from slices + each dimension_source. Applied to
    # a slice's own breakdown only (see kpi_pipeline/kpi_long._filter_frames_for_dimension).
    slice_value_filters = dict(cfg["slices"].get("value_filters", {}) or {})
    for src in dimension_sources:
        for dim, allowed in (src.get("value_filters", {}) or {}).items():
            slice_value_filters[dim] = allowed
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

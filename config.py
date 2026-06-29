# Edit CONFIG below, then in main.ipynb:  %run ./config  ->  settings = materialize(fund.paste)

import copy
import datetime
import os
from typing import Any, Callable, Dict, Optional

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
        "min_percentile": 0.2,
        "min_weeks_for_filter": 2,  # skip filter when pair has this many weeks or fewer
    },
    "scope": {
        # True  = defined scope + score backfill for weeks missing from defined scope
        # False = defined scope only
        "use_hybrid_scope": True,
        # True  = compute score scope and annual defined-vs-score KPI diff (scope_diff)
        # False = skip score scope unless required for hybrid backfill (default)
        "run_scope_diff": False,
    },
    "comparable_pairs": {
        # GATED, opt-in like-for-like view (mirrors v4's same-pair YoY). When enabled, for each
        # comparison (YoY/QoQ/MoM/WoW) the metrics are recomputed over ONLY the (product_id,
        # store_id) pairs present in BOTH compared periods, then compared — isolating like-for-like
        # change from mix shifts (new/closed pairs). Pair-level data exists only for the current run
        # window, so a comparable comparison appears only when the run window SPANS both compared
        # periods (e.g. a multi-year window for comparable YoY). Output:
        #   * comparable_kpi_long Delta table (per-period comparable metrics + comparable_pair_count)
        #   * comparable_comparison_{yoy,qoq,mom,wow} Delta tables
        #   * a second "Comparable pairs" comparison table per panel in the HTML report
        "enabled": False,
    },
    "scope_adjustments": {
        # Optional manual adds/removes applied after hybrid/defined scope is built.
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
    "dimension_sources": [
        # GATED, OPT-IN. Pull extra slice dimensions from tables OTHER than master-data/products.
        # Default = nothing enabled => slices come from the products master only (see "slices").
        # Enable a source ONLY when a breakdown column does not live on the products table
        # (e.g. NVROUT membership lives on operation/extended_product, not products).
        #
        # Each enabled source is left-joined onto the product attribute table by `join_key`
        # (normally product_id, and it must already be a products column). Its raw `columns`
        # and `derived` SQL expressions (evaluated against the SOURCE table) become slice
        # dimensions automatically — no need to also list them under slices.dimensions.
        # The source is reduced to ONE row per join_key before the join (dropDuplicates), so
        # pre-aggregate to one row per product if the raw table has many (see README).
        #
        # An ENABLED source fails loudly on a bad path / missing column / bad expression —
        # it will not silently drop the segment you added it to produce.
        {
            "enabled": False,
            "label": "extended_product",
            "source": "delta",  # "delta" | "csv"
            "path_segments": ["operation", "extended_product"],
            # CSV alternatives (source auto-detected from .csv extension):
            # "source": "csv",
            # "path": "/Workspace/Users/you@invent.ai/lists/nvrout.csv",  # or a datastore path
            # "location": "workspace",  # "datastore" (default) or "workspace" (/Workspace/... via file: scheme)
            # "csv_options": {"header": True, "inferSchema": True},
            "join_key": "product_id",
            "columns": [],  # raw columns from the source to carry over as dimensions
            "derived": {
                # Spark SQL over the SOURCE table's columns -> new dimension column(s).
                "is_nvrout": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END",
            },
        }
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
        # DATE path: date_col -> fiscal_cal -> Year/Week. NATIVE path: set date_col=None, year_col/week_col.
        "product_col": "product_id",
        "store_col": "store_id",  # omit or set None for product-week scope (no store grain)
        "date_col": "week_start_date",
        "year_col": None,
        "week_col": None,
    },
    "input_filters": {
        # Optional Spark SQL expressions applied when reading each source.
        # Used by the pipeline and the notebook input-preview cells.
        # Example: "brand = 'NIKE'" or "store_id NOT IN (829, 639, 917)"
        "defined_scope": [],
        "lost_sales": [],
        "daily_data": [],
    },
    "slices": {
        "dimensions": ["brand"],
        "derived_dimensions": {},
    },
    "metrics": {
        "metric_cols": [
            "total_sales_quantity",
            "total_sales_revenue",
            "AUR",
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
        "key_metrics": [
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
        # recompute YoY/QoQ/MoM/WoW from the full merged history (not just this run's window)
        # and overwrite the comparison tables in this partition. Lets a single-week refresh still
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
        "run_week_start_date": run_week_start,
        "run_week_end_date": run_week_end,
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
    }

    window = _resolve_report_window(
        datetime.date.fromisoformat(rw["as_of_date"]),
        run_min,
    )

    defined_scope = {**cfg["defined_scope"], "path": paths["PATH_DEFINED_SCOPE"]}

    # Resolve optional dimension-source paths up front (same pattern as PATH_* above) so
    # fiscal.py reads absolute paths without needing fund_paste. path_segments are joined
    # under the bucket; an explicit "path" (e.g. a /Workspace CSV) is passed through as-is.
    dimension_sources = []
    for src in cfg.get("dimension_sources", []) or []:
        resolved = dict(src)
        if not resolved.get("path") and resolved.get("path_segments"):
            resolved["path"] = fund_paste(bucket, *resolved["path_segments"])
        dimension_sources.append(resolved)

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
        "SCOPE_ADJUSTMENTS": cfg.get("scope_adjustments", {}),
        **paths,
        "DEFINED_SCOPE": defined_scope,
        "INPUT_FILTERS": cfg.get("input_filters", {}),
        "SLICE_DIMENSIONS": cfg["slices"]["dimensions"],
        "DERIVED_SLICE_DIMENSIONS": cfg["slices"]["derived_dimensions"],
        "DIMENSION_SOURCES": dimension_sources,
        "METRIC_COLS": metrics["metric_cols"],
        "KEY_METRICS": metrics["key_metrics"],
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

# Retail-insights-pipeline config: TBretail's deployed values (config.py is the generic template).
# docs/CONFIG.md documents every key in depth; the one-time CSV exports are in docs/TBRETAIL.md, "tbretail setup".
#
# How to use (Databricks, next to main.ipynb):
#     %run ./tbretail_config
#     settings = materialize(fund.paste)
#
# Layout: reference lists -> CONFIG (the only part you edit) -> helpers -> materialize(). materialize() checks
# CONFIG, applies any KPI_* environment overrides (docs/CONFIG.md: Environment variable overrides) and returns the flat
# settings dict the pipeline reads. Every section and key stays in the file; unused ones are switched off.
#
# Before each run: update reporting_window.as_of_date to a day noob/daily-data has reached.
#
# What this config turns on:
#   Scope       the platform scope table (solution 21, family mains, active products) at product_store grain,
#               daily, nothing added or removed; NVROUT, COMP and NON-COMP are labels from dimension_sources
#   Sales       sales_basis "net": noob/daily-data sales, net of returns ("gross" reads operation/transactional_sales)
#   Window      report_end "latest_day": YTD runs to as_of_date, other views show complete periods; Half tab on
#   In-stock    instock.method "daily" (on-hand or store GIT, count start "earliest", ECOM stores left out);
#               metrics.population_filters leaves NON-COMP out of in_stock_rate only
#   Lost sales  report_dfu through lost_sales_source (ECOM stores left out of the sales denominator);
#               lost_sales_ensemble OFF
#   Blocked     blocked_scope (store solution 21, DC solution 22, rule after_scope_start): blocked days leave
#               the inventory, WOS, in-stock metrics; sales, AUR / AUC, distinct counts and lost_sales_pct keep them
#   GIT         goods_in_transit (shift -1): daily in-stock day, DC in-stock day and every inventory metric
#   Comparable  like-for-like ytd / yoy / quarter / half over an all-years pair universe
#   Dimensions  IS_NVROUT (NVROUT root), IS_COMP (LFL root), brand (shown as Banner), SMW
#   Report      TBretail KPI Report; wos_revenue, weighted_instock_rate, dc_in_stock_rate not reported
#               (dc_instock OFF)

import copy
import datetime
import os
from typing import Any, Callable, Dict, List, Optional

# =============================================================================
# REFERENCE LISTS -- the allowed values some CONFIG keys pick from
# =============================================================================
# Period-over-period comparison kinds, in canonical order: comparisons.enabled.
COMPARISON_KINDS_ALL = ("yoy", "ytd")

# Like-for-like kinds, in canonical order: comparable_pairs.kinds (docs/LOGIC_FLOW.md: Comparable pairs).
COMPARABLE_KINDS_ALL = ("ytd", "yoy", "quarter", "half")

# Every metric the pipeline reports, in canonical order: metrics.metric_cols, metrics.scope_diff_metrics and
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

# Which sales the sales metrics read: sales_basis. "net" = noob/daily-data (net of returns); "gross" = the
# non-return rows of operation/transactional_sales, every day of them (docs/CONFIG.md: sales_basis).
SALES_BASES = ("net", "gross")

CONFIG: Dict[str, Any] = {
    # =============================================================================
    # 1. RUN & DATES -- whose data, which days, which calendar
    # =============================================================================
    "customer": "tbretail",  # datastore bucket: /mnt/invent-{customer}-datastore
    "run": {
        "mode": "full",  # "full" computes from the source tables; "html_only" renders saved outputs
    },
    "reporting_window": {  # docs/LOGIC_FLOW.md: Reporting window and report end
        "as_of_date": "2026-10-02",  # last day of data to report; update before each run
        "run_min_date": "2024-02-06",  # resolves to Sunday 2024-02-04
        "report_end": "latest_day",  # YTD to as_of_date; fiscal_cal upload must extend past it
    },
    "fiscal_calendar": {  # docs/LOGIC_FLOW.md: Fiscal calendar vs native time grain
        "use_fiscal_calendar": True,  # True = periods from the fiscal_cal upload; False = from daily-data dates
        "half_periods": True,  # Half tab (H1 = Q1-Q2, H2 = Q3-Q4) and the "half" comparable kind
        "column_map": {  # fiscal year runs Feb-Jan: month_name is read verbatim for the Monthly tab
            "quarter_col": "Quarter",
            "month_col": "Month",
            "month_name_col": "month_name",
        },
        "daily_time_columns": {  # raw daily-data columns; week is civil path only (unused here)
            "date": "date",
            "week": "week",
        },
    },
    # =============================================================================
    # 2. SOURCE TABLES -- where each input lives and how it is read
    # =============================================================================
    "path_segments": {  # folders under the datastore bucket
        "fiscal": ["one_time_uploads", "fiscal_cal"],  # fiscal calendar upload
        "daily_data": ["noob", "daily-data"],  # store x product daily sales and inventory
        "inventory_warehouse": ["operation", "inventory_warehouse"],  # DC daily inventory
        "item_family": ["operation", "item_family"],  # parent / child product map
        "scope": ["operation", "scope"],  # the scope table (platform operation/scope)
        "goods_in_transit": ["operation", "goods_in_transit"],  # destination_type 0 = store, 1 = warehouse
        "products": ["master-data", "products"],  # product attributes: slice dimensions, active flag
        "lost_sales": ["reporting", "future_visibility", "reporting_inv_fc_dfu", "report_dfu"],  # report_dfu
        "product_planning_level": ["operation", "product_planning_level"],  # product_agg_level -> product_id map
        "transactional_sales": ["operation", "transactional_sales"],  # read only with sales_basis "gross" (non-return rows)
    },
    # Spark SQL expressions applied when reading each source (docs/CONFIG.md: input_filters). To keep a store out of
    # every metric, filter it in daily_data here and, for the daily in-stock, in instock.daily.input_filters.
    "input_filters": {
        "scope": [],
        "lost_sales": [],
        "daily_data": ["usable = 1"],
        "inventory_warehouse": [],
        "item_family": [],
        "transactional_sales": ["sales_type != 'return'"],  # read only with sales_basis "gross"; return rows are stored positive
    },
    "item_family_source": {  # column names of path_segments.item_family (docs/CONFIG.md: item_family)
        "product_col": "product_id",
        "parent_col": "parent_id",
        "is_main_col": "is_main",
    },
    "item_family_rollup": {  # lost_sales OFF: the source is already rolled upstream (the scope rolls via scope.roll_to_family_main)
        "daily_data": True,
        "lost_sales": False,
        "inventory_warehouse": True,
        "transactional_sales": True,  # read only with sales_basis "gross"; must equal daily_data (checked)
    },
    # Which sales every sales metric reads (docs/CONFIG.md: sales_basis). "net": noob/daily-data as it is. "gross": its
    # sales_revenue / sales_quantity are replaced by ALL the transactional_sales of the product x store x day that pass
    # input_filters.transactional_sales (0 on a daily-data day with none); a transactional day daily-data has no row for
    # (or removed, e.g. usable = 1) is added as a sales-only day: no inventory, no daily-data filter, no ECOM or blocked-day
    # removal -- only the family-main roll-up (item_family_rollup.transactional_sales, which must equal daily_data), the
    # scope and the active filter. Changes what saved kpi_long means: use output.save_mode "full_refresh" when you change it.
    "sales_basis": "net",
    # =============================================================================
    # 3. SCOPE -- which product x store pairs count, and on which days
    # =============================================================================
    "scope": {  # which product x store pairs count, from one table (docs/CONFIG.md: scope)
        "time": "daily",  # "daily": every pair counts on every day of the window; "weekly": rows carry their own week
        "grain": "product_store",  # "product" | "product_store"; daily in-stock needs stores
        "columns": {  # column names of path_segments.scope; None = column not used
            "product": "product_id",
            "store": "location_id",  # required for grain product_store; for scope.dc_solution_id it holds the warehouse
            "start": "start_date",  # daily only: rows open on the run date, scope_start = earliest start
            "end": "end_date",  # daily only: null or on / after the run date = open
            "solution": "solution_id",  # rows of scope.solution_id / dc_solution_id only
            "run_date": "run_date",  # rows of the run date only
            "date": None,  # weekly only: the row's week as a date, OR year + week below
            "year": None,
            "week": None,
        },
        "solution_id": 21,  # store scope solution(s), int or list (blocks: blocked_scope.solution_id); needs columns.solution
        "dc_solution_id": 22,  # DC (network) scope: DC metrics' warehouse pairs, among the store scope's products; DC blocks
        "run_date": None,  # "YYYY-MM-DD" Sunday; None = latest Sunday on or before today; needs columns.run_date / end
        "roll_to_family_main": True,  # roll each scope row onto its family main
        "active_only": True,  # drop pairs of inactive products
        # In-stock only at stores where the main item itself is eligible (sub-only stores stay in every
        # other metric). Needs columns.start, roll_to_family_main and instock.method "daily".
        "instock_main_eligible_only": True,
        # In-stock leaves out sizes not in a supersession whose class color is (likely NGF); they stay
        # in every other metric. Needs instock.method "daily".
        "instock_exclude_unsuperseded_sizes": True,
        # products column that groups the sizes of one class color for that removal (option_code at tbretail).
        "instock_unsuperseded_group_column": "option_code",
        "backfill_leading_gap": False,  # weekly only
        "use_hybrid_scope": False,  # True also backfills the weeks the scope table leaves uncovered
        "run_scope_diff": False,  # True adds the scope-vs-score comparison
    },
    "score_scope": {  # activity-based scope, used only by use_hybrid_scope and run_scope_diff (both OFF)
        "min_percentile": 0.2,  # a pair-week counts when sales and inventory reach this percentile (20 = 0.2)
        "min_weeks_for_filter": 2,  # pairs with this many weeks or fewer skip the filter
    },
    # UI blocked days, dropped from the metrics named here (docs/CONFIG.md: blocked_scope).
    # Needs scope.columns.start and store. Sales units / revenue, AUR, AUC, distinct counts and
    # lost_sales_pct keep blocked days: a blocked pair can still sell its existing stock.
    "blocked_scope": {
        # Airflow variable ui_parameters_path (the newest folder, 2026-09-30-204511_..., has only solution 51 blocks)
        "ui_parameters_path": "ui-data/parameter_config/2026-10-02-065120_9b661f5c-9e01-4431-8f8e-9a415d5cb4e7",
        "rule": "after_scope_start",  # or "all"
        "folder": "blocked_scope",  # store blocks folder under ui_parameters_path; None = store blocks not read
        "dc_folder": "dc_blocked_scope",  # DC blocks folder under ui_parameters_path; None = DC blocks not read
        "solution_id": 21,  # store blocks of these solution(s) only (int or list), whatever scope reads
        "dc_solution_id": 22,  # DC blocks of these solution(s); None = no DC blocks (also needs scope.dc_solution_id)
        "kinds": ["product", "product_destination", "destination"],  # store block folders read
        "dc_kinds": ["product", "product_destination"],  # DC block folders read (no destination folder)
        "metrics": [  # metrics that drop blocked days: "all" or a list from METRICS_ALL
            "in_stock_rate", "weighted_instock_rate", "dc_in_stock_rate", "total_inventory", "mean_stock",
            "mean_stock_retail", "mean_stock_cost", "dc_mean_stock", "total_mean_stock", "WOS", "wos_revenue",
            "wos_cost", "WOS_DC", "WOS_TOTAL", "inventory_turnover_rate",
        ],
    },
    # =============================================================================
    # 4. IN-STOCK & INVENTORY
    # =============================================================================
    # Where in_stock_rate comes from (docs/CONFIG.md: instock). Both sub-sections stay; only the method's one is used.
    #   "daily"              built from daily-data over the scope pairs (store-level scope grain)
    #   "weekly_source"      read from a separate weekly table (weekly_source below)
    #   "lost_sales_source"  read from lost_sales_source's in_stock_col / total_days_col
    # tbretail: "daily"; weekly_source (report_dfu) stays configured for a switch back.
    "instock": {
        "method": "daily",
        "daily": {
            "count_start": "earliest",  # earlier of scope start and first daily row
            "require_daily_data": True,  # drop pairs with no daily-data row at all
            "history_start": "2024-02-04",  # first day searched for a pair's first daily row
            "usable_only": True,  # usable != 1 days leave the store-days and the in-stock days
            "sales_counts_as_stocked": False,  # a day with sales_quantity > 0 but inventory <= 0 counts as stocked
            "input_filters": ["store_id NOT IN (829, 639, 917)"],  # ECOM stores leave in-stock only
        },
        "weekly_source": {
            "path_segments": ["reporting", "future_visibility", "reporting_inv_fc_dfu", "report_dfu"],
            "week_col": "TY_week_start_date",
            "product_col": None,
            "store_col": None,
            "in_stock_col": "TY_total_days_instock",
            "total_days_col": "TY_total_day",
            "product_agg_level_col": "product_agg_level",
            "fallback_sources": [  # LY_ / LLY_ columns fill weeks that rolled off TY_
                {"week_col": "LY_week_start_date", "in_stock_col": "LY_total_days_instock", "total_days_col": "LY_total_day"},
                {"week_col": "LLY_week_start_date", "in_stock_col": "LLY_total_days_instock", "total_days_col": "LLY_total_day"},
            ],
        },
    },
    # dc_in_stock_rate from an expanded inventory_warehouse grid (docs/CONFIG.md: dc_instock). DC metrics read
    # path_segments.inventory_warehouse for the store scope's products; with scope.dc_solution_id set,
    # only the DC scope's product x warehouse pairs among them count (None = every warehouse).
    # tbretail: OFF, no dc_in_stock_rate in the report.
    "dc_instock": {
        "enabled": False,
        "stock_threshold": 0,  # a day is stocked when inventory > stock_threshold
    },
    "goods_in_transit": {  # add goods in transit (GIT) to on-hand, on everywhere (docs/CONFIG.md: goods_in_transit)
        "date_shift_days": -1,  # snapshot dated D+1 is the end of day D
        "roll_to_family_main": True,  # roll every GIT read onto the family main
        "store_instock": True,  # a daily in-stock day also counts store GIT
        "dc_instock": True,  # a DC in-stock day also counts DC GIT (dc_instock is OFF)
        "inventory_metrics": [  # every INVENTORY_GIT_METRICS_ALL name
            "total_inventory", "mean_stock", "mean_stock_retail", "mean_stock_cost", "dc_mean_stock",
            "total_mean_stock", "WOS", "wos_revenue", "wos_cost", "WOS_DC", "WOS_TOTAL", "inventory_turnover_rate",
        ],
    },
    # =============================================================================
    # 5. LOST SALES
    # =============================================================================
    # Column mapping of path_segments.lost_sales (docs/CONFIG.md: lost_sales_source): report_dfu, future_visibility's
    # pre-blended fast / slow output. It has no store_id, so lost_sales is restricted at product x week.
    "lost_sales_source": {
        "week_col": "TY_week_start_date",
        "product_col": None,  # keyed by product_agg_level_col instead (mapped via path_segments.product_planning_level)
        "store_col": None,
        "lost_sales_col": "lost_sales",
        "in_stock_col": "TY_total_days_instock",  # read only when instock.method is "lost_sales_source"
        "total_days_col": "TY_total_day",
        "product_agg_level_col": "product_agg_level",
        "sales_filter": ["store_id NOT IN (829, 639, 917)"],  # the ECOM stores report_dfu's model leaves out
    },
    "lost_sales_ensemble": {  # OFF: report_dfu is already blended; kept as the fallback path
        "enabled": False,
        "slow_path_segments": ["noob", "lost-sales", "model_id=top_down_excluding_ecom_365days"],
        "speed_cluster_path_segments": ["noob", "product-cluster-attributes-snapshot"],
        "speed_cluster_format": "long",  # "long" = one row per product x attribute; "wide" = own column
        "speed_cluster_attribute_name": "sales_speed",  # "long" format only
        "speed_cluster_value_col": "product_speed_cluster",  # "wide" format only
        "fast_mover_clusters": [1, 2, 3],  # these clusters take the fast model, everyone else the slow one
    },
    # =============================================================================
    # 6. BREAKDOWNS -- slice dimensions and root tabs
    # =============================================================================
    "slices": {  # brand: products column; SMW: KNG vs SMW (docs/LOGIC_FLOW.md: Roots and cuts)
        "dimensions": ["brand"],
        "derived_dimensions": {"SMW": "CASE WHEN brand = 'KNG' THEN 'KNG' ELSE 'SMW' END"},
        "value_filters": {},  # per-dimension include / exclude of values (docs/LOGIC_FLOW.md: Value filters)
    },
    # External tables that add root tabs (docs/LOGIC_FLOW.md: Dimension sources). IS_NVROUT: 'yes' = NVROUT root.
    # IS_COMP: NGF CSV lists the 'no' (NON-COMP) products, fillna makes the rest 'yes' = LFL root.
    "dimension_sources": [
        {
            "enabled": True,
            "label": "extended_product",
            "source": "delta",
            "path_segments": ["operation", "extended_product"],
            "join_key": "product_id",
            "columns": [],  # source columns taken as they are
            "derived": {"IS_NVROUT": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END"},
            "fillna": {"IS_NVROUT": "no"},  # products absent from extended_product get NULL, not 'no'
            "root_values": {"IS_NVROUT": {"yes": "nvrout"}},  # 'no' / NULL are not their own root
        },
        {
            "enabled": True,
            "label": "ngf_comp_split",
            "source": "csv",
            "path": (
                "/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
                "KPI-NEW/data/non_comp_ids_20260817.csv"
            ),
            "location": "workspace",
            "csv_options": {"header": True, "inferSchema": True},
            "join_key": "product_id",
            "columns": [],
            "derived": {"IS_COMP": "'no'"},
            "fillna": {"IS_COMP": "yes"},
            "root_values": {"IS_COMP": {"yes": "comp"}},  # NON-COMP is not its own root
        },
    ],
    # =============================================================================
    # 7. METRICS
    # =============================================================================
    "metrics": {
        "metric_cols": [  # metrics reported, from METRICS_ALL
            "total_sales_quantity", "total_sales_revenue", "AUR", "AUC", "total_inventory",
            "distinct_product_count", "distinct_store_count", "distinct_pair_count",
            "mean_stock", "mean_stock_retail", "mean_stock_cost", "dc_mean_stock", "total_mean_stock",
            "WOS", "wos_cost", "WOS_DC", "WOS_TOTAL", "inventory_turnover_rate",
            "in_stock_rate", "lost_sales_pct",
        ],
        "scope_diff_metrics": [  # metrics in the scope diff (scope.run_scope_diff), from metric_cols
            "total_sales_quantity", "total_sales_revenue", "total_inventory",
            "distinct_product_count", "distinct_pair_count",
            "WOS", "WOS_DC", "WOS_TOTAL",
            "in_stock_rate", "lost_sales_pct",
        ],
        "labels": {  # display name per metric
            "total_sales_revenue": "Sales Revenue",
            "total_sales_quantity": "Sales Units",
            "AUR": "AUR",
            "AUC": "AUC",
            "total_inventory": "Total Inventory",
            "mean_stock": "Daily stock avg (M units)",
            "mean_stock_retail": "Daily stock avg retail (M $)",
            "mean_stock_cost": "Daily stock avg cost (M $)",
            "dc_mean_stock": "Daily DC stock avg (units)",
            "total_mean_stock": "Daily total stock avg (units)",
            "WOS": "WOS (units)",
            "wos_cost": "WOS cost",
            "WOS_DC": "WOS (DC)",
            "WOS_TOTAL": "WOS (Total)",
            "inventory_turnover_rate": "Inventory Turnover Rate",
            "in_stock_rate": "In-Stock Rate",
            "lost_sales_pct": "Lost Sales %",
            "distinct_product_count": "Distinct products",
            "distinct_store_count": "Distinct stores",
            "distinct_pair_count": "Distinct pairs",
        },
        # Rate metrics whose change is shown in percentage points, not percent.
        "pp_change_metrics": ["in_stock_rate", "lost_sales_pct"],
        # {metric: {dim_col: value filter}} narrows one metric (docs/METRICS.md: Population filters).
        # NON-COMP (IS_COMP 'no') leaves in_stock_rate only, even in Overall.
        "population_filters": {
            "in_stock_rate": {"IS_COMP": {"exclude": ["no"]}},
        },
    },
    # =============================================================================
    # 8. COMPARISONS
    # =============================================================================
    "comparisons": {  # docs/CONFIG.md: Selecting which comparisons to run
        "enabled": ["yoy"],  # any of COMPARISON_KINDS_ALL
    },
    "comparable_pairs": {  # like-for-like; needs run_min_date spanning 2+ years (docs/LOGIC_FLOW.md: Comparable pairs)
        "enabled": True,
        "kinds": ["ytd", "yoy", "quarter", "half"],  # any of COMPARABLE_KINDS_ALL
        "grain": "product_store",  # or "product"
        "pair_days": "unblocked",  # pairs present on "unblocked" days in every year, or on "all" (incl. blocked)
    },
    # =============================================================================
    # 9. OUTPUT & HTML REPORT
    # =============================================================================
    "output": {  # Delta saves (docs/OUTPUTS.md: Output saves)
        "save_outputs": True,
        "path_segments": ["analysis", "tbretail_kpis", "outputs"],  # output folder under the bucket
        "run_date": "2026-10-02",  # run_date partition written; None = as_of_date
        "save_mode": "full_refresh",  # "initial" | "incremental" | "full_refresh"
        "allow_overwrite_existing": True,  # incremental: replace periods already saved
        "recompute_comparisons_from_history": True,  # incremental: rebuild comparisons from the merged history
    },
    "html_report": {  # docs/HTML_REPORT.md: HTML report
        "enabled": True,
        "filename": "kpi_report_{customer}_{report_end}.html",  # placeholders {customer} and {report_end} only
        "report_title": "TBretail KPI Report",
        "output_path_segments": None,  # None = local only; else a datastore folder to save to as well
        "metric_definitions": {},  # overrides of the Metric Details tab text
        "weekly_display_weeks": 5,  # periods shown per tab; None = all
        "monthly_display_months": 5,
        "quarterly_display_quarters": 5,
        "half_display_halves": 4,
        "yearly_display_years": None,
        "root_labels": {"comp": "LFL", "nvrout": "NVROUT"},  # root id -> tab label
        "dimension_labels": {"brand": "Banner"},  # slice dimension -> tab label
    },
}

# =============================================================================
# HELPERS -- parsing and validation used by materialize()
# =============================================================================
def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in ("1", "true", "yes")


def _as_fraction(value):
    """A percentile written as a percentage (20) becomes a fraction (0.2)."""
    return value / 100.0 if value > 1 else value


def _strip_lower(raw: str) -> str:
    return raw.strip().lower()


def _strip_or_none(raw: str) -> Optional[str]:
    return raw.strip() or None


def _comma_list(raw: str) -> List[str]:
    return [s.strip() for s in raw.split(",") if s.strip()]


def _optional_int(raw: str) -> Optional[int]:
    raw = raw.strip()
    return int(raw) if raw else None


# Optional KPI_* environment overrides (docs/CONFIG.md: Environment variable overrides):
# (variable, CONFIG key path, how the text is parsed).
_ENV_OVERRIDES = (
    ("KPI_CUSTOMER", ("customer",), str),
    ("KPI_RUN_MODE", ("run", "mode"), _strip_lower),
    ("KPI_AS_OF_DATE", ("reporting_window", "as_of_date"), str),
    ("KPI_RUN_MIN_DATE", ("reporting_window", "run_min_date"), _strip_or_none),
    ("KPI_REPORT_END", ("reporting_window", "report_end"), _strip_lower),
    ("KPI_USE_FISCAL_CALENDAR", ("fiscal_calendar", "use_fiscal_calendar"), _parse_bool),
    ("KPI_HALF_PERIODS", ("fiscal_calendar", "half_periods"), _parse_bool),
    ("KPI_SALES_BASIS", ("sales_basis",), _strip_lower),
    ("KPI_SCOPE_MIN_PERCENTILE", ("score_scope", "min_percentile"), lambda raw: _as_fraction(float(raw))),
    ("KPI_SCOPE_MIN_WEEKS_FOR_FILTER", ("score_scope", "min_weeks_for_filter"), int),
    ("KPI_USE_HYBRID_SCOPE", ("scope", "use_hybrid_scope"), _parse_bool),
    ("KPI_RUN_SCOPE_DIFF", ("scope", "run_scope_diff"), _parse_bool),
    ("KPI_COMPARABLE_PAIRS", ("comparable_pairs", "enabled"), _parse_bool),
    ("KPI_COMPARISONS", ("comparisons", "enabled"), lambda raw: [c.lower() for c in _comma_list(raw)]),
    ("KPI_LOST_SALES_ENSEMBLE", ("lost_sales_ensemble", "enabled"), _parse_bool),
    ("KPI_LOST_SALES_SLOW_PATH", ("lost_sales_ensemble", "slow_path_segments"), _comma_list),
    ("KPI_SPEED_CLUSTER_PATH", ("lost_sales_ensemble", "speed_cluster_path_segments"), _comma_list),
    ("KPI_SPEED_CLUSTER_FORMAT", ("lost_sales_ensemble", "speed_cluster_format"), _strip_lower),
    ("KPI_SPEED_CLUSTER_ATTRIBUTE", ("lost_sales_ensemble", "speed_cluster_attribute_name"), str.strip),
    ("KPI_SPEED_CLUSTER_VALUE_COL", ("lost_sales_ensemble", "speed_cluster_value_col"), str.strip),
    ("KPI_FAST_MOVER_CLUSTERS", ("lost_sales_ensemble", "fast_mover_clusters"),
     lambda raw: [int(c) for c in _comma_list(raw)]),
    ("KPI_LOST_SALES_WEEK_COL", ("lost_sales_source", "week_col"), str.strip),
    ("KPI_LOST_SALES_PRODUCT_COL", ("lost_sales_source", "product_col"), str.strip),
    ("KPI_LOST_SALES_STORE_COL", ("lost_sales_source", "store_col"), str.strip),
    ("KPI_LOST_SALES_COL", ("lost_sales_source", "lost_sales_col"), str.strip),
    ("KPI_LOST_SALES_IN_STOCK_COL", ("lost_sales_source", "in_stock_col"), str.strip),
    ("KPI_LOST_SALES_TOTAL_DAYS_COL", ("lost_sales_source", "total_days_col"), str.strip),
    ("KPI_INSTOCK_METHOD", ("instock", "method"), _strip_lower),
    ("KPI_INSTOCK_SOURCE_PATH", ("instock", "weekly_source", "path_segments"), _comma_list),
    ("KPI_INSTOCK_WEEK_COL", ("instock", "weekly_source", "week_col"), str.strip),
    ("KPI_INSTOCK_PRODUCT_COL", ("instock", "weekly_source", "product_col"), str.strip),
    ("KPI_INSTOCK_STORE_COL", ("instock", "weekly_source", "store_col"), str.strip),
    ("KPI_INSTOCK_IN_STOCK_COL", ("instock", "weekly_source", "in_stock_col"), str.strip),
    ("KPI_INSTOCK_TOTAL_DAYS_COL", ("instock", "weekly_source", "total_days_col"), str.strip),
    ("KPI_SAVE_OUTPUTS", ("output", "save_outputs"), _parse_bool),
    ("KPI_OUTPUT_PATH", ("output", "path_segments"), _comma_list),
    ("KPI_OUTPUT_RUN_DATE", ("output", "run_date"), _strip_or_none),
    ("KPI_OUTPUT_SAVE_MODE", ("output", "save_mode"), _strip_lower),
    ("KPI_ALLOW_OVERWRITE_EXISTING", ("output", "allow_overwrite_existing"), _parse_bool),
    ("KPI_RECOMPUTE_COMPARISONS", ("output", "recompute_comparisons_from_history"), _parse_bool),
    ("KPI_SLICE_DIMENSIONS", ("slices", "dimensions"), _comma_list),
    ("KPI_HTML_ENABLED", ("html_report", "enabled"), _parse_bool),
    ("KPI_HTML_FILENAME", ("html_report", "filename"), str.strip),
    ("KPI_HTML_TITLE", ("html_report", "report_title"), str.strip),
    ("KPI_HTML_OUTPUT_PATH", ("html_report", "output_path_segments"), _comma_list),
    ("KPI_HTML_WEEKLY_WEEKS", ("html_report", "weekly_display_weeks"), _optional_int),
    ("KPI_HTML_MONTHLY_MONTHS", ("html_report", "monthly_display_months"), _optional_int),
    ("KPI_HTML_QUARTERLY_QUARTERS", ("html_report", "quarterly_display_quarters"), _optional_int),
    ("KPI_HTML_HALF_HALVES", ("html_report", "half_display_halves"), _optional_int),
    ("KPI_HTML_YEARLY_YEARS", ("html_report", "yearly_display_years"), _optional_int),
)


def _apply_env_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """A copy of cfg with every set KPI_* variable of _ENV_OVERRIDES applied (missing sections are added)."""
    out = copy.deepcopy(cfg)
    for env_var, key_path, parse in _ENV_OVERRIDES:
        section = out
        for key in key_path[:-1]:
            section = section.setdefault(key, {})
        if env_var in os.environ:
            section[key_path[-1]] = parse(os.environ[env_var])
    return out


def _solution_ids(raw: Any, key: str) -> List[int]:
    """One solution id or a non-empty list of them, as a list of ints (bool rejected)."""
    ids = list(raw) if isinstance(raw, (list, tuple)) else [raw]
    if not ids or any(type(i) is not int for i in ids):
        raise ValueError(f"{key} must be an integer or a non-empty list of integers; got {raw!r}")
    return ids


def _optional_solution_ids(raw: Any, key: str) -> Optional[List[int]]:
    return _solution_ids(raw, key) if raw is not None else None


def _kinds_in_canonical_order(requested: Any, allowed: tuple, key: str) -> List[str]:
    """The requested kinds (case / space insensitive) in the order of allowed; unknown kinds raise."""
    requested_set = {str(k).strip().lower() for k in requested}
    invalid = sorted(requested_set - set(allowed))
    if invalid:
        raise ValueError(f"{key} has invalid kinds {invalid}; allowed: {list(allowed)}")
    return [k for k in allowed if k in requested_set]


def _validate_value_filters(value_filters: Dict[str, Any]) -> None:
    """Each entry is a list (include only) or a dict with any of include / exclude / keep_null."""
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
    """Each key is a metric_col; each value a {dim_col: value filter} dict shaped like slices.value_filters."""
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
    """Every scope_diff_metrics name must be in metric_cols (the scope diff indexes a frame by it)."""
    known = set(metric_cols)
    unknown = [m for m in scope_diff_metrics if m not in known]
    if unknown:
        raise ValueError(
            f"metrics.scope_diff_metrics has entr{'y' if len(unknown) == 1 else 'ies'} "
            f"{unknown} not in metrics.metric_cols {sorted(known)}."
        )


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


# =============================================================================
# MATERIALIZE -- CONFIG -> validated flat settings dict
# =============================================================================
DC_METRICS = ("dc_mean_stock", "total_mean_stock", "WOS_DC", "WOS_TOTAL", "dc_in_stock_rate")


def _switch_off_unset_sources(cfg: Dict[str, Any], paths: Dict[str, Optional[str]]) -> None:
    """Switch off, in cfg, whatever reads a path_segments entry left unset (None or []), and print what is off.
    A source that is not set is never read, so nothing fails and nothing that needs it happens:
    item_family -> every family-main roll-up and the in-stock main-eligible / unsuperseded-size removals;
    inventory_warehouse -> the DC metrics, DC scope and DC blocks; goods_in_transit -> goods in transit;
    transactional_sales -> sales_basis "gross" (net is used); the lost-sales ensemble paths -> the ensemble.
    """
    if paths["PATH_ITEM_FAMILY"] is None:
        print(
            "note: path_segments.item_family is not set, so it is not read and no product is rolled to its family "
            "main (scope, daily_data, lost_sales, inventory_warehouse, transactional_sales, goods_in_transit), "
            "and scope.instock_main_eligible_only / instock_exclude_unsuperseded_sizes do not apply"
        )
        cfg["item_family_rollup"] = {
            key: False for key in ("daily_data", "lost_sales", "inventory_warehouse", "transactional_sales")
        }
        cfg["scope"]["roll_to_family_main"] = False
        cfg["scope"]["instock_main_eligible_only"] = False
        cfg["scope"]["instock_exclude_unsuperseded_sizes"] = False
        cfg["goods_in_transit"]["roll_to_family_main"] = False

    if paths["PATH_INVENTORY_WAREHOUSE"] is None:
        print(
            f"note: path_segments.inventory_warehouse is not set, so it is not read and the DC metrics {list(DC_METRICS)}, "
            "the DC scope (scope.dc_solution_id) and the DC blocks (blocked_scope.dc_solution_id) are off"
        )
        cfg["dc_instock"]["enabled"] = False
        cfg["scope"]["dc_solution_id"] = None
        cfg["blocked_scope"]["dc_solution_id"] = None
        cfg["goods_in_transit"]["dc_instock"] = False
        cfg["goods_in_transit"]["inventory_metrics"] = [
            m for m in cfg["goods_in_transit"]["inventory_metrics"] if m not in DC_METRICS
        ]
        metrics = cfg["metrics"]
        metrics["metric_cols"] = [m for m in metrics["metric_cols"] if m not in DC_METRICS]
        metrics["scope_diff_metrics"] = [m for m in metrics["scope_diff_metrics"] if m not in DC_METRICS]
        metrics["population_filters"] = {
            m: f for m, f in (metrics.get("population_filters") or {}).items() if m not in DC_METRICS
        }
        if isinstance(cfg["blocked_scope"]["metrics"], (list, tuple)):
            cfg["blocked_scope"]["metrics"] = [m for m in cfg["blocked_scope"]["metrics"] if m not in DC_METRICS]

    if paths["PATH_GOODS_IN_TRANSIT"] is None:
        print("note: path_segments.goods_in_transit is not set, so it is not read and no goods in transit are added anywhere")
        git = cfg["goods_in_transit"]
        git["date_shift_days"] = None
        git["store_instock"] = False
        git["dc_instock"] = False
        git["inventory_metrics"] = []

    if paths["PATH_TRANSACTIONAL_SALES"] is None and cfg["sales_basis"] == "gross":
        print("note: path_segments.transactional_sales is not set, so it is not read and sales_basis 'gross' does not apply: sales are net")
        cfg["sales_basis"] = "net"

    ensemble = cfg["lost_sales_ensemble"]
    if ensemble.get("enabled") and (paths["PATH_LOST_SALES_SLOW"] is None or paths["PATH_SPEED_CLUSTER"] is None):
        print(
            "note: lost_sales_ensemble.slow_path_segments or speed_cluster_path_segments is not set, so they are not "
            "read and the lost-sales ensemble does not apply: the single model at path_segments.lost_sales is used"
        )
        ensemble["enabled"] = False


def materialize(fund_paste: Callable[..., str], cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Resolve CONFIG into flat settings dict for KPIRunner (paths, dates, metrics, slices)."""
    cfg = _apply_env_overrides(cfg or CONFIG)

    customer = cfg["customer"]
    rw = cfg["reporting_window"]
    run_min_raw = rw.get("run_min_date")
    run_min = run_min_raw.strip() if isinstance(run_min_raw, str) and run_min_raw.strip() else None

    # --- Source paths -----------------------------------------------------------
    bucket = os.environ.get("KPI_BUCKET", f"/mnt/invent-{customer}-datastore")
    path_segments = cfg["path_segments"]
    for required in ("daily_data", "products", "scope", "lost_sales"):
        if not path_segments.get(required):
            raise ValueError(f"path_segments.{required} is required: the report cannot be built without it")

    def optional_path(segments: Optional[List[str]]) -> Optional[str]:
        return fund_paste(bucket, *segments) if segments else None

    paths = {
        "PATH_FISCAL": optional_path(path_segments.get("fiscal")),
        "PATH_DAILY_DATA": fund_paste(bucket, *path_segments["daily_data"]),
        "PATH_INVENTORY_WAREHOUSE": optional_path(path_segments.get("inventory_warehouse")),
        "PATH_ITEM_FAMILY": optional_path(path_segments.get("item_family")),
        "PATH_PRODUCTS": fund_paste(bucket, *path_segments["products"]),
        "PATH_LOST_SALES": fund_paste(bucket, *path_segments["lost_sales"]),
        "PATH_LOST_SALES_SLOW": optional_path(cfg["lost_sales_ensemble"].get("slow_path_segments")),
        "PATH_SPEED_CLUSTER": optional_path(cfg["lost_sales_ensemble"].get("speed_cluster_path_segments")),
        "PATH_PRODUCT_PLANNING_LEVEL": optional_path(path_segments.get("product_planning_level")),
        "PATH_SCOPE": fund_paste(bucket, *path_segments["scope"]),
        "PATH_GOODS_IN_TRANSIT": optional_path(path_segments.get("goods_in_transit")),
        "PATH_TRANSACTIONAL_SALES": optional_path(path_segments.get("transactional_sales")),
    }
    _switch_off_unset_sources(cfg, paths)
    if cfg["fiscal_calendar"]["use_fiscal_calendar"] and paths["PATH_FISCAL"] is None:
        raise ValueError(
            "fiscal_calendar.use_fiscal_calendar is True but path_segments.fiscal is not set: "
            "set the fiscal_cal folder, or set use_fiscal_calendar to False"
        )
    planning_level_users = [
        cfg.get("lost_sales_source", {}).get("product_agg_level_col"),
    ]
    if cfg["instock"]["method"] == "weekly_source":
        weekly_source = cfg["instock"]["weekly_source"]
        planning_level_users += [
            weekly_source.get("product_agg_level_col"),
            *[fb.get("product_agg_level_col") for fb in weekly_source.get("fallback_sources", [])],
        ]
    if any(planning_level_users) and paths["PATH_PRODUCT_PLANNING_LEVEL"] is None:
        raise ValueError(
            "a source sets product_agg_level_col but path_segments.product_planning_level is not set: "
            "it maps that level to product_id"
        )

    # --- Column maps: lost sales, weekly in-stock source, item family -------------
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
        "transactional_sales": bool(
            item_family_rollup_cfg.get("transactional_sales", item_family_rollup_cfg.get("daily_data", True))
        ),
    }

    sales_basis = cfg["sales_basis"]
    if sales_basis not in SALES_BASES:
        raise ValueError(f"sales_basis must be one of {list(SALES_BASES)}; got {sales_basis!r}")
    if sales_basis == "gross" and item_family_rollup["transactional_sales"] != item_family_rollup["daily_data"]:
        raise ValueError(
            "item_family_rollup.transactional_sales must equal item_family_rollup.daily_data when sales_basis is "
            "'gross': gross sales are joined onto daily-data, which must share its product id space"
        )

    dc_instock_cfg = cfg["dc_instock"]
    dc_instock_enabled = bool(dc_instock_cfg["enabled"])
    dc_instock_stock_threshold = dc_instock_cfg["stock_threshold"]

    # --- Reporting window ---------------------------------------------------------
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

    # --- Lost sales ensemble ------------------------------------------------------
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

    # --- Scope --------------------------------------------------------------------
    scope_cfg = cfg["scope"]
    scope_run_date = scope_cfg["run_date"]
    scope = {
        "path": paths["PATH_SCOPE"],
        "time": scope_cfg["time"],
        "grain": scope_cfg["grain"],
        "columns": {
            key: scope_cfg["columns"][key]
            for key in ("product", "store", "start", "end", "solution", "run_date", "date", "year", "week")
        },
        "solution_id": _optional_solution_ids(scope_cfg["solution_id"], "scope.solution_id"),
        "dc_solution_id": _optional_solution_ids(scope_cfg["dc_solution_id"], "scope.dc_solution_id"),
        "run_date": datetime.date.fromisoformat(scope_run_date) if scope_run_date else None,
        "roll_to_family_main": bool(scope_cfg["roll_to_family_main"]),
        "active_only": bool(scope_cfg["active_only"]),
        "instock_main_eligible_only": bool(scope_cfg["instock_main_eligible_only"]),
        "instock_exclude_unsuperseded_sizes": bool(scope_cfg["instock_exclude_unsuperseded_sizes"]),
        "instock_unsuperseded_group_column": scope_cfg["instock_unsuperseded_group_column"],
        "backfill_leading_gap": bool(scope_cfg["backfill_leading_gap"]),
        "use_hybrid_scope": scope_cfg["use_hybrid_scope"],
        "run_scope_diff": scope_cfg["run_scope_diff"],
    }
    columns = scope["columns"]
    if scope["time"] not in ("daily", "weekly"):
        raise ValueError(f"scope.time must be 'daily' or 'weekly'; got {scope['time']!r}")
    if scope["grain"] not in ("product", "product_store"):
        raise ValueError(f"scope.grain must be 'product' or 'product_store'; got {scope['grain']!r}")
    if not columns["product"]:
        raise ValueError("scope.columns.product is required")
    if scope["grain"] == "product_store" and not columns["store"]:
        raise ValueError("scope.grain='product_store' requires scope.columns.store")
    if scope["time"] == "weekly":
        if not (columns["date"] or (columns["year"] and columns["week"])):
            raise ValueError("scope.time='weekly' requires scope.columns.date OR both scope.columns.year and week")
        if columns["start"] or columns["end"]:
            raise ValueError("scope.time='weekly' does not support scope.columns.start / end (set them to None)")
    elif columns["date"] or columns["year"] or columns["week"]:
        raise ValueError("scope.columns.date / year / week apply to scope.time='weekly' only (set them to None)")
    if columns["solution"] and scope["solution_id"] is None:
        raise ValueError("scope.columns.solution requires scope.solution_id")
    if scope["run_date"] is not None and scope["run_date"].weekday() != 6:
        raise ValueError(f"scope.run_date must be a Sunday; got {scope['run_date']}")
    if scope["dc_solution_id"] is not None and not (columns["solution"] and columns["start"] and columns["store"]):
        raise ValueError("scope.dc_solution_id requires scope.columns.solution, start and store")

    # --- Blocked scope ------------------------------------------------------------
    blocked_scope_cfg = cfg["blocked_scope"]
    ui_parameters_path = blocked_scope_cfg["ui_parameters_path"]
    ui_segments = ui_parameters_path.strip("/").split("/") if ui_parameters_path is not None else None
    store_folder = blocked_scope_cfg.get("folder", "blocked_scope")
    dc_folder = blocked_scope_cfg.get("dc_folder", "dc_blocked_scope")
    if ui_segments is not None:
        for key, folder in (("folder", store_folder), ("dc_folder", dc_folder)):
            if not folder:
                print(f"note: blocked_scope.{key} is not set, so those blocks are not read and do not apply")
    blocked_scope = {
        "rule": blocked_scope_cfg["rule"],
        "path": fund_paste(bucket, *ui_segments, store_folder) if ui_segments is not None and store_folder else None,
        # DC blocks of the same snapshot, read only when scope.dc_solution_id is set.
        "dc_path": fund_paste(bucket, *ui_segments, dc_folder) if ui_segments is not None and dc_folder else None,
    }
    if blocked_scope["rule"] not in ("after_scope_start", "all"):
        raise ValueError(f"blocked_scope.rule must be 'after_scope_start' or 'all'; got {blocked_scope['rule']!r}")
    if (blocked_scope["path"] is not None or blocked_scope["dc_path"] is not None) and not (
        columns["start"] and columns["store"]
    ):
        raise ValueError("blocked_scope.ui_parameters_path requires scope.columns.start and scope.columns.store")

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
    # in_stock_rate and weighted_instock_rate read one in-stock frame, so they are blocked together: listing
    # either drops blocked days from that frame and from the weighted sales weights (pipeline.build_instock_daily,
    # metrics.compute_kpis read the list). Under instock.method weekly_source / lost_sales_source, which only
    # have in-stock days per week, a pair-week (or product-week, for a source without a store column: "product"
    # blocks only) is dropped from the in-stock frame when every day of the week in the window is blocked.
    instock_pair = {"in_stock_rate", "weighted_instock_rate"}
    if blocked_requested & instock_pair and not instock_pair <= blocked_requested:
        print("note: in_stock_rate and weighted_instock_rate are blocked together; both are now in blocked_scope.metrics")
        blocked_requested |= instock_pair
    blocked_scope["metrics"] = [m for m in METRICS_ALL if m in blocked_requested]
    for key in ("kinds", "dc_kinds"):
        kinds = list(blocked_scope_cfg[key])
        unknown_kinds = sorted(set(kinds) - {"product", "product_destination", "destination"})
        if unknown_kinds or not kinds:
            raise ValueError(
                f"blocked_scope.{key} must be a non-empty subset of product / product_destination / destination; "
                f"got {kinds}"
            )
        blocked_scope[key] = kinds
    blocked_scope["solution_id"] = _solution_ids(blocked_scope_cfg["solution_id"], "blocked_scope.solution_id")
    blocked_scope["dc_solution_id"] = _optional_solution_ids(
        blocked_scope_cfg["dc_solution_id"], "blocked_scope.dc_solution_id"
    )
    if blocked_scope["dc_solution_id"] is not None and scope["dc_solution_id"] is None:
        raise ValueError("blocked_scope.dc_solution_id needs scope.dc_solution_id (the DC pairs the blocks match)")
    if blocked_scope["dc_path"] is None:
        blocked_scope["dc_solution_id"] = None

    # --- Daily in-stock -----------------------------------------------------------
    instock_daily_cfg = instock_cfg["daily"]
    history_start_raw = instock_daily_cfg["history_start"]
    daily_instock = {
        "count_start": instock_daily_cfg["count_start"],
        "require_daily_data": bool(instock_daily_cfg["require_daily_data"]),
        "history_start": datetime.date.fromisoformat(history_start_raw) if history_start_raw else None,
        "usable_only": bool(instock_daily_cfg["usable_only"]),
        "sales_counts_as_stocked": bool(instock_daily_cfg["sales_counts_as_stocked"]),
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
        if scope["grain"] == "product":
            raise ValueError("instock.method='daily' requires a store-level scope.grain (product_store)")
        if lse.get("enabled"):
            raise ValueError("instock.method='daily' and lost_sales_ensemble.enabled cannot both be True")
        if daily_instock["count_start"] != "first_daily_row" and not columns["start"]:
            raise ValueError(
                f"instock.daily.count_start={daily_instock['count_start']!r} needs the scope start "
                "date: set scope.columns.start or count_start='first_daily_row'."
            )
        if daily_instock["count_start"] == "first_daily_row" and not daily_instock["require_daily_data"]:
            raise ValueError("instock.daily.count_start='first_daily_row' requires require_daily_data=True")
    if scope["instock_main_eligible_only"] and not (
        columns["start"] and scope["roll_to_family_main"] and instock_method == "daily"
    ):
        raise ValueError(
            "scope.instock_main_eligible_only requires scope.columns.start, "
            "roll_to_family_main=True and instock.method='daily'"
        )
    if scope["instock_exclude_unsuperseded_sizes"] and instock_method != "daily":
        raise ValueError("scope.instock_exclude_unsuperseded_sizes requires instock.method='daily'")
    if report_end_mode == "latest_day" and instock_method != "daily":
        raise ValueError(
            "reporting_window.report_end='latest_day' requires instock.method='daily': the YTD cut at the "
            "latest day splits the daily in-stock frame's fiscal week, which weekly in-stock sources cannot do"
        )

    # --- Goods in transit ---------------------------------------------------------
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

    # --- Dimension sources, roots and slice value filters -------------------------
    dimension_sources = []
    for src in cfg.get("dimension_sources", []) or []:
        resolved = dict(src)
        if not resolved.get("path") and resolved.get("path_segments"):
            resolved["path"] = fund_paste(bucket, *resolved["path_segments"])
        dimension_sources.append(resolved)

    # Every column an enabled dimension source contributes (columns + derived) defines roots, never a cut
    # (docs/LOGIC_FLOW.md: Dimension sources). root_values maps values to root names; a column without an entry gets one
    # root per distinct value (fiscal._resolve_root_definitions). A root_values key that is not one of the
    # source's own columns raises here instead of silently never applying.
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

    # Applied to a slice's own breakdown (kpi_pipeline/kpi_long._stack_roots_and_cuts).
    slice_value_filters = dict(cfg["slices"].get("value_filters", {}) or {})
    _validate_value_filters(slice_value_filters)

    # --- Comparisons and comparable pairs -----------------------------------------
    comparisons_cfg = cfg.get("comparisons", {}) or {}
    requested_kinds = comparisons_cfg.get("enabled")
    if requested_kinds is None:
        requested_kinds = list(COMPARISON_KINDS_ALL)
    comparison_kinds = _kinds_in_canonical_order(requested_kinds, COMPARISON_KINDS_ALL, "comparisons.enabled")
    if not comparison_kinds:
        raise ValueError(
            "comparisons.enabled resolved to an empty list; "
            f"choose at least one of {list(COMPARISON_KINDS_ALL)}"
        )

    # Kinds are validated always but resolve to an empty list while comparable_pairs is disabled.
    comparable_pairs_cfg = cfg.get("comparable_pairs", {}) or {}
    comparable_pairs_enabled = bool(comparable_pairs_cfg.get("enabled", False))
    requested_comparable_kinds = comparable_pairs_cfg.get("kinds")
    if requested_comparable_kinds is None:
        requested_comparable_kinds = ["ytd"]
    comparable_kinds_ordered = _kinds_in_canonical_order(
        requested_comparable_kinds, COMPARABLE_KINDS_ALL, "comparable_pairs.kinds"
    )
    comparable_kinds = comparable_kinds_ordered if comparable_pairs_enabled else []
    if comparable_pairs_enabled and not comparable_kinds:
        raise ValueError(
            "comparable_pairs.kinds resolved to an empty list while comparable_pairs.enabled=True; "
            f"choose at least one of {list(COMPARABLE_KINDS_ALL)}"
        )

    # Grain of the like-for-like pair population itself, not scope.grain.
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

    # --- Output, metrics, score scope, HTML report, run mode -------
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

    score_scope = cfg["score_scope"]
    min_pct = _as_fraction(score_scope["min_percentile"])

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

    column_map = cfg["fiscal_calendar"].get("column_map", {})
    return {
        "CONFIG": cfg,
        "CUSTOMER": customer,
        "RUN_MODE": run_mode,
        "BUCKET": bucket,
        **window,
        "REPORT_END_MODE": report_end_mode,
        "USE_FISCAL_CALENDAR": cfg["fiscal_calendar"]["use_fiscal_calendar"],
        "HALF_PERIODS": half_periods,
        "FISCAL_QUARTER_COL": column_map.get("quarter_col"),
        "FISCAL_MONTH_COL": column_map.get("month_col"),
        "FISCAL_MONTH_NAME_COL": column_map.get("month_name_col"),
        "DAILY_TIME_COLUMNS": cfg["fiscal_calendar"]["daily_time_columns"],
        "SCOPE_MIN_PERCENTILE": min_pct,
        "SCOPE_MIN_WEEKS_FOR_FILTER": score_scope["min_weeks_for_filter"],
        "COMPARABLE_PAIRS_ENABLED": comparable_pairs_enabled,
        "COMPARABLE_KINDS": comparable_kinds,
        "COMPARABLE_PAIRS_GRAIN": comparable_pairs_grain,
        "COMPARABLE_PAIRS_PAIR_DAYS": comparable_pairs_pair_days,
        "COMPARISON_KINDS": comparison_kinds,
        # .get() here: the checks above require fast_mover_clusters only when enabled and
        # speed_cluster_attribute_name only for the "long" format.
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
        "SALES_BASIS": sales_basis,
        "SCOPE": scope,
        "BLOCKED_SCOPE": blocked_scope,
        "INSTOCK_DAILY": daily_instock,
        "GOODS_IN_TRANSIT": goods_in_transit,
        "DC_INSTOCK_ENABLED": dc_instock_enabled,
        "DC_INSTOCK_STOCK_THRESHOLD": dc_instock_stock_threshold,
        **paths,
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

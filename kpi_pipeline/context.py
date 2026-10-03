"""Shared runtime context for the KPI pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

# goods_in_transit.inventory_metrics names by the goods in transit they read: store GIT (build_scoped_daily)
# or DC GIT (build_dc_daily); total_mean_stock and WOS_TOTAL read both.
STORE_GIT_METRICS = (
    "total_inventory", "mean_stock", "mean_stock_retail", "mean_stock_cost", "total_mean_stock",
    "WOS", "wos_revenue", "wos_cost", "WOS_TOTAL", "inventory_turnover_rate",
)
DC_GIT_METRICS = ("dc_mean_stock", "total_mean_stock", "WOS_DC", "WOS_TOTAL")


@dataclass
class KPIContext:
    spark: SparkSession
    settings: Dict[str, Any]

    fiscal_cal: Optional[DataFrame] = None
    fiscal_week: Optional[DataFrame] = None
    products_attr: Optional[DataFrame] = None
    product_dims: Optional[DataFrame] = None
    active_slice_dimensions: List[str] = field(default_factory=list)
    # Slice dimensions minus the root-defining dimension_source columns: the breakdowns ("cuts") inside
    # every root.
    cut_dimensions: List[str] = field(default_factory=list)
    # One entry per root population besides the implicit "overall", e.g.
    # {"root": "nvrout", "dim_col": "IS_NVROUT", "value": "yes"} (fiscal._resolve_root_definitions).
    root_definitions: List[Dict[str, Any]] = field(default_factory=list)
    available_fiscal_months: Optional[List[int]] = None
    # {"Fiscal_Quarter", "Fiscal_Month"[, "Fiscal_Half"][, "Year", "Week"]: df} of the (Year, period) keys
    # fully elapsed inside the window (fiscal.complete_fiscal_periods); None in html_only mode.
    complete_fiscal_periods: Optional[Dict[str, DataFrame]] = None

    # report_end="latest_day" only (fiscal.build_latest_day_windows):
    #   ytd_through_day            K, the day of the fiscal year (1-based) of REPORT_END_DATE.
    #   ytd_years                  years whose days 1..K lie inside the window.
    #   ytd_lost_sales_last_week   fiscal week of the last Saturday on or before REPORT_END_DATE.
    #   day_calendar               (date, Year, Week, day_index, last_day_index); the week containing K is
    #                              split into the days <= K and the days after.
    ytd_through_day: Optional[int] = None
    ytd_years: Optional[List[int]] = None
    ytd_lost_sales_last_week: Optional[int] = None
    day_calendar: Optional[DataFrame] = None

    scope_table_keys: Optional[DataFrame] = None
    scope_keys: List[str] = field(default_factory=list)
    # scope.columns.start only: cached (product_id, store_id, scope_start, main_eligible) after the family
    # roll-up and active filter.
    scope_pairs: Optional[DataFrame] = None
    # blocked_scope on only: cached disjoint (product_id, store_id, first_day, last_day) block intervals.
    blocked_days: Optional[DataFrame] = None
    # scope.dc_solution_id only: cached (product_id, warehouse_id, scope_start, main_eligible) of the
    # DC (network) scope.
    dc_scope_pairs: Optional[DataFrame] = None
    # DC blocks on only: cached disjoint (product_id, warehouse_id, first_day, last_day) block intervals.
    dc_blocked_days: Optional[DataFrame] = None

    hybrid_scope_keys: Optional[DataFrame] = None
    score_only_scope_keys: Optional[DataFrame] = None
    # Removal sets, built once by scope.build_scope_removals (None when their rule is off): cached
    # (product_id, store_id) of hybrid_scope_keys blocked on every window day; cached (product_id, store_id)
    # scope pairs where only a sub item is eligible; cached product_ids of unsuperseded sizes.
    fully_blocked_pairs: Optional[DataFrame] = None
    instock_sub_only_pairs: Optional[DataFrame] = None
    instock_unsuperseded_products: Optional[DataFrame] = None
    hybrid_frames: Optional[Dict[str, DataFrame]] = None
    scope_frames: Optional[Dict[str, DataFrame]] = None
    score_frames: Optional[Dict[str, DataFrame]] = None

    # Full computed history: saved to Delta and compared. kpi_long_display is the HTML report's copy,
    # trimmed to recent periods (kpi_long.trim_periods_to_recent).
    kpi_long: Optional[pd.DataFrame] = None
    kpi_long_display: Optional[pd.DataFrame] = None
    comparison_yoy: Optional[pd.DataFrame] = None
    comparison_ytd: Optional[pd.DataFrame] = None
    scope_diff: Optional[pd.DataFrame] = None

    # comparable_pairs: comparable_kpi_long holds every enabled kind (comparison_type column);
    # comparable_comparison_<kind> holds that kind's comparison rows.
    comparable_kpi_long: Optional[pd.DataFrame] = None
    comparable_comparison_ytd: Optional[pd.DataFrame] = None
    comparable_comparison_yoy: Optional[pd.DataFrame] = None
    comparable_comparison_quarter: Optional[pd.DataFrame] = None
    comparable_comparison_half: Optional[pd.DataFrame] = None

    yoy_display: Optional[pd.DataFrame] = None
    ytd_display: Optional[pd.DataFrame] = None
    comparable_ytd_display: Optional[pd.DataFrame] = None
    comparable_yoy_display: Optional[pd.DataFrame] = None
    comparable_quarter_display: Optional[pd.DataFrame] = None
    comparable_half_display: Optional[pd.DataFrame] = None
    save_plan: Optional[Any] = None

    # Per-run caches (runner._reset_run_caches).
    daily_data_raw: Optional[DataFrame] = None
    daily_data_excluded_days: Optional[DataFrame] = None
    lost_sales_weekly_base: Optional[DataFrame] = None
    instock_weekly_base: Optional[DataFrame] = None
    item_family_raw: Optional[DataFrame] = None
    inventory_warehouse_rolled: Optional[DataFrame] = None

"""Shared runtime context for the KPI pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

# goods_in_transit.inventory_metrics names (config.py's INVENTORY_GIT_METRICS_ALL) by the goods in transit
# they read: store metrics build_scoped_daily's store GIT, DC metrics build_dc_daily's DC GIT;
# total_mean_stock and WOS_TOTAL read both.
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
    # Root-defining dimension_source columns (config.py's dimension_sources[*].root_values)
    # excluded -- what's left is what actually gets its own breakdown ("cut") inside every root,
    # including "overall". See kpi_long.build_kpi_long / fiscal._resolve_root_definitions.
    cut_dimensions: List[str] = field(default_factory=list)
    # One entry per root population (e.g. {"root": "nvrout", "dim_col": "IS_NVROUT", "value": "yes"}) --
    # "overall" (no restriction) is always implicit and not listed here. Resolved at runtime in
    # fiscal.build_fiscal_and_products from config.py's dimension_sources[*].root_values (explicit
    # value->name mapping) or auto-discovered (one root per distinct value) when root_values is
    # absent for a given dimension_source column.
    root_definitions: List[Dict[str, Any]] = field(default_factory=list)
    available_fiscal_months: Optional[List[int]] = None
    # {"Fiscal_Quarter": df, "Fiscal_Month": df, plus "Fiscal_Half": df when half_periods} -- the (Year, period) pairs that
    # have FULLY ELAPSED as of REPORT_END_DATE (fiscal.complete_fiscal_periods). Semi-joined onto
    # every metric frame by kpi_long._period_frames so the Quarter/Half/Monthly value-trend tabs
    # never carry an in-progress trailing period. Set by fiscal.build_fiscal_and_products; stays
    # None in html_only mode, which renders saved rows and never period-frames a Spark frame.
    complete_fiscal_periods: Optional[Dict[str, DataFrame]] = None

    # report_end="latest_day" only (fiscal.build_latest_day_windows; None otherwise). YTD runs to the
    # latest day, the same fiscal day for every year:
    #   ytd_through_day            K = day of the fiscal year (1-based) of REPORT_END_DATE.
    #   ytd_years                  years whose days 1..K lie inside the report window (the YTD tab and
    #                              the comparable ytd kind only use these).
    #   ytd_lost_sales_last_week   fiscal week number of the last Saturday on or before REPORT_END_DATE;
    #                              lost-sales YTD is whole weeks 1..this for every year.
    #   day_calendar               fiscal_cal's (date, Year, Week) plus day_index (day of the fiscal
    #                              year) and last_day_index (day_index of the last day of the row's
    #                              fiscal-week part: the week containing day K is split into the days
    #                              <= K and the days after).
    ytd_through_day: Optional[int] = None
    ytd_years: Optional[List[int]] = None
    ytd_lost_sales_last_week: Optional[int] = None
    day_calendar: Optional[DataFrame] = None

    defined_scope_keys: Optional[DataFrame] = None
    scope_keys: List[str] = field(default_factory=list)
    # scope_source.mode="operation_scope" only: (product_id, store_id, scope_start) of the platform
    # scope after the family roll-up (earliest start per pair) and active filter. Source of each
    # pair's scope start date for the blocked-scope rule and the daily in-stock count start.
    operation_scope_pairs: Optional[DataFrame] = None
    # blocked_scope.ui_parameters_path set only: cached (product_id, store_id, date) blocked days. Flagged
    # (is_blocked) on scoped_daily, and removed from the metrics named in blocked_scope.metrics. None when
    # blocked scope is off.
    blocked_days: Optional[DataFrame] = None
    # scope_source.dc_solution_id set only: (product_id, warehouse_id, scope_start, main_eligible) of the DC
    # (network) scope: the DC metrics read these pairs among the store scope's products, and the DC blocked
    # days take their start dates from them. None = store scope's products at every warehouse.
    dc_scope_pairs: Optional[DataFrame] = None
    # scope_source.dc_solution_id and blocked_scope.ui_parameters_path set only: cached (product_id, warehouse_id, date) blocked days. Flagged
    # (is_blocked) on dc_daily and removed from the DC metrics named in blocked_scope.metrics. None otherwise.
    dc_blocked_days: Optional[DataFrame] = None

    scope_adjustments_applied: bool = False
    scope_before_adjustments: Optional[DataFrame] = None
    scope_adjustment_steps: List[Dict[str, Any]] = field(default_factory=list)

    hybrid_scope_keys: Optional[DataFrame] = None
    score_only_scope_keys: Optional[DataFrame] = None
    hybrid_frames: Optional[Dict[str, DataFrame]] = None
    defined_frames: Optional[Dict[str, DataFrame]] = None
    score_frames: Optional[Dict[str, DataFrame]] = None

    kpi_long: Optional[pd.DataFrame] = None
    # Trimmed-to-recent copy for HTML rendering only (see kpi_long.trim_periods_to_recent).
    # kpi_long itself always stays the FULL computed/loaded history — it's what gets saved
    # to Delta and what comparisons are built from; only this display copy is ever narrowed.
    kpi_long_display: Optional[pd.DataFrame] = None
    comparison_yoy: Optional[pd.DataFrame] = None
    comparison_ytd: Optional[pd.DataFrame] = None
    scope_diff: Optional[pd.DataFrame] = None

    # Gated comparable-pairs (like-for-like) output: metrics over only the pairs present in every
    # year of the run window. comparable_kpi_long carries ALL enabled kinds (comparable_pairs.kinds
    # -- see config.py), tagged by its own comparison_type column ("ytd"/"yoy"/"quarter"/"half");
    # comparable_comparison_{ytd,yoy,quarter,half} are that kind's own long-format comparison rows.
    # Populated only when comparable_pairs.enabled=True and the kind is in comparable_pairs.kinds.
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

    daily_data_raw: Optional[DataFrame] = None
    # Scope-independent (pair, date) days input_filters.daily_data removes, built once and shared by
    # every build_scoped_daily call when a store goods_in_transit.inventory_metrics gate is on.
    daily_data_excluded_days: Optional[DataFrame] = None
    lost_sales_weekly_base: Optional[DataFrame] = None
    instock_weekly_base: Optional[DataFrame] = None
    inventory_warehouse_raw: Optional[DataFrame] = None
    # item_family_raw: read unconditionally whenever inventory_warehouse is configured -- also
    # backs build_dc_daily's item-family rollup, not just dc_in_stock_rate.
    item_family_raw: Optional[DataFrame] = None
    # Parent-rolled inventory_warehouse: scope-independent, so built once and shared by
    # build_dc_daily/build_dc_inst across every scope variant.
    inventory_warehouse_rolled: Optional[DataFrame] = None

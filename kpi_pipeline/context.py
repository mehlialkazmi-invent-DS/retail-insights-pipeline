"""Shared runtime context for the KPI pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession


@dataclass
class KPIContext:
    spark: SparkSession
    settings: Dict[str, Any]

    fiscal_cal: Optional[DataFrame] = None
    fiscal_week: Optional[DataFrame] = None
    products_attr: Optional[DataFrame] = None
    product_dims: Optional[DataFrame] = None
    active_slice_dimensions: List[str] = field(default_factory=list)
    available_fiscal_quarters: Optional[List[int]] = None

    defined_scope_keys: Optional[DataFrame] = None
    scope_keys: List[str] = field(default_factory=list)

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
    comparison_qoq: Optional[pd.DataFrame] = None
    comparison_mom: Optional[pd.DataFrame] = None
    comparison_wow: Optional[pd.DataFrame] = None
    comparison_ytd: Optional[pd.DataFrame] = None
    scope_diff: Optional[pd.DataFrame] = None

    # Gated comparable-pairs (like-for-like) outputs: metrics over only the (product_id, store_id)
    # pairs present in BOTH compared periods. Populated only when comparable_pairs.enabled=True.
    comparable_kpi_long: Optional[pd.DataFrame] = None
    comparable_comparison_yoy: Optional[pd.DataFrame] = None
    comparable_comparison_qoq: Optional[pd.DataFrame] = None
    comparable_comparison_mom: Optional[pd.DataFrame] = None
    comparable_comparison_wow: Optional[pd.DataFrame] = None
    comparable_comparison_ytd: Optional[pd.DataFrame] = None

    yoy_display: Optional[pd.DataFrame] = None
    qoq_display: Optional[pd.DataFrame] = None
    mom_display: Optional[pd.DataFrame] = None
    wow_display: Optional[pd.DataFrame] = None
    ytd_display: Optional[pd.DataFrame] = None
    comparable_yoy_display: Optional[pd.DataFrame] = None
    comparable_qoq_display: Optional[pd.DataFrame] = None
    comparable_mom_display: Optional[pd.DataFrame] = None
    comparable_wow_display: Optional[pd.DataFrame] = None
    comparable_ytd_display: Optional[pd.DataFrame] = None
    save_plan: Optional[Any] = None

    daily_data_raw: Optional[DataFrame] = None
    lost_sales_weekly_base: Optional[DataFrame] = None

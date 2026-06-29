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

    defined_scope: Optional[DataFrame] = None
    defined_scope_psw: Optional[DataFrame] = None
    scope_keys: List[str] = field(default_factory=list)

    scope_adjustments_applied: bool = False
    scope_before_adjustments: Optional[DataFrame] = None
    scope_adjustment_steps: List[Dict[str, Any]] = field(default_factory=list)

    hybrid_scope_psw: Optional[DataFrame] = None
    score_only_psw: Optional[DataFrame] = None
    hybrid_frames: Optional[Dict[str, DataFrame]] = None
    defined_frames: Optional[Dict[str, DataFrame]] = None
    score_frames: Optional[Dict[str, DataFrame]] = None

    kpi_long: Optional[pd.DataFrame] = None
    comparison_yoy: Optional[pd.DataFrame] = None
    comparison_qoq: Optional[pd.DataFrame] = None
    comparison_mom: Optional[pd.DataFrame] = None
    comparison_wow: Optional[pd.DataFrame] = None
    scope_diff: Optional[pd.DataFrame] = None

    yoy_display: Optional[pd.DataFrame] = None
    qoq_display: Optional[pd.DataFrame] = None
    mom_display: Optional[pd.DataFrame] = None
    wow_display: Optional[pd.DataFrame] = None
    save_plan: Optional[Any] = None

    daily_data_raw: Optional[DataFrame] = None
    lost_sales_weekly_base: Optional[DataFrame] = None

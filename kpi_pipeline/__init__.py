"""KPI hybrid-scope pipeline package."""

from kpi_pipeline.comparisons import slice_comparison_view
from kpi_pipeline.context import KPIContext
from kpi_pipeline.io import SavePlan, build_save_plan, load_saved_outputs
from kpi_pipeline.runner import KPIRunner

from kpi_pipeline.inputs import (
    preview_input_table,
    read_daily_data_source,
    read_defined_scope_source,
    read_lost_sales_source,
)

__all__ = [
    "KPIContext",
    "KPIRunner",
    "SavePlan",
    "build_save_plan",
    "load_saved_outputs",
    "preview_input_table",
    "read_daily_data_source",
    "read_defined_scope_source",
    "read_lost_sales_source",
    "slice_comparison_view",
]

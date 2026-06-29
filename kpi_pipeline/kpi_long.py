"""Tidy long KPI output across slices and periods.

Each row: period_type | period | dimension | dimension_value | METRIC_COLS...
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.context import KPIContext
from kpi_pipeline.metrics import build_kpi_table

PERIODS: List[Tuple[str, str]] = [("annual", "Year"), ("quarter", "period_key"), ("weekly", "Year_Week")]


def _with_period_key(df: DataFrame) -> DataFrame:
    return df.withColumn("period_key", F.concat_ws("-", F.col("Year").cast("string"), F.col("Fiscal_Quarter").cast("string")))


def _period_frames(frames: Dict[str, DataFrame], period_name: str) -> Dict[str, DataFrame]:
    if period_name == "quarter":
        out = dict(frames)
        out["scoped_daily"] = _with_period_key(frames["scoped_daily"])
        out["inst_data"] = _with_period_key(frames["inst_data"])
        out["lost_base"] = _with_period_key(frames["lost_base"])
        return out
    return frames


def _period_label(period_name: str, row: pd.Series) -> str:
    if period_name == "annual":
        return str(int(row["Year"]))
    if period_name == "quarter":
        y, q = str(row["period_key"]).split("-")
        return f"{int(y)}-Q{int(q)}"
    return str(row["Year_Week"])


def trim_weekly_to_recent(kpi_long: pd.DataFrame, ctx: KPIContext) -> pd.DataFrame:
    """Drop weekly rows outside the N most recent fiscal weeks (sorted by week_start_date)."""
    n = ctx.settings.get("HTML_REPORT_WEEKLY_DISPLAY_WEEKS")
    if not n:
        return kpi_long
    fw_pd = ctx.fiscal_week.select("Year_Week", "week_start_date").toPandas()
    recent_weeks = set(
        fw_pd.sort_values("week_start_date", ascending=False).head(n)["Year_Week"].tolist()
    )
    is_weekly = kpi_long["period_type"] == "weekly"
    return pd.concat(
        [kpi_long[~is_weekly], kpi_long[is_weekly & kpi_long["period"].isin(recent_weeks)]],
        ignore_index=True,
    )


def build_kpi_long(ctx: KPIContext, frames: Dict[str, DataFrame]) -> pd.DataFrame:
    """Build kpi_long for overall + each active slice dimension across annual/quarter/weekly periods."""
    metric_cols = ctx.settings["METRIC_COLS"]
    slices: List[Tuple[str, List[str]]] = [("overall", [])] + [
        (dim, [dim]) for dim in ctx.active_slice_dimensions
    ]
    rows: List[dict] = []
    for period_name, period_col in PERIODS:
        pf = _period_frames(frames, period_name)
        for slice_name, gk in slices:
            tbl = build_kpi_table(ctx, pf, period_col, gk)
            for _, r in tbl.iterrows():
                rec = {
                    "period_type": period_name,
                    "period": _period_label(period_name, r),
                    "dimension": slice_name,
                    "dimension_value": ("ALL" if not gk else r[gk[0]]),
                }
                for m in metric_cols:
                    rec[m] = r.get(m)
                rows.append(rec)
    return pd.DataFrame(rows, columns=["period_type", "period", "dimension", "dimension_value"] + metric_cols)

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

PERIODS: List[Tuple[str, str]] = [
    ("annual", "Year"),
    ("quarter", "period_key"),
    ("monthly", "month_key"),
    ("weekly", "Year_Week"),
]


def _with_period_key(df: DataFrame) -> DataFrame:
    return df.withColumn("period_key", F.concat_ws("-", F.col("Year").cast("string"), F.col("Fiscal_Quarter").cast("string")))


def _with_month_key(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "month_key",
        F.concat(F.col("Year").cast("string"), F.lit("-"), F.format_string("%02d", F.col("Fiscal_Month"))),
    )


def _period_frames(frames: Dict[str, DataFrame], period_name: str) -> Dict[str, DataFrame]:
    if period_name == "quarter":
        out = dict(frames)
        out["scoped_daily"] = _with_period_key(frames["scoped_daily"])
        out["inst_data"] = _with_period_key(frames["inst_data"])
        out["lost_base"] = _with_period_key(frames["lost_base"])
        return out
    if period_name == "monthly":
        out = dict(frames)
        out["scoped_daily"] = _with_month_key(frames["scoped_daily"])
        out["inst_data"] = _with_month_key(frames["inst_data"])
        out["lost_base"] = _with_month_key(frames["lost_base"])
        return out
    return frames


def _period_label(period_name: str, row: pd.Series) -> str:
    if period_name == "annual":
        return str(int(row["Year"]))
    if period_name == "quarter":
        y, q = str(row["period_key"]).split("-")
        return f"{int(y)}-Q{int(q)}"
    if period_name == "monthly":
        return str(row["month_key"])
    return str(row["Year_Week"])


def trim_periods_to_recent(kpi_long: pd.DataFrame, ctx: KPIContext) -> pd.DataFrame:
    """Trim each period_type to the N most recent periods in kpi_long and saved Delta."""
    settings = ctx.settings
    trim_cfg: List[Tuple[str, object, bool]] = [
        ("weekly", settings.get("HTML_REPORT_WEEKLY_DISPLAY_WEEKS"), True),
        ("monthly", settings.get("HTML_REPORT_MONTHLY_DISPLAY_MONTHS"), False),
        ("quarter", settings.get("HTML_REPORT_QUARTERLY_DISPLAY_QUARTERS"), False),
        ("annual", settings.get("HTML_REPORT_YEARLY_DISPLAY_YEARS"), False),
    ]

    if not any(n for _, n, _ in trim_cfg):
        return kpi_long

    parts = []
    for period_type in kpi_long["period_type"].unique():
        chunk = kpi_long[kpi_long["period_type"] == period_type]
        n = next((n for pt, n, _ in trim_cfg if pt == period_type), None)
        use_fiscal_week = next((u for pt, _, u in trim_cfg if pt == period_type), False)

        if not n:
            parts.append(chunk)
            continue

        if use_fiscal_week:
            fw_pd = ctx.fiscal_week.select("Year_Week", "week_start_date").toPandas()
            recent = set(fw_pd.sort_values("week_start_date", ascending=False).head(n)["Year_Week"].tolist())
        else:
            all_periods = sorted(chunk["period"].unique(), reverse=True)
            recent = set(all_periods[:n])

        parts.append(chunk[chunk["period"].isin(recent)])

    return pd.concat(parts, ignore_index=True) if parts else kpi_long.iloc[0:0].copy()


def build_kpi_long(ctx: KPIContext, frames: Dict[str, DataFrame]) -> pd.DataFrame:
    """Build kpi_long for overall + each active slice dimension across annual/quarter/monthly/weekly periods."""
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

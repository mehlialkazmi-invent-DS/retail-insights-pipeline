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


# Frames that carry the slice dimension columns and therefore get value-filtered.
_VALUE_FILTERED_FRAMES = ("scoped_daily", "inst_data", "lost_base")


def _normalize_value_filter(spec) -> Dict[str, object]:
    """Normalise one ``value_filters`` entry into a canonical include/exclude/keep_null form.

    ``value_filters[dim]`` accepts two shapes:

    LIST form (include-only; the original, still fully supported)::

        []           -> keep all NON-NULL values (drop the NULL bucket)
        ["A", "B"]   -> keep ONLY 'A' and 'B' (NULL and anything else dropped)

    DICT form (include and/or exclude, NULL-aware; the new richer form)::

        {"include": ["A", "B"]}              -> keep ONLY 'A' and 'B'          (NULL dropped)
        {"exclude": ["X", "Y"]}              -> keep EVERYTHING EXCEPT 'X'/'Y' (NULL KEPT)
        {"include": [...], "exclude": [...]} -> keep the include set, then remove the excludes
        {..., "keep_null": True/False}       -> optional override of the NULL bucket

    Default NULL handling (when ``keep_null`` is not given):

      * ``include`` present -> NULL is DROPPED (you asked for a specific set of values)
      * ``include`` absent  -> NULL is KEPT   (``exclude`` keeps the whole complement)

    :param spec: the raw ``value_filters[dim]`` value (a list or a dict).
    :return: ``{"include": Optional[list], "exclude": list, "keep_null": bool}``.
    :rtype: Dict[str, object]
    :raises ValueError: on an unrecognised shape or an unknown dict key.
    """
    # LIST form (include-only).
    if isinstance(spec, (list, tuple, set)):
        values = list(spec)
        if not values:  # [] -> keep all non-null
            return {"include": None, "exclude": [], "keep_null": False}
        return {"include": values, "exclude": [], "keep_null": False}

    # DICT form (include and/or exclude, NULL-aware).
    if isinstance(spec, dict):
        allowed_keys = {"include", "exclude", "keep_null"}
        unknown = set(spec) - allowed_keys
        if unknown:
            raise ValueError(
                f"value_filters entry has unknown key(s) {sorted(unknown)}; "
                f"allowed keys: {sorted(allowed_keys)}"
            )
        include = spec.get("include")
        include = None if include is None else list(include)
        exclude = list(spec.get("exclude", []) or [])
        # include present -> default drop NULL; include absent -> default keep NULL.
        keep_null = bool(spec.get("keep_null", include is None))
        return {"include": include, "exclude": exclude, "keep_null": keep_null}

    raise ValueError(
        "value_filters entry must be a list (include-only) or a dict with "
        f"include/exclude/keep_null keys; got {type(spec).__name__}: {spec!r}"
    )


def _apply_value_filter(df: DataFrame, dim: str, spec) -> DataFrame:
    """Filter ``df`` on one slice dimension per the normalised value_filters spec.

    A row is kept when EITHER

      * its value is NON-NULL and satisfies the include/exclude rules, OR
      * its value is NULL and ``keep_null`` is True.

    Spark ``isin`` / ``NOT isin`` both evaluate to NULL for a NULL cell, so the NULL
    bucket is handled explicitly here rather than left to SQL three-valued logic.

    :param df: frame carrying the ``dim`` column.
    :param dim: slice dimension column name.
    :param spec: raw ``value_filters[dim]`` value (list or dict).
    :return: the filtered frame.
    :rtype: DataFrame
    """
    norm = _normalize_value_filter(spec)
    col = F.col(dim)

    # Predicate applied to NON-NULL values only.
    value_match = F.lit(True)
    if norm["include"] is not None:
        value_match = value_match & col.isin(norm["include"])
    if norm["exclude"]:
        value_match = value_match & ~col.isin(norm["exclude"])

    keep = col.isNotNull() & value_match
    if norm["keep_null"]:
        keep = keep | col.isNull()
    return df.filter(keep)


def _filter_frames_for_dimension(
    frames: Dict[str, DataFrame], dim: str, value_filters: Dict[str, object]
) -> Dict[str, DataFrame]:
    """Restrict a slice's frames to the dimension values configured in ``value_filters``.

    Only a per-slice breakdown is filtered; the 'overall' totals never pass through here
    (build_kpi_long / comparable._comparable_period_rows call this only when a slice
    dimension is present). See ``_normalize_value_filter`` for the accepted shapes: a plain
    list (include-only) or a dict with include / exclude / keep_null.
    """
    if dim not in value_filters:
        return frames
    spec = value_filters[dim]
    out = dict(frames)
    for key in _VALUE_FILTERED_FRAMES:
        df = out.get(key)
        if df is None:
            continue
        out[key] = _apply_value_filter(df, dim, spec)
    return out


def build_kpi_long(ctx: KPIContext, frames: Dict[str, DataFrame]) -> pd.DataFrame:
    """Build kpi_long for overall + each active slice dimension across annual/quarter/monthly/weekly periods."""
    metric_cols = ctx.settings["METRIC_COLS"]
    slices: List[Tuple[str, List[str]]] = [("overall", [])] + [
        (dim, [dim]) for dim in ctx.active_slice_dimensions
    ]
    value_filters = ctx.settings.get("SLICE_VALUE_FILTERS", {}) or {}
    rows: List[dict] = []
    for period_name, period_col in PERIODS:
        pf = _period_frames(frames, period_name)
        for slice_name, gk in slices:
            sf = _filter_frames_for_dimension(pf, gk[0], value_filters) if gk else pf
            tbl = build_kpi_table(ctx, sf, period_col, gk)
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

"""YoY / YTD comparison tables (overall + slice dimensions) and defined-vs-score diff.

YoY compares the latest two full years present. YTD compares each year's elapsed
(fully-closed-quarters) window against the prior year's same window, chained across every
consecutive pair of years present (e.g. also 2025 YTD vs 2024 YTD if a third year exists).
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd

from kpi_pipeline.context import KPIContext

_DISTINCT_METRICS = frozenset({"distinct_product_count", "distinct_store_count", "distinct_pair_count"})
_FRACTIONAL_RATE_METRICS = frozenset({"in_stock_rate", "weighted_instock_rate"})


def _format_metric_value(metric: str, value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    if metric == "total_sales_revenue":
        return f"${value / 1e6:.1f}M"
    if metric == "total_sales_quantity":
        return f"{value / 1e6:.2f}M"
    if metric == "total_inventory":
        return f"{value / 1e6:.2f}M"
    if metric in ("mean_stock",):
        return f"{value / 1e6:.2f}M"
    if metric in ("mean_stock_retail", "mean_stock_cost"):
        return f"${value / 1e6:.1f}M"
    if metric in ("AUR", "AUC"):
        return f"${value:.2f}"
    if metric in _FRACTIONAL_RATE_METRICS:
        return f"{value * 100:.1f}%"
    if metric == "lost_sales_pct":
        return f"{value:.1f}%"
    if metric in _DISTINCT_METRICS:
        return f"{int(value):,}"
    if metric in ("WOS", "wos_revenue", "wos_cost", "inventory_turnover_rate"):
        return f"{value:.1f}"
    return f"{value:,.2f}"


def _format_change(metric: str, current, prior, pp_change_metrics) -> str:
    if current is None or prior is None or pd.isna(current) or pd.isna(prior):
        return "—"
    if metric in pp_change_metrics:
        delta_pp = (current - prior) * 100 if metric in _FRACTIONAL_RATE_METRICS else current - prior
        return f"{delta_pp:+.1f}pp"
    if prior == 0:
        return "—"
    return f"{(current - prior) / abs(prior) * 100:+.1f}%"


def _metric_change_values(metric: str, current, prior, pp_change_metrics):
    change_pct, change_pp = None, None
    if current is None or prior is None or pd.isna(current) or pd.isna(prior):
        return change_pct, change_pp
    if metric in pp_change_metrics:
        change_pp = (current - prior) * 100 if metric in _FRACTIONAL_RATE_METRICS else current - prior
    elif prior != 0:
        change_pct = (current - prior) / abs(prior) * 100
    return change_pct, change_pp


def build_comparison_long(
    ctx: KPIContext,
    prior_label: str,
    current_label: str,
    change_label: str,
    prior_values: Dict[str, object],
    current_values: Dict[str, object],
    metrics: Sequence[str],
    comparison_type: str,
    dimension: str,
    dimension_value: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    labels = ctx.settings["METRIC_LABELS"]
    pp_metrics = ctx.settings["PP_CHANGE_METRICS"]
    rows = []
    for metric in metrics:
        prior_v = prior_values.get(metric)
        current_v = current_values.get(metric)
        change_pct, change_pp = _metric_change_values(metric, current_v, prior_v, pp_metrics)
        rows.append(
            {
                "comparison_type": comparison_type,
                "dimension": dimension,
                "dimension_value": dimension_value,
                "KPI": labels.get(metric, metric),
                "metric_key": metric,
                "prior_period": prior_label,
                "current_period": current_label,
                "prior_value": prior_v,
                "current_value": current_v,
                "change_pct": change_pct,
                "change_pp": change_pp,
                "change_display": _format_change(metric, current_v, prior_v, pp_metrics),
                "prior_display": _format_metric_value(metric, prior_v),
                "current_display": _format_metric_value(metric, current_v),
            }
        )
    save_df = pd.DataFrame(rows)
    display_df = save_df[["KPI", "prior_display", "current_display", "change_display"]].rename(
        columns={
            "prior_display": prior_label,
            "current_display": current_label,
            "change_display": change_label,
        }
    )
    return display_df, save_df


def _comparison_dimensions(ctx: KPIContext) -> List[str]:
    return ["overall"] + list(ctx.active_slice_dimensions)


def _prepare_period_tables_for_slice(
    ctx: KPIContext, dimension: str
) -> Dict[str, pd.DataFrame]:
    metric_cols = ctx.settings["METRIC_COLS"]
    sub = ctx.kpi_long[ctx.kpi_long["dimension"] == dimension].copy()

    annual = sub[sub["period_type"] == "annual"].copy()
    annual["Year"] = annual["period"].astype(int)
    annual = annual[["Year", "dimension_value"] + metric_cols]

    ytd = sub[sub["period_type"] == "ytd"].copy()
    if not ytd.empty:
        ytd["Year"] = ytd["period"].str.replace("YTD-", "", regex=False).astype(int)
    else:
        ytd["Year"] = pd.Series(dtype=int)
    ytd = ytd[["Year", "dimension_value"] + metric_cols]

    return {"annual": annual, "ytd": ytd}


def _series_groups(df: pd.DataFrame, dimension: str) -> List[Tuple[str, pd.DataFrame]]:
    if df.empty:
        return []
    if dimension == "overall":
        return [("ALL", df)]
    groups = []
    for dval, grp in df.groupby("dimension_value", sort=False):
        groups.append((str(dval), grp))
    return groups


def _consecutive_year_pairs(df: pd.DataFrame) -> List[Tuple[pd.Series, pd.Series]]:
    """Every consecutive pair of years present in ``df`` (must have a ``Year`` column), sorted
    ascending. Empty if fewer than 2 years are present — the shared "gracefully skip" guard for
    a single-year window."""
    ordered = df.sort_values("Year").reset_index(drop=True)
    return [(ordered.iloc[i], ordered.iloc[i + 1]) for i in range(len(ordered) - 1)]


def _concat_pair_comparisons(
    pairs_with_display: List[Tuple[Any, Any, pd.DataFrame]]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Combine multiple (sort_key, display, save) comparison results into one (display, save).

    ``display`` returned is the LATEST pair's pivoted view (for at-a-glance inspection); the full
    multi-pair detail is in the concatenated ``save`` (long-format) table.
    """
    if not pairs_with_display:
        return pd.DataFrame(), pd.DataFrame()
    pairs_with_display.sort(key=lambda t: t[0])
    save_all = pd.concat([p[2] for p in pairs_with_display], ignore_index=True)
    display_last = pairs_with_display[-1][1]
    return display_last, save_all


def yoy_comparison_long(
    ctx: KPIContext,
    annual: pd.DataFrame,
    metrics: Sequence[str],
    dimension: str,
    dimension_value: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    annual = annual.sort_values("Year")
    if len(annual) < 2:
        return pd.DataFrame(), pd.DataFrame()
    prior, current = annual.iloc[-2], annual.iloc[-1]
    prior_label, current_label = str(int(prior["Year"])), str(int(current["Year"]))
    return build_comparison_long(
        ctx,
        prior_label,
        current_label,
        f"YoY {current_label}",
        prior.to_dict(),
        current.to_dict(),
        metrics,
        "yoy",
        dimension,
        dimension_value,
    )


def ytd_comparison_long(
    ctx: KPIContext,
    ytd: pd.DataFrame,
    metrics: Sequence[str],
    dimension: str,
    dimension_value: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Each year's elapsed (fully-closed-quarters) window vs the prior year's same window,
    chained across every consecutive pair of years present (2026 YTD vs 2025 YTD, 2025 YTD vs
    2024 YTD, ...). A single-year window produces no comparison."""
    pairs_with_display: List[Tuple[Any, pd.DataFrame, pd.DataFrame]] = []
    for prior, current in _consecutive_year_pairs(ytd):
        prior_label, current_label = f"{int(prior['Year'])} YTD", f"{int(current['Year'])} YTD"
        disp, save = build_comparison_long(
            ctx,
            prior_label,
            current_label,
            f"YTD {int(current['Year'])}",
            prior.to_dict(),
            current.to_dict(),
            metrics,
            "ytd",
            dimension,
            dimension_value,
        )
        if not save.empty:
            pairs_with_display.append((int(current["Year"]), disp, save))
    return _concat_pair_comparisons(pairs_with_display)


def _build_comparison_for_dimension(
    ctx: KPIContext,
    dimension: str,
    comparison_kind: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    metric_cols = ctx.settings["METRIC_COLS"]
    tables = _prepare_period_tables_for_slice(ctx, dimension)
    save_parts: List[pd.DataFrame] = []
    display_overall: pd.DataFrame = pd.DataFrame()

    if comparison_kind == "yoy":
        compare_fn = yoy_comparison_long
        period_df, key_cols = tables["annual"], ["Year"]
    else:
        compare_fn = ytd_comparison_long
        period_df, key_cols = tables["ytd"], ["Year"]

    for dval, grp in _series_groups(period_df, dimension):
        cols = key_cols + metric_cols
        display, save = compare_fn(ctx, grp[cols], metric_cols, dimension, dval)
        if not save.empty:
            save_parts.append(save)
        if dimension == "overall" and dval == "ALL" and not display.empty:
            display_overall = display

    save_all = pd.concat(save_parts, ignore_index=True) if save_parts else pd.DataFrame()
    return display_overall, save_all


# kind -> (save attribute on ctx, overall-display attribute on ctx)
_KIND_CTX_ATTRS = {
    "yoy": ("comparison_yoy", "yoy_display"),
    "ytd": ("comparison_ytd", "ytd_display"),
}


def _selected_comparison_kinds(ctx: KPIContext) -> List[str]:
    """Comparison kinds to build, in canonical order (defaults to both)."""
    selected = set(ctx.settings.get("COMPARISON_KINDS") or _KIND_CTX_ATTRS.keys())
    return [k for k in _KIND_CTX_ATTRS if k in selected]


def build_comparisons(ctx: KPIContext) -> None:
    kinds = _selected_comparison_kinds(ctx)

    # Reset every comparison table + overall display; unselected kinds stay empty so save,
    # HTML render, and notebook cells all treat them as "not requested".
    for save_attr, display_attr in _KIND_CTX_ATTRS.values():
        setattr(ctx, save_attr, pd.DataFrame())
        setattr(ctx, display_attr, pd.DataFrame())

    saves: Dict[str, List[pd.DataFrame]] = {k: [] for k in kinds}
    for dimension in _comparison_dimensions(ctx):
        for kind in kinds:
            disp, save = _build_comparison_for_dimension(ctx, dimension, kind)
            if dimension == "overall":
                setattr(ctx, _KIND_CTX_ATTRS[kind][1], disp)
            if not save.empty:
                saves[kind].append(save)

    for kind in kinds:
        setattr(
            ctx,
            _KIND_CTX_ATTRS[kind][0],
            pd.concat(saves[kind], ignore_index=True) if saves[kind] else pd.DataFrame(),
        )

    slice_dims = ctx.active_slice_dimensions
    print(
        "comparisons:",
        f"kinds={kinds}",
        f"YoY rows={len(ctx.comparison_yoy)}",
        f"YTD rows={len(ctx.comparison_ytd)}",
        "| slice dimensions:",
        slice_dims or "(none)",
    )


def slice_comparison_view(comparison_df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    """Readable view of comparisons for one slice dimension."""
    if comparison_df is None or comparison_df.empty:
        return pd.DataFrame()
    sub = comparison_df[comparison_df["dimension"] == dimension].copy()
    if sub.empty:
        return sub
    return sub[
        [
            "dimension_value",
            "KPI",
            "prior_period",
            "current_period",
            "prior_display",
            "current_display",
            "change_display",
        ]
    ].reset_index(drop=True)


def build_scope_diff(ctx: KPIContext) -> None:
    """Annual key-metric diff: defined-only scope vs score-only scope (hybrid sanity check)."""
    from kpi_pipeline.metrics import build_kpi_table

    scope_diff_metrics = ctx.settings["SCOPE_DIFF_METRICS"]
    defined_annual = build_kpi_table(ctx, ctx.defined_frames, "Year", [])[["Year"] + scope_diff_metrics]
    score_annual = build_kpi_table(ctx, ctx.score_frames, "Year", [])[["Year"] + scope_diff_metrics]
    merged = defined_annual.merge(score_annual, on="Year", suffixes=("_defined", "_score"))
    records = []
    for _, r in merged.iterrows():
        for m in scope_diff_metrics:
            dv, sv = r[f"{m}_defined"], r[f"{m}_score"]
            records.append(
                {
                    "Year": int(r["Year"]),
                    "metric": m,
                    "defined": dv,
                    "score": sv,
                    "abs_diff": (sv - dv) if pd.notna(sv) and pd.notna(dv) else None,
                    "pct_diff": ((sv - dv) / abs(dv) * 100) if pd.notna(sv) and pd.notna(dv) and dv != 0 else None,
                }
            )
    ctx.scope_diff = pd.DataFrame(
        records, columns=["Year", "metric", "defined", "score", "abs_diff", "pct_diff"]
    ).sort_values(["Year", "metric"]).reset_index(drop=True)

"""Render a standalone HTML KPI report from KPIContext.

Entry point
-----------
    render_kpi_html(ctx, output_path, ...)

Produces a self-contained, offline HTML file with:
  * Executive-style header (client, reporting window, scope, slices)
  * CSS-only major tabs: Annual / Quarter / Weekly / Metric Details
  * Within each period tab: dimension tabs (Overall + every slice column in data)
  * Within each slice dimension: vertical value tabs (one KPI panel per value)
  * Weekly tab columns limited to the most recent N fiscal weeks (configurable, default 5)
  * KPI tables with category row coloring (revenue / service / inventory / scale)
  * Comparison section per value panel (YoY / QoQ / WoW)
  * Metric Details tab: definition, store scope, and formula for every active metric

Slice dimensions and values are inferred from ``kpi_long`` — no hard-coded brand logic.
"""
from __future__ import annotations

import datetime
import html as _html
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Metric category → CSS class
# ---------------------------------------------------------------------------

_CAT: Dict[str, str] = {
    "total_sales_revenue": "revenue",
    "total_sales_quantity": "revenue",
    "AUR": "revenue",
    "AUC": "revenue",
    "in_stock_rate": "service",
    "weighted_instock_rate": "service",
    "lost_sales_pct": "service",
    "mean_stock": "inventory",
    "mean_stock_retail": "inventory",
    "mean_stock_cost": "inventory",
    "WOS": "inventory",
    "wos_revenue": "inventory",
    "wos_cost": "inventory",
    "inventory_turnover_rate": "inventory",
    "total_inventory": "inventory",
    "distinct_product_count": "scale",
    "distinct_store_count": "scale",
    "distinct_pair_count": "scale",
}


# ---------------------------------------------------------------------------
# Default metric definitions (label, definition, store scope, formula)
# ---------------------------------------------------------------------------

DEFAULT_METRIC_DEFINITIONS: Dict[str, Dict[str, str]] = {
    "total_sales_revenue": {
        "label": "Sales Revenue",
        "definition": "Total net sales revenue across all scoped stores for the period.",
        "store_scope": "All scoped stores",
        "formula": "Σ(daily_sales_revenue)",
    },
    "total_sales_quantity": {
        "label": "Sales Units",
        "definition": "Total units sold across all scoped stores for the period.",
        "store_scope": "All scoped stores",
        "formula": "Σ(daily_sales_quantity)",
    },
    "AUR": {
        "label": "AUR",
        "definition": "Average Unit Retail — average net selling price per unit in the period.",
        "store_scope": "All scoped stores",
        "formula": "Sales Revenue ÷ Sales Units",
    },
    "AUC": {
        "label": "AUC",
        "definition": "Average Unit Cost — average cost per unit sold in the period.",
        "store_scope": "All scoped stores",
        "formula": "Sales Cost ÷ Sales Units",
    },
    "total_inventory": {
        "label": "Total Inventory",
        "definition": "Sum of daily inventory units across the period for all scoped stores.",
        "store_scope": "All scoped stores",
        "formula": "Σ(daily inventory units)",
    },
    "distinct_product_count": {
        "label": "Distinct Products",
        "definition": "Count of unique product IDs present in scope during the period.",
        "store_scope": "All scoped stores",
        "formula": "COUNT DISTINCT product_id",
    },
    "distinct_store_count": {
        "label": "Distinct Stores",
        "definition": "Count of unique store IDs present in scope during the period.",
        "store_scope": "All scoped stores",
        "formula": "COUNT DISTINCT store_id",
    },
    "distinct_pair_count": {
        "label": "Distinct Pairs",
        "definition": "Count of unique product × store combinations in scope during the period.",
        "store_scope": "All scoped stores",
        "formula": "COUNT DISTINCT (product_id, store_id)",
    },
    "mean_stock": {
        "label": "Daily Stock Avg (units)",
        "definition": (
            "Average of daily total inventory units across service stores. "
            "Computed as the mean of each day's summed inventory — not the average of weekly averages."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "AVG over days of Σ_store(daily_inventory_units)",
    },
    "mean_stock_retail": {
        "label": "Daily Stock Avg Retail ($)",
        "definition": "Average of daily total inventory at retail price across service stores.",
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "AVG over days of Σ_store(daily_inventory × retail_price)",
    },
    "mean_stock_cost": {
        "label": "Daily Stock Avg Cost ($)",
        "definition": "Average of daily total inventory at cost across service stores.",
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "AVG over days of Σ_store(daily_inventory × cost)",
    },
    "WOS": {
        "label": "WOS (units)",
        "definition": (
            "Weeks of Supply based on units. Daily inventory and sales are summed across service "
            "stores to product×date, then weekly WOS = avg daily inventory ÷ weekly sales at "
            "product×fiscal week (not product×store×week). Sales-weighted rollup to the period — "
            "never computed directly at the period level."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "Σ(weekly_wos × weekly_sales) ÷ Σ(weekly_sales)",
    },
    "wos_revenue": {
        "label": "WOS Revenue",
        "definition": (
            "Weeks of Supply based on retail revenue. Service stores aggregated to product×date, "
            "then weekly WOS at product×fiscal week; same sales-weighted period rollup as WOS."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "Σ(weekly_wos_revenue × weekly_sales_revenue) ÷ Σ(weekly_sales_revenue)",
    },
    "wos_cost": {
        "label": "WOS Cost",
        "definition": (
            "Weeks of Supply based on cost. Service stores aggregated to product×date, "
            "then weekly WOS at product×fiscal week; same sales-weighted period rollup as WOS."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "Σ(weekly_wos_cost × weekly_sales_cost) ÷ Σ(weekly_sales_cost)",
    },
    "inventory_turnover_rate": {
        "label": "Inventory Turnover Rate",
        "definition": (
            "Rate at which inventory is sold and replaced over the reporting period in each tab — "
            "annual turnover in the Annual tab, quarterly turnover in the Quarter tab, and weekly "
            "turnover in the Weekly tab. Higher values indicate faster sell-through relative to "
            "the stock held during that period."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "Sales Units ÷ Mean Stock (for the same period grain)",
    },
    "in_stock_rate": {
        "label": "In-Stock Rate",
        "definition": (
            "Fraction of available store-days where the product was in stock, "
            "derived from the top-down lost-sales model output — not from daily inventory directly. "
            "100% = always in stock during tracked days."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "Σ(in_stock_days) ÷ Σ(available_days)",
    },
    "weighted_instock_rate": {
        "label": "Weighted In-Stock Rate",
        "definition": (
            "Sales-weighted in-stock rate. At product×store×week grain, weekly instock = "
            "in_stock_days ÷ available_days. Period rollup weights each week by its sales units — "
            "high-volume products and weeks have proportionally more impact than unweighted in-stock rate."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "Σ(weekly_instock_rate × weekly_sales_units) ÷ Σ(weekly_sales_units)",
    },
    "lost_sales_pct": {
        "label": "Lost Sales %",
        "definition": (
            "Estimated percentage of potential demand lost due to stockouts. "
            "The denominator uses corrected demand (actual sales + imputed lost demand) "
            "so the rate is not understated when in-stock days are low."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "100 × Σ(lost_sales) ÷ Σ(floor(weekly_sales + lost_sales))",
    },
}


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

_CSS_BASE = """\
  :root {
    --ink: #0c1222;
    --ink-soft: #3d4a63;
    --bg: #eef1f6;
    --surface: #ffffff;
    --text: #3d4a63;
    --muted: #6b7a94;
    --border: #dde3ed;
    --accent: #1a3a6b;
    --accent-light: #e8eef8;
    --good: #047857;
    --bad: #b91c1c;
    --neutral-bg: #f7f9fc;
    --header-bg: linear-gradient(135deg, #0c1a33 0%, #1a3a6b 100%);
    --radius: 10px;
    --shadow: 0 2px 8px rgba(12,18,34,.08);
  }
  *, *::before, *::after { box-sizing: border-box; }
  body {
    margin: 0;
    min-height: 100vh;
    background: var(--bg);
    color: var(--text);
    font-family: "Inter", ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
    font-feature-settings: "tnum" 1;
    -webkit-font-smoothing: antialiased;
  }
  .wrap { max-width: min(1720px, 100vw - 40px); margin: 0 auto; padding: 24px 20px 64px; }

  /* --- Executive header --- */
  .site-header {
    margin-bottom: 24px;
    padding: 28px 32px;
    background: var(--header-bg);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
    color: #fff;
  }
  .header-top {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    justify-content: space-between;
    gap: 16px 32px;
    margin-bottom: 22px;
  }
  .header-brand .eyebrow {
    margin: 0 0 6px;
    font-size: .7rem;
    font-weight: 600;
    letter-spacing: .14em;
    text-transform: uppercase;
    color: rgba(255,255,255,.65);
  }
  .site-header h1 {
    margin: 0;
    font-size: 1.65rem;
    font-weight: 700;
    letter-spacing: -.025em;
    color: #fff;
    line-height: 1.2;
  }
  .header-asof {
    text-align: right;
    font-size: .8rem;
    color: rgba(255,255,255,.75);
  }
  .header-asof strong {
    display: block;
    font-size: 1rem;
    color: #fff;
    margin-top: 2px;
  }
  .header-meta {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 12px;
  }
  .meta-card {
    padding: 12px 16px;
    background: rgba(255,255,255,.08);
    border: 1px solid rgba(255,255,255,.12);
    border-radius: 8px;
  }
  .meta-label {
    display: block;
    font-size: .65rem;
    font-weight: 600;
    letter-spacing: .08em;
    text-transform: uppercase;
    color: rgba(255,255,255,.55);
    margin-bottom: 4px;
  }
  .meta-value {
    display: block;
    font-size: .875rem;
    font-weight: 600;
    color: #fff;
    line-height: 1.35;
  }

  /* --- Main panel card --- */
  .panel {
    position: relative;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
    overflow: hidden;
    margin-bottom: 20px;
  }

  /* --- Hidden radio anchors --- */
  .top-tab-anchor,
  .dim-tab-anchor,
  .value-tab-anchor {
    position: absolute;
    opacity: 0;
    width: 0;
    height: 0;
    pointer-events: none;
  }

  /* --- Top-level period tabs --- */
  .top-tab-bar {
    display: flex;
    border-bottom: 1px solid var(--border);
    background: var(--neutral-bg);
  }
  .top-tab {
    flex: 1;
    padding: 13px 12px;
    text-align: center;
    font-size: .875rem;
    font-weight: 600;
    color: var(--muted);
    cursor: pointer;
    user-select: none;
    border-right: 1px solid var(--border);
    transition: color .12s, background .12s;
  }
  .top-tab:last-child { border-right: none; }
  .top-panel { display: none; padding: 22px 24px 28px; }

  /* --- Dimension tabs (horizontal) --- */
  .dim-tab-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-bottom: 20px;
    padding-bottom: 16px;
    border-bottom: 1px solid var(--border);
  }
  .dim-tab {
    padding: 7px 18px;
    border-radius: 6px;
    border: 1px solid var(--border);
    background: var(--neutral-bg);
    font-size: .8125rem;
    font-weight: 600;
    color: var(--muted);
    cursor: pointer;
    user-select: none;
    transition: background .12s, color .12s, border-color .12s;
  }
  .kpi-dim-panel { display: none; }

  /* --- Value tabs (vertical sidebar) --- */
  .value-shell { position: relative; }
  .value-layout {
    display: flex;
    gap: 0;
    min-height: 200px;
  }
  .value-tab-bar {
    display: flex;
    flex-direction: column;
    flex-shrink: 0;
    width: 168px;
    max-height: 520px;
    overflow-y: auto;
    border-right: 1px solid var(--border);
    background: var(--neutral-bg);
    border-radius: 6px 0 0 6px;
  }
  .value-tab {
    display: block;
    padding: 10px 14px;
    font-size: .8125rem;
    font-weight: 500;
    color: var(--ink-soft);
    cursor: pointer;
    user-select: none;
    border-bottom: 1px solid var(--border);
    transition: background .12s, color .12s;
    text-align: left;
    line-height: 1.3;
    word-break: break-word;
  }
  .value-tab:last-child { border-bottom: none; }
  .value-panels {
    flex: 1;
    min-width: 0;
    padding: 0 0 0 20px;
  }
  .value-panel { display: none; }

  /* --- KPI table --- */
  .table-wrap { overflow-x: auto; }
  table.kpi-table {
    width: max-content;
    min-width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: .875rem;
  }
  .kpi-table th,
  .kpi-table td {
    padding: 9px 13px;
    border-bottom: 1px solid #eef2f8;
    text-align: right;
    white-space: nowrap;
  }
  .kpi-table thead th {
    background: var(--accent-light);
    color: var(--accent);
    font-weight: 700;
    font-size: .73rem;
    letter-spacing: .04em;
    text-transform: uppercase;
  }
  .kpi-table .cell-kpi {
    text-align: left;
    min-width: 200px;
    max-width: 300px;
    white-space: normal;
    border-left: 4px solid transparent;
    padding-left: 14px;
  }
  .kpi-table thead .cell-kpi { border-left-color: transparent; }
  tbody tr:nth-child(even) td { background-color: #fafbfc; }

  tr.cat-revenue .cell-kpi  { border-left-color: #1e3a5f; }
  tr.cat-service .cell-kpi  { border-left-color: #0f766e; }
  tr.cat-inventory .cell-kpi { border-left-color: #9a3412; }
  tr.cat-scale .cell-kpi    { border-left-color: #6b21a8; }
  tr.cat-general .cell-kpi  { border-left-color: #475569; }

  /* --- Comparison table --- */
  .cmp-section { margin-top: 28px; }
  .cmp-label {
    margin: 0 0 10px;
    font-size: .73rem;
    font-weight: 700;
    letter-spacing: .06em;
    text-transform: uppercase;
    color: var(--muted);
  }
  table.cmp-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: .8125rem;
  }
  .cmp-table th,
  .cmp-table td {
    padding: 8px 13px;
    border-bottom: 1px solid #eef2f8;
  }
  .cmp-table thead th {
    background: var(--neutral-bg);
    color: var(--muted);
    font-weight: 600;
    font-size: .73rem;
    letter-spacing: .04em;
    text-transform: uppercase;
    text-align: left;
  }
  .cmp-table td { text-align: right; }
  .cmp-table td:first-child { text-align: left; }
  .chg-pos { color: var(--good); font-weight: 700; }
  .chg-neg { color: var(--bad);  font-weight: 700; }

  /* --- Metric definitions table --- */
  table.def-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: .8125rem;
  }
  .def-table th,
  .def-table td {
    padding: 10px 14px;
    border-bottom: 1px solid #eef2f8;
    vertical-align: top;
  }
  .def-table thead th {
    background: var(--accent-light);
    color: var(--accent);
    font-size: .73rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .04em;
    text-align: left;
  }
  .def-table td:first-child { font-weight: 600; white-space: nowrap; min-width: 160px; }
  .scope-badge {
    display: inline-block;
    padding: 2px 9px;
    border-radius: 20px;
    font-size: .7rem;
    font-weight: 600;
    white-space: nowrap;
  }
  .scope-all     { background: #eff6ff; color: #1e40af; }
  .scope-service { background: #ecfdf5; color: #065f46; }
  .def-table .formula {
    font-family: ui-monospace, "Cascadia Code", Menlo, monospace;
    font-size: .75rem;
    color: var(--muted);
  }

  .footnote {
    margin-top: 24px;
    padding-top: 12px;
    font-size: .8125rem;
    color: var(--muted);
    border-top: 1px solid var(--border);
  }

  @media print {
    body { background: #fff; }
    .site-header { background: #0c1a33; }
    .site-header, .panel { box-shadow: none; }
    .top-tab-anchor, .dim-tab-anchor, .value-tab-anchor { display: none; }
    .top-panel, .kpi-dim-panel, .value-panel { display: block !important; }
  }
"""


def _safe_id(*parts: str) -> str:
    """Build a CSS-safe id fragment from arbitrary strings."""
    raw = "-".join(str(p) for p in parts)
    return re.sub(r"[^a-zA-Z0-9_-]", "-", raw)


def _dim_label(dimension: str) -> str:
    if dimension == "overall":
        return "Overall"
    return dimension.replace("_", " ").title()


def _infer_dimensions(kpi_long: pd.DataFrame, configured_slices: List[str]) -> List[str]:
    """Return dimension tab order: overall first, then configured slices, then any extras in data."""
    data_dims = set(kpi_long["dimension"].unique())
    dims = ["overall"] if "overall" in data_dims else []
    for d in configured_slices:
        if d in data_dims and d not in dims:
            dims.append(d)
    for d in sorted(data_dims):
        if d != "overall" and d not in dims:
            dims.append(d)
    return dims or ["overall"]


def _dimension_values(
    kpi_long: pd.DataFrame,
    period_type: str,
    dimension: str,
) -> List[str]:
    sub = kpi_long[
        (kpi_long["period_type"] == period_type) & (kpi_long["dimension"] == dimension)
    ]
    if sub.empty:
        return []
    return sorted(sub["dimension_value"].unique().tolist(), key=lambda x: str(x))


def _tab_visibility_css(period_types: List[str], dims_by_period: Dict[str, List[str]], values_by_dim: Dict[str, List[str]]) -> str:
    """Generate CSS rules for three-level tab visibility."""
    lines: List[str] = []

    for pt in period_types:
        lines.append(
            f"  #kpi-tab-{_safe_id(pt)}:checked ~ .top-panels .top-panel-{_safe_id(pt)} {{ display: block; }}"
        )
        lines.append(
            f"  #kpi-tab-{_safe_id(pt)}:checked ~ .top-tab-bar label[for='kpi-tab-{_safe_id(pt)}'] "
            f"{{ background: var(--surface); color: var(--ink); "
            f"box-shadow: inset 0 -3px 0 var(--accent); }}"
        )

        dims = dims_by_period.get(pt, ["overall"])
        for di, dim in enumerate(dims):
            dim_id = _safe_id(pt, dim)
            lines.append(
                f"  #kpi-dim-{dim_id}:checked "
                f"~ .dim-panels-{_safe_id(pt)} .dim-panel-{dim_id} {{ display: block; }}"
            )
            lines.append(
                f"  #kpi-dim-{dim_id}:checked "
                f"~ .dim-tab-bar-{_safe_id(pt)} label[for='kpi-dim-{dim_id}'] "
                f"{{ background: var(--accent); color: #fff; border-color: var(--accent); }}"
            )

            if dim == "overall":
                continue
            val_key = f"{pt}|{dim}"
            for vi, _ in enumerate(values_by_dim.get(val_key, [])):
                val_id = _safe_id(pt, dim, str(vi))
                lines.append(
                    f"  #kpi-val-{val_id}:checked "
                    f"~ .value-layout .value-panel-{val_id} {{ display: block; }}"
                )
                lines.append(
                    f"  #kpi-val-{val_id}:checked "
                    f"~ .value-layout .value-tab-bar label[for='kpi-val-{val_id}'] "
                    f"{{ background: var(--accent-light); color: var(--accent); font-weight: 700; "
                    f"border-left: 3px solid var(--accent); }}"
                )

    lines.append(
        "  #kpi-tab-details:checked ~ .top-panels .top-panel-details { display: block; }"
    )
    lines.append(
        "  #kpi-tab-details:checked ~ .top-tab-bar label[for='kpi-tab-details'] "
        "{ background: var(--surface); color: var(--ink); "
        "box-shadow: inset 0 -3px 0 var(--accent); }"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Value formatting
# ---------------------------------------------------------------------------

def _fmt(metric: str, value: Any) -> str:
    if value is None:
        return "—"
    try:
        if isinstance(value, float) and pd.isna(value):
            return "—"
    except (TypeError, ValueError):
        pass
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    if metric == "total_sales_revenue":
        return f"${v / 1e6:.1f}M"
    if metric == "total_sales_quantity":
        return f"{v / 1e6:.2f}M"
    if metric == "total_inventory":
        return f"{v / 1e6:.2f}M"
    if metric == "mean_stock":
        return f"{v / 1e6:.2f}M"
    if metric in ("mean_stock_retail", "mean_stock_cost"):
        return f"${v / 1e6:.1f}M"
    if metric in ("AUR", "AUC"):
        return f"${v:.2f}"
    if metric in ("in_stock_rate", "weighted_instock_rate"):
        return f"{v * 100:.1f}%"
    if metric == "lost_sales_pct":
        return f"{v:.1f}%"
    if metric in ("distinct_product_count", "distinct_store_count", "distinct_pair_count"):
        return f"{int(v):,}"
    if metric in ("WOS", "wos_revenue", "wos_cost", "inventory_turnover_rate"):
        return f"{v:.1f}"
    return f"{v:,.2f}"


def _esc(text: Any) -> str:
    return _html.escape(str(text))


def _chg_class(change_str: str) -> str:
    s = (change_str or "").strip()
    if s.startswith("+"):
        return "chg-pos"
    if s.startswith("-"):
        return "chg-neg"
    return ""


# ---------------------------------------------------------------------------
# KPI table builder
# ---------------------------------------------------------------------------

def _sort_period_labels(
    periods: List[str],
    period_type: str,
    week_start_by_period: Optional[Dict[str, Any]] = None,
) -> List[str]:
    if period_type == "weekly" and week_start_by_period:
        return sorted(periods, key=lambda p: week_start_by_period.get(p, p))
    return sorted(periods)


def _display_period_labels(
    periods: List[str],
    period_type: str,
    week_start_by_period: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Return sorted period labels for HTML display (trim already applied to kpi_long at data level)."""
    return _sort_period_labels(periods, period_type, week_start_by_period)


def _pivot_single_value(
    kpi_long: pd.DataFrame,
    period_type: str,
    dimension: str,
    dimension_value: str,
    metric_cols: List[str],
    week_start_by_period: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    sub = kpi_long[
        (kpi_long["period_type"] == period_type)
        & (kpi_long["dimension"] == dimension)
        & (kpi_long["dimension_value"].astype(str) == str(dimension_value))
    ].copy()
    if sub.empty:
        return pd.DataFrame(), []
    periods = _display_period_labels(
        sub["period"].unique().tolist(),
        period_type,
        week_start_by_period,
    )
    sub = sub[sub["period"].isin(periods)]
    keep = ["period"] + [m for m in metric_cols if m in sub.columns]
    return sub[keep].drop_duplicates(subset=["period"]), periods


def _kpi_table_html(
    sub: pd.DataFrame,
    periods: List[str],
    metric_cols: List[str],
    labels: Dict[str, str],
    period_type: Optional[str] = None,
) -> str:
    if sub.empty or not periods:
        return '<p style="color:#64748b;font-size:.875rem">No data for this selection.</p>'

    grp = sub.drop_duplicates(subset=["period"]).set_index("period")
    per_ths = "".join(f"<th>{_esc(p)}</th>" for p in periods)
    head = f"<thead><tr><th class='cell-kpi'>Metric</th>{per_ths}</tr></thead>"

    rows: List[str] = []
    for metric in metric_cols:
        if metric not in grp.columns:
            continue
        cat = _CAT.get(metric, "general")
        label = _metric_display_label(metric, labels, period_type)
        cells = "".join(
            f"<td>{_esc(_fmt(metric, grp.at[p, metric]) if p in grp.index else '—')}</td>"
            for p in periods
        )
        rows.append(
            f"<tr class='cat-{_esc(cat)}'>"
            f"<td class='cell-kpi'>{_esc(label)}</td>"
            f"{cells}"
            f"</tr>"
        )

    return (
        "<div class='table-wrap'>"
        f"<table class='kpi-table'>{head}<tbody>{''.join(rows)}</tbody></table>"
        "</div>"
    )


def _comparison_html(
    comp_df: Optional[pd.DataFrame],
    dimension: str,
    dimension_value: str,
    comp_label: str,
    labels: Optional[Dict[str, str]] = None,
    period_type: Optional[str] = None,
) -> str:
    if comp_df is None or comp_df.empty:
        return ""
    sub = comp_df[
        (comp_df["dimension"] == dimension)
        & (comp_df["dimension_value"].astype(str) == str(dimension_value))
    ]
    if sub.empty:
        return ""

    first = sub.iloc[0]
    prior_col = str(first.get("prior_period", "Prior"))
    current_col = str(first.get("current_period", "Current"))

    head = (
        f"<thead><tr>"
        f"<th>Metric</th>"
        f"<th>{_esc(prior_col)}</th>"
        f"<th>{_esc(current_col)}</th>"
        f"<th>Change</th>"
        f"</tr></thead>"
    )

    rows: List[str] = []
    labels = labels or {}
    for _, row in sub.iterrows():
        chg = str(row.get("change_display", "—"))
        metric_key = row.get("metric_key")
        if metric_key and period_type:
            kpi_label = _metric_display_label(str(metric_key), labels, period_type)
        else:
            kpi_label = str(row.get("KPI", "—"))
        rows.append(
            f"<tr>"
            f"<td>{_esc(kpi_label)}</td>"
            f"<td>{_esc(str(row.get('prior_display', '—')))}</td>"
            f"<td>{_esc(str(row.get('current_display', '—')))}</td>"
            f"<td class='{_esc(_chg_class(chg))}'>{_esc(chg)}</td>"
            f"</tr>"
        )

    return (
        "<div class='cmp-section'>"
        f"<p class='cmp-label'>{_esc(comp_label)} Comparison</p>"
        "<div class='table-wrap'>"
        f"<table class='cmp-table'>{head}<tbody>{''.join(rows)}</tbody></table>"
        "</div>"
        "</div>"
    )


def _value_panel_content(
    kpi_long: pd.DataFrame,
    period_type: str,
    dimension: str,
    dimension_value: str,
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_df: Optional[pd.DataFrame],
    comp_label: str,
    week_start_by_period: Optional[Dict[str, Any]] = None,
    comparable_comp_df: Optional[pd.DataFrame] = None,
    comparable_label: str = "",
) -> str:
    sub, periods = _pivot_single_value(
        kpi_long, period_type, dimension, dimension_value, metric_cols,
        week_start_by_period,
    )
    table = _kpi_table_html(sub, periods, metric_cols, labels, period_type)
    cmp = _comparison_html(comp_df, dimension, dimension_value, comp_label, labels, period_type)
    comparable_cmp = _comparison_html(
        comparable_comp_df, dimension, dimension_value, comparable_label, labels, period_type
    )
    return table + cmp + comparable_cmp


def _value_tabs_html(
    kpi_long: pd.DataFrame,
    period_type: str,
    dimension: str,
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_df: Optional[pd.DataFrame],
    comp_label: str,
    week_start_by_period: Optional[Dict[str, Any]] = None,
    comparable_comp_df: Optional[pd.DataFrame] = None,
    comparable_label: str = "",
) -> str:
    values = _dimension_values(kpi_long, period_type, dimension)
    if not values:
        return '<p style="color:#64748b;font-size:.875rem">No values for this dimension.</p>'
    if len(values) == 1:
        return _value_panel_content(
            kpi_long, period_type, dimension, values[0],
            metric_cols, labels, comp_df, comp_label, week_start_by_period,
            comparable_comp_df, comparable_label,
        )

    radios = "".join(
        f"<input type='radio' class='value-tab-anchor' name='val-{_safe_id(period_type, dimension)}' "
        f"id='kpi-val-{_safe_id(period_type, dimension, str(i))}'{' checked' if i == 0 else ''}>"
        for i, _ in enumerate(values)
    )

    tab_labels = "".join(
        f"<label for='kpi-val-{_safe_id(period_type, dimension, str(i))}' class='value-tab'>"
        f"{_esc(str(v))}</label>"
        for i, v in enumerate(values)
    )

    panels = "".join(
        f"<div class='value-panel value-panel-{_safe_id(period_type, dimension, str(i))}'>"
        f"{_value_panel_content(kpi_long, period_type, dimension, v, metric_cols, labels, comp_df, comp_label, week_start_by_period, comparable_comp_df, comparable_label)}"
        f"</div>"
        for i, v in enumerate(values)
    )

    return (
        f"<div class='value-shell'>"
        f"{radios}"
        f"<div class='value-layout'>"
        f"<div class='value-tab-bar'>{tab_labels}</div>"
        f"<div class='value-panels'>{panels}</div>"
        f"</div>"
        f"</div>"
    )


def _period_tab_html(
    kpi_long: pd.DataFrame,
    period_type: str,
    dims: List[str],
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_df: Optional[pd.DataFrame],
    comp_label: str,
    week_start_by_period: Optional[Dict[str, Any]] = None,
    comparable_comp_df: Optional[pd.DataFrame] = None,
    comparable_label: str = "",
) -> str:
    pt_id = _safe_id(period_type)

    radios = "".join(
        f"<input type='radio' class='dim-tab-anchor' name='dim-{pt_id}' "
        f"id='kpi-dim-{_safe_id(period_type, dim)}'{' checked' if i == 0 else ''}>"
        for i, dim in enumerate(dims)
    )

    tab_labels = "".join(
        f"<label for='kpi-dim-{_safe_id(period_type, dim)}' class='dim-tab'>"
        f"{_esc(_dim_label(dim))}</label>"
        for dim in dims
    )
    tab_bar = f"<div class='dim-tab-bar dim-tab-bar-{pt_id}'>{tab_labels}</div>"

    panels: List[str] = []
    for dim in dims:
        dim_id = _safe_id(period_type, dim)
        if dim == "overall":
            values = _dimension_values(kpi_long, period_type, "overall")
            dval = values[0] if values else "overall"
            content = _value_panel_content(
                kpi_long, period_type, "overall", dval,
                metric_cols, labels, comp_df, comp_label, week_start_by_period,
                comparable_comp_df, comparable_label,
            )
        else:
            content = _value_tabs_html(
                kpi_long, period_type, dim, metric_cols, labels,
                comp_df, comp_label, week_start_by_period,
                comparable_comp_df, comparable_label,
            )
        panels.append(f"<div class='kpi-dim-panel dim-panel-{dim_id}'>{content}</div>")

    panels_wrap = f"<div class='dim-panels-{pt_id}'>{''.join(panels)}</div>"
    return radios + tab_bar + panels_wrap


def _metric_details_html(
    metric_cols: List[str],
    labels: Dict[str, str],
    defs: Dict[str, Dict[str, str]],
) -> str:
    head = (
        "<thead><tr>"
        "<th>Metric</th><th>Definition</th><th>Store Scope</th><th>Formula</th>"
        "</tr></thead>"
    )
    rows: List[str] = []
    for metric in metric_cols:
        d = defs.get(metric, {})
        label = d.get("label") or _metric_display_label(metric, labels)
        definition = d.get("definition", "—")
        scope_str = d.get("store_scope", "All scoped stores")
        formula = d.get("formula", "—")
        scope_cls = "scope-service" if "service" in scope_str.lower() else "scope-all"
        rows.append(
            f"<tr>"
            f"<td>{_esc(label)}</td>"
            f"<td>{_esc(definition)}</td>"
            f"<td><span class='scope-badge {scope_cls}'>{_esc(scope_str)}</span></td>"
            f"<td class='formula'>{_esc(formula)}</td>"
            f"</tr>"
        )

    return (
        "<div class='table-wrap'>"
        f"<table class='def-table'>{head}<tbody>{''.join(rows)}</tbody></table>"
        "</div>"
    )


def _report_info_html(
    settings: Dict[str, Any],
    active_slice_dimensions: Optional[List[str]] = None,
    inferred_dimensions: Optional[List[str]] = None,
) -> str:
    customer = settings.get("CUSTOMER", "—")
    as_of = settings.get("AS_OF_DATE", "—")
    report_start = settings.get("REPORT_START_DATE", "—")
    report_end = settings.get("REPORT_END_DATE", "—")
    scope_mode = (
        "Hybrid"
        if settings.get("USE_HYBRID_SCOPE")
        else "Defined only"
    )
    slice_dims = inferred_dimensions or active_slice_dimensions or settings.get("SLICE_DIMENSIONS") or []
    slice_labels = ", ".join(_dim_label(d) for d in slice_dims if d != "overall") or "Overall only"
    generated = datetime.datetime.now().strftime("%d %b %Y, %H:%M")

    return f"""<div class="header-top">
      <div class="header-brand">
        <p class="eyebrow">Retail Performance</p>
        <h1>{_esc(settings.get('HTML_REPORT_TITLE') or f'{str(customer).upper()} KPI Report')}</h1>
      </div>
      <div class="header-asof">
        Data as of
        <strong>{_esc(str(as_of))}</strong>
      </div>
    </div>
    <div class="header-meta">
      <div class="meta-card">
        <span class="meta-label">Reporting window</span>
        <span class="meta-value">{_esc(str(report_start))} – {_esc(str(report_end))}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Client</span>
        <span class="meta-value">{_esc(str(customer))}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Scope</span>
        <span class="meta-value">{_esc(scope_mode)}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Slice dimensions</span>
        <span class="meta-value">{_esc(slice_labels)}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Generated</span>
        <span class="meta-value">{_esc(generated)}</span>
      </div>
    </div>"""


_PERIOD_ORDER = ["annual", "quarter", "monthly", "weekly"]
_PERIOD_LABELS = {"annual": "Annual", "quarter": "Quarter", "monthly": "Monthly", "weekly": "Weekly"}
_PERIOD_COMP_LABEL = {"annual": "YoY", "quarter": "QoQ", "monthly": "MoM", "weekly": "WoW"}

_TURNOVER_PERIOD_LABELS = {
    "annual": "Annual Inventory Turnover Rate",
    "quarter": "Quarterly Inventory Turnover Rate",
    "monthly": "Monthly Inventory Turnover Rate",
    "weekly": "Weekly Inventory Turnover Rate",
}


def _metric_display_label(
    metric: str,
    labels: Dict[str, str],
    period_type: Optional[str] = None,
) -> str:
    """Return the HTML row label for a metric; turnover is period-specific per tab."""
    if metric == "inventory_turnover_rate" and period_type:
        return _TURNOVER_PERIOD_LABELS.get(
            period_type,
            labels.get(metric, "Inventory Turnover Rate"),
        )
    return labels.get(metric, metric.replace("_", " ").title())


def render_kpi_html(
    ctx: Any,
    output_path: "str | Path",
    *,
    report_title: Optional[str] = None,
    metric_definitions: Optional[Dict[str, Dict[str, str]]] = None,
) -> str:
    """
    Render a standalone, offline HTML KPI report from a completed KPIContext.

    Slice dimensions and values are inferred from ``ctx.kpi_long`` automatically.
    """
    kpi_long = ctx.kpi_long
    settings = ctx.settings

    if kpi_long is None or kpi_long.empty:
        raise ValueError(
            "ctx.kpi_long is empty — run the pipeline first (runner.run(...)) "
            "or load saved outputs (run.mode=html_only)."
        )

    defs: Dict[str, Dict[str, str]] = {
        **DEFAULT_METRIC_DEFINITIONS,
        **(metric_definitions or {}),
    }

    customer = settings.get("CUSTOMER", "")
    title = report_title or f"{customer.upper()} KPI Report"

    metric_cols: List[str] = [
        c for c in (settings.get("METRIC_COLS") or []) if c in kpi_long.columns
    ]
    labels: Dict[str, str] = settings.get("METRIC_LABELS") or {}

    configured_slices = list(settings.get("SLICE_DIMENSIONS") or [])
    active_dims = list(ctx.active_slice_dimensions or [])
    slice_source = active_dims or configured_slices
    dims = _infer_dimensions(kpi_long, slice_source)

    present_period_types = kpi_long["period_type"].unique().tolist()
    period_types = [pt for pt in _PERIOD_ORDER if pt in present_period_types]

    if not period_types:
        raise ValueError("kpi_long contains no recognised period_types (annual/quarter/monthly/weekly).")

    comp_map: Dict[str, Optional[pd.DataFrame]] = {
        "annual": ctx.comparison_yoy,
        "quarter": ctx.comparison_qoq,
        "monthly": getattr(ctx, "comparison_mom", None),
        "weekly": ctx.comparison_wow,
    }

    # Gated comparable (like-for-like) comparison tables — rendered as a second comparison table
    # per panel when comparable_pairs was enabled and data is present (else _comparison_html is a
    # no-op on the empty/None frame).
    comparable_comp_map: Dict[str, Optional[pd.DataFrame]] = {
        "annual": getattr(ctx, "comparable_comparison_yoy", None),
        "quarter": getattr(ctx, "comparable_comparison_qoq", None),
        "monthly": getattr(ctx, "comparable_comparison_mom", None),
        "weekly": getattr(ctx, "comparable_comparison_wow", None),
    }
    _COMPARABLE_LABELS = {"annual": "Comparable YoY", "quarter": "Comparable QoQ",
                          "monthly": "Comparable MoM", "weekly": "Comparable WoW"}

    week_start_by_period: Dict[str, Any] = {}
    if getattr(ctx, "fiscal_week", None) is not None:
        fw = ctx.fiscal_week.select("Year_Week", "week_start_date").distinct().toPandas()
        week_start_by_period = dict(zip(fw["Year_Week"], fw["week_start_date"].astype(str)))

    dims_by_period = {pt: dims for pt in period_types}
    values_by_dim: Dict[str, List[str]] = {}
    for pt in period_types:
        for dim in dims:
            if dim != "overall":
                values_by_dim[f"{pt}|{dim}"] = _dimension_values(kpi_long, pt, dim)

    dyn_css = _tab_visibility_css(period_types, dims_by_period, values_by_dim)

    top_radios = "".join(
        f"<input type='radio' class='top-tab-anchor' name='kpi-top' "
        f"id='kpi-tab-{_safe_id(pt)}'{' checked' if i == 0 else ''}>"
        for i, pt in enumerate(period_types)
    )
    top_radios += "<input type='radio' class='top-tab-anchor' name='kpi-top' id='kpi-tab-details'>"

    tab_labels = "".join(
        f"<label for='kpi-tab-{_safe_id(pt)}' class='top-tab'>{_esc(_PERIOD_LABELS.get(pt, pt.title()))}</label>"
        for pt in period_types
    )
    tab_labels += "<label for='kpi-tab-details' class='top-tab'>Metric Details</label>"
    top_tab_bar = f"<div class='top-tab-bar'>{tab_labels}</div>"

    period_panels = "".join(
        f"<div class='top-panel top-panel-{_safe_id(pt)}'>"
        f"{_period_tab_html(kpi_long, pt, dims, metric_cols, labels, comp_map.get(pt), _PERIOD_COMP_LABEL.get(pt, ''), week_start_by_period, comparable_comp_map.get(pt), _COMPARABLE_LABELS.get(pt, ''))}"
        f"</div>"
        for pt in period_types
    )

    details_panel = (
        f"<div class='top-panel top-panel-details'>"
        f"{_metric_details_html(metric_cols, labels, defs)}"
        f"</div>"
    )

    all_panels = f"<div class='top-panels'>{period_panels}{details_panel}</div>"

    main_panel = (
        f"<div class='panel'>"
        f"{top_radios}"
        f"{top_tab_bar}"
        f"{all_panels}"
        f"</div>"
    )

    slice_dim_labels = [d for d in dims if d != "overall"]
    report_info = _report_info_html(
        {**settings, "HTML_REPORT_TITLE": title},
        active_slice_dimensions=slice_dim_labels,
        inferred_dimensions=slice_dim_labels,
    )

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{_esc(title)}</title>
  <style>
{_CSS_BASE}
{dyn_css}
  </style>
</head>
<body>
  <div class="wrap">
    <header class="site-header">
      {report_info}
    </header>
    <main>
      {main_panel}
      <p class="footnote">
        Dollar values are in local currency.
        Service metrics (WOS, In-Stock Rate, Lost Sales %, Mean Stock) exclude e-com stores
        configured in <code>service_metrics.excluded_store_ids</code>.
      </p>
    </main>
  </div>
</body>
</html>"""

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html_doc, encoding="utf-8")
    print(f"HTML report written: {out}")
    return str(out)

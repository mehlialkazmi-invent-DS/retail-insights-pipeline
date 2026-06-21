"""Render a standalone HTML KPI report from KPIContext.

Entry point
-----------
    render_kpi_html(ctx, output_path, ...)

Produces a self-contained, offline HTML file with:
  * site-header with report-info chips (client, period, scope mode, generated timestamp)
  * CSS-only major tabs: Annual / Quarter / Weekly / Metric Details
  * Within each period tab: dim sub-tabs (Overall + each active slice dimension)
  * KPI tables with category row coloring (revenue / service / inventory / scale)
  * Comparison section per dim (YoY in Annual, QoQ in Quarter, WoW in Weekly)
  * Metric Details tab: definition, store scope, and formula for every active metric
"""
from __future__ import annotations

import datetime
import html as _html
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
    "in_stock_rate": "service",
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
            "Rate at which inventory is sold and replaced over the period. "
            "Higher values indicate faster sell-through relative to the stock held."
        ),
        "store_scope": "Service stores only (e-com excluded)",
        "formula": "Sales Units ÷ Mean Stock",
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
    --ink: #0f172a;
    --ink-soft: #334155;
    --bg: #f1f5f9;
    --surface: #ffffff;
    --text: #334155;
    --muted: #64748b;
    --border: #e2e8f0;
    --accent: #1e40af;
    --good: #047857;
    --bad: #b91c1c;
    --neutral-bg: #f8fafc;
    --radius: 8px;
    --shadow: 0 1px 3px rgba(15,23,42,.07);
  }
  *, *::before, *::after { box-sizing: border-box; }
  body {
    margin: 0;
    min-height: 100vh;
    background: var(--bg);
    color: var(--text);
    font-family: ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
    font-feature-settings: "tnum" 1;
    -webkit-font-smoothing: antialiased;
  }
  .wrap { max-width: min(1680px, 100vw - 32px); margin: 0 auto; padding: 20px 16px 56px; }

  /* --- Site header --- */
  .site-header {
    margin-bottom: 20px;
    padding: 18px 22px 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
  }
  .site-header h1 {
    margin: 0;
    font-size: 1.25rem;
    font-weight: 700;
    letter-spacing: -.02em;
    color: var(--ink);
  }

  /* --- Report info chips --- */
  .report-info {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 14px;
  }
  .info-chip {
    padding: 4px 11px;
    border-radius: 20px;
    border: 1px solid var(--border);
    background: var(--neutral-bg);
    font-size: .75rem;
    color: var(--ink-soft);
    white-space: nowrap;
  }
  .info-chip strong { color: var(--ink); }

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
  .dim-tab-anchor {
    position: absolute;
    opacity: 0;
    width: 0;
    height: 0;
    pointer-events: none;
  }

  /* --- Top-level tab bar (Annual / Quarter / Weekly / Metric Details) --- */
  .top-tab-bar {
    display: flex;
    border-bottom: 1px solid var(--border);
    background: var(--neutral-bg);
  }
  .top-tab {
    flex: 1;
    padding: 11px 10px;
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

  .top-panel { display: none; padding: 20px 22px 24px; }

  /* --- Dim sub-tab bar --- */
  .dim-tab-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 18px;
  }
  .dim-tab {
    padding: 6px 15px;
    border-radius: 20px;
    border: 1px solid var(--border);
    background: var(--neutral-bg);
    font-size: .8125rem;
    font-weight: 600;
    color: var(--muted);
    cursor: pointer;
    user-select: none;
    transition: background .12s, color .12s, border-color .12s;
  }

  /* --- Dim panels --- */
  .kpi-dim-panel { display: none; }

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
    background: #eef2ff;
    color: #1e3a8a;
    font-weight: 700;
    font-size: .75rem;
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

  /* KPI category row coloring */
  tr.cat-revenue .cell-kpi  { border-left-color: #1e3a5f; }
  tr.cat-service .cell-kpi  { border-left-color: #0f766e; }
  tr.cat-inventory .cell-kpi { border-left-color: #9a3412; }
  tr.cat-scale .cell-kpi    { border-left-color: #6b21a8; }
  tr.cat-general .cell-kpi  { border-left-color: #475569; }

  /* Dimension group header row inside KPI table */
  tr.group-header td {
    font-weight: 700;
    font-size: .8rem;
    color: var(--ink-soft);
    background: #f0f4ff;
    border-top: 2px solid #d5ddf5;
    padding: 6px 14px;
    text-align: left;
  }
  tr.group-header:first-child td { border-top: none; }

  /* --- Comparison table --- */
  .cmp-section { margin-top: 28px; }
  .cmp-label {
    margin: 0 0 10px;
    font-size: .75rem;
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
  .cmp-table .cell-dim {
    font-weight: 700;
    font-size: .8rem;
    color: var(--ink-soft);
    background: #f0f4ff;
    border-top: 2px solid #d5ddf5;
    text-align: left;
  }
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
    background: #eef2ff;
    color: #1e3a8a;
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
    font-family: ui-monospace,"Cascadia Code",Menlo,monospace;
    font-size: .75rem;
    color: var(--muted);
  }

  /* --- Footnote --- */
  .footnote {
    margin-top: 24px;
    padding-top: 12px;
    font-size: .8125rem;
    color: var(--muted);
    border-top: 1px solid var(--border);
  }

  @media print {
    body { background: #fff; }
    .site-header, .panel { box-shadow: none; }
    .top-tab-anchor, .dim-tab-anchor { display: none; }
    .top-panel, .kpi-dim-panel { display: block !important; }
  }
"""


def _tab_visibility_css(period_types: List[str], dims: List[str]) -> str:
    """Generate CSS rules for tab visibility based on the actual period types and dims present."""
    lines: List[str] = []

    # Top-level panel visibility
    for pt in period_types:
        lines.append(
            f"  #kpi-tab-{pt}:checked ~ .top-panels .top-panel-{pt} {{ display: block; }}"
        )
    lines.append(
        "  #kpi-tab-details:checked ~ .top-panels .top-panel-details { display: block; }"
    )

    # Top tab active label highlight
    for pt in period_types:
        lines.append(
            f"  #kpi-tab-{pt}:checked ~ .top-tab-bar label[for='kpi-tab-{pt}'] "
            f"{{ background: var(--surface); color: var(--ink); "
            f"box-shadow: inset 0 -3px 0 var(--accent); }}"
        )
    lines.append(
        "  #kpi-tab-details:checked ~ .top-tab-bar label[for='kpi-tab-details'] "
        "{ background: var(--surface); color: var(--ink); "
        "box-shadow: inset 0 -3px 0 var(--accent); }"
    )

    # Dim panel visibility and active label highlight per period
    for pt in period_types:
        for i in range(len(dims)):
            lines.append(
                f"  #kpi-dim-{pt}-{i}:checked "
                f"~ .dim-panels-{pt} .dim-panel-{pt}-{i} {{ display: block; }}"
            )
            lines.append(
                f"  #kpi-dim-{pt}-{i}:checked "
                f"~ .dim-tab-bar-{pt} label[for='kpi-dim-{pt}-{i}'] "
                f"{{ background: var(--accent); color: #fff; border-color: var(--accent); }}"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Value formatting
# ---------------------------------------------------------------------------

def _fmt(metric: str, value: Any) -> str:
    """Format a KPI value for display in the HTML table."""
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
    if metric == "AUR":
        return f"${v:.2f}"
    if metric == "in_stock_rate":
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
    """Sort period labels chronologically (weekly uses week_start_date, not Year_Week strings)."""
    if period_type == "weekly" and week_start_by_period:
        return sorted(periods, key=lambda p: week_start_by_period.get(p, p))
    return sorted(periods)


def _pivot_period(
    kpi_long: pd.DataFrame,
    period_type: str,
    dimension: str,
    metric_cols: List[str],
    week_start_by_period: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Filter kpi_long to (period_type, dimension) and return a tidy frame
    plus the sorted list of periods.
    """
    sub = kpi_long[
        (kpi_long["period_type"] == period_type)
        & (kpi_long["dimension"] == dimension)
    ].copy()
    if sub.empty:
        return pd.DataFrame(), []
    periods = _sort_period_labels(
        sub["period"].unique().tolist(),
        period_type,
        week_start_by_period,
    )
    keep = ["period", "dimension_value"] + [m for m in metric_cols if m in sub.columns]
    return sub[keep], periods


def _kpi_table_html(
    sub: pd.DataFrame,
    periods: List[str],
    metric_cols: List[str],
    labels: Dict[str, str],
) -> str:
    if sub.empty or not periods:
        return '<p style="color:#64748b;font-size:.875rem">No data for this period and dimension.</p>'

    dvals = sorted(sub["dimension_value"].unique().tolist(), key=lambda x: str(x))
    show_group_header = len(dvals) > 1

    per_ths = "".join(f"<th>{_esc(p)}</th>" for p in periods)
    head = f"<thead><tr><th class='cell-kpi'>Metric</th>{per_ths}</tr></thead>"

    rows: List[str] = []
    for dval in dvals:
        grp = (
            sub[sub["dimension_value"] == dval]
            .drop_duplicates(subset=["period"])
            .set_index("period")
        )
        if show_group_header:
            rows.append(
                f"<tr class='group-header'>"
                f"<td colspan='{1 + len(periods)}'>{_esc(str(dval))}</td>"
                f"</tr>"
            )
        for metric in metric_cols:
            if metric not in grp.columns:
                continue
            cat = _CAT.get(metric, "general")
            label = labels.get(metric, metric.replace("_", " ").title())
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


# ---------------------------------------------------------------------------
# Comparison section
# ---------------------------------------------------------------------------

def _comparison_html(comp_df: Optional[pd.DataFrame], dimension: str, comp_label: str) -> str:
    if comp_df is None or comp_df.empty:
        return ""
    sub = comp_df[comp_df["dimension"] == dimension]
    if sub.empty:
        return ""

    # Derive column headers from first row (consistent within one comparison run)
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

    dvals = sorted(sub["dimension_value"].unique().tolist(), key=lambda x: str(x))
    show_group_header = len(dvals) > 1

    rows: List[str] = []
    for dval in dvals:
        grp = sub[sub["dimension_value"] == dval]
        if show_group_header:
            rows.append(
                f"<tr><td colspan='4' class='cell-dim'>{_esc(str(dval))}</td></tr>"
            )
        for _, row in grp.iterrows():
            chg = str(row.get("change_display", "—"))
            rows.append(
                f"<tr>"
                f"<td>{_esc(str(row.get('KPI', '—')))}</td>"
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


# ---------------------------------------------------------------------------
# Dim panel content (KPI table + comparison)
# ---------------------------------------------------------------------------

def _dim_panel_content(
    kpi_long: pd.DataFrame,
    period_type: str,
    dimension: str,
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_df: Optional[pd.DataFrame],
    comp_label: str,
    week_start_by_period: Optional[Dict[str, Any]] = None,
) -> str:
    sub, periods = _pivot_period(
        kpi_long, period_type, dimension, metric_cols, week_start_by_period
    )
    table = _kpi_table_html(sub, periods, metric_cols, labels)
    cmp = _comparison_html(comp_df, dimension, comp_label)
    return table + cmp


# ---------------------------------------------------------------------------
# Period tab builder (dim sub-tabs + content)
# ---------------------------------------------------------------------------

def _period_tab_html(
    kpi_long: pd.DataFrame,
    period_type: str,
    dims: List[str],
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_df: Optional[pd.DataFrame],
    comp_label: str,
    week_start_by_period: Optional[Dict[str, Any]] = None,
) -> str:
    # Radio inputs for dim tabs (must precede tab bar and panels in DOM)
    radios = "".join(
        f"<input type='radio' class='dim-tab-anchor' name='dim-{_esc(period_type)}' "
        f"id='kpi-dim-{_esc(period_type)}-{i}'{' checked' if i == 0 else ''}>"
        for i, _ in enumerate(dims)
    )

    # Dim tab labels
    tab_labels = "".join(
        f"<label for='kpi-dim-{_esc(period_type)}-{i}' class='dim-tab'>"
        f"{_esc(dim.replace('_', ' ').title() if dim != 'overall' else 'Overall')}"
        f"</label>"
        for i, dim in enumerate(dims)
    )
    tab_bar = f"<div class='dim-tab-bar dim-tab-bar-{_esc(period_type)}'>{tab_labels}</div>"

    # Dim panels
    panels = "".join(
        f"<div class='kpi-dim-panel dim-panel-{_esc(period_type)}-{i}'>"
        f"{_dim_panel_content(kpi_long, period_type, dim, metric_cols, labels, comp_df, comp_label, week_start_by_period)}"
        f"</div>"
        for i, dim in enumerate(dims)
    )
    panels_wrap = f"<div class='dim-panels-{_esc(period_type)}'>{panels}</div>"

    return radios + tab_bar + panels_wrap


# ---------------------------------------------------------------------------
# Metric Details tab
# ---------------------------------------------------------------------------

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
        label = labels.get(metric, metric.replace("_", " ").title())
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


# ---------------------------------------------------------------------------
# Report header info chips
# ---------------------------------------------------------------------------

def _report_info_html(settings: Dict[str, Any], active_slice_dimensions: Optional[List[str]] = None) -> str:
    customer = settings.get("CUSTOMER", "—")
    as_of = settings.get("AS_OF_DATE", "—")
    report_start = settings.get("REPORT_START_DATE", "—")
    report_end = settings.get("REPORT_END_DATE", "—")
    scope_mode = (
        "Hybrid (defined + score backfill)"
        if settings.get("USE_HYBRID_SCOPE")
        else "Defined scope only"
    )
    slices = active_slice_dimensions if active_slice_dimensions is not None else (settings.get("SLICE_DIMENSIONS") or [])
    slices_str = ", ".join(slices) if slices else "none"
    generated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    chips = [
        ("Client", customer),
        ("Period", f"{report_start} → {report_end}"),
        ("As of date", str(as_of)),
        ("Scope mode", scope_mode),
        ("Slice dimensions", slices_str),
        ("Generated", generated),
    ]
    chip_html = "".join(
        f"<span class='info-chip'><strong>{_esc(k)}:</strong> {_esc(str(v))}</span>"
        for k, v in chips
    )
    return f"<div class='report-info'>{chip_html}</div>"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

_PERIOD_ORDER = ["annual", "quarter", "weekly"]
_PERIOD_LABELS = {"annual": "Annual", "quarter": "Quarter", "weekly": "Weekly"}
_PERIOD_COMP_LABEL = {"annual": "YoY", "quarter": "QoQ", "weekly": "WoW"}


def render_kpi_html(
    ctx: Any,
    output_path: "str | Path",
    *,
    report_title: Optional[str] = None,
    metric_definitions: Optional[Dict[str, Dict[str, str]]] = None,
) -> str:
    """
    Render a standalone, offline HTML KPI report from a completed KPIContext.

    Parameters
    ----------
    ctx:
        KPIContext with .kpi_long, .comparison_yoy/qoq/wow, .settings, and
        .active_slice_dimensions populated (i.e. runner.run() has completed).
    output_path:
        Path to write the HTML file. Parent directories are created if needed.
    report_title:
        Override the default "<Customer> KPI Report" title.
    metric_definitions:
        Override or extend DEFAULT_METRIC_DEFINITIONS.  Keys are metric column
        names; each value is a dict with keys: label, definition, store_scope, formula.

    Returns
    -------
    str — absolute path of the written file.
    """
    kpi_long = ctx.kpi_long
    settings = ctx.settings

    if kpi_long is None or kpi_long.empty:
        raise ValueError(
            "ctx.kpi_long is empty — run the pipeline first (runner.run(...))."
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
    active_dims: List[str] = list(ctx.active_slice_dimensions or [])
    dims: List[str] = ["overall"] + active_dims

    present_period_types = kpi_long["period_type"].unique().tolist()
    period_types = [pt for pt in _PERIOD_ORDER if pt in present_period_types]

    if not period_types:
        raise ValueError("kpi_long contains no recognised period_types (annual/quarter/weekly).")

    comp_map: Dict[str, Optional[pd.DataFrame]] = {
        "annual": ctx.comparison_yoy,
        "quarter": ctx.comparison_qoq,
        "weekly": ctx.comparison_wow,
    }

    week_start_by_period: Dict[str, Any] = {}
    if getattr(ctx, "fiscal_week", None) is not None:
        fw = ctx.fiscal_week.select("Year_Week", "week_start_date").distinct().toPandas()
        week_start_by_period = dict(zip(fw["Year_Week"], fw["week_start_date"]))

    # --- Build dynamic CSS (tab visibility rules) ---
    dyn_css = _tab_visibility_css(period_types, dims)

    # --- Radio inputs for top-level tabs (must precede tab bar and panels) ---
    top_radios = "".join(
        f"<input type='radio' class='top-tab-anchor' name='kpi-top' "
        f"id='kpi-tab-{pt}'{' checked' if i == 0 else ''}>"
        for i, pt in enumerate(period_types)
    )
    top_radios += (
        "<input type='radio' class='top-tab-anchor' name='kpi-top' id='kpi-tab-details'>"
    )

    # --- Top tab bar ---
    tab_labels = "".join(
        f"<label for='kpi-tab-{pt}' class='top-tab'>{_esc(_PERIOD_LABELS.get(pt, pt.title()))}</label>"
        for pt in period_types
    )
    tab_labels += "<label for='kpi-tab-details' class='top-tab'>Metric Details</label>"
    top_tab_bar = f"<div class='top-tab-bar'>{tab_labels}</div>"

    # --- Period panels ---
    period_panels = "".join(
        f"<div class='top-panel top-panel-{pt}'>"
        f"{_period_tab_html(kpi_long, pt, dims, metric_cols, labels, comp_map.get(pt), _PERIOD_COMP_LABEL.get(pt, ''), week_start_by_period)}"
        f"</div>"
        for pt in period_types
    )

    # --- Metric Details panel ---
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

    report_info = _report_info_html(settings, active_slice_dimensions=active_dims)

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
      <h1>{_esc(title)}</h1>
      {report_info}
    </header>
    <main>
      {main_panel}
      <p class="footnote">
        * Dollar values are in local currency.
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

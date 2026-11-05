"""Standalone offline HTML KPI report from a KPIContext (render_kpi_html).

Executive header; period tabs (Annual / YTD / Quarter / Half / Monthly / Weekly) and Metric Details; inside
each period tab, dimension tabs (Overall + each cut found in kpi_long) and vertical value tabs, every tab
label upper case (_tab_label). KPI tables are colored by metric category. Annual and YTD panels add the
YoY / YTD comparison; Quarter / Half / Monthly / Weekly show the most recent N periods' values only. An
outer root tab level appears with more than one root.
"""

from __future__ import annotations

import calendar
import datetime
import html as _html
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from kpi_pipeline.fiscal import last_saturday_on_or_before


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
    "dc_in_stock_rate": "service",
    "lost_sales_pct": "service",
    "mean_stock": "inventory",
    "mean_stock_retail": "inventory",
    "mean_stock_cost": "inventory",
    "dc_mean_stock": "inventory",
    "total_mean_stock": "inventory",
    "WOS": "inventory",
    "wos_revenue": "inventory",
    "wos_cost": "inventory",
    "WOS_DC": "inventory",
    "WOS_TOTAL": "inventory",
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
        "definition": (
            "Total sales revenue across all scoped stores for the period, net of returns. A product-store-day "
            "whose returns outweigh its sales is not counted."
        ),
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
            "Average of daily total inventory units across all scoped stores. "
            "Computed as the mean of each day's summed inventory — not the average of weekly averages."
        ),
        "store_scope": "All scoped stores",
        "formula": "AVG over days of Σ_store(daily_inventory_units)",
    },
    "mean_stock_retail": {
        "label": "Daily Stock Avg Retail ($)",
        "definition": "Average of daily total inventory at retail price across all scoped stores.",
        "store_scope": "All scoped stores",
        "formula": "AVG over days of Σ_store(daily_inventory × retail_price)",
    },
    "mean_stock_cost": {
        "label": "Daily Stock Avg Cost ($)",
        "definition": "Average of daily total inventory at cost across all scoped stores.",
        "store_scope": "All scoped stores",
        "formula": "AVG over days of Σ_store(daily_inventory × cost)",
    },
    "WOS": {
        "label": "WOS (units)",
        "definition": (
            "Weeks of Supply based on units. Daily inventory and sales are summed across all "
            "scoped stores to product×date, then weekly WOS = avg daily inventory ÷ weekly sales at "
            "product×fiscal week (not product×store×week). Sales-weighted rollup to the period — "
            "never computed directly at the period level."
        ),
        "store_scope": "All scoped stores",
        "formula": "Σ(weekly_wos × weekly_sales) ÷ Σ(weekly_sales)",
    },
    "dc_mean_stock": {
        "label": "Daily DC Stock Avg (M units)",
        "definition": (
            "Average of daily DC/warehouse inventory units, restricted to the same in-scope "
            "product population as every other metric in this report."
        ),
        "store_scope": "DC/warehouse only",
        "formula": "AVG over days of Σ_warehouse(daily_inventory_units)",
    },
    "total_mean_stock": {
        "label": "Daily Total Stock Avg (M units)",
        "definition": "Average of daily store + DC combined inventory units, for the in-scope product population.",
        "store_scope": "All scoped stores + DC/warehouse",
        "formula": "AVG over days of (Σ_store(daily_inventory_units) + Σ_warehouse(daily_inventory_units))",
    },
    "WOS_DC": {
        "label": "WOS (DC)",
        "definition": (
            "Weeks of Supply based on DC/warehouse inventory only. Same product×fiscal-week grain "
            "and sales-weighted period rollup as WOS (units) — weekly DC WOS = avg daily DC "
            "inventory ÷ weekly sales units, at product×fiscal week. Weeks with no DC record "
            "contribute 0 DC inventory, not a dropped week."
        ),
        "store_scope": "DC/warehouse only",
        "formula": "Σ(weekly_wos_dc × weekly_sales_units) ÷ Σ(weekly_sales_units)",
    },
    "WOS_TOTAL": {
        "label": "WOS (Total)",
        "definition": (
            "Weeks of Supply based on store + DC combined inventory. Same product×fiscal-week "
            "grain and sales-weighted period rollup as WOS (units); weekly total WOS = avg daily "
            "(store + DC) inventory ÷ weekly sales units."
        ),
        "store_scope": "All scoped stores + DC/warehouse",
        "formula": "Σ(weekly_wos_total × weekly_sales_units) ÷ Σ(weekly_sales_units)",
    },
    "dc_in_stock_rate": {
        "label": "DC In-Stock Rate",
        "definition": (
            "In-stock rate at DC/warehouse level, over a day-by-day grid for each product and warehouse. "
            "Each grid runs from the pair's first stocked day to the end of the report window, and every "
            "day without stock counts as a stockout, so a pair that stops being stocked keeps adding "
            "stockout days. A pair never stocked inside the window is not counted."
        ),
        "store_scope": "DC/warehouse only",
        "formula": "Σ(dc_stocked_days) ÷ Σ(dc_available_days)",
    },
    "wos_revenue": {
        "label": "WOS Revenue",
        "definition": (
            "Weeks of Supply based on retail revenue. All scoped stores aggregated to product×date, "
            "then weekly WOS at product×fiscal week; same sales-weighted period rollup as WOS."
        ),
        "store_scope": "All scoped stores",
        "formula": "Σ(weekly_wos_revenue × weekly_sales_revenue) ÷ Σ(weekly_sales_revenue)",
    },
    "wos_cost": {
        "label": "WOS Cost",
        "definition": (
            "Weeks of Supply based on cost. All scoped stores aggregated to product×date, "
            "then weekly WOS at product×fiscal week; same sales-weighted period rollup as WOS."
        ),
        "store_scope": "All scoped stores",
        "formula": "Σ(weekly_wos_cost × weekly_sales_cost) ÷ Σ(weekly_sales_cost)",
    },
    "inventory_turnover_rate": {
        "label": "Inventory Turnover Rate",
        "definition": (
            "Rate at which inventory is sold and replaced over the reporting period in each tab — "
            "labelled per-tab (Annual / YTD / Quarterly / Monthly / Weekly Inventory Turnover Rate). "
            "Higher values indicate faster sell-through relative to the stock held during that period."
        ),
        "store_scope": "All scoped stores",
        "formula": "Sales Units ÷ Mean Stock (for the same period grain)",
    },
    "in_stock_rate": {
        "label": "In-Stock Rate",
        "definition": (
            "Fraction of available store-days where the product was in stock, "
            "derived from the top-down lost-sales model output — not from daily inventory directly. "
            "100% = always in stock during tracked days."
        ),
        "store_scope": "All scoped stores",
        "formula": "Σ(in_stock_days) ÷ Σ(available_days)",
    },
    "weighted_instock_rate": {
        "label": "Weighted In-Stock Rate",
        "definition": (
            "Sales-weighted in-stock rate. At product×store×week grain, weekly instock = "
            "in_stock_days ÷ available_days. Period rollup weights each week by its sales units — "
            "high-volume products and weeks have proportionally more impact than unweighted in-stock rate."
        ),
        "store_scope": "All scoped stores",
        "formula": "Σ(weekly_instock_rate × weekly_sales_units) ÷ Σ(weekly_sales_units)",
    },
    "lost_sales_pct": {
        "label": "Lost Sales %",
        "definition": (
            "Estimated percentage of potential demand lost due to stockouts. "
            "The denominator uses corrected demand (actual sales + imputed lost demand) "
            "so the rate is not understated when in-stock days are low."
        ),
        "store_scope": "All scoped stores",
        "formula": "100 × Σ(lost_sales) ÷ Σ(floor(weekly_sales + lost_sales))",
    },
}


# Metrics that read the store blocked days (scoped daily-data rows, or the daily in-stock / lost-sales
# sales built from them): each notes the exclusion when it is in blocked_scope.metrics. The DC metrics
# (_DC_BLOCKED_DAY_METRICS) note DC blocked days; total_mean_stock / WOS_TOTAL / WOS_DC have both sides.
_BLOCKED_DAY_METRICS = (
    "total_sales_revenue", "total_sales_quantity", "AUR", "AUC", "distinct_product_count",
    "distinct_store_count", "distinct_pair_count", "total_inventory", "mean_stock", "mean_stock_retail",
    "mean_stock_cost", "total_mean_stock", "WOS", "wos_revenue", "wos_cost", "WOS_DC", "WOS_TOTAL",
    "inventory_turnover_rate", "weighted_instock_rate", "lost_sales_pct",
)
_DC_BLOCKED_DAY_METRICS = ("dc_mean_stock", "total_mean_stock", "WOS_DC", "WOS_TOTAL")


# Metrics that read daily-data's sales columns (the rest of them follow sales_basis): the Metric Details text of
# each notes the gross basis.
_SALES_BASIS_METRICS = (
    "AUR", "AUC", "WOS", "wos_revenue", "wos_cost", "WOS_DC", "WOS_TOTAL", "inventory_turnover_rate",
    "weighted_instock_rate", "lost_sales_pct",
)

# Inventory metrics that can count goods in transit (goods_in_transit.inventory_metrics) -> what they then count.
_GIT_METRIC_NOTES: Dict[str, str] = {
    **{m: "store" for m in (
        "total_inventory", "mean_stock", "mean_stock_retail", "mean_stock_cost",
        "WOS", "wos_revenue", "wos_cost", "inventory_turnover_rate",
    )},
    **{m: "DC" for m in ("dc_mean_stock", "WOS_DC")},
    **{m: "store and DC" for m in ("total_mean_stock", "WOS_TOTAL")},
}


def _settings_metric_definitions(settings: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Definition overrides that depend on the run's settings: the gross sales basis (sales_basis "gross"),
    the daily in-stock method (instock.method "daily"), the blocked-days note on the metrics named in
    blocked_scope.metrics, the goods-in-transit notes on the inventory metrics
    (goods_in_transit.inventory_metrics), the lost-sales period and WOS part-week notes (report_end
    "latest_day") and the DC goods-in-transit / DC blocked-days notes on DC In-Stock Rate (dc_instock)."""
    out: Dict[str, Dict[str, str]] = {}
    if settings["SALES_BASIS"] == "gross":
        filters = settings["INPUT_FILTERS"].get("transactional_sales") or []
        passing = (
            "every sales transaction except returns" if filters == ["sales_type != 'return'"]
            else "sales transactions matching " + (_plain_filters(filters) or "no filter")
        )
        out["total_sales_revenue"] = {
            **DEFAULT_METRIC_DEFINITIONS["total_sales_revenue"],
            "definition": (
                "Total gross sales revenue across all scoped stores for the period: " + passing
                + ", on every day it happened. Before returns."
            ),
            "formula": "Σ(daily gross sales revenue)",
        }
        out["total_sales_quantity"] = {
            **DEFAULT_METRIC_DEFINITIONS["total_sales_quantity"],
            "definition": (
                "Total gross units sold across all scoped stores for the period: " + passing
                + ", on every day it happened. Before returns."
            ),
            "formula": "Σ(daily gross sales quantity)",
        }
        for metric in _SALES_BASIS_METRICS:
            base = DEFAULT_METRIC_DEFINITIONS[metric]
            out[metric] = {**base, "definition": base["definition"] + " Sales are gross (before returns)."}
    blocked_metrics = settings["BLOCKED_SCOPE"]["metrics"]
    if settings["BLOCKED_SCOPE"]["path"] is not None:
        for metric in _BLOCKED_DAY_METRICS:
            if metric in blocked_metrics:
                base = out.get(metric, DEFAULT_METRIC_DEFINITIONS[metric])
                out[metric] = {
                    **base,
                    "definition": base["definition"] + " Days an item was blocked in the planning screens are excluded.",
                }
    if settings["BLOCKED_SCOPE"]["dc_solution_id"] is not None and settings["BLOCKED_SCOPE"]["dc_path"] is not None:
        for metric in _DC_BLOCKED_DAY_METRICS:
            if metric in blocked_metrics:
                base = out.get(metric, DEFAULT_METRIC_DEFINITIONS[metric])
                out[metric] = {
                    **base,
                    "definition": base["definition"] + " DC days blocked in the planning screens are excluded.",
                }
    goods_in_transit = settings["GOODS_IN_TRANSIT"]
    for metric in goods_in_transit["inventory_metrics"]:
        base = out.get(metric, DEFAULT_METRIC_DEFINITIONS[metric])
        out[metric] = {
            **base,
            "definition": base["definition"]
            + f" Inventory counts {_GIT_METRIC_NOTES[metric]} goods in transit on top of on-hand.",
        }
    if settings["REPORT_END_MODE"] == "latest_day":
        last_saturday = last_saturday_on_or_before(settings["REPORT_END_DATE"])
        base = out.get("lost_sales_pct", DEFAULT_METRIC_DEFINITIONS["lost_sales_pct"])
        out["lost_sales_pct"] = {
            **base,
            "definition": base["definition"]
            + f" Lost sales only has data through the last complete Saturday ({last_saturday}): every tab uses "
            "whole weeks up to it, and YTD uses weeks 1 to that Saturday's fiscal week of every year, so the "
            "days after it never dilute the rate.",
        }
        # YTD stops on the latest day, mid-week: its last fiscal week counts only the days it covers.
        for metric in ("WOS", "wos_revenue", "wos_cost", "WOS_DC", "WOS_TOTAL"):
            base = out.get(metric, DEFAULT_METRIC_DEFINITIONS[metric])
            out[metric] = {
                **base,
                "definition": base["definition"]
                + " In YTD, which ends on the latest day, the last fiscal week counts only its elapsed days: "
                "its average inventory is weighted by elapsed days / 7.",
            }
    cfg = settings["INSTOCK_DAILY"]
    if settings["INSTOCK_METHOD"] == "daily":
        in_stock = "stock on hand"
        if cfg["sales_counts_as_stocked"]:
            in_stock += ", or the item sold that day"
        if goods_in_transit["store_instock"]:
            in_stock += ", or stock on its way to the store"
        blocked_excluded = settings["BLOCKED_SCOPE"]["path"] is not None and "in_stock_rate" in blocked_metrics
        excluded = " and ".join(
            (["Blocked days"] if blocked_excluded else [])
            + (["unusable days"] if cfg["usable_only"] else [])
        )
        excluded = excluded[:1].upper() + excluded[1:]
        store_scope = "All scoped stores"
        if cfg["input_filters"]:
            store_scope += " (" + _plain_filters(cfg["input_filters"]) + ")"
        if settings["SCOPE"]["instock_main_eligible_only"]:
            store_scope += "; only stores where the main (new) item itself is eligible"
        if settings["SCOPE"]["instock_exclude_unsuperseded_sizes"]:
            store_scope += "; sizes of a superseded class color that are not in the supersession are excluded"
        out["in_stock_rate"] = {
            "label": "In-Stock Rate",
            "definition": (
                f"Share of counted store-days on which the product was in stock ({in_stock}), "
                "from daily inventory. Each product-store pair counts from its count start; a day "
                "without a daily record is out of stock."
                + (f" {excluded} are excluded." if excluded else "")
            ),
            "store_scope": store_scope,
            "formula": "Σ(in_stock_days) ÷ Σ(available_days)",
        }
    if settings["DC_INSTOCK_ENABLED"]:
        dc_notes = []
        if goods_in_transit["dc_instock"]:
            dc_notes.append("A day with goods in transit to the DC also counts as stocked.")
        if settings["BLOCKED_SCOPE"]["dc_solution_id"] is not None and settings["BLOCKED_SCOPE"]["dc_path"] is not None and "dc_in_stock_rate" in blocked_metrics:
            dc_notes.append("DC days blocked in the planning screens are excluded.")
        if dc_notes:
            base = DEFAULT_METRIC_DEFINITIONS["dc_in_stock_rate"]
            out["dc_in_stock_rate"] = {**base, "definition": base["definition"] + " " + " ".join(dc_notes)}
    return out


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
    text-align: center;
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
    text-align: center;
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
    text-align: center;
  }
  .cmp-table td { text-align: center; }
  .chg-pos { color: var(--good); font-weight: 700; }
  .chg-neg { color: var(--bad);  font-weight: 700; }

  /* --- Comparable (like-for-like) group divider: separates it from the value trend / regular
     comparison tables above it, especially now that "quarter" / "half" render one sub-table per
     period number. --- */
  .cmp-group-divider {
    margin-top: 30px;
    padding-top: 16px;
    border-top: 2px solid var(--border);
  }
  .cmp-group-label {
    margin: 0 0 12px;
    font-size: .8rem;
    font-weight: 800;
    color: var(--ink);
  }

  /* --- One like-for-like block per quarter / half number: heading, then a rule and extra space
     between consecutive blocks so Q1 / Q2 / ... (H1 / H2) read as separate tables. --- */
  .cmp-period-block .cmp-section { margin-top: 12px; }
  .cmp-period-block + .cmp-period-block {
    margin-top: 36px;
    padding-top: 22px;
    border-top: 2px solid var(--border);
  }
  .cmp-period-heading {
    margin: 0 0 4px;
    font-size: .95rem;
    font-weight: 800;
    letter-spacing: 0;
    text-transform: none;
    color: var(--ink);
  }

  /* --- Metric definitions table --- */
  table.def-table {
    width: 100%;
    table-layout: fixed;
    border-collapse: separate;
    border-spacing: 0;
    font-size: .8125rem;
  }
  .def-table th,
  .def-table td {
    padding: 14px 16px;
    border-bottom: 1px solid #eef2f8;
    vertical-align: middle;
    text-align: center;
    line-height: 1.5;
    overflow-wrap: break-word;
  }
  .def-table th:nth-child(1) { width: 14%; }
  .def-table th:nth-child(2) { width: 41%; }
  .def-table th:nth-child(3) { width: 19%; }
  .def-table th:nth-child(4) { width: 26%; }
  .def-table thead th {
    background: transparent;
    color: var(--muted);
    font-size: .7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .08em;
    border-bottom: 2px solid var(--border);
  }
  .def-table tbody tr:hover { background: #f7f9fd; }
  .def-table td:first-child { font-weight: 700; color: var(--ink); }
  .def-table td:nth-child(2) { color: var(--ink-soft); }
  .scope-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 14px;
    font-size: .7rem;
    font-weight: 600;
    line-height: 1.4;
  }
  .scope-all     { background: #eff6ff; color: #1e40af; }
  .scope-service { background: #ecfdf5; color: #065f46; }
  .def-table .formula {
    font-family: ui-monospace, "Cascadia Code", Menlo, monospace;
    font-size: .75rem;
    color: var(--accent);
  }

  /* --- Methodology tab --- */
  .method-lead { margin: 4px 0 18px; font-size: .95rem; color: var(--ink-soft); }
  .method-grid { columns: 4 270px; column-gap: 14px; margin-bottom: 30px; }
  .method-card {
    display: inline-block;
    width: 100%;
    margin: 0 0 14px;
    break-inside: avoid;
    background: var(--surface);
    border: 1px solid var(--border);
    border-top: 3px solid var(--accent);
    border-radius: 10px;
    padding: 14px 18px 12px;
  }
  .method-card h4 {
    margin: 0 0 8px;
    font-size: .72rem;
    font-weight: 800;
    letter-spacing: .08em;
    text-transform: uppercase;
    color: var(--accent);
  }
  .method-card ul { margin: 0; padding-left: 17px; }
  .method-card li { margin: 0 0 6px; font-size: .84rem; line-height: 1.5; color: var(--ink-soft); }
  .method-heading {
    margin: 0 0 8px;
    font-size: .72rem;
    font-weight: 800;
    letter-spacing: .08em;
    text-transform: uppercase;
    color: var(--muted);
  }
  .method-heading-gap { margin-top: 30px; }
  .method-path { margin-left: 10px; font-weight: 600; letter-spacing: 0; text-transform: none; color: var(--ink-soft); }
  .method-code {
    display: inline-block;
    padding: 1px 8px;
    border-radius: 6px;
    background: #f4f7fb;
    color: var(--ink-soft);
    font-size: .76rem;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  }
  table.method-table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: .8125rem; }
  .method-table th, .method-table td { padding: 10px 14px; border-bottom: 1px solid #eef2f8; text-align: center; vertical-align: middle; line-height: 1.45; }
  .method-table thead th {
    color: var(--muted);
    font-size: .7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .08em;
    border-bottom: 2px solid var(--border);
  }
  .method-table td:first-child { font-weight: 600; color: var(--ink); }
  .method-table td { color: var(--ink-soft); }
  .method-table td:first-child { color: var(--ink); }
  .method-table td div { margin: 2px 0; }
  .method-table .method-group td {
    background: var(--accent-light);
    color: var(--accent);
    font-size: .7rem;
    font-weight: 800;
    letter-spacing: .08em;
    text-transform: uppercase;
    padding: 6px 16px;
  }
  .method-pill {
    display: inline-block;
    min-width: 44px;
    padding: 2px 12px;
    border-radius: 14px;
    font-size: .72rem;
    font-weight: 700;
  }
  .method-pill.on { background: var(--accent-light); color: var(--accent); }
  .method-pill.off { background: #f1f3f7; color: var(--muted); }

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


def _capitalize_words(text: Any) -> str:
    """Capitalize each all-lowercase word; words with capitals (LFL, YTD, SMW) stay as they are."""
    return " ".join(w[:1].upper() + w[1:] if w.islower() else w for w in str(text).split(" "))


def _tab_label(text: Any) -> str:
    """A tab label (root, period, dimension, value or Metric Details tab) in upper case: OVERALL, ANNUAL,
    BANNER, LFL, METRIC DETAILS. Display only."""
    return str(text).upper()


def _root_display_label(root: str, root_display_labels: Dict[str, str]) -> str:
    return root_display_labels.get(root, "Overall" if root == "overall" else root)


def _safe_id(*parts: str) -> str:
    """Build a CSS-safe id fragment from arbitrary strings."""
    raw = "-".join(str(p) for p in parts)
    return re.sub(r"[^a-zA-Z0-9_-]", "-", raw)


def _dim_label(dimension: str, dimension_labels: Dict[str, str]) -> str:
    if dimension in dimension_labels:
        return dimension_labels[dimension]
    if dimension == "overall":
        return "Overall"
    return _capitalize_words(dimension.replace("_", " "))


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


def _tab_visibility_css(
    period_types: List[str],
    dims_by_period: Dict[str, List[str]],
    values_by_dim: Dict[str, List[str]],
    root: str = "overall",
) -> str:
    """Generate CSS rules for three-level tab visibility. `root` namespaces every generated id so
    multiple roots' tab groups (see _render_root_period_tabs) don't collide on one page."""
    lines: List[str] = []

    for pt in period_types:
        lines.append(
            f"  #kpi-tab-{_safe_id(root, pt)}:checked ~ .top-panels .top-panel-{_safe_id(root, pt)} {{ display: block; }}"
        )
        lines.append(
            f"  #kpi-tab-{_safe_id(root, pt)}:checked ~ .top-tab-bar label[for='kpi-tab-{_safe_id(root, pt)}'] "
            f"{{ background: var(--surface); color: var(--ink); "
            f"box-shadow: inset 0 -3px 0 var(--accent); }}"
        )

        dims = dims_by_period.get(pt, ["overall"])
        for di, dim in enumerate(dims):
            dim_id = _safe_id(root, pt, dim)
            lines.append(
                f"  #kpi-dim-{dim_id}:checked "
                f"~ .dim-panels-{_safe_id(root, pt)} .dim-panel-{dim_id} {{ display: block; }}"
            )
            lines.append(
                f"  #kpi-dim-{dim_id}:checked "
                f"~ .dim-tab-bar-{_safe_id(root, pt)} label[for='kpi-dim-{dim_id}'] "
                f"{{ background: var(--accent); color: #fff; border-color: var(--accent); }}"
            )

            if dim == "overall":
                continue
            val_key = f"{pt}|{dim}"
            for vi, _ in enumerate(values_by_dim.get(val_key, [])):
                val_id = _safe_id(root, pt, dim, str(vi))
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

    return "\n".join(lines)


# "Methodology" and "Metric Details" are root-independent: tabs that are peers of the outermost tab level (the
# period tabs with one root, the root tabs with several).
def _extra_tab_css(tab_id: str) -> str:
    return (
        f"  #kpi-tab-{tab_id}:checked ~ .top-panels .top-panel-{tab_id} {{ display: block; }}\n"
        f"  #kpi-tab-{tab_id}:checked ~ .top-tab-bar label[for='kpi-tab-{tab_id}'] "
        "{ background: var(--surface); color: var(--ink); box-shadow: inset 0 -3px 0 var(--accent); }"
    )


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
    if metric in ("mean_stock", "dc_mean_stock", "total_mean_stock"):
        return f"{v / 1e6:.2f}M"
    if metric in ("mean_stock_retail", "mean_stock_cost"):
        return f"${v / 1e6:.1f}M"
    if metric in ("AUR", "AUC"):
        return f"${v:.2f}"
    if metric in ("in_stock_rate", "weighted_instock_rate", "dc_in_stock_rate"):
        return f"{v * 100:.1f}%"
    if metric == "lost_sales_pct":
        return f"{v:.1f}%"
    if metric in ("distinct_product_count", "distinct_store_count", "distinct_pair_count"):
        return f"{int(v):,}"
    if metric in ("WOS", "wos_revenue", "wos_cost", "WOS_DC", "WOS_TOTAL"):
        return f"{math.floor(v)}"
    if metric == "inventory_turnover_rate":
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


def _month_labels_from_fiscal_cal(ctx: Any) -> Dict[str, str]:
    """{"YYYY-MM": label} from the fiscal_cal upload's month-name column (fiscal_calendar.month_name_col),
    verbatim, for the periods where it is filled; _derive_month_display_labels covers the rest."""
    fw = ctx.fiscal_week
    if "Fiscal_Month_Name" not in fw.columns:
        return {}
    fw_pd = fw.select("Year", "Fiscal_Month", "Fiscal_Month_Name").dropna(subset=["Fiscal_Month_Name"]).distinct().toPandas()
    if fw_pd.empty:
        return {}
    # One name per (Year, Fiscal_Month), even if the upload is inconsistent across its weeks.
    fw_pd = fw_pd.drop_duplicates(subset=["Year", "Fiscal_Month"])
    out: Dict[str, str] = {}
    for row in fw_pd.itertuples(index=False):
        key = f"{int(row.Year)}-{int(row.Fiscal_Month):02d}"
        out[key] = f"{int(row.Year)}-{row.Fiscal_Month_Name}"
    return out


def _derive_month_display_labels(ctx: Any) -> Dict[str, str]:
    """{"YYYY-MM": label} naming each fiscal month after the real calendar month holding most of its days:
    a fiscal month number need not match the calendar month (tbretail's fiscal month 07 spanned 8/2-8/29,
    real August). On the civil calendar Fiscal_Month already is the calendar month."""
    fw_pd = ctx.fiscal_week.select("Year", "Week", "Fiscal_Month").distinct().toPandas()
    fc_pd = ctx.fiscal_cal.select("Year", "Week", "date").toPandas()
    fc_pd["calendar_month"] = pd.to_datetime(fc_pd["date"]).dt.month
    merged = fc_pd.merge(fw_pd, on=["Year", "Week"], how="inner")
    if merged.empty:
        return {}
    day_counts = (
        merged.groupby(["Year", "Fiscal_Month", "calendar_month"]).size().reset_index(name="n_days")
    )
    majority = day_counts.sort_values("n_days", ascending=False).drop_duplicates(
        subset=["Year", "Fiscal_Month"]
    )
    out: Dict[str, str] = {}
    for row in majority.itertuples(index=False):
        key = f"{int(row.Year)}-{int(row.Fiscal_Month):02d}"
        out[key] = f"{int(row.Year)}-{calendar.month_abbr[int(row.calendar_month)]}"
    return out


def _build_month_display_labels(ctx: Any) -> Dict[str, str]:
    """{"YYYY-MM": Monthly-tab label}: the upload's own month name where present, else the derived one."""
    from_source = _month_labels_from_fiscal_cal(ctx)
    derived = _derive_month_display_labels(ctx)
    return {**derived, **from_source}


def _period_th_label(
    period: str,
    period_type: Optional[str],
    month_display_by_period: Optional[Dict[str, str]] = None,
) -> str:
    """Column-header text of one period. Display only: ``period`` stays the sortable "YYYY-MM" fiscal key
    everywhere else. A monthly header comes from month_display_by_period (actual dates), never from the
    fiscal month number; an unmapped period shows its raw key."""
    if period_type == "monthly":
        mapped = (month_display_by_period or {}).get(str(period))
        if mapped:
            return mapped
    return period


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
    periods = _sort_period_labels(
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
    month_display_by_period: Optional[Dict[str, str]] = None,
) -> str:
    if sub.empty or not periods:
        return '<p style="color:#64748b;font-size:.875rem">No data for this selection.</p>'

    grp = sub.drop_duplicates(subset=["period"]).set_index("period")
    per_ths = "".join(
        f"<th>{_esc(_period_th_label(p, period_type, month_display_by_period))}</th>" for p in periods
    )
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


def _comparison_wide_html(
    value_sub: pd.DataFrame,
    periods: List[str],
    comp_df: Optional[pd.DataFrame],
    dimension: str,
    dimension_value: str,
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_label: str,
    period_type: Optional[str] = None,
    month_display_by_period: Optional[Dict[str, str]] = None,
) -> str:
    """One YoY / YTD table: the panel's value columns (``value_sub`` / ``periods``, already pivoted) plus one
    delta column per consecutive-period link in ``comp_df``; "" without a comparison for this value."""
    if comp_df is None or comp_df.empty or value_sub.empty or not periods:
        return ""
    comp_sub = comp_df[
        (comp_df["dimension"] == dimension)
        & (comp_df["dimension_value"].astype(str) == str(dimension_value))
    ]
    if comp_sub.empty:
        return ""

    value_grp = value_sub.set_index("period")

    links = comp_sub[["prior_period", "current_period"]].drop_duplicates().sort_values("current_period")
    link_pairs = list(zip(links["prior_period"], links["current_period"]))

    value_ths = "".join(
        f"<th>{_esc(_period_th_label(p, period_type, month_display_by_period))}</th>" for p in periods
    )
    delta_ths = "".join(f"<th>&Delta; {_esc(current)}</th>" for _, current in link_pairs)
    head = f"<thead><tr><th class='cell-kpi'>Metric</th>{value_ths}{delta_ths}</tr></thead>"

    rows: List[str] = []
    for metric in metric_cols:
        if metric not in value_grp.columns:
            continue
        metric_rows = comp_sub[comp_sub["metric_key"] == metric]
        if metric_rows.empty:
            continue
        cat = _CAT.get(metric, "general")
        label = _metric_display_label(metric, labels, period_type)
        value_cells = "".join(
            f"<td>{_esc(_fmt(metric, value_grp.at[p, metric]) if p in value_grp.index else '—')}</td>"
            for p in periods
        )
        delta_cells: List[str] = []
        for prior, current in link_pairs:
            link_row = metric_rows[
                (metric_rows["prior_period"] == prior) & (metric_rows["current_period"] == current)
            ]
            chg = str(link_row.iloc[0]["change_display"]) if not link_row.empty else "—"
            delta_cells.append(f"<td class='{_esc(_chg_class(chg))}'>{_esc(chg)}</td>")
        rows.append(
            f"<tr class='cat-{_esc(cat)}'>"
            f"<td class='cell-kpi'>{_esc(label)}</td>"
            f"{value_cells}{''.join(delta_cells)}"
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


def _comparable_wide_html(
    comparable_kpi_long: Optional[pd.DataFrame],
    comparable_comp_df: Optional[pd.DataFrame],
    dimension: str,
    dimension_value: str,
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_label: str,
    period_type: Optional[str] = None,
    month_display_by_period: Optional[Dict[str, str]] = None,
    title: Optional[str] = None,
) -> str:
    """One comparable-pairs (like-for-like) table: a value column per year from ``comparable_kpi_long`` (its
    own restricted population) plus a delta column per consecutive-year link. ``title`` replaces the
    default "<comp_label> Comparison" caption (the per-quarter / per-half blocks)."""
    if comparable_comp_df is None or comparable_comp_df.empty:
        return ""
    comp_sub = comparable_comp_df[
        (comparable_comp_df["dimension"] == dimension)
        & (comparable_comp_df["dimension_value"].astype(str) == str(dimension_value))
    ]
    if comp_sub.empty:
        return ""

    value_sub, periods = _pivot_single_value(
        comparable_kpi_long if comparable_kpi_long is not None else pd.DataFrame(),
        period_type or "ytd", dimension, dimension_value, metric_cols,
    )
    if value_sub.empty or not periods:
        return '<p style="color:#64748b;font-size:.875rem">No data for this selection.</p>'
    value_grp = value_sub.set_index("period")

    # One delta column per consecutive-year link, ascending by the link's current year.
    links = comp_sub[["prior_period", "current_period"]].drop_duplicates().sort_values("current_period")
    link_pairs = list(zip(links["prior_period"], links["current_period"]))

    value_ths = "".join(
        f"<th>{_esc(_period_th_label(p, period_type, month_display_by_period))}</th>" for p in periods
    )
    delta_ths = "".join(f"<th>&Delta; {_esc(current)}</th>" for _, current in link_pairs)
    head = f"<thead><tr><th class='cell-kpi'>Metric</th>{value_ths}{delta_ths}</tr></thead>"

    rows: List[str] = []
    for metric in metric_cols:
        if metric not in value_grp.columns:
            continue
        cat = _CAT.get(metric, "general")
        label = _metric_display_label(metric, labels, period_type)
        value_cells = "".join(
            f"<td>{_esc(_fmt(metric, value_grp.at[p, metric]) if p in value_grp.index else '—')}</td>"
            for p in periods
        )
        delta_cells: List[str] = []
        for prior, current in link_pairs:
            link_row = comp_sub[
                (comp_sub["prior_period"] == prior)
                & (comp_sub["current_period"] == current)
                & (comp_sub["metric_key"] == metric)
            ]
            chg = str(link_row.iloc[0]["change_display"]) if not link_row.empty else "—"
            delta_cells.append(f"<td class='{_esc(_chg_class(chg))}'>{_esc(chg)}</td>")
        rows.append(
            f"<tr class='cat-{_esc(cat)}'>"
            f"<td class='cell-kpi'>{_esc(label)}</td>"
            f"{value_cells}{''.join(delta_cells)}"
            f"</tr>"
        )

    caption = (
        f"<p class='cmp-label cmp-period-heading'>{_esc(title)}</p>"
        if title
        else f"<p class='cmp-label'>{_esc(comp_label)} Comparison</p>"
    )
    return (
        "<div class='cmp-section'>"
        f"{caption}"
        "<div class='table-wrap'>"
        f"<table class='cmp-table'>{head}<tbody>{''.join(rows)}</tbody></table>"
        "</div>"
        "</div>"
    )


_NUMBERED_PERIOD_PREFIX = {"quarter": "Q", "half": "H"}


def _comparable_numbered_section_html(
    comparable_kpi_long: Optional[pd.DataFrame],
    comparable_comp_df: Optional[pd.DataFrame],
    dimension: str,
    dimension_value: str,
    metric_cols: List[str],
    labels: Dict[str, str],
    period_type: str,
    month_display_by_period: Optional[Dict[str, str]] = None,
) -> str:
    """One value + delta block per quarter / half number (each has its own pair universe and years); a
    number without qualifying data adds no block."""
    prefix = _NUMBERED_PERIOD_PREFIX[period_type]
    tag_col = f"{period_type}_number"
    if (
        comparable_comp_df is None
        or comparable_comp_df.empty
        or tag_col not in comparable_comp_df.columns
    ):
        return ""
    sections: List[str] = []
    for n in sorted(int(n) for n in comparable_comp_df[tag_col].dropna().unique()):
        comp_n = comparable_comp_df[comparable_comp_df[tag_col] == n]
        kpi_n = (
            comparable_kpi_long[comparable_kpi_long[tag_col] == n]
            if comparable_kpi_long is not None
            and not comparable_kpi_long.empty
            and tag_col in comparable_kpi_long.columns
            else pd.DataFrame()
        )
        html = _comparable_wide_html(
            kpi_n, comp_n, dimension, dimension_value, metric_cols, labels,
            f"{prefix}{n}", period_type, month_display_by_period,
            title=f"{prefix}{n} · Like-for-like",
        )
        if html:
            sections.append(f"<div class='cmp-period-block'>{html}</div>")
    return "".join(sections)


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
    month_display_by_period: Optional[Dict[str, str]] = None,
    comparable_kpi_long: Optional[pd.DataFrame] = None,
) -> str:
    sub, periods = _pivot_single_value(
        kpi_long, period_type, dimension, dimension_value, metric_cols,
        week_start_by_period,
    )
    # annual / ytd with a comparison: one value + delta table; otherwise the plain value table.
    table = _comparison_wide_html(
        sub, periods, comp_df, dimension, dimension_value, metric_cols, labels, comp_label,
        period_type, month_display_by_period,
    ) or _kpi_table_html(sub, periods, metric_cols, labels, period_type, month_display_by_period)
    if period_type in _NUMBERED_PERIOD_PREFIX:
        comparable_cmp = _comparable_numbered_section_html(
            comparable_kpi_long, comparable_comp_df, dimension, dimension_value, metric_cols, labels,
            period_type, month_display_by_period,
        )
    else:
        comparable_cmp = _comparable_wide_html(
            comparable_kpi_long, comparable_comp_df, dimension, dimension_value, metric_cols, labels,
            comparable_label, period_type, month_display_by_period,
        )
    if comparable_cmp:
        comparable_cmp = (
            "<div class='cmp-group-divider'>"
            "<p class='cmp-group-label'>Comparable (Like-for-Like)</p>"
            f"{comparable_cmp}"
            "</div>"
        )
    return table + comparable_cmp


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
    month_display_by_period: Optional[Dict[str, str]] = None,
    root: str = "overall",
    comparable_kpi_long: Optional[pd.DataFrame] = None,
) -> str:
    values = _dimension_values(kpi_long, period_type, dimension)
    if not values:
        return '<p style="color:#64748b;font-size:.875rem">No values for this dimension.</p>'
    if len(values) == 1:
        return _value_panel_content(
            kpi_long, period_type, dimension, values[0],
            metric_cols, labels, comp_df, comp_label, week_start_by_period,
            comparable_comp_df, comparable_label, month_display_by_period, comparable_kpi_long,
        )

    radios = "".join(
        f"<input type='radio' class='value-tab-anchor' name='val-{_safe_id(root, period_type, dimension)}' "
        f"id='kpi-val-{_safe_id(root, period_type, dimension, str(i))}'{' checked' if i == 0 else ''}>"
        for i, _ in enumerate(values)
    )

    tab_labels = "".join(
        f"<label for='kpi-val-{_safe_id(root, period_type, dimension, str(i))}' class='value-tab'>"
        f"{_esc(_tab_label(v))}</label>"
        for i, v in enumerate(values)
    )

    panels = "".join(
        f"<div class='value-panel value-panel-{_safe_id(root, period_type, dimension, str(i))}'>"
        f"{_value_panel_content(kpi_long, period_type, dimension, v, metric_cols, labels, comp_df, comp_label, week_start_by_period, comparable_comp_df, comparable_label, month_display_by_period, comparable_kpi_long)}"
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
    month_display_by_period: Optional[Dict[str, str]] = None,
    root: str = "overall",
    comparable_kpi_long: Optional[pd.DataFrame] = None,
    *,
    dimension_labels: Dict[str, str],
) -> str:
    pt_id = _safe_id(root, period_type)

    radios = "".join(
        f"<input type='radio' class='dim-tab-anchor' name='dim-{pt_id}' "
        f"id='kpi-dim-{_safe_id(root, period_type, dim)}'{' checked' if i == 0 else ''}>"
        for i, dim in enumerate(dims)
    )

    tab_labels = "".join(
        f"<label for='kpi-dim-{_safe_id(root, period_type, dim)}' class='dim-tab'>"
        f"{_esc(_tab_label(_dim_label(dim, dimension_labels)))}</label>"
        for dim in dims
    )
    tab_bar = f"<div class='dim-tab-bar dim-tab-bar-{pt_id}'>{tab_labels}</div>"

    panels: List[str] = []
    for dim in dims:
        dim_id = _safe_id(root, period_type, dim)
        if dim == "overall":
            values = _dimension_values(kpi_long, period_type, "overall")
            dval = values[0] if values else "overall"
            content = _value_panel_content(
                kpi_long, period_type, "overall", dval,
                metric_cols, labels, comp_df, comp_label, week_start_by_period,
                comparable_comp_df, comparable_label, month_display_by_period, comparable_kpi_long,
            )
        else:
            content = _value_tabs_html(
                kpi_long, period_type, dim, metric_cols, labels,
                comp_df, comp_label, week_start_by_period,
                comparable_comp_df, comparable_label, month_display_by_period, root,
                comparable_kpi_long,
            )
        panels.append(f"<div class='kpi-dim-panel dim-panel-{dim_id}'>{content}</div>")

    panels_wrap = f"<div class='dim-panels-{pt_id}'>{''.join(panels)}</div>"
    return radios + tab_bar + panels_wrap


def _comp_df_for_root(comp_map: Dict[str, Optional[pd.DataFrame]], root: str) -> Dict[str, Optional[pd.DataFrame]]:
    """Restrict each period_type's comparison table to one root's rows (comparison_yoy/ytd and
    comparable_comparison_ytd all carry a "root" column now -- see kpi_pipeline/comparisons.py)."""
    out: Dict[str, Optional[pd.DataFrame]] = {}
    for pt, df in comp_map.items():
        out[pt] = df[df["root"] == root] if df is not None and not df.empty and "root" in df.columns else df
    return out


def _build_root_period_tabs(
    kpi_long_root: pd.DataFrame,
    root: str,
    period_types: List[str],
    dims: List[str],
    metric_cols: List[str],
    labels: Dict[str, str],
    comp_map: Dict[str, Optional[pd.DataFrame]],
    comparable_comp_map: Dict[str, Optional[pd.DataFrame]],
    week_start_by_period: Dict[str, Any],
    month_display_by_period: Dict[str, str],
    extra_tabs: Optional[List[Tuple[str, str, str]]] = None,
    comparable_kpi_long_map: Optional[Dict[str, Optional[pd.DataFrame]]] = None,
    *,
    dimension_labels: Dict[str, str],
) -> Tuple[str, str]:
    """(css, body_html) of one root's period tab group, from that root's rows. ``extra_tabs`` (id, label,
    panel_html), Methodology and Metric Details with a single root, join the same radio group as peers of the
    period tabs."""
    dims_by_period = {pt: dims for pt in period_types}
    values_by_dim: Dict[str, List[str]] = {}
    for pt in period_types:
        for dim in dims:
            if dim != "overall":
                values_by_dim[f"{pt}|{dim}"] = _dimension_values(kpi_long_root, pt, dim)
    css = _tab_visibility_css(period_types, dims_by_period, values_by_dim, root)

    group_name = f"kpi-top-{_safe_id(root)}"
    radios = "".join(
        f"<input type='radio' class='top-tab-anchor' name='{group_name}' "
        f"id='kpi-tab-{_safe_id(root, pt)}'{' checked' if i == 0 else ''}>"
        for i, pt in enumerate(period_types)
    )
    tab_labels = "".join(
        f"<label for='kpi-tab-{_safe_id(root, pt)}' class='top-tab'>{_esc(_tab_label(_PERIOD_LABELS.get(pt, pt)))}</label>"
        for pt in period_types
    )
    comparable_kpi_long_map = comparable_kpi_long_map or {}
    period_panels = "".join(
        f"<div class='top-panel top-panel-{_safe_id(root, pt)}'>"
        f"{_period_tab_html(kpi_long_root, pt, dims, metric_cols, labels, comp_map.get(pt), _PERIOD_COMP_LABEL.get(pt, ''), week_start_by_period, comparable_comp_map.get(pt), _COMPARABLE_LABELS.get(pt, ''), month_display_by_period, root, comparable_kpi_long_map.get(pt), dimension_labels=dimension_labels)}"
        f"</div>"
        for pt in period_types
    )

    for extra_id, extra_label, extra_panel_html in extra_tabs or []:
        radios += f"<input type='radio' class='top-tab-anchor' name='{group_name}' id='kpi-tab-{extra_id}'>"
        tab_labels += f"<label for='kpi-tab-{extra_id}' class='top-tab'>{_esc(_tab_label(extra_label))}</label>"
        period_panels += f"<div class='top-panel top-panel-{extra_id}'>{extra_panel_html}</div>"
        css += "\n" + _extra_tab_css(extra_id)

    tab_bar = f"<div class='top-tab-bar'>{tab_labels}</div>"
    body = radios + tab_bar + f"<div class='top-panels'>{period_panels}</div>"
    return css, body


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


_LOGIC_SALES_METRICS = ("total_sales_quantity", "total_sales_revenue", "AUR", "AUC", "distinct_product_count",
                        "distinct_store_count", "distinct_pair_count")
_LOGIC_DC_METRICS = ("dc_mean_stock", "total_mean_stock", "WOS_DC", "WOS_TOTAL")
_LOGIC_GROUPS = (("revenue", "Sales"), ("inventory", "Inventory and supply"), ("service", "Service level"),
                 ("scale", "Range size"))


def _plain_filters(filters: Any) -> str:
    """A filter list in business words: "store_id NOT IN (a, b)" reads "stores a, b excluded"; any other
    expression is shown as written."""
    parts = []
    for expr in filters or []:
        match = re.fullmatch(r"\s*store_id\s+NOT\s+IN\s*\((.*)\)\s*", expr, re.IGNORECASE)
        parts.append(f"stores {match.group(1)} excluded" if match else expr)
    return "; ".join(parts)


def _population_text(dim: str, rule: Dict[str, Any], settings: Dict[str, Any]) -> str:
    """A metric population filter in business words: a yes/no root dimension reads "non-LFL products left out" /
    "LFL products only"; any other filter reads "{dim} = values left out" / "only".
    """
    include = "include" in rule
    values = [str(v) for v in (rule.get("include") if include else rule.get("exclude"))]
    root_label = next(
        (settings["HTML_REPORT_ROOT_LABELS"].get(root, root)
         for spec in settings["ROOT_SPECS"] if spec["dim_col"] == dim for root in spec["root_values"].values()),
        None,
    )
    if root_label is not None and len(values) == 1 and values[0] in ("yes", "no"):
        who = f"{root_label} products" if values[0] == "yes" else f"non-{root_label} products"
        return f"{who} {'only' if include else 'left out'}"
    return f"{dim} = {', '.join(values)} {'only' if include else 'left out'}"


def _upper_first(text: str) -> str:
    return text[:1].upper() + text[1:]


def _metric_names(metrics: List[str], metric_cols: List[str], labels: Dict[str, str]) -> str:
    return ", ".join(_metric_display_label(m, labels) for m in metric_cols if m in metrics)


def _instock_rules(settings: Dict[str, Any]) -> List[str]:
    """The in-stock rules of the run in business words, one line per rule, each switched on by its setting."""
    if settings["INSTOCK_METHOD"] != "daily":
        return ["In-stock days come from the weekly in-stock source of the forecasting data."]
    cfg, scope, git = settings["INSTOCK_DAILY"], settings["SCOPE"], settings["GOODS_IN_TRANSIT"]
    stocked = "A store-day counts as in stock when the store has stock on hand"
    if cfg["sales_counts_as_stocked"]:
        stocked += ", or sold the item that day"
    if git["store_instock"]:
        stocked += ", or has stock on its way to the store"
    start = {
        "earliest": "the earlier of its assortment start date and its first inventory record",
        "scope_start": "its assortment start date",
        "first_daily_row": "its first inventory record",
    }[cfg["count_start"]]
    rules = [
        stocked + ".",
        f"Each product-store is counted from {start}.",
        "Days the data flags as unusable are left out." if cfg["usable_only"] else "",
        "Product-stores with no daily records at all are left out." if cfg["require_daily_data"] else "",
        f"Store filter: {_plain_filters(cfg['input_filters'])}." if cfg["input_filters"] else "",
        "Stores where only a replacement (sub) item is assorted are left out." if scope["instock_main_eligible_only"] else "",
        "Sizes outside an item supersession are left out." if scope["instock_exclude_unsuperseded_sizes"] else "",
    ]
    for dim, rule in settings["METRIC_POPULATION_FILTERS"].get("in_stock_rate", {}).items():
        rules.append(_upper_first(_population_text(dim, rule, settings)) + ".")
    return [r for r in rules if r]


def _methodology_cards(settings: Dict[str, Any], metric_cols: List[str], labels: Dict[str, str]) -> List[Tuple[str, List[str]]]:
    """(title, lines) cards of what applies to the run, in business words, each line read from the settings."""
    scope, blocked, git = settings["SCOPE"], settings["BLOCKED_SCOPE"], settings["GOODS_IN_TRANSIT"]
    blocked_metrics = blocked["metrics"] if blocked["path"] is not None or blocked["dc_path"] is not None else []
    gross = settings["SALES_BASIS"] == "gross"
    sales = [
        "Sales are counted before returns: every sales transaction except returns, on every day it happened."
        if gross else
        "Sales are net of returns. A day where returns outweigh sales is not counted.",
    ]
    if blocked_metrics and "total_sales_quantity" not in blocked_metrics:
        sales.append("Blocked days still count for sales, prices and counts: a blocked item can sell its remaining stock.")
    cards: List[Tuple[str, List[str]]] = [
        ("Sales", sales),
        ("What is included", [
            f"Product-store pairs in the assortment (planning scope) on each day, at {scope['grain'].replace('_', '-')} level.",
            "New items are counted under their family's main item." if scope["roll_to_family_main"] else "",
            "Inactive products are excluded." if scope["active_only"] else "",
        ]),
        ("Reporting period", [
            f"Data from {settings['EFFECTIVE_REPORT_START_DATE']} to {settings['REPORT_END_DATE']}.",
            "Year-to-date runs to the latest day, compared with the same fiscal day of earlier years; all other views show complete periods only."
            if settings["REPORT_END_MODE"] == "latest_day" else "All views show complete months.",
        ]),
        ("Views", [
            "Populations: Overall" + "".join(
                f", {settings['HTML_REPORT_ROOT_LABELS'].get(root, root)}"
                for spec in settings["ROOT_SPECS"] for root in spec["root_values"].values()
            ) + ".",
            "Split by " + ", ".join(
                settings["HTML_REPORT_DIMENSION_LABELS"].get(dim, dim)
                for dim in settings["SLICE_DIMENSIONS"] + list(settings["DERIVED_SLICE_DIMENSIONS"])
            ) + ".",
        ]),
    ]
    if blocked_metrics:
        cards.append(("Blocked days", [
            "Days an item was blocked in the planning screens are removed from: "
            + _metric_names(blocked_metrics, metric_cols, labels) + ".",
        ]))
    gated = [m for m in git["inventory_metrics"] if m in metric_cols]
    if gated or git["store_instock"]:
        cards.append(("Stock in transit", [
            "Stock on its way to a store or distribution centre counts as inventory in: "
            + _metric_names(gated, metric_cols, labels) + "." if gated else "",
            "In transit stock counts as being in stock." if git["store_instock"] else "",
        ]))
    if "in_stock_rate" in metric_cols:
        cards.append(("In-stock rate", _instock_rules(settings)))
    if "lost_sales_pct" in metric_cols:
        lost = ["Lost sales are the demand the forecasting model estimates was missed while an item was out of stock."]
        if settings["LOST_SALES_SALES_FILTER"]:
            lost.append(f"Sales in the percentage's denominator use the same stores as the model ({_plain_filters(settings['LOST_SALES_SALES_FILTER'])}).")
        cards.append(("Lost sales", lost))
    if settings["COMPARABLE_PAIRS_ENABLED"]:
        cards.append(("Like-for-like", [
            "Compares only product-stores that traded in every compared year"
            f" ({', '.join(settings['COMPARABLE_KINDS'])} views), so range changes do not distort the trend.",
        ]))
    return [(title, [line for line in lines if line]) for title, lines in cards]


def _metric_basis(metric: str, settings: Dict[str, Any]) -> Tuple[str, List[str], bool, bool]:
    """(based on, filters, blocked days removed, stock in transit included) of one metric in business words."""
    gross = settings["SALES_BASIS"] == "gross"
    blocked = settings["BLOCKED_SCOPE"]
    git = settings["GOODS_IN_TRANSIT"]
    sales = "Sales transactions, before returns" if gross else "Daily store sales, net of returns"
    filters: List[str] = []
    if metric in _LOGIC_SALES_METRICS:
        based_on = sales
    elif metric in _LOGIC_DC_METRICS:
        based_on = "Store and DC inventory" if metric in ("total_mean_stock", "WOS_TOTAL") else "DC inventory"
        if metric in ("WOS_DC", "WOS_TOTAL"):
            based_on += " and store sales"
    elif metric in ("in_stock_rate", "weighted_instock_rate"):
        based_on = "Daily store inventory" if settings["INSTOCK_METHOD"] == "daily" else "Weekly in-stock source"
        filters = [_upper_first(rule) for rule in _instock_rules(settings)[1:]]
    elif metric == "dc_in_stock_rate":
        based_on = "Daily DC inventory"
    elif metric == "lost_sales_pct":
        based_on = "Forecasting lost-sales model and store sales"
        if settings["LOST_SALES_SALES_FILTER"]:
            filters = [_upper_first(_plain_filters(settings["LOST_SALES_SALES_FILTER"]))]
    else:
        based_on = "Daily store inventory" + (" and sales" if metric in _SALES_BASIS_METRICS else "")
    if metric not in ("in_stock_rate", "weighted_instock_rate"):
        for dim, rule in settings["METRIC_POPULATION_FILTERS"].get(metric, {}).items():
            filters.append(_upper_first(_population_text(dim, rule, settings)))
    blocked_folder = blocked["dc_path"] if metric in ("dc_mean_stock", "WOS_DC", "dc_in_stock_rate") else blocked["path"]
    blocked_on = blocked_folder is not None and metric in blocked["metrics"]
    git_on = (
        metric in git["inventory_metrics"]
        or (metric in ("in_stock_rate", "weighted_instock_rate") and git["store_instock"])
        or (metric == "dc_in_stock_rate" and git["dc_instock"])
    )
    return based_on, filters, blocked_on, git_on


def _coverage_html(coverage: List[Dict[str, Any]]) -> str:
    """The Methodology tab's data section: per source its table, the first and last date of each measure and the
    input filters in force, as inputs.collect_data_coverage found them."""
    rows = ""
    for source in coverage:
        path = f" <span class='method-path'>{_esc(source['path'])}</span>" if source["path"] else ""
        rows += f"<tr class='method-group'><td colspan='3'>{_esc(source['title'])}{path}</td></tr>"
        for label, first, last in source["rows"]:
            rows += f"<tr><td>{_esc(label)}</td><td>{_esc(first) if first is not None else '—'}</td><td>{_esc(last) if last is not None else '—'}</td></tr>"
        if source["filters"]:
            code = "".join(f"<div><code class='method-code'>{_esc(f)}</code></div>" for f in source["filters"])
            rows += f"<tr><td>Filters applied</td><td colspan='2'>{code}</td></tr>"
        else:
            rows += "<tr><td>Filters applied</td><td colspan='2'>None</td></tr>"
        for note in source["notes"]:
            rows += f"<tr><td>Note</td><td colspan='2'>{_esc(note)}</td></tr>"
    return (
        "<h4 class='method-heading method-heading-gap'>Data available, by source (for checking)</h4>"
        "<div class='table-wrap'><table class='method-table'>"
        "<thead><tr><th>Data</th><th>First date</th><th>Last date</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


def _methodology_html(
    metric_cols: List[str], labels: Dict[str, str], settings: Dict[str, Any], coverage: Optional[List[Dict[str, Any]]] = None
) -> str:
    """The Methodology tab: how the numbers are built, for the client. Cards of what applies to the whole
    report, then per metric what it is based on and which adjustments it carries. Generated from the settings."""
    cards = "".join(
        f"<div class='method-card'><h4>{_esc(title)}</h4><ul>"
        + "".join(f"<li>{_esc(line)}</li>" for line in lines) + "</ul></div>"
        for title, lines in _methodology_cards(settings, metric_cols, labels)
    )
    groups = []
    for cat, title in _LOGIC_GROUPS:
        rows = ""
        for metric in (m for m in metric_cols if _CAT.get(m) == cat):
            based_on, filters, blocked_on, git_on = _metric_basis(metric, settings)
            filter_cell = "".join(f"<div>{_esc(f)}</div>" for f in filters) or "No extra filters"
            rows += (
                f"<tr><td>{_esc(_metric_display_label(metric, labels))}</td><td>{_esc(based_on)}</td>"
                f"<td>{filter_cell}</td>"
                f"<td><span class='method-pill {'on' if blocked_on else 'off'}'>{'Yes' if blocked_on else 'No'}</span></td>"
                f"<td><span class='method-pill {'on' if git_on else 'off'}'>{'Yes' if git_on else 'No'}</span></td></tr>"
            )
        if rows:
            groups.append(f"<tr class='method-group'><td colspan='5'>{_esc(title)}</td></tr>" + rows)
    return (
        "<div class='method'>"
        "<p class='method-lead'>How the numbers in this report are built. Every figure follows these rules.</p>"
        f"<div class='method-grid'>{cards}</div>"
        "<h4 class='method-heading'>What each metric is based on</h4>"
        "<div class='table-wrap'><table class='method-table'>"
        "<thead><tr><th>Metric</th><th>Based on</th><th>Filters and conditions</th><th>Blocked days removed</th>"
        "<th>Stock in transit included</th></tr></thead>"
        f"<tbody>{''.join(groups)}</tbody></table></div>"
        + (_coverage_html(coverage) if coverage else "")
        + "</div>"
    )


def _report_info_html(
    settings: Dict[str, Any],
    active_slice_dimensions: Optional[List[str]] = None,
    inferred_dimensions: Optional[List[str]] = None,
) -> str:
    customer = settings.get("CUSTOMER", "—")
    as_of = settings.get("AS_OF_DATE", "—")
    report_start = settings.get("EFFECTIVE_REPORT_START_DATE", "—")
    report_end = settings.get("REPORT_END_DATE", "—")
    scope_mode = "Hybrid" if settings["SCOPE"]["use_hybrid_scope"] else "Scope table only"
    period_basis_card = ""
    if settings["REPORT_END_MODE"] == "latest_day":
        period_basis = (
            f"YTD to {report_end} (same fiscal day every year); other tabs: complete periods only; "
            f"lost sales through {last_saturday_on_or_before(settings['REPORT_END_DATE'])}"
        )
        period_basis_card = f"""
      <div class="meta-card">
        <span class="meta-label">Period basis</span>
        <span class="meta-value">{_esc(period_basis)}</span>
      </div>"""
    slice_dims = inferred_dimensions or active_slice_dimensions or settings.get("SLICE_DIMENSIONS") or []
    slice_labels = ", ".join(
        _capitalize_words(_dim_label(d, settings["HTML_REPORT_DIMENSION_LABELS"])) for d in slice_dims if d != "overall"
    ) or "Overall only"
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
      </div>{period_basis_card}
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


_PERIOD_ORDER = ["annual", "ytd", "quarter", "half", "monthly", "weekly"]
_PERIOD_LABELS = {
    "annual": "Annual",
    "ytd": "YTD",
    "quarter": "Quarter",
    "half": "Half",
    "monthly": "Monthly",
    "weekly": "Weekly",
}
_PERIOD_COMP_LABEL = {"annual": "YoY", "ytd": "YTD"}
_COMPARABLE_LABELS = {"ytd": "Comparable YTD", "annual": "Comparable YoY"}

_TURNOVER_PERIOD_LABELS = {
    "annual": "Annual Inventory Turnover Rate",
    "ytd": "YTD Inventory Turnover Rate",
    "quarter": "Quarterly Inventory Turnover Rate",
    "half": "Half-Yearly Inventory Turnover Rate",
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
    """Write the standalone HTML report of a completed KPIContext to ``output_path``; slice dimensions and
    values come from kpi_long."""
    # The display copy trimmed to recent periods; ctx.kpi_long for a KPIContext built without KPIRunner.
    kpi_long = ctx.kpi_long_display if ctx.kpi_long_display is not None else ctx.kpi_long
    settings = ctx.settings

    if kpi_long is None or kpi_long.empty:
        raise ValueError(
            "ctx.kpi_long is empty — run the pipeline first (runner.run(...)) "
            "or load saved outputs (run.mode=html_only)."
        )

    defs: Dict[str, Dict[str, str]] = {
        **DEFAULT_METRIC_DEFINITIONS,
        **_settings_metric_definitions(settings),
        **(metric_definitions or {}),
    }

    customer = settings.get("CUSTOMER", "")
    title = report_title or f"{customer.upper()} KPI Report"

    metric_cols: List[str] = [
        c for c in (settings.get("METRIC_COLS") or []) if c in kpi_long.columns
    ]
    labels: Dict[str, str] = settings.get("METRIC_LABELS") or {}

    # cut_dimensions, not active_slice_dimensions: a root column (e.g. IS_NVROUT) is not a dimension tab.
    configured_slices = list(settings.get("SLICE_DIMENSIONS") or [])
    active_dims = list(getattr(ctx, "cut_dimensions", None) or [])
    slice_source = active_dims or configured_slices
    dims = _infer_dimensions(kpi_long, slice_source)

    present_period_types = kpi_long["period_type"].unique().tolist()
    period_types = [pt for pt in _PERIOD_ORDER if pt in present_period_types]

    if not period_types:
        raise ValueError("kpi_long contains no recognised period_types (annual/quarter/half/monthly/weekly).")

    # Quarter / Half / Monthly / Weekly tabs show values only, no comparison table.
    comp_map: Dict[str, Optional[pd.DataFrame]] = {
        "annual": ctx.comparison_yoy,
        "ytd": getattr(ctx, "comparison_ytd", None),
        "quarter": None,
        "half": None,
        "monthly": None,
        "weekly": None,
    }

    # comparable_kpi_long holds every kind (comparison_type); each tab gets its own kind's rows.
    _full_comparable_kpi_long = getattr(ctx, "comparable_kpi_long", None)

    def _comparable_kpi_long_for(comparison_type: str) -> Optional[pd.DataFrame]:
        if _full_comparable_kpi_long is None or _full_comparable_kpi_long.empty:
            return None
        if "comparison_type" not in _full_comparable_kpi_long.columns:
            return _full_comparable_kpi_long
        return _full_comparable_kpi_long[_full_comparable_kpi_long["comparison_type"] == comparison_type]

    comparable_comp_map: Dict[str, Optional[pd.DataFrame]] = {
        "annual": getattr(ctx, "comparable_comparison_yoy", None),
        "ytd": getattr(ctx, "comparable_comparison_ytd", None),
        "quarter": getattr(ctx, "comparable_comparison_quarter", None),
        "half": getattr(ctx, "comparable_comparison_half", None),
        "monthly": None,
        "weekly": None,
    }
    comparable_kpi_long_map: Dict[str, Optional[pd.DataFrame]] = {
        "annual": _comparable_kpi_long_for("yoy"),
        "ytd": _comparable_kpi_long_for("ytd"),
        "quarter": _comparable_kpi_long_for("quarter"),
        "half": _comparable_kpi_long_for("half"),
        "monthly": None,
        "weekly": None,
    }

    week_start_by_period: Dict[str, Any] = {}
    if getattr(ctx, "fiscal_week", None) is not None:
        fw = ctx.fiscal_week.select("Year_Week", "week_start_date").distinct().toPandas()
        week_start_by_period = dict(zip(fw["Year_Week"], fw["week_start_date"].astype(str)))

    month_display_by_period: Dict[str, str] = {}
    if "monthly" in period_types and getattr(ctx, "fiscal_cal", None) is not None and getattr(ctx, "fiscal_week", None) is not None:
        month_display_by_period = _build_month_display_labels(ctx)

    # Roots with rows in kpi_long: one root renders without a root tab level; several get an outer tab each,
    # holding a complete period tab set.
    root_display_labels: Dict[str, str] = settings["HTML_REPORT_ROOT_LABELS"]
    dimension_labels: Dict[str, str] = settings["HTML_REPORT_DIMENSION_LABELS"]
    present_roots = set(kpi_long["root"].unique()) if "root" in kpi_long.columns else {"overall"}
    roots = [r for r in (["overall"] + [rd["root"] for rd in ctx.root_definitions]) if r in present_roots]
    if not roots:
        roots = ["overall"]

    # Unwrapped: each call site wraps it once in the panel class its CSS shows (a nested .top-panel stays
    # hidden).
    metric_details_html = _metric_details_html(metric_cols, labels, defs)
    methodology_html = _methodology_html(metric_cols, labels, settings, getattr(ctx, "data_coverage", None))
    extra_tabs = [("methodology", "Methodology", methodology_html), ("details", "Metric Details", metric_details_html)]

    if len(roots) == 1:
        root = roots[0]
        kpi_long_root = kpi_long[kpi_long["root"] == root] if "root" in kpi_long.columns else kpi_long
        dyn_css, body = _build_root_period_tabs(
            kpi_long_root, root, period_types, dims, metric_cols, labels,
            _comp_df_for_root(comp_map, root), _comp_df_for_root(comparable_comp_map, root),
            week_start_by_period, month_display_by_period,
            extra_tabs=extra_tabs,
            comparable_kpi_long_map=_comp_df_for_root(comparable_kpi_long_map, root),
            dimension_labels=dimension_labels,
        )
        main_panel = f"<div class='panel'>{body}</div>"
    else:
        css_parts: List[str] = []
        root_panels: List[str] = []
        for root in roots:
            kpi_long_root = kpi_long[kpi_long["root"] == root]
            css, body = _build_root_period_tabs(
                kpi_long_root, root, period_types, dims, metric_cols, labels,
                _comp_df_for_root(comp_map, root), _comp_df_for_root(comparable_comp_map, root),
                week_start_by_period, month_display_by_period,
                comparable_kpi_long_map=_comp_df_for_root(comparable_kpi_long_map, root),
                dimension_labels=dimension_labels,
            )
            css_parts.append(css)
            root_panels.append((root, body))

        root_radios = "".join(
            f"<input type='radio' class='top-tab-anchor' name='kpi-root' "
            f"id='kpi-root-{_safe_id(root)}'{' checked' if i == 0 else ''}>"
            for i, (root, _) in enumerate(root_panels)
        )
        for extra_id, _, _ in extra_tabs:
            root_radios += f"<input type='radio' class='top-tab-anchor' name='kpi-root' id='kpi-root-{extra_id}'>"

        root_labels_html = "".join(
            f"<label for='kpi-root-{_safe_id(root)}' class='top-tab'>"
            f"{_esc(_tab_label(_root_display_label(root, root_display_labels)))}</label>"
            for root, _ in root_panels
        )
        for extra_id, extra_label, _ in extra_tabs:
            root_labels_html += f"<label for='kpi-root-{extra_id}' class='top-tab'>{_esc(_tab_label(extra_label))}</label>"
        root_tab_bar = f"<div class='top-tab-bar'>{root_labels_html}</div>"

        root_panels_html = "".join(
            f"<div class='top-panel top-panel-root-{_safe_id(root)}'><div class='panel'>{body}</div></div>"
            for root, body in root_panels
        )
        for extra_id, _, extra_html in extra_tabs:
            root_panels_html += f"<div class='top-panel top-panel-root-{extra_id}'>{extra_html}</div>"

        root_css_lines: List[str] = []
        for root, _ in root_panels:
            rid = _safe_id(root)
            root_css_lines.append(
                f"  #kpi-root-{rid}:checked ~ .top-panels .top-panel-root-{rid} {{ display: block; }}"
            )
            root_css_lines.append(
                f"  #kpi-root-{rid}:checked ~ .top-tab-bar label[for='kpi-root-{rid}'] "
                f"{{ background: var(--surface); color: var(--ink); box-shadow: inset 0 -3px 0 var(--accent); }}"
            )
        for extra_id, _, _ in extra_tabs:
            root_css_lines.append(
                f"  #kpi-root-{extra_id}:checked ~ .top-panels .top-panel-root-{extra_id} {{ display: block; }}"
            )
            root_css_lines.append(
                f"  #kpi-root-{extra_id}:checked ~ .top-tab-bar label[for='kpi-root-{extra_id}'] "
                "{ background: var(--surface); color: var(--ink); box-shadow: inset 0 -3px 0 var(--accent); }"
            )

        dyn_css = "\n".join(css_parts) + "\n" + "\n".join(root_css_lines)
        main_panel = (
            f"<div class='panel'>{root_radios}{root_tab_bar}"
            f"<div class='top-panels'>{root_panels_html}</div></div>"
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

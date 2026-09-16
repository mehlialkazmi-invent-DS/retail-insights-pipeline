# retail-insights-pipeline vs Orbit — Metrics Comparison

> Compares this pipeline's KPI methodology (`kpi_pipeline/metrics.py`, `pipeline.py`,
> `comparable.py`, `comparisons.py`) against `cx-orbit-ai`'s `kpi-review` skill
> (`orbit/skills/kpi-review/references/formulas.md`, verified there against the Tableau
> workbook and `rocks-reporting`'s `weekly_lost_sales_report.py`).
>
> Generated 2026-09-16.

## Metric-by-metric

| Metric | retail-insights-pipeline | Orbit (`cx-orbit-ai` kpi-review) | Same? | Notes |
|---|---|---|---|---|
| In-Stock Rate | `SUM(stocked_pairs) / SUM(available_days)` from `lost_sales_source`'s `in_stock_days`/`total_days` (`metrics.py:199-202`) | `SUM(in_stock) / SUM(total_days)` — same day-count columns from `weekly_lost_sales_report` (`formulas.md:29`) | ✅ Same lineage & formula | Both ultimately read the same `rocks-reporting` day-count columns; retail-insights-pipeline's NON-COMP `population_filters` fix (excludes `IS_COMP=='no'`) is not mirrored in orbit's doc |
| Weighted (sales-weighted) In-Stock Rate | `weighted_instock_rate`: weekly pair in-stock ratio weighted by weekly sales qty (`metrics.py:204-226`) | Not present in `formulas.md` | ❌ Orbit lacks this | No sales-weighted variant documented for orbit |
| WOS (store) | `AVG(daily_total_inventory over the week) / weekly_sales_units`, sales-weighted across weeks (`metrics.py:86-121,155-156`) — real daily inventory | `SUM(eow_inventory) / SUM(sales_quantity)` — `eow_inventory` is a **snapshot**: last day of week, 0-filled if missing (`formulas.md:30,55-60`) | ❌ Different | Average-over-week vs end-of-week-snapshot; diverges in volatile-stock weeks |
| WOS_DC | `AVG(daily DC inventory over the week) / weekly_sales_units`, from the real daily `inventory_warehouse` table (`metrics.py:123-134,143-159`) | `WoS – warehouse = warehouse_stock / SUM(sales_quantity)`; `warehouse_stock = FIXED [Product],[Year Week]: AVG(eow_supply_quantity)` — a dedup trick over a snapshot column that repeats once per store the DC serves (`formulas.md:16-18,31,37`) | ❌ Different | Same average-vs-snapshot divergence as store WOS, plus orbit's value needs a dedup step retail-insights-pipeline's daily source doesn't need |
| WOS_TOTAL (store + DC combined) | `AVG(daily_total_inventory + daily_dc_inventory) / weekly_sales_units` (`metrics.py:135-152`) | No combined store+DC WoS metric documented | ❌ Orbit lacks this | — |
| dc_mean_stock / total_mean_stock | Plain average of daily DC inventory (and store+DC combined) (`metrics.py:166-184`) | No standalone average-stock metric; only the WoS ratio and the intermediate `warehouse_stock` value used inside it | ❌ Orbit lacks a standalone metric | — |
| DC In-Stock Rate | **Not implemented** (proposed, not yet built) | **No rate metric.** Only a binary scope flag: `Is In Warehouse? = IF eow_supply_quantity > [Wh Stock Limit] THEN 1 ELSE 0` (`formulas.md:35`), used to drop products the DC doesn't hold meaningfully — not reported as a % | ⚠️ Neither has a true rate | Orbit's flag is a scope filter, not a KPI; a proper `dc_in_stock_rate` from real daily `inventory_warehouse` data (stocked days ÷ available days) would be a **new capability**, not parity |
| Lost Sales Quantity % | `lost_sales / (lost_sales + sales_quantity)` from the same `rocks-reporting` source (`pipeline.py`'s `lost_base`, `comparisons.py`) | `SUM(lost_sales) / SUM(lost_sales + sales_quantity)` (`formulas.md:28`) | ✅ Same | — |
| Lost Sales Revenue / % | **Not implemented** | `lost_sales × ASP` where `ASP` is a 5-level fallback chain (`row-level → asp_cum → asp_price → avg_asp_price_store`) × `conversion_rate` (`formulas.md:32-33,42-51`) | ❌ Orbit-only | — |
| Turnover | `sales_units / mean_stock` over a `turnover` population override (`metrics.py:186-197`) | Not documented in `formulas.md` | — | Can't compare — orbit's doc doesn't cover it |
| Gross Margin / GM% / GMROI | **Not implemented** | `SUM(sales_revenue) − SUM(cogs)` **on matched (non-null cogs) rows only**; `GM% = gross_margin / SUM(sales_revenue_matched)`; `GMROI = gross_margin / avg_inventory_value` (`formulas.md:74-76`) | ❌ Orbit-only | See cogs handling below — this is the metric orbit explicitly guards, and retail-insights-pipeline doesn't have the metric at all yet |
| cogs handling (AUC / inventory_cost / sales_cost) | Multiplies by `cogs` with **no null/zero filter or coverage check** (`pipeline.py:270-272`) | Explicitly filters `cogs IS NOT NULL AND cogs > 0` for both revenue and cost, and reports coverage % alongside GM (`formulas.md:79-94`) — documented case: GM% swings from 32.7% (matched-only) to 46.4% (all rows) in one month | ❌ Different, and retail-insights-pipeline is unguarded | Same class of bug orbit already caught and fixed for margin; retail-insights-pipeline's `AUC`/`wos_cost`/`WOS_TOTAL`-adjacent cost metrics are exposed to it unflagged |
| Like-for-like / comparable pairs | Pair-level (`product_id, store_id` present in **every** year of the window), **YTD-only** (`comparable.py:1-17`) | Store-level only (excludes stores opened/closed near a cutoff date), runs over **any** month range | ❌ Different | retail-insights-pipeline's definition is the stricter, more standard retail LFL |
| Sales / distinct counts (AUR, AUC, product/store/pair counts) | `AUR`/`AUC` = revenue or cost ÷ sales qty; `distinct_product_count`/`distinct_store_count`/`distinct_pair_count` via `countDistinct` (`metrics.py:57-80`) | `Avg. Sales Price = SUM(revenue)/SUM(quantity)`; `Store (Unit)/Product (Unit) = COUNT(DISTINCT …)` with sales in period (`formulas.md:71-77`) | ✅ Same shape | Orbit has no cost-side `AUC` equivalent documented |

## Summary

- **Aligned**: In-Stock Rate, Lost Sales Quantity %, average sales price / distinct counts —
  same source lineage (`rocks-reporting`'s `weekly_lost_sales_report`) and same formulas.
- **Diverge by design, not bug**: WOS / WOS_DC — retail-insights-pipeline averages true daily
  inventory; orbit is stuck with an end-of-week Tableau snapshot. retail-insights-pipeline's
  version is more accurate where the daily source exists.
- **Orbit-only, not yet in retail-insights-pipeline**: Lost Sales Revenue (ASP fallback chain),
  Gross Margin / GM% / GMROI (with cogs-null guarding).
- **retail-insights-pipeline-only**: WOS_TOTAL (store+DC combined), `dc_mean_stock`/
  `total_mean_stock` as standalone metrics, sales-weighted in-stock rate.
- **Real open gap on both sides**: neither system has a genuine DC in-stock **rate**. Orbit
  only has a binary "meaningfully stocked" scope flag; retail-insights-pipeline has the raw
  daily DC data to build a proper stocked-days/available-days rate but hasn't yet (see Open
  Items).
- **Known bug class carried over**: retail-insights-pipeline's `cogs`-based cost metrics
  (`AUC`, `inventory_cost`, `sales_cost`) have no null/zero guard — the exact issue orbit
  already found and fixed for its own gross-margin metric.

## Open Items

- [ ] Add a `dc_in_stock_rate` metric (stocked days ÷ available days from `inventory_warehouse`),
      decide threshold (`inventory > 0` vs a configurable `dc_stock_limit` mirroring orbit's
      `Wh Stock Limit`) and `available_days` semantics (record-count vs full calendar days).
- [ ] Decide whether to add cogs null/zero filtering + coverage reporting to `AUC`/
      `inventory_cost`/`sales_cost`, matching orbit's guard for the same underlying data
      quality issue.
- [ ] Decide whether Lost Sales Revenue / Gross Margin / GMROI are in scope for
      retail-insights-pipeline, or remain orbit-only reporting.

---
*Sources: `kpi_pipeline/metrics.py`, `kpi_pipeline/pipeline.py`, `kpi_pipeline/comparable.py`,
`kpi_pipeline/comparisons.py`, `kpi_pipeline/filters.py` (this repo); `orbit/skills/kpi-review/
references/formulas.md` @ `inventanalytics/cx-orbit-ai` (fetched via `gh api`).*

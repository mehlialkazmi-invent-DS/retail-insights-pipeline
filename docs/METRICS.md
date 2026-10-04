# Metrics

Default metrics (configurable in `CONFIG["metrics"]`):

- Sales / inventory: `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `total_inventory`
- Coverage: `distinct_product_count`, `distinct_store_count`, `distinct_pair_count`
- Stock/service: `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate`, `in_stock_rate`, `weighted_instock_rate`, `lost_sales_pct`
- DC/warehouse + combined inventory (needs `path_segments.inventory_warehouse`, see [`inventory_warehouse`](CONFIG.md#inventory_warehouse)): `dc_mean_stock`, `total_mean_stock`, `WOS_DC`, `WOS_TOTAL`
- DC in-stock (gated by `dc_instock.enabled`, needs `path_segments.item_family`, see [`dc_instock`](CONFIG.md#dc_instock)): `dc_in_stock_rate`
- The inventory metrics can each count goods in transit on top of on-hand (gated, `goods_in_transit.inventory_metrics`, see [`goods_in_transit`](CONFIG.md#goods_in_transit))

Every metric uses all scoped stores; there is no global store-exclusion key. The per-metric exceptions are `instock.daily.input_filters` (stores out of in-stock under `instock.method = "daily"`), `lost_sales_source.sales_filter` (out of `lost_sales_pct`) and `metrics.population_filters` (a metric's product population). tbretail uses exactly these: **NON-COMP** (`IS_COMP == "no"`) is removed from in-stock only (`population_filters.in_stock_rate`), **ECOM** stores 829 / 639 / 917 from in-stock and lost sales only (`instock.daily.input_filters`, `lost_sales_source.sales_filter` and the ECOM-excluding model behind `report_dfu`). Every other metric keeps both on every root tab (Overall, NVROUT, LFL), period type, comparison, comparable table and the HTML (all share one `compute_kpis` call); the LFL (`comp`) root is `IS_COMP == "yes"`, so NON-COMP is absent there. To drop a store from everything use `input_filters.daily_data` (plus `instock.daily.input_filters` under `"daily"`, whose read skips `input_filters.daily_data`) or exclude it upstream; for **`lost_sales_pct` only** use [`lost_sales_source.sales_filter`](CONFIG.md#lost_sales_source).

**One population per source.** Every metric read from `noob/daily-data` (sales, `total_inventory`, `mean_stock`, `WOS`, turnover, weighted-instock's weights) uses the **same** rows under every `scope.grain`, except that [`goods_in_transit`](CONFIG.md#goods_in_transit) adds GIT-only days for the metrics it gates and [`blocked_scope`](CONFIG.md#blocked_scope) reads or drops blocked days per `blocked_scope.metrics` (see [Inventory metrics](#inventory-metrics-blocked-days-and-goods-in-transit)); `population_filters` / `sales_filter` are the only other deviations. Lost sales and in-stock come from their own source restricted at that source's grain; DC inventory comes from `inventory_warehouse`, family-rolled to parent `product_id` ([`dc_instock`](CONFIG.md#dc_instock)).

**Lost Sales %** = `100 × sum(lost_sales) / sum(floor(weekly_sales + lost_sales))`; the denominator includes imputed lost demand. `weekly_sales` comes from `daily_data` and `lost_sales` from `lost_sales_source`, so a narrower-population source makes the ratio read low (see [`lost_sales_source.sales_filter`](CONFIG.md#lost_sales_source)).

**In-Stock Rate** = `sum(in_stock_days) / sum(available_days)` from the [`instock`](CONFIG.md#instock) method's source: daily-data store-days (`daily`), a separate weekly table (`weekly_source`) or the top-down lost-sales output (`lost_sales_source`).

**Weighted In-Stock Rate** = sales-weighted average of weekly in-stock rates (each fiscal week weighted by its sales volume when rolling up to the period). Reported as pp-change in comparisons.

**WOS** = per-product per-fiscal-week WOS after summing daily inventory/sales across all scoped stores at product×date (`avg_daily_inventory / weekly_sales`), rolled up to the period with a sales-weighted average (not computed at product×store×week grain). Average daily inventory is weighted by `week_days / 7` (`week_days` = calendar days of that fiscal week in the view, 7 for a whole week), so `WOS = Σ(avg_daily_inventory × week_days/7) ÷ Σ(weekly_sales)` differs from the plain form only for the week `latest_day` cuts mid-week ([Latest-day report end](LOGIC_FLOW.md#latest-day-report-end-report_end--latest_day)); the same weighting applies to `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`. Inventory is on-hand, or on-hand + GIT for metrics in [`goods_in_transit.inventory_metrics`](CONFIG.md#goods_in_transit) (sales, in-stock and lost sales never include GIT); a WOS metric drops blocked days from inventory and sales when named in [`blocked_scope.metrics`](CONFIG.md#blocked_scope).

**WOS (DC)** and **WOS (Total)** use the same grain and rollup on the SAME `daily_data_week` frame (each metric with its own gate columns) left-joined with a DC-inventory frame (weeks with no DC record fill 0, not dropped): `WOS_DC = avg_daily_dc_inventory / weekly_sales`; `WOS_TOTAL = (avg_daily_total_inventory + avg_daily_dc_inventory) / weekly_sales`. DC inventory is restricted to the same in-scope product-weeks as every other metric ([`inventory_warehouse`](CONFIG.md#inventory_warehouse)).

**DC In-Stock Rate** = `F.greatest(0.0, sum(dc_stocked_days) / sum(dc_available_days))` at DC/warehouse level (gated by `dc_instock.enabled`, [`dc_instock`](CONFIG.md#dc_instock)). Unlike other DC metrics **the denominator is an expanded grid, not a row count**: each pair's daily rows run from its first `inventory_warehouse` row to `REPORT_END_DATE` with gaps 0-filled as stockouts, whereas `dc_mean_stock`/`WOS_DC` average existing rows. A pair that **stops** being stocked keeps adding stockout days here while the others go quiet for it; a pair **never stocked** in the window is absent from all. With `enabled=False` (default) it is a literal `null` column.

**Inventory Turnover Rate** = Sales Units ÷ Mean Stock for the same period grain (mean stock includes GIT when named in `goods_in_transit.inventory_metrics`, sales units never; both drop blocked days when named in `blocked_scope.metrics`). The HTML report labels it per tab: **Annual**, **YTD**, **Quarterly**, **Monthly**, or **Weekly** Inventory Turnover Rate.

## Inventory metrics: blocked days and goods in transit

Two independent per-metric gates act on the metrics read from `noob/daily-data` / `inventory_warehouse`:

| Metric | Frame | Drops blocked days when in [`blocked_scope.metrics`](CONFIG.md#blocked_scope) | Adds goods in transit when in [`goods_in_transit.inventory_metrics`](CONFIG.md#goods_in_transit) |
| --- | --- | --- | --- |
| `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `distinct_*_count` | `scoped_daily` real rows | yes, each its own gate (`AUR` / `AUC`: numerator and sales units both) | never |
| `total_inventory`, `mean_stock` (+ retail / cost), `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate` | `scoped_daily` | yes (inventory and the sales it divides by; a blocked day drops its GIT too) | store GIT |
| `dc_mean_stock`, `WOS_DC` | `dc_daily` | yes (DC blocked days; `WOS_DC`'s store sales denominator uses store blocked days, same gate) | DC GIT |
| `total_mean_stock`, `WOS_TOTAL` | `scoped_daily` + `dc_daily` | yes (store and DC blocked days) | store and DC GIT |
| `in_stock_rate`, `weighted_instock_rate` | daily in-stock frame (`pipeline.build_instock_daily`) | frame built without blocked store-days, on-hand days and GIT days when `in_stock_rate` is named (weekly methods: fully blocked pair-weeks dropped; the two metrics are named together); `weighted_instock_rate`'s sales weights when it is named | `goods_in_transit.store_instock` (the frame's in-stock days) |
| `dc_in_stock_rate` | DC grid (`pipeline.build_dc_inst`) | leaves DC blocked days out of stocked and available days when named | `goods_in_transit.dc_instock` |
| `lost_sales_pct` | `lost_base` | the daily-data sales half of the denominator (`daily_for_lost` in `build_pipeline_frames`), when named; the weekly numerator never changes | never |

A metric not named in `blocked_scope.metrics` reads blocked and unblocked rows alike. A day counts in a per-day average (`mean_stock`, `dc_mean_stock`, `total_mean_stock`, WOS average daily inventory, turnover mean stock) only when it has a row the metric reads (without GIT a real row, with GIT any row), so a gated metric skips a day whose rows are all blocked. One conditional-aggregation pass per frame in `metrics.compute_kpis` (`_reads`, `_read_only`, `_day_stock`, `_day_avg`).

## Population filters (restrict ONE metric's own population)

`metrics.population_filters` narrows one metric's product population without touching scope, roots or other metrics (e.g. "in-stock rate should never count NON-COMP, even in the `overall` root"). It applies **on top of** the root/cut in effect, so it is a no-op inside a root that already restricts to the same value (filtering `IS_COMP` inside `comp`).

```python
"metrics": {
    ...
    "population_filters": {
        "in_stock_rate":         {"IS_COMP": {"exclude": ["no"]}},
        "weighted_instock_rate": {"IS_COMP": {"exclude": ["no"]}},
        "WOS":                   {"IS_NVROUT": {"exclude": ["yes"]}},
    },
},
```

Shape: `{metric_col: {dim_col: value_filter_spec}}`; `dim_col` is any `dimension_sources`/`slices` column already joined onto products (`IS_COMP`, `IS_NVROUT`, `brand`), and `value_filter_spec` has the same list/dict shape as [`slices.value_filters`](LOGIC_FLOW.md#value-filters-restrict-cut-values--drop-the-null-bucket).

**Metrics computed in one shared aggregation pass can only be filtered together** (`kpi_pipeline/filters.py`'s `METRIC_FILTER_GROUPS`):

| Group | Metric cols |
| ----- | ----------- |
| `sales` | `total_sales_quantity`, `total_sales_revenue`, `total_inventory`, `AUR`, `AUC`, `distinct_product_count`, `distinct_store_count`, `distinct_pair_count` |
| `wos` | `WOS`, `wos_revenue`, `wos_cost` |
| `wos_dc_total` | `WOS_DC`, `WOS_TOTAL` |
| `mean_stock` | `mean_stock`, `mean_stock_retail`, `mean_stock_cost` |
| `dc_inventory` | `dc_mean_stock`, `total_mean_stock` |
| `turnover` | `inventory_turnover_rate` |
| `instock` | `in_stock_rate` |
| `weighted_instock` | `weighted_instock_rate` |
| `dc_instock` | `dc_in_stock_rate` |
| `lost_sales` | `lost_sales_pct` |

An entry on one column applies to the whole group; *conflicting* specs on two columns of a group fail loudly at run time. `materialize()` fails loudly on an unknown `metric_col` or a malformed `value_filter_spec`. A metric with no entry is unaffected.

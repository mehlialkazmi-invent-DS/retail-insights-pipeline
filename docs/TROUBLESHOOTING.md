# Troubleshooting and performance

## Performance notes

Patterns to preserve when changing internals:

- **Products table**: read once, cached and broadcast, reused by all scope variants.
- **Cache release**: whatever a rebuild replaces is unpersisted first (`context.release` / `release_frames`): `_reset_run_caches`, `build_scope`, `build_blocked_days`, `build_kpis`, `build_scope_comparison`. The non-hybrid `hybrid_scope_keys` is a view of `scope_table_keys`, not a second cache.
- **Daily data and lost sales**: cached once per run via `get_daily_data_raw` and `read_lost_sales_weekly` (the notebook previews read the sources directly). `get_daily_data_raw` caches only the report window's rows (`input_filters.daily_data` applied, rolled to the family main, and under `sales_basis = "gross"` with its sales columns replaced by the filtered `transactional_sales` (`input_filters.transactional_sales`): one windowed read, one aggregation and one join inside this same cache) and only the columns its readers use (`product_id`, `store_id`, `date`, `sales_revenue`, `sales_quantity`, `inventory`, plus the week column on the civil calendar). The daily in-stock method reads `noob/daily-data` separately (`get_instock_daily_raw`: no input filters, from `instock.daily.history_start`); the two reads differ in rows and id space, so they are not shared.
- **Score scope join**: fiscal-calendar equi-join, not a date-range join on week bounds (avoids a nested-loop scan of the daily frame).
- **Score scope inventory**: last available daily snapshot in the fiscal week (`max_by(inventory, date)` in `build_weekly_scope`), not Saturday-only.
- **HTML weekly columns**: sorted by `week_start_date` from `fiscal_week`, not lexicographic `Year_Week`.
- **Collapse and stacking**: each period type (and each comparable build) is filtered, summed across stores / warehouses to product level once (`metrics.collapse_frames`), stacked per root × cut (`kpi_long._stack_roots_and_cuts`) and aggregated in one `build_kpi_table` call; see [How each KPI table is computed](LOGIC_FLOW.md#how-each-kpi-table-is-computed-performance). **Collapse invariant:** `metrics._COLLAPSE` lists, per metric frame, the location column and its additive measure columns; every other column is a collapse group key. A new additive per-store (or per-warehouse) column must be added to `metrics._COLLAPSE`, or it is treated as a group key and the collapse no longer sums it. A cut column must be a string.
- **KPI sort**: final sort in pandas after `toPandas()`, not Spark `orderBy` (removes a shuffle stage from every aggregation).

If runs are slow, check that the scope table path is correct (scope table not empty) and `run_min_date` is Sunday-aligned (or null for full YTD).

## Known limitations

- **Weekly tab display**: `weekly_display_weeks` counts the N most recent **fiscal weeks** of the fiscal calendar (by `week_start_date`; under `report_end = "latest_day"` the trailing partial week is excluded and takes no slot), not the weeks present in `kpi_long` (`kpi_long.trim_periods_to_recent`). After a narrow `run_min_date` or partial backfill a week with no `kpi_long` row still takes a slot, so the tab can show fewer than N columns. The monthly, quarter, half and annual tabs count the periods present in `kpi_long`.
- **Quarter/Half/Monthly trend tabs never show an in-progress period** and there is no toggle ([Complete periods only](LOGIC_FLOW.md#complete-periods-only-quarter-half--monthly-trend-tabs)); read `kpi_long`'s Weekly rows if you need it.
- **`report_end = "latest_day"`** needs `instock.method = "daily"`; a fiscal year starting before `EFFECTIVE_REPORT_START_DATE` is not shown in YTD; YTD lost sales is whole weeks to the last Saturday, so it covers fewer days than other YTD metrics.

## Troubleshooting

| Symptom                     | Likely cause                                                                |
| --------------------------- | --------------------------------------------------------------------------- |
| `lost_sales_pct` reads lower than expected | Lost-sales source covers a narrower population than `daily_data` (classically ecom-excluded), inflating the denominator. Use [`lost_sales_source.sales_filter`](CONFIG.md#lost_sales_source) |
| Sales Revenue / Units differ from the net numbers after setting `sales_basis = "gross"` | Expected: gross is before returns, and counts every transactional day, including days `input_filters.daily_data` removes (e.g. `usable = 1`) or daily-data dropped for net quantity <= 0. Compare against a `"net"` run, and use `output.save_mode = "full_refresh"` when changing the basis: an incremental save would mix the two in one table. |
| `ValueError: sales_basis must be one of ['net', 'gross']` | Bad `sales_basis` or `KPI_SALES_BASIS` value ([`sales_basis`](CONFIG.md#sales_basis)). |
| `initial save blocked`      | Output tables already exist; use `incremental` or `full_refresh` (see [Output saves](OUTPUTS.md#output-saves)) |
| Overlapping periods skipped / saved Delta stale vs notebook | Expected with `incremental` + `allow_overwrite_existing=False`; set `True` or use `full_refresh` |
| Empty cut dimension       | Column missing from `master-data/products` or derived SQL failed validation. If it lives on another table, add a [dimension source](LOGIC_FLOW.md#dimension-sources--roots-population-tabs-from-other-tables) (a root, not a cut) |
| Dimension source errors on read | An **enabled** source fails loudly on bad path / missing column / bad expression; fix it or set `enabled: False` |
| A root you expected is missing, or you want only one root value | Set `root_values` on that entry (`{"yes": "nvrout"}` makes exactly one root); omit it to auto-discover one root per distinct value. `NULL` never gets its own root. |
| Ensemble run fails loudly on read | Wrong `slow_path_segments` / `speed_cluster_path_segments`, or the attributes table lacks the `attribute_name` value. Fix it or set `enabled: False`. For a **wide** cluster table (no `attribute_name`/`attribute_value`) set `speed_cluster_format: "wide"` and `speed_cluster_value_col` |
| Monthly tab empty or stops earlier than Quarterly/Annual | Fiscal weeks in the window have a null `Fiscal_Month` in `one_time_uploads/fiscal_cal` while `Fiscal_Quarter`/`Fiscal_Year` are complete. `kpi_pipeline/fiscal.py` raises listing the weeks; fix the upload or narrow the window to covered months. |
| `time="weekly"`: a pair's earliest weeks are missing though it is active | Pairs tied to the source's earliest week are backfilled by default (`scope.backfill_leading_gap`, [`scope`](CONFIG.md#scope)); a later first-seen week is left as-is (a real new store/product). Set `False` for existing history saved without the option. |
| `time="weekly"` raises `ValueError` about `columns.date` on load | `use_fiscal_calendar=True` requires `scope.columns.date`; native `columns.year`/`columns.week` is valid only under `use_fiscal_calendar=False`. |
| A comparable `quarter` link looks wrong / never appears though 2+ years have data | That quarter must be **fully elapsed** inside the window in those years ([Comparable pairs](LOGIC_FLOW.md#comparable-pairs-like-for-like-ytd--yoy--quarter--half)); the latest year's in-progress quarter, or an earliest year cut by a `run_min_date` not on a quarter boundary, is excluded |
| Latest Quarter/Monthly row missing, or a stale partial-period row in saved Delta | The period has not fully elapsed ([Complete periods only](LOGIC_FLOW.md#complete-periods-only-quarter-half--monthly-trend-tabs)). A partial row saved by an earlier run stays until `allow_overwrite_existing=True` or `full_refresh` |
| `ValueError: reporting_window.report_end='latest_day' requires instock.method='daily'` | `latest_day` splits the daily in-stock frame's fiscal week, which weekly sources cannot do. Use `instock.method = "daily"` or `as_of` / `complete_month`. |
| Current fiscal year missing from Annual/YoY under `latest_day` | By design: Annual shows complete years only; the current year is in YTD. A year is left out of YTD when its first date precedes `EFFECTIVE_REPORT_START_DATE`: move `run_min_date` back to that year's start. The run prints `YTD years` and the years left out. |
| `ValueError: goods_in_transit...` on config load | Unknown `inventory_metrics` name; `date_shift_days` is `None` while `store_instock` / `dc_instock` / `inventory_metrics` ask for GIT, or not an int (bool rejected); `store_instock` without `instock.method = "daily"`; non-empty `inventory_metrics` with `use_fiscal_calendar` `False`. See [`goods_in_transit`](CONFIG.md#goods_in_transit). |
| `ValueError: scope (time=...) has no keys inside the report window` | The scope table has no rows for these weeks / `solution_id` / `run_date`, or `active_only` removed all; check the table and the window. |
| `ValueError: blocked_scope.metrics...` on config load | Not `"all"` or a list, or a name not in `METRICS_ALL` (the message lists them). See [`blocked_scope`](CONFIG.md#blocked_scope). |

# tbretail setup

Notes behind `tbretail_config.py` (its comments point here).

**One-time CSV exports** (Databricks cell; CSVs under `/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/KPI-NEW/data/`):

1. NON-COMP (NFG list) product IDs (`dimension_sources["ngf_comp_split"]`, file `non_comp_ids_20260817.csv`): join an NFG Excel list (`itemcode` column, `Grand Total` row dropped) against products and write the `product_id` values.

   ```python
   pdf_nfg = pd.read_excel(NFG_EXCEL_PATH)
   col_ic = [c for c in pdf_nfg.columns if str(c).strip().lower() == "itemcode"]
   item_col = col_ic[0] if col_ic else pdf_nfg.columns[2]
   nfg_codes = [
       c for c in pdf_nfg[item_col].dropna().astype(str).str.strip().unique()
       if c and c.lower() != "grand total"
   ]
   ```

2. NGF item product IDs (`ngf_product_ids.csv`): same join on an NGF item list (`pdf_ngf["itemcode"]`). NGF items stay in daily-data / scope and Overall and are only flagged (`IS_COMP = 'no'`), so comp vs non-comp can be sliced in one report. `fillna: {"IS_COMP": "yes"}` sets every non-NGF product to `'yes'` (otherwise NULL, since the CSV only covers NGF items). The configured `ngf_comp_split.path` currently points at `non_comp_ids_20260817.csv` (the step-1 file), not `ngf_product_ids.csv`; unclear whether that is deliberate reuse or the step-2 CSV was never generated, so verify before treating either as correct.

**tbretail scope is the open scope table (platform `operation/scope`) only.** Former manual additions (JAB, NGF products, `nvrout_scope_backfill`) and the NON-COMP removal no longer exist: the additions put products into scope at every store with daily data, outside the scope table and its rules (no roll-up, active filter, scope start or blocks), which pulled NVROUT in-stock 5-20 points below the in-stock script that matched the client. NVROUT / COMP / NON-COMP are labels from `dimension_sources`.

**NON-COMP in-stock rule.** kpi-skill-toolkit's Overall in-stock excludes NON-COMP products (`inst_products = nvr_ids ∪ comp_ids_primary`). This pipeline's `overall` root has no restriction of its own, so NON-COMP products (then in scope via the since-removed "NGF products" addition) counted toward Overall's in-stock rate, the confirmed cause of the 2026-09-03/04 Overall-instock-only mismatch (COMP / NVROUT matched, since NON-COMP is in neither root). `metrics.population_filters.in_stock_rate = {"IS_COMP": {"exclude": ["no"]}}` aligns Overall without touching scope, roots or other metrics.

**Blocked-scope snapshot.** `blocked_scope.ui_parameters_path` comes from the Airflow variable `ui_parameters_path`. The newest folder when it was set (`2026-09-30-204511_...`) only had solution 51 blocks; the configured folder is `2026-10-02-065120_...`. Always pin the folder.

**Fiscal calendar.** The fiscal year runs Feb-Jan, so fiscal month / quarter numbers don't match the real calendar (fiscal month 07 observed spanning real 8/2-8/29): `fiscal_calendar.column_map.month_name_col` is read verbatim for the Monthly tab label. `run_min_date` 2024-02-06 resolves to Sunday 2024-02-04.

**Lost sales source.** `path_segments.lost_sales` is `report_dfu` (future_visibility's pre-blended fast / slow output), used instead of `lost_sales_ensemble` (OFF, the fallback if `report_dfu` stops being usable). `report_dfu` has no `store_id`, so `store_col = None` ([`lost_sales_source`](CONFIG.md#lost_sales_source)); its model is `top_down_excluding_ecom`, hence `sales_filter` excludes the same three ECOM stores (829 / 639 / 917, from customer-analysis-tbretail's `store_replenishment/future_visibility/lost_sales_product_120dayslookback.py:66` and the identical filter in `_365dayslookback.py:67`; keep the two in sync).

**Sales basis.** `sales_basis` is `"net"` (daily-data's sales, net of returns), the same numbers as before the setting existed. `"gross"` reads the non-return rows of `operation/transactional_sales` instead, the definition future_visibility uses for `sales_revenue_gross` (`customer-analysis-tbretail`'s `05_future_visibility_data_prep.py`: `sales_type != 'return'`, rolled to the main item, summed per product x store x date, left-joined to daily data). It follows `usable = 1` and the scope like every other sales number, and a day daily-data has no row for is not added. Switch with `KPI_SALES_BASIS` or the config key, and set `output.save_mode` to `full_refresh` (deployed value) when you do: see [`sales_basis`](CONFIG.md#sales_basis).

**Metrics not reported.** `wos_revenue`, `weighted_instock_rate` and `dc_in_stock_rate` are left out of `metrics.metric_cols` (and `dc_instock` is OFF, so `dc_in_stock_rate` would be null); they stay in `blocked_scope.metrics` and `goods_in_transit.inventory_metrics`, so enabling one needs no other edit.

**Dimension sources.** `IS_NVROUT` comes from `operation/extended_product`, which must have one row per `product_id` (an arbitrary row is kept otherwise); absent products get NULL, which `fillna` turns into `'no'`. If a product can have several program values, pre-aggregate to one NVROUT flag per product and point `path` at that instead of `path_segments`.

# Modules

What each file of the repository does. The code is the source of truth; function names are the main entry points of each module. Run order and the logic each module implements are in [LOGIC_FLOW.md](LOGIC_FLOW.md).

## Repository layout

```
retail-insights-pipeline/
├── README.md           # Setup and run guide
├── docs/               # Detailed documentation (see README, Documentation)
├── config.py           # Generic reference template -- copy + customize per client
├── tbretail_config.py  # tbretail's deployed config
├── main.ipynb          # Databricks runner — compute, preview save plan, write, HTML report
└── kpi_pipeline/       # Pipeline logic (imported by main.ipynb)
    ├── runner.py       # KPIRunner orchestrates the full run + HTML report generation
    ├── scope.py        # Scope table, hybrid/score scope, blocked scope
    ├── scope_debug.py  # Pre-flight distinct product/store counts overall + per slice
    ├── fiscal.py       # Fiscal calendar + product attributes / slice dims
    ├── inputs.py       # Cached Delta reads (daily_data, lost_sales) + input_filters
    ├── pipeline.py     # Scoped daily, lost sales, instock input frames per scope
    ├── metrics.py      # KPI aggregation (sales, WOS, instock, lost sales %, …)
    ├── kpi_long.py     # Long-format output across periods and slices
    ├── comparisons.py  # YoY / YTD + scope vs score diff
    ├── comparable.py   # Gated like-for-like YTD comparison (all-years pair restriction)
    ├── io.py           # Incremental Delta saves + save plan preview
    ├── html_report.py  # Standalone HTML renderer (offline, tabbed report)
    └── context.py      # Shared runtime state (KPIContext)
```

## `config.py` / `tbretail_config.py`

`CONFIG` (the only part you edit), the allowed-name tuples (`METRICS_ALL`, `INVENTORY_GIT_METRICS_ALL`, `COMPARISON_KINDS_ALL`, `COMPARABLE_KINDS_ALL`, `INSTOCK_METHODS`) and `materialize()`, which applies `KPI_*` environment overrides, builds the paths, validates the config, resolves the reporting window and returns the flat `settings` dict. Every key is documented in [CONFIG.md](CONFIG.md).

## `main.ipynb`

The runner notebook: loads the config, previews inputs, runs the pipeline, shows what was written and builds the HTML report. The cell-by-cell walkthrough is in the README.

## `kpi_pipeline/__init__.py`

Package exports: `KPIRunner`, `KPIContext`, `SavePlan`, `build_save_plan`, `load_saved_outputs`, `slice_comparison_view` and the input readers/preview helpers used by the notebook (`preview_input_table`, `read_daily_data_source`, `read_lost_sales_source`, `read_scope_source`, ...).

## `kpi_pipeline/runner.py`

`KPIRunner` orchestrates a run on a `KPIContext`. `run(fund_paste, save)` calls `prepare_scopes()` (unless already done), then `build_kpis`, `build_comparisons`, `build_comparable_pairs`, `build_scope_comparison`, saving each table right after its step when `save=True` and `output.save_outputs` is on. In `html_only` mode `run_html_only` loads the saved outputs instead.

- `prepare_scopes()` resets the per-run caches, then runs `build_dimensions()` (`fiscal.apply_report_end_mode` + `fiscal.build_fiscal_and_products`) and `build_scopes()` (scope, blocked days, DC scope, DC blocked days, hybrid scope, removal sets).
- `print_config_summary()`, `preview_save_plan()`, `latest_overall_kpis()`, `scope_debug_summary()`, `hybrid_scope_summary()`, `build_html_report(local_dir)`.
- `RunProgress` prints the plan (`PLAN: N KPI tables ...`) and one progress line per KPI table (one period type or comparable build, covering every root x cut) with elapsed and estimated time left.

## `kpi_pipeline/context.py`

`KPIContext` is the shared runtime state: the Spark session, `settings`, the fiscal calendar and product frames, roots and cuts (`root_definitions`, `cut_dimensions`), scope frames (`scope_table_keys`, `scope_pairs`, `blocked_days`, `dc_scope_pairs`, `dc_blocked_days`, `hybrid_scope_keys`, removal sets), the metric frames per scope variant, the output pandas tables (`kpi_long`, `comparison_*`, `comparable_*`, `scope_diff`, display copies), `save_plan`, `progress`, and the per-run caches. `release` / `release_frames` unpersist cached frames before a rebuild replaces them.

## `kpi_pipeline/inputs.py`

Cached Delta reads and previews with the config `input_filters` applied: `read_daily_data_source`, `read_lost_sales_source`, `read_instock_source`, `read_speed_cluster_source`, `read_scope_source`, `read_blocked_scope_source`, `read_goods_in_transit_source`, `read_inventory_warehouse_source`, `read_item_family_source`, `read_active_product_ids`, CSV reading for dimension sources (`read_csv_source`, `resolve_csv_path`), column renaming to canonical names, and the per-run caches `get_daily_data_raw` (which, with `sales_basis = "gross"`, replaces the sales columns by the filtered `operation/transactional_sales` (`input_filters.transactional_sales`) through `_gross_sales_by_day` / `_with_gross_sales`), `get_daily_data_excluded_days`, `get_item_family_raw` and `get_instock_daily_raw`. `roll_to_item_family_parent` rolls any frame onto the family-main id space; `pipeline.py` and `scope.py` import it from here. `scope_run_date` resolves `scope.run_date`; `preview_input_table` backs the notebook's Cell 2.

## `kpi_pipeline/fiscal.py`

Fiscal calendar, reporting-window periods and product dimensions. `build_fiscal_and_products` fills `ctx.fiscal_cal`, `ctx.fiscal_week`, `ctx.products_attr` and the active slice dimensions, joins `dimension_sources` and resolves the roots. Report-end handling: `apply_report_end_mode` (`complete_month`), `build_latest_day_windows` and `week_day_counts` (`latest_day`), `complete_fiscal_periods` (periods fully elapsed on both edges), `require_complete_time_grain` (every date of the window must be in the calendar), `last_saturday_on_or_before`. `build_time_grain_from_daily_data` / `build_fiscal_cal_and_week_from_upload` build the time grain for the civil and fiscal paths; `build_fiscal_week_only` is the `html_only` variant.

## `kpi_pipeline/scope.py`

Scope table, blocked days, DC scope and the hybrid union. `build_scope` reads the scope table into `ctx.scope_table_keys` at the configured grain and time; `build_blocked_days`, `build_blocked_product_days`, `build_dc_scope`, `build_dc_blocked_days` build the cached block intervals and DC scope pairs; `build_weekly_scope` / `build_score_scope_keys` compute the score scope; `build_hybrid_scope` sets `ctx.hybrid_scope_keys` (with `scope_origin`); `build_scope_removals` builds the removal sets (`fully_blocked_pairs`, `instock_sub_only_pairs`, `instock_unsuperseded_products`); `scope_summary_by_origin` backs the notebook's scope summary cell.

## `kpi_pipeline/scope_debug.py`

`scope_universe_counts(ctx)` returns the distinct product / store / pair counts per removal stage (`scope`, `unblocked`, `instock`), overall and per active slice dimension value. It is what `runner.scope_debug_summary()` shows.

## `kpi_pipeline/pipeline.py`

Builds the metric frames of one scope variant. `build_pipeline_frames(ctx, scope_in)` returns `scoped_daily`, `inst_data`, `lost_base`, `dc_daily`, `dc_inst` (plus scope frames), each restricted to scope on its own. Per frame: `build_scoped_daily` (daily sales / inventory, blocked flag, store goods in transit), `build_instock_daily` (the `daily` in-stock method), `read_instock_weekly` (`weekly_source`), `read_lost_sales_weekly` (lost sales, optional fast / slow ensemble), `build_dc_daily`, `build_dc_inst` (DC in-stock grid). Helpers handle item-family roll-up, blocked-day flags and goods-in-transit quantities.

## `kpi_pipeline/metrics.py`

KPI aggregation. `collapse_frames(ctx, frames, period_col, period_filter)` applies the period filter, then sums each metric frame across stores / warehouses to product level once (`_collapse`, driven by `_COLLAPSE`: per frame, the location column and its additive measures; every other column is a group key) and builds the distinct `sales_pairs` frame behind the store and pair counts; its output is `KPI_FRAMES` (`scoped_daily`, `sales_pairs`, `inst_data`, `lost_base`, `dc_daily`, `dc_inst`). A frame whose root / cut / population filter reads the location column or a measure stays uncollapsed. A new additive per-store column must be listed in `_COLLAPSE`. `compute_kpis(ctx, frames, period_col, group_keys)` takes that frames dict and aggregates it at a period grain and group keys in one conditional-aggregation pass (blocked-day gates per metric, goods in transit for the named metrics, population filters per metric group); `build_kpi_table` joins it with `lost_sales_pct` and returns a pandas table.

## `kpi_pipeline/filters.py`

Value filters shared by slice cuts, roots and per-metric population filters: `normalize_value_filter`, `value_filter_condition` (the Spark condition a filter keeps, used to label roots and cuts when stacking), `apply_value_filter`, `resolve_group_population_filter`, `apply_group_population_filter` (uses `METRIC_FILTER_GROUPS`).

## `kpi_pipeline/kpi_long.py`

Long-format output: `build_kpi_long(ctx, frames)` loops over period types and returns one pandas table (`period_type`, `period`, `root`, `dimension`, `dimension_value`, metrics). For each period type `kpi_rows` frames the period (complete periods only, YTD filters: closed fiscal months, or days 1..K under `latest_day`; a partial trailing week is dropped), collapses the frames (`metrics.collapse_frames`), stacks them per root x cut (`_stack_roots_and_cuts`, one copy of each row per root x cut that keeps it, labelled `_root` / `_dimension` / `_dimension_value`) and runs one `build_kpi_table` call, so one aggregation and one `toPandas` per period type. A cut (slices) column must be a string, otherwise a `ValueError` is raised. A new metric frame goes into `_METRIC_FRAMES` here and into `metrics._COLLAPSE`. `trim_periods_to_recent` makes the display copy for the HTML report.

## `kpi_pipeline/comparisons.py`

`build_comparisons(ctx)` builds `comparison_yoy` / `comparison_ytd` per root and cut from the annual / ytd `kpi_long` rows (`yoy_comparison_long`, `ytd_comparison_long`, chained across consecutive year pairs) with formatted display columns; `slice_comparison_view` extracts one dimension; `build_scope_diff` builds `scope_diff`.

## `kpi_pipeline/comparable.py`

Like-for-like comparisons. `build_comparable_pairs(ctx)` builds, per enabled kind (`ytd`, `yoy`, `quarter`, `half`), the fixed pair universe, restricts the frames to it and writes `comparable_kpi_long` and `comparable_comparison_{kind}`; `rebuild_comparable_kind_from_saved_rows` recomputes a kind's comparison from saved history.

## `kpi_pipeline/io.py`

Delta persistence with incremental merge. `build_save_plan` / `SavePlan` (append / overwrite / skip counts per table), `merge_table_incremental`, `save_pandas_table`, `save_outputs` (write every table at once), `load_saved_outputs` (for `html_only` and for recomputing comparisons from history), and `OutputSaver`, which `runner.run` uses to write each table as soon as its step builds it. Merge keys are in `TABLE_ROW_KEYS`; behaviour is described in [OUTPUTS.md](OUTPUTS.md).

## `kpi_pipeline/html_report.py`

Standalone offline HTML renderer: `render_kpi_html(ctx, path, report_title=...)`, with `DEFAULT_METRIC_DEFINITIONS` for the Metric Details tab. Tabs, config and overrides are in [HTML_REPORT.md](HTML_REPORT.md).

## Programmatic use

```python
from kpi_pipeline import KPIRunner
from kpi_pipeline.io import save_outputs, load_saved_outputs

# Pre-flight scope debug (distinct product/store counts overall + per slice)
runner = KPIRunner(spark, settings)
runner.prepare_scopes()
print(runner.scope_debug_summary())

# Full run: each table is saved as soon as it is built (output.save_outputs=True), with progress lines
ctx = runner.run(fund_paste=fund.paste, save=True)

# Or compute only, preview, then write everything at once
ctx = runner.run(fund_paste=fund.paste, save=False)
runner.preview_save_plan(fund.paste)
save_outputs(ctx, fund.paste)

# HTML only from saved outputs
# settings = materialize(fund.paste)  with run.mode = "html_only"
ctx = runner.run(fund_paste=fund.paste, save=False)  # loads saved Delta, skips pipeline
html_path = runner.build_html_report(local_dir=".")
```

**Inline results.** `KPIRunner.run` displays results as each step finishes: after `build_kpis`, the overall kpi_long rows (root and dimension `"overall"`) of the latest period of each `period_type` (`runner.latest_overall_kpis()`); after `build_comparisons`, the overall YoY / YTD displays; after `build_comparable_pairs`, each enabled comparable kind's overall display.

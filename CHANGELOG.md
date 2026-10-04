# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

#### KPI tables computed on product-level rows, all roots × cuts in one aggregation; docs restructured (no change to any value) - 2026-10-04

Each KPI table used to re-aggregate the product × store × day rows from scratch, once per period type × root ×
cut (54 kpi_long tables, plus 9 per comparable build), with about 12 shuffles each. Every row filter still runs
on the store rows first: scope, input filters, the comparable pair restriction, period framing and
`period_filter`. After that, `metrics.collapse_frames` sums each period type's frames across stores /
warehouses to product level once. It groups by every column except the location and the measures, so
`is_blocked`, `has_daily_row` / `has_inventory_row`, the calendar and the product attributes stay as keys. Every
per-metric gate, population filter, root and cut then reads the same rows as before. Goods in transit valued at
retail / cost is rounded per store row before the sum, as before. `distinct_store_count` / `distinct_pair_count`
come from a distinct `sales_pairs` frame of the same rows. A frame whose root / cut / population filter reads a
store, warehouse or measure column is left uncollapsed. `kpi_long._stack_roots_and_cuts` then labels each
product-level row once for every root × cut that keeps it (the same three-valued filter logic as filtering on the
root and then the cut). The unchanged `compute_kpis` runs once per period type, grouped by those labels: one
aggregation and one `toPandas` instead of one per root × cut. kpi_long rows keep the same values, nulls, row set
and order. A slice (cut) column must now be a string, or the run raises `ValueError` (cast it in
`slices.derived_dimensions`). Before, a non-string cut could not be saved anyway. A new additive per-store column
must be listed in `metrics._COLLAPSE`. Run progress now prints one line per period type / comparable build.
Verified by code review only; not yet run on Spark.

Docs: `README.md` is now a short setup-and-run guide. The detail moved to `docs/` (`CONFIG.md`, `LOGIC_FLOW.md`,
`METRICS.md`, `MODULES.md`, `OUTPUTS.md`, `HTML_REPORT.md`, `TBRETAIL.md`, `TROUBLESHOOTING.md`,
`ORBIT_METRICS_COMPARISON.md`). The `(README: …)` pointers in the config files, the notebook and the skill doc
now point there.

**Affected:** `kpi_pipeline/metrics.py`, `kpi_pipeline/kpi_long.py`, `kpi_pipeline/filters.py`,
`kpi_pipeline/comparable.py`, `kpi_pipeline/comparisons.py`, `kpi_pipeline/runner.py`, `kpi_pipeline/scope_debug.py`,
`config.py`, `tbretail_config.py` (comments), `main.ipynb`, `README.md`, `docs/`, `.claude/commands/retail-insights-help.md`

#### Save each table as soon as it is built; run progress per KPI table (no change to any value) - 2026-10-04

`run(save=True)` now writes each output table right after the step that builds it (`io.OutputSaver`):
`kpi_long` after `build_kpis`, the comparison tables after `build_comparisons` (recomputed from the merged
history first under incremental, as before), the comparable tables after `build_comparable_pairs`, and
`scope_diff` last. A slow or interrupted later step no longer loses the tables already built. The incremental
history sources are looked up once before the first write (as before, the run's own partition never counts as
its own source), and `save_mode="initial"` checks every table before the run computes anything instead of
failing at the end. `save_outputs` uses the same saver to write everything at once (after `save=False`, or to
save what an interrupted run built; empty tables are still skipped). The notebook's Cell 3 runs `save=True` and
binds `ctx = runner.ctx` first, Cell 4 prints what was written, and Cell 5 re-saves only with `RESAVE = True`.

`KPIRunner.run` also prints progress (`runner.RunProgress`, `ctx.progress`). Once the scopes exist it plans the
KPI tables of every stage: kpi_long = period types × roots × cuts, comparable = `comparable.PLANNED_BUILDS` per
kind (ytd / yoy 1, quarter 4, half 2) × roots × cuts, scope_diff = 2. Each KPI table (one `build_kpi_table`, one
`toPandas`) then prints `[stage n/N | run n/N] section · root · cut — took | elapsed | ~left`, and skipped comparable
builds are dropped from the plan.

**Affected:** `kpi_pipeline/io.py`, `kpi_pipeline/runner.py`, `kpi_pipeline/context.py`,
`kpi_pipeline/kpi_long.py`, `kpi_pipeline/comparable.py`, `kpi_pipeline/comparisons.py`, `main.ipynb`,
`README.md`, `docs/LOGIC_FLOW.md`, `.claude/commands/retail-insights-help.md`

#### Blocked days on every in-stock method; in-stock metrics blocked together; empty scope fails; cache hygiene (no change to any tbretail value) - 2026-10-04

`in_stock_rate` and `weighted_instock_rate` read one in-stock frame, so listing either in `blocked_scope.metrics`
now adds the other in `materialize()` (with a printed note). Blocked days also apply under every
`instock.method`. `daily` is unchanged (blocked days leave the counted store-days per day).
`weekly_source` / `lost_sales_source` only have in-stock days per week, so a pair-week whose every window day is
blocked is dropped from the in-stock frame (`pipeline._drop_fully_blocked_weeks`); a partly blocked week stays,
and lost sales / `lost_base` keep every week. An in-stock source without a store column has no pairs to block, so
only `"product"`-kind blocks apply, matched to each product's earliest `scope_start` under `blocked_scope.rule`
(`scope.build_blocked_product_days`, new `ctx.blocked_product_days`); every store-level frame (sales, WOS,
turnover, inventory) still applies every kind, `product_destination` and `destination` included; the run prints both. A scope with no keys inside the report window
(daily or weekly) now raises `ValueError` in `scope.build_scope`. The daily in-stock frame counts blocked days per
pair-week by interval / week overlap instead of exploding one row per blocked day (same values). Cache hygiene:
`_reset_run_caches` unpersists what it clears, `build_scope`, `build_blocked_days`, `build_kpis` and
`build_scope_comparison` release the previous caches before rebuilding, the non-hybrid `hybrid_scope_keys` is no
longer a second cache of `scope_table_keys`, `context.release` / `release_frames` are the unpersist helpers, and
`scope_core` is in the `build_pipeline_frames` dict so it can be released. tbretail (`instock.method = "daily"`,
both in-stock metrics already blocked) is unchanged.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/context.py`, `kpi_pipeline/pipeline.py`,
`kpi_pipeline/runner.py`, `kpi_pipeline/scope.py`, `README.md`, `docs/LOGIC_FLOW.md`,
`.claude/commands/retail-insights-help.md`

#### Scope debug counts after removals; removal sets built once (no change to any output) - 2026-10-04

`runner.scope_debug_summary()` now shows distinct product / store / pair counts for each removal stage, one
column set per stage: `scope_*` (the final scope: sales, revenue, AUR, AUC, distinct counts and every other
metric that keeps all scope data), `unblocked_*` (blocked scope on: scope pairs minus pairs blocked on every
in-window scope day, the population of the metrics in `blocked_scope.metrics`) and `instock_*`
(`instock.method = "daily"`: the pairs `in_stock_rate` and `weighted_instock_rate` count, after
`instock.daily.input_filters`, `instock_main_eligible_only` and `instock_exclude_unsuperseded_sizes`, starting
from `unblocked` when `in_stock_rate` is in `blocked_scope.metrics`). Day-level rules (`usable`, count start,
`require_daily_data`) are not pair counts and are not shown. The removal sets (`ctx.fully_blocked_pairs`,
`ctx.instock_sub_only_pairs`, `ctx.instock_unsuperseded_products`) are built once by
`scope.build_scope_removals` at the end of `build_scopes()`, and the debug summary and `build_instock_daily`
both read them through `pipeline.instock_daily_pairs`, so unsuperseded sizes no longer re-read `item_family`
and products on every `build_pipeline_frames` call. Each metric reads the same data as before: blocked days
are still flagged per day and gated per metric, `fully_blocked_pairs` feeds only the debug counts, and the
in-stock rules still apply only to the in-stock frame.

**Affected:** `kpi_pipeline/scope.py`, `kpi_pipeline/scope_debug.py`, `kpi_pipeline/pipeline.py`,
`kpi_pipeline/context.py`, `kpi_pipeline/runner.py`, `main.ipynb`, `README.md`,
`.claude/commands/retail-insights-help.md`

#### One scope definition (no change to any output except the `scope_diff` column and `scope_origin` value)

The two scope modes (`scope_source.mode` `"operation_scope"` / `"defined_scope"`) and the three config sections
`scope_source`, `defined_scope` and `scope` are one `scope` section with one table (`path_segments.scope`) and
one reader (`inputs.read_scope_source`). The user defines the scope in one place: `time` (`daily` / `weekly`),
`grain` (`product` / `product_store`) and a `columns` map (`None` = column not used). Roll-up to the family main,
`active_only`, blocked scope and the in-stock client rules apply the same way to any source; a rule that needs a
column that is not configured raises a `ValueError` at `materialize()` (`dc_solution_id`, blocked scope, a
`count_start` other than `first_daily_row` and `instock_main_eligible_only` need `columns.start`; weekly with a
start column is invalid, like operation scope with `product_store_week` was). The settings `SCOPE_SOURCE`,
`DEFINED_SCOPE`, `USE_HYBRID_SCOPE`, `RUN_SCOPE_DIFF` and `PATH_DEFINED_SCOPE` are one `SCOPE` dict.
tbretail resolves to the same behaviour as before (solution 21, open on the run date, family mains, earliest
start, active only, DC solution 22, blocked days, in-stock client rules); `config.py` resolves to the same
distinct pairs x every window week.

| Old | New |
|---|---|
| `scope_source.mode = "operation_scope"` | `scope.time = "daily"` with `scope.columns` `start` / `end` / `solution` / `run_date` set (tbretail: `product_id`, `location_id`, `start_date`, `end_date`, `solution_id`, `run_date`) |
| `scope_source.mode = "defined_scope"`, grain `product` / `product_store` | `scope.time = "daily"`, `scope.grain` the same, `columns.start` / `end` / `solution` / `run_date` = `None` |
| `defined_scope.grain = "product_store_week"` | `scope.time = "weekly"`, `scope.grain = "product_store"` |
| `defined_scope.product_col` / `store_col` | `scope.columns.product` / `store` |
| `defined_scope.date_col` / `year_col` / `week_col` | `scope.columns.date` / `year` / `week` (weekly only; `None` under daily) |
| `defined_scope.backfill_leading_gap` | `scope.backfill_leading_gap` (weekly only) |
| `scope_source.solution_id` / `dc_solution_id` / `run_date` | `scope.solution_id` / `dc_solution_id` / `run_date` |
| `scope_source.roll_to_family_main` / `active_only` | `scope.roll_to_family_main` / `active_only` |
| `item_family_rollup.defined_scope` (applied to a defined-scope table only; the scope source's `roll_to_family_main` / `active_only` were ignored there) | `scope.roll_to_family_main`; `config.py` sets it and `active_only` to `False` |
| `scope_source.instock_main_eligible_only` / `instock_exclude_unsuperseded_sizes` | `scope.instock_main_eligible_only` / `instock_exclude_unsuperseded_sizes` |
| `scope.use_hybrid_scope` / `run_scope_diff` | unchanged keys; settings `USE_HYBRID_SCOPE` / `RUN_SCOPE_DIFF` are `SCOPE["use_hybrid_scope"]` / `SCOPE["run_scope_diff"]` |
| `path_segments.defined_scope` / `path_segments.scope` | `path_segments.scope` (one path) |
| `input_filters.defined_scope` | `input_filters.scope` (applies to the DC scope read as well) |
| env `KPI_ITEM_FAMILY_ROLLUP_DEFINED_SCOPE` | `KPI_SCOPE_ROLL_TO_FAMILY_MAIN` |
| settings `SCOPE_SOURCE`, `DEFINED_SCOPE`, `USE_HYBRID_SCOPE`, `RUN_SCOPE_DIFF`, `PATH_DEFINED_SCOPE`, `PATH_SCOPE` | `SCOPE` (its `path` is the one scope path; `PATH_SCOPE` stays) |
| `ctx.defined_scope_keys` / `ctx.operation_scope_pairs` / `ctx.defined_frames` | `ctx.scope_table_keys` / `ctx.scope_pairs` / `ctx.scope_frames` |
| `build_defined_scope`, `read_defined_scope_source`, `read_operation_scope_source` | `build_scope`, `read_scope_source` (`scope._scope_run_date` is `inputs.scope_run_date`) |
| `scope_origin` value `"defined"` | `"scope"` |
| `scope_diff` column `defined` | `scope` (renames a column of the saved `scope_diff` Delta table: refresh that table, or drop it, before an incremental save) |
| HTML header scope mode `Operation scope` / `Defined only` | `Scope table only` (`Hybrid` unchanged) |

New (only reachable by configuring it): `scope.time = "weekly"` with `scope.grain = "product"`, and
`roll_to_family_main` / `active_only` on a weekly or start-less scope. The empty-scope check
(wrong `solution_id` or `run_date`) now applies whenever `columns.solution` or `columns.run_date` is set.

> The entries below were written as each feature landed; later in this release the keys were merged.
> Read `inventory_git` / `instock_daily.git_date_shift_days` / `dc_instock.git_date_shift_days` /
> `item_family_rollup.goods_in_transit` as `goods_in_transit.*`, `instock_daily` / `instock_source` as
> `instock.method` + `instock.daily` / `instock.weekly_source`, and "blocked days removed" as flagged and
> removed per `blocked_scope.metrics` (see "Config simplification" and "Per-metric blocked scope gate").

#### Scope adjustments removed; tbretail scope is the open operation scope only (changes tbretail's numbers)

`scope_adjustments` (manual additions and removals from CSV / Delta) is deleted: added pairs could not go through
the scope rules (no scope start, so no blocks; no main-eligible), and a pair inside the scope needs no addition.
The scope now comes only from the scope source; NVROUT / COMP / NON-COMP are labels from `dimension_sources`, and
`metrics.population_filters` leaves a labelled group out of a metric. `KPIRunner.prepare_scopes()` /
`build_scopes()` no longer take `fund_paste`; `scope_before_adjustments_summary` and
`scope_adjustment_steps_table` are gone. For tbretail this drops the "NGF products" and `nvrout_scope_backfill`
additions, which put products at every store with daily data outside the scope rules and pulled NVROUT in-stock
5–20 points below the in-stock script that matched the client.

#### Faster runs, results shown as they finish, simpler configs (no metric changes)

Every metric computes exactly as before; the configs resolve to identical settings.
- **Blocked days as date intervals**: `scope._applied_block_intervals` merges each pair's blocks into
  disjoint `(first_day, last_day)` intervals instead of one row per pair-day; `scoped_daily`, `dc_daily`,
  the DC in-stock grid and daily in-stock flag or drop blocked days by a range join on the pair.
- **Smaller caches, fewer passes**: the cached daily-data keeps only the report window and the columns its
  readers use; scope-adjustment steps are cached; mean_stock / total_mean_stock / turnover share one
  day-level groupBy when their populations match; redundant `distinct()`s and the full-history
  `inventory_warehouse` cache removed.
- **Comparable pairs**: each kind's metrics are computed once over all its years (every aggregation groups
  by year, so a year's values read only its own rows) and split into consecutive-year links in pandas,
  instead of recomputing per link (a middle year twice); the restricted rows are cached once per kind, and
  the pairs present in every year are found in one groupBy pass instead of one scan per year chained with
  `intersect`.
- **Scope built once**: the notebook's Scope debug cell calls `KPIRunner.prepare_scopes` (reset caches,
  dimensions, scopes) and the next `run()` reuses that scope instead of building it again.
- **Early output**: `KPIRunner` displays the latest overall KPIs right after `kpi_long` is built, then the
  overall comparisons and comparable views as each finishes.
- **Configs**: `config.py` / `tbretail_config.py` regrouped into nine numbered sections with short
  comments; env overrides are one `_ENV_OVERRIDES` table. Keys and values unchanged.
- **Cleanup**: duplicated kpi_long / comparable row building, time-grain setup, change formatting and
  saved-table lists merged; dead guards and unused parameters removed; README and docstrings trimmed.

#### Block solutions separate from scope solutions: `blocked_scope.solution_id` / `dc_solution_id`

The store and DC blocked-scope snapshots are filtered to their own solution lists (tbretail 21 / 22),
independently of `scope_source`, so reading more scope solutions (e.g. 24, 51) never pulls their blocks in.
`blocked_scope.dc_solution_id` needs `scope_source.dc_solution_id`; None = no DC blocks.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{scope,html_report}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-03

#### DC (network) scope named in config: `scope_source.dc_solution_id`

`scope_source.dc_solution_id` (int, list or None; tbretail 22, generic None) replaces
`blocked_scope.dc_solution_id`, so both scopes are read side by side in `scope_source` (21 stores, 22 DC).
`scope.build_dc_scope` reads that solution's `operation/scope` pairs (product x warehouse, rolled to the
main, earliest start, active) into `ctx.dc_scope_pairs`; they give the DC blocked days their start dates
(blocks apply when `blocked_scope.ui_parameters_path` is also set). The store scope leads: the DC metrics
read only the DC scope's product x warehouse pairs whose product is in the store scope (with None,
every warehouse, as before).

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{scope,pipeline,runner,context,html_report}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-03

#### Several solutions per scope: `scope_source.solution_id` / `blocked_scope.dc_solution_id` take a list

Both accept one id or a non-empty list of ints (normalized to a list in settings). The operation scope
reads rows of any listed solution (rolled to the main, earliest start, so a pair in two solutions counts
once), and the blocked-scope snapshot is filtered to the same list. tbretail stays on 21 (stores) and 22 (DC).

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{inputs,scope}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-03

#### DC blocked scope reads only the `product` and `product_destination` kinds

`dc_blocked_scope` has no `destination` folder (a live run failed with PATH_NOT_FOUND on it); DC blocks
come only per product and per product x warehouse. New `blocked_scope.kinds` / `dc_kinds` (required, subsets of
`product` / `product_destination` / `destination`) choose which folders are read; both configs: stores all three,
DC `product` + `product_destination`.

**Affected (also):** `config.py`, `tbretail_config.py`

**Affected:** `kpi_pipeline/{inputs,scope}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-03

#### In-stock leaves out sizes not in a supersession (`scope_source.instock_exclude_unsuperseded_sizes`)

Client rule: a size of a superseded class color (`products.option_code` with any size in `item_family`)
that is itself in no `item_family` row was not created in the supersession and is likely NGF, so it leaves
in-stock and weighted in-stock but stays in every other metric (`pipeline._unsuperseded_sizes`). Requires
`instock.method="daily"`. tbretail `True`, generic `False`.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{pipeline,html_report}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-03

#### In-stock only where the main item is eligible (`scope_source.instock_main_eligible_only`)

Client rule for supersessions: a store where only the superseded (sub) item is eligible, not the main, was
intentionally not assorted the new item, so it leaves in-stock (and weighted in-stock); it stays in sales,
revenue, inventory, WOS, turnover and lost sales so all volume and inventory is captured. The pair's start
stays the earliest start of the main and sub rows. `_scope_start_pairs` adds `main_eligible`;
`build_instock_daily` drops the operation-scope pairs without it (scope additions are kept). Requires
operation scope, `roll_to_family_main` and `instock.method="daily"`. tbretail `True`, generic `False`.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{scope,pipeline,html_report}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

#### Comparable pairs: `comparable_pairs.pair_days`

`comparable_pairs.pair_days` (`"unblocked"` | `"all"`, required) chooses whether blocked days make a pair
present in a year for the like-for-like universe (`comparable._build_comparable_kind`). `"unblocked"` keeps
the previous behaviour; each metric then applies its own `blocked_scope.metrics` gate on the comparable
pairs, as on the other tabs. Also: the sales output rows drop blocked days only when every reported
metric (`metric_cols`) drops them, so no metric loses rows; the HTML blocked-day notes now cover every
listed metric (distinct counts, weighted in-stock's weights, lost sales %'s denominator, both sides of the
combined metrics); dead fallbacks for older settings dicts removed (`DEFAULT_*_COLUMN_MAP`,
`INSTOCK_SOURCE_ENABLED`, `INSTOCK_DAILY["enabled"]`, soft `dc_instock` reads).

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{comparable,metrics,pipeline,inputs,html_report}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

### ✨ Added

#### Gated goods in transit on the inventory metrics (`inventory_git`)

`inventory_git` = `{"git_date_shift_days": None | int, "metrics": [...]}` (both keys required; default
off) adds goods in transit (GIT) to on-hand on the metrics named in `metrics`; every metric not named keeps
on-hand only and its exact previous value. Each metric is chosen on its own from `INVENTORY_GIT_METRICS_ALL` (`total_inventory`, `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `dc_mean_stock`, `total_mean_stock`, `WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`, `inventory_turnover_rate`); `total_mean_stock` / `WOS_TOTAL` count store and DC GIT when named. `metrics.metric_cols` is now validated against `METRICS_ALL`, the list of every metric the pipeline can report. A gated metric uses on-hand + GIT units (retail = units x `price_without_tax`, cost = units
x `cogs`, rounded like `inventory_retail` / `inventory_cost`). `materialize()` raises on an unknown name, a
non-empty `metrics` with `git_date_shift_days` None, a non-int (or bool) shift, and a non-empty `metrics`
without `use_fiscal_calendar=True`; settings key `INVENTORY_GIT`.

Store side (`pipeline.build_scoped_daily`, only when a store-GIT metric is named, else no GIT read): daily data
(restricted to the scoped pairs first, which commutes with the blocked-day removal) ->
store GIT quantity per product x store x day (`destination_type 0`, `quantity > 0`, shifted, summed, rolled
to the family main, window, scoped pairs) -> full outer join (a GIT-only day gets zero sales / on-hand and
`has_daily_row=False`; the daily rows are summed per pair-day first so a quantity attaches once) -> GIT-only
days whose daily row `input_filters.daily_data` removed (`usable = 1`) dropped
(`inputs.get_daily_data_excluded_days`, built once per run and cached on `ctx.daily_data_excluded_days`; the GIT
frame is used once and not cached) -> blocked scope removes both kinds of rows -> scope product-week semi-join.
DC side (`pipeline.build_dc_daily`, only when a DC-GIT metric is named): DC GIT
(`destination_type 1`) full-outer-joined to the rolled `inventory_warehouse` rows (`has_inventory_row`). In
`metrics.compute_kpis`, sales, distinct counts and weighted-instock's sales weights read real rows only; WOS,
mean stock, total mean stock, turnover and the DC metrics each build their own frame (all rows with on-hand
+ GIT when gated, real rows on-hand otherwise) and are joined as before. Sales, in-stock, lost sales and the
DC in-stock rate never change; the comparable-pairs pair universe uses real rows only. The in-stock GIT days
now derive from the same `_goods_in_transit_quantity` helper (unchanged behaviour). The Metric Details text
of a gated metric states that it counts goods in transit. `tbretail_config.py` sets `-1` and every inventory metric.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{context,inputs,pipeline,metrics,comparable,html_report,runner}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

#### `reporting_window.report_end = "latest_day"` (YTD to the latest day, complete periods elsewhere)

A third `report_end` mode (`as_of` and `complete_month` unchanged; env `KPI_REPORT_END` works). `REPORT_END_DATE`
is `as_of_date` itself, set in `materialize()` so every reader sees it (`fiscal.apply_report_end_mode` returns
unchanged). Annual, Quarter, Half, Monthly and Weekly show **complete periods only** (`fiscal.complete_fiscal_periods`
now also at `"Year"` and `"Week"` grain, from the unclipped fiscal calendar; civil calendar analytically), so the
current fiscal year appears in YTD only and the trailing partial week is dropped. **YTD = days 1..K of every
fiscal year**, K = day of the fiscal year of `REPORT_END_DATE` (`fiscal.build_latest_day_windows`: `ctx.ytd_through_day`,
`ctx.ytd_years` = years whose days 1..K are inside the window, `ctx.day_calendar` with `day_index` /
`last_day_index`). `scoped_daily` / `dc_daily` are cut on `day_index`; `build_instock_daily` / `build_dc_inst` split the
fiscal week containing day K into the days `<= K` and the days after (two rows per pair, own stocked / blocked /
unusable / available counts, `last_day_index`), so YTD takes `last_day_index <= K` while every other view sums both
parts. **Lost sales** only has data through the last Saturday on or before `REPORT_END_DATE`: `lost_base` keeps weeks
ending on or before it in every view, and YTD uses weeks 1..(that Saturday's fiscal week) for every year, so the
missing days never dilute `lost_sales_pct` or misalign YoY YTD. Comparisons and comparable kinds follow (YoY =
complete fiscal years, YTD = same-fiscal-day windows, quarter / half = complete periods). Incremental merge always
replaces an existing `ytd` row of `kpi_long` / `comparable_kpi_long` (`io.merge_table_incremental`). **WOS and the part
week:** YTD ends on day K, so the fiscal week containing K enters `WOS`, `wos_revenue`, `wos_cost`, `WOS_DC` and `WOS_TOTAL`
with only its days up to K; `scoped_daily` carries `week_days` (7 outside this mode; the YTD frames swap in the days up
to K, `fiscal.week_day_counts`) and `metrics.compute_kpis` weights each product-week's average daily inventory by
`week_days / 7`, so a part week is not counted as a full week of inventory against a part week of sales (whole weeks,
and every other view and mode, are unchanged). `scope_diff`'s Annual table uses the Annual tab's complete fiscal years.
HTML: a **Period basis** card in the header, the Lost Sales % definition and the WOS definitions state the basis.
Requires `instock_daily.enabled=True` (raises otherwise). `tbretail_config.py` switches to `latest_day`.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{context,fiscal,pipeline,metrics,kpi_long,comparable,comparisons,io,html_report,runner}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

#### DC in-stock: goods in transit and DC blocked scope

`dc_instock.git_date_shift_days` (None = off, int not bool) counts a DC grid day as stocked when goods
are in transit to the DC (`goods_in_transit` `destination_type=1`, `quantity>0`, rolled to the family
main; snapshot D+1 = end of day D, so `-1`), unioned with `inventory > stock_threshold`.
`blocked_scope.dc_solution_id` (None = off, int not bool; first added as
`dc_instock.blocked_scope_solution_id`) removes `{blocked_scope.ui_parameters_path}/dc_blocked_scope`
days of that solution by `blocked_scope.rule`, against each DC pair's `scope_start` from
`operation/scope` of that solution (same run_date, roll-up, earliest start and active filter as
`scope_source`), from every DC metric like store blocks on store metrics: `dc_mean_stock`, `WOS_DC`, the
DC part of `WOS_TOTAL` / `total_mean_stock` (removed in `build_dc_daily` after the DC goods-in-transit
join) and `dc_in_stock_rate`. DC pairs outside it get no blocks; requires
`blocked_scope.ui_parameters_path`. Built once per run as `ctx.dc_blocked_days`
(`scope.build_dc_blocked_days`). The Metric Details text mentions both when on. `tbretail_config.py` sets
`-1` and `22` (`dc_instock` itself still off; DC blocks now apply to DC inventory and DC WOS).
`item_family_rollup.goods_in_transit` (default True, env `KPI_ITEM_FAMILY_ROLLUP_GOODS_IN_TRANSIT` in
`config.py`) controls whether every goods-in-transit read (in-stock, DC in-stock, inventory GIT) is
rolled to the family main.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{inputs,scope,pipeline,runner,context,html_report}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

### 🔄 Changed

#### Config simplification: one `instock`, one `goods_in_transit`; both configs shorter

`config.py` and `tbretail_config.py` keep **every** section and key (switched-off features included) but merge
the keys that described one feature in several places, and lose their long comment essays (rationale, history,
caveats and examples moved to `README.md`; the config comments are a short header per section plus a pointer;
`tbretail_config.py`'s one-time CSV export notes are now README's "tbretail setup"). Every other key and every
value is unchanged. `materialize()` (duplicated in both files, changed identically) indexes the new keys directly.

| Old key | New key |
| --- | --- |
| `instock_daily.enabled = True` | `instock.method = "daily"` |
| `instock_source.enabled = True` | `instock.method = "weekly_source"` |
| both off (generic default) | `instock.method = "lost_sales_source"` |
| `instock_daily.{count_start, require_daily_data, history_start, usable_only, input_filters}` | `instock.daily.{...}` (same keys) |
| `instock_source.{path_segments, week_col, product_col, store_col, in_stock_col, total_days_col, product_agg_level_col, fallback_sources}` | `instock.weekly_source.{...}` (same keys) |
| `instock_daily.git_date_shift_days` | `goods_in_transit.date_shift_days` + `goods_in_transit.store_instock = True` |
| `dc_instock.git_date_shift_days` | `goods_in_transit.date_shift_days` + `goods_in_transit.dc_instock = True` (the key is removed from `dc_instock`) |
| `inventory_git.git_date_shift_days` | `goods_in_transit.date_shift_days` |
| `inventory_git.metrics` | `goods_in_transit.inventory_metrics` |
| `item_family_rollup.goods_in_transit` | `goods_in_transit.roll_to_family_main` (env `KPI_ITEM_FAMILY_ROLLUP_GOODS_IN_TRANSIT` dropped) |
| env `KPI_INSTOCK_SOURCE_ENABLED` | env `KPI_INSTOCK_METHOD` (`daily` / `weekly_source` / `lost_sales_source`); the other `KPI_INSTOCK_*` names are unchanged and set `instock.weekly_source.*` |
| settings `INVENTORY_GIT`, `DC_INSTOCK_GIT_DATE_SHIFT_DAYS`, `INSTOCK_DAILY["git_date_shift_days"]`, `ITEM_FAMILY_ROLLUP["goods_in_transit"]` | one `GOODS_IN_TRANSIT = {date_shift_days, roll_to_family_main, store_instock, dc_instock, inventory_metrics}`; new `INSTOCK_METHOD` (`INSTOCK_DAILY`, `INSTOCK_SOURCE_ENABLED`, `INSTOCK_SOURCE_COLUMN_MAP` stay, derived from `instock`) |

`goods_in_transit.date_shift_days = None` means goods in transit is off everywhere: `store_instock` / `dc_instock` must
then be `False` and `inventory_metrics` empty (else `ValueError`); `store_instock = True` requires
`instock.method = "daily"`; a non-empty `inventory_metrics` still requires `use_fiscal_calendar`. All existing
validations keep working under the new names (`latest_day` requires method `daily`; the `lost_sales_ensemble`
incompatibilities; the `count_start` rules). `instock.method` rejects unknown names with the allowed list. Values:
tbretail `-1` / roll `True` / `store_instock True` / `dc_instock True` / all 12 inventory metrics; generic `None` /
`True` / `False` / `False` / `[]`.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{context,inputs,pipeline,metrics,html_report,runner}.py`, `README.md`, `.claude/commands/retail-insights-help.md`, `main.ipynb`

**Date:** 2026-10-02

#### Per-metric blocked scope gate (`blocked_scope.metrics`)

New key `blocked_scope.metrics`: the metrics that drop blocked days, `"all"` or a list from `METRICS_ALL` (unknown
names raise; `settings["BLOCKED_SCOPE"]["metrics"]` is the resolved list in `METRICS_ALL` order). Store blocks
(`ctx.blocked_days`) and DC blocks (`ctx.dc_blocked_days`, `blocked_scope.dc_solution_id`) are no longer removed
from every row: a metric in the list reads only unblocked rows, a metric not in the list reads blocked and
unblocked rows alike. `build_scoped_daily` / `build_dc_daily` flag the blocked days (`is_blocked`, one left join)
instead of anti-joining them; `metrics.compute_kpis` keeps one aggregation pass per frame and family with
conditional aggregation (`F.when(~is_blocked, x)` in sum / avg / countDistinct, and per-metric `<metric>_day` /
`<metric>_has` day columns replacing `_day_sums`). Per family: the sales group (`total_sales_quantity`,
`total_sales_revenue`, `AUR`, `AUC`, the distinct counts) each by its own gate; `total_inventory`, `mean_stock`
(+ retail / cost), `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate` (sales units and mean stock) and the
store part of `total_mean_stock` / `WOS_TOTAL` each by their own gate; `dc_mean_stock`, `WOS_DC` and the DC part of
`total_mean_stock` / `WOS_TOTAL` drop DC blocked days by that metric's gate; `in_stock_rate`: the daily in-stock frame
(`build_instock_daily`) removes blocked store-days, on-hand days and GIT days only when it is in the list, and
`weighted_instock_rate` uses that frame with its sales weights dropping blocked days when it is in the list;
`dc_in_stock_rate`: `build_dc_inst` removes DC blocked days only when in the list (new `dc_unblocked_days` column
for the pair universe); `lost_sales_pct`: the daily-data sales in its denominator drop blocked days only when in the
list (the weekly lost-sales numerator never changes). The comparable-pairs pair / year universe stays the real,
unblocked rows; `scope_diff` and the scope debug are unchanged. The Metric Details blocked-days notes appear only on
listed metrics. Generic default `"all"`: turning blocked scope on behaves exactly as before. `tbretail_config.py`
lists `in_stock_rate`, `weighted_instock_rate`, `dc_in_stock_rate`, `total_inventory`, `mean_stock`, `mean_stock_retail`,
`mean_stock_cost`, `dc_mean_stock`, `total_mean_stock`, `WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL` and
`inventory_turnover_rate`: sales units, revenue, AUR, AUC, the distinct counts and `lost_sales_pct` now **keep**
blocked days (a blocked pair can still sell its existing stock), so those tbretail numbers change; the other
tbretail metrics keep their blocked-day behaviour.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{context,pipeline,metrics,comparable,html_report}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

#### Population rules confirmed for tbretail; `lost_base` weeks and Weekly display slots under `latest_day`

Verified, no code change needed: NON-COMP (`IS_COMP == "no"`) leaves in-stock only (`metrics.population_filters`),
ECOM stores 829 / 639 / 917 leave in-stock and lost sales only (`instock_daily.input_filters`,
`lost_sales_source.sales_filter`, the ECOM-excluding model behind `report_dfu`), on every root, period type,
comparison, comparable table and in the HTML (all go through `metrics.compute_kpis`); the LFL root is
`IS_COMP == "yes"` by definition. `tbretail_config.py`'s header comment and the README / help text now state
this. With `latest_day`, `trim_periods_to_recent` ignores the trailing partial week when picking the N most recent
Weekly columns, and `lost_base` keeps only weeks up to the last Saturday in every view.

**Affected:** `tbretail_config.py`, `kpi_pipeline/{pipeline,kpi_long}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

#### Operation scope rolled to the family main; blocks on the main's own product_id only

`scope_source.main_items_only` is replaced by `scope_source.roll_to_family_main`: every scope row is
rolled to its family main (`coalesce(parent_id, product_id)`; no main keeps its own id), so a store
where only a sub-item is in scope now gets the main, and each pair keeps its **earliest** start_date
as `scope_start` (was: sub-item rows dropped, latest start_date kept). Block product_ids are no
longer rolled to the family main: only blocks on the main's own product_id apply, as in the client
reference script. Store and DC blocks share `scope._applied_block_days`; store and DC goods in
transit share `pipeline._goods_in_transit_days`. Expect more store pairs (stores reached only via a
sub-item) and earlier scope starts, so more blocks pass the `after_scope_start` rule.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/{inputs,scope,pipeline,context}.py`, `README.md`, `.claude/commands/retail-insights-help.md`

**Date:** 2026-10-02

## [Released]

### 🐛 Fixed

#### `defined_scope.backfill_leading_gap` restricted to the source's own earliest pairs

Previously backfilled *every* `product_store_week` pair whose own first-seen week was later than
the report window's start, including pairs that are genuinely new (a new store, a new product) —
incorrectly granting them retroactive scope membership before they existed. Now only the pairs
tied to the scope source's own earliest available week (across every pair) are backfilled, since
that shared earliest point is what signals a data-availability limit of the source itself rather
than a real pair start. A pair whose first-seen week is later than the source's own earliest week
is left untouched.

**Affected:** `config.py`, `kpi_pipeline/scope.py`

**Date:** 2026-09-20

### ✨ Added

#### Operation scope, blocked scope and daily in-stock (client-approved in-stock method)

`scope_source.mode="operation_scope"` builds the scope from `operation/scope` (solution, latest Sunday
run or `run_date`, rows open on that date, main items only, active products only). `blocked_scope`
removes UI-blocked pair-days (client rule: a block applies only when it starts on or after the pair's
scope start, same day applies) from every daily-data-derived metric and from in-stock; it is on
exactly when `blocked_scope.ui_parameters_path` is set, uses `scope_source.solution_id` and rolls
block product_ids to the family main when `scope_source.main_items_only`. `instock_daily` builds
`in_stock_rate` from `noob/daily-data` (on-hand days, plus goods-in-transit days when
`git_date_shift_days` is set; count start = `first_daily_row` / `scope_start` / `earliest`, blocked
and unusable days removed without double subtraction) in the same shape as the weekly in-stock frame.
Every metric, in-stock included, uses the same scope: tbretail's `scope_adjustments` additions (JAB,
NGF products, `nvrout_scope_backfill`) count in in-stock too, from their first daily row (they have no
scope start), with no main-item, active or blocked-scope filter. The in-stock Metric Details text in the
HTML report is built from these
settings (it mentions blocked days only when `blocked_scope` is on), and notes blocked days on the sales
/ inventory metrics. `path_segments.scope` /
`goods_in_transit` added. Everything is off by default (generic `count_start` default is
`first_daily_row`, which works without operation scope); `tbretail_config.py` turns it on, drops
`wos_revenue`, `weighted_instock_rate` and `dc_in_stock_rate` from its report and disables
`dc_instock` and `instock_source`. `instock_daily.git_date_shift_days` is read straight from the
config dict (a missing key raises) and rejects `bool`; the new settings are indexed directly (no
silent defaults) and `solution_id` rejects `bool`.

**Limitations and behaviour changes to know about:**

- `lost_sales_source` (weekly, no store grain, e.g. `report_dfu`) is not filtered by blocked scope.
- `scope_adjustments` additions are not operation-scope pairs: they skip the main-item / active
  filters, have no scope start and receive no blocks. A product-only addition (`store_col=None`)
  becomes every store with a daily-data row in the window at the `product_store` grain. They are
  part of the one scope every metric uses, in-stock included (counted from their first daily row).
- `instock_daily` raises unless daily-data's latest date for the scope pairs reaches `REPORT_END_DATE`
  (days after it would otherwise count as out of stock). It filters the daily-data read on the raw
  date column so Delta file pruning applies, and caches the scope-joined, blocked-removed daily frame
  instead of the raw read.
- `input_filters.defined_scope` is not applied in operation-scope mode (tbretail's
  `week_start_date < '2026-08-02'` entry is unused there); `input_filters.daily_data`,
  `scope_adjustments` and `population_filters` still apply.
- The scope `run_date` defaults to the latest Sunday on or before today, not the report's as-of
  date, so a backdated run is not reproducible unless `scope_source.run_date` is set.
- `lost_sales_ensemble` cannot be combined with `instock_daily`. A missing `blocked_scope/<kind>`
  snapshot folder fails the run.
- Block product_ids are rolled to the family main (a block on a sub-item code blocks the main's
  pairs); the reference script ignores such blocks, so tbretail numbers can sit slightly below it.
- **tbretail numbers move.** tbretail switched from the `product` to the `product_store` scope
  grain and now removes blocked days, so every sales, inventory, WOS, WOS_DC, turnover and
  lost-sales-denominator number changes against earlier reports. `lost_sales_pct` also needs a
  before / after check on one fiscal year: its numerator (`report_dfu`, product level) is not
  blocked-day filtered while the denominator's sales are.

- **Deployment.** `../tbretail_config.py` and `../tbretail_config_2.py` (the deployed copies outside
  the repo) must be replaced by this branch's `tbretail_config.py` before running: they lack the new
  keys (`SCOPE_SOURCE`, `BLOCKED_SCOPE`, `INSTOCK_DAILY`, `HALF_PERIODS`, `REPORT_END_MODE`,
  `HTML_REPORT_FILENAME_TEMPLATE`, `HTML_REPORT_DIMENSION_LABELS`, ...) and raise `KeyError` against
  this branch.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/scope.py`, `kpi_pipeline/inputs.py`,
`kpi_pipeline/pipeline.py`, `kpi_pipeline/context.py`, `kpi_pipeline/runner.py`,
`kpi_pipeline/html_report.py`, `README.md`

**Date:** 2026-10-01

#### HTML report: centered tables, root tab labels, capitalized tabs

Every table column in the HTML report is center-aligned. `html_report.root_labels` maps a root id to
its tab label (tbretail: `comp` -> `LFL`, `nvrout` -> `NVROUT`). Every tab label's all-lowercase
words are capitalized (`annual` -> `Annual`; a value tab such as `jab` becomes `Jab`); words with
capitals (`YTD`, `SMW`) are kept. Centering and capitalization are global style changes: they apply
to every client, not only tbretail, and a root without a `root_labels` entry (e.g. `nvrout`) shows
as `Nvrout`. The header's scope mode reads "Operation scope" in operation-scope mode.

**Affected:** `kpi_pipeline/html_report.py`, `config.py`, `tbretail_config.py`

**Date:** 2026-10-01

#### HTML report: `html_report.dimension_labels`

`html_report.dimension_labels` maps a slice dimension name to its display label (generic `{}`,
tbretail `{"brand": "Banner"}`). It is used wherever a dimension name is shown in the HTML report:
the dimension tabs and the header's "Slice dimensions" card (still passed through the tab-label
capitalization). It is display only: `kpi_long` and the saved outputs keep the raw dimension key.
Materialized as `HTML_REPORT_DIMENSION_LABELS`; `materialize()` raises unless it is a dict of
`str -> str`. A dimension not listed keeps its title-cased name.

**Affected:** `kpi_pipeline/html_report.py`, `config.py`, `tbretail_config.py`, `README.md`

**Date:** 2026-10-01

#### Complete-month report cutoff (`reporting_window.report_end`)

`report_end = "as_of"` (default) keeps `REPORT_END_DATE` at the last completed Saturday.
`"complete_month"` cuts it back to the last day of the most recent fully elapsed month on or before
that Saturday (`fiscal.apply_report_end_mode`, run first thing in `build_dimensions` and
`run_html_only`), so monthly, quarter, half, YTD, annual, weekly, comparisons, comparable pairs, the
daily in-stock and the blocked-days window all stop at that date. YTD sums the complete months of
each year.

- With a fiscal calendar the month is a fiscal month, read from the unclipped `fiscal_cal` upload.
  The upload must extend past the as-of Saturday (otherwise the run raises: an upload ending earlier
  would make its own last date look like a month end), and `run_min_date` must reach the start of the
  cut month: otherwise the run raises ("no month ends between ..." when none ends in the window, or
  "the cut month ... starts before the report start" when the cut month begins before it; on the civil
  path the month starts on the 1st).
- Without one (`use_fiscal_calendar=False`) it is a calendar month. The cut is usually mid-week: the
  clipped trailing week is dropped from the Weekly tab, and months are bucketed by each week's start
  date, so this is a calendar-month cut, not exact calendar-month totals.
- The HTML output filename is a template (`HTML_REPORT_FILENAME_TEMPLATE`, placeholders `{customer}`
  and `{report_end}`, validated in `materialize`); `HTML_REPORT_OUTPUT_DIR` is the optional datastore
  folder. `KPIRunner.build_html_report` formats the filename once the cut date is known
  (`HTML_REPORT_FILENAME` / `HTML_REPORT_OUTPUT_PATH` are gone).
- Incremental saves: a narrow `run_min_date` (the weekly-refresh pattern) now raises because it no
  longer reaches the cut month. With `incremental` and `allow_overwrite_existing=False`, switching
  from `as_of` to `complete_month` keeps the `as_of` versions of the current annual row and of weekly
  rows after the cut in history; use `full_refresh` when switching (tbretail does).
- `OUTPUT_RUN_DATE` (the saved `run_date=` partition) still follows `as_of_date`.

`tbretail_config.py` uses `"complete_month"`; env `KPI_REPORT_END`.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/fiscal.py`, `kpi_pipeline/kpi_long.py`,
`kpi_pipeline/runner.py`, `README.md`

**Date:** 2026-10-01

#### Fail-loud time grain check

The calendar must contain every date between the window start and end. On the civil path it is built
from the dates present in `noob/daily-data` (after `input_filters.daily_data`), so a missing day used
to drop silently from the metrics and shift week bounds; `fiscal.require_complete_time_grain` now
raises `time grain (...) is missing N date(s) between ... : [first 20]` for either calendar path.

**Affected:** `kpi_pipeline/fiscal.py`

**Date:** 2026-10-01

#### Half periods (H1 / H2) and the `half` comparable kind

`fiscal_calendar.half_periods` (default `False`; tbretail `True`) adds a `Fiscal_Half` column
(fiscal quarters 1-2 -> 1, 3-4 -> 2) to the fiscal week frame and every metric frame, a `half`
`kpi_long` period type labelled `2025-H1` (only complete halves, like quarters), a Half tab in the
HTML report (`html_report.half_display_halves`, default 4) and a `half` entry in
`comparable_pairs.kinds`: built like `quarter`, independently per half number, only years where that
half is complete. `comparable_kpi_long` half rows carry `half_number`;
`comparable_comparison_half` keys on `half_number` so saved rows never collide. The quarter/half
code in `comparable.py` now shares one path (`_NUMBERED_KINDS`, `_complete_period_years` replaces
`_complete_quarter_years`; it reads the per-run `ctx.complete_fiscal_periods`). The Fiscal_Half
completeness set is only computed when `half_periods` is on. Env `KPI_HALF_PERIODS`,
`KPI_HTML_HALF_HALVES`.

**Affected:** `config.py`, `tbretail_config.py`, `kpi_pipeline/fiscal.py`, `kpi_pipeline/pipeline.py`,
`kpi_pipeline/kpi_long.py`, `kpi_pipeline/comparable.py`, `kpi_pipeline/io.py`,
`kpi_pipeline/context.py`, `kpi_pipeline/html_report.py`, `README.md`

**Date:** 2026-10-01

#### HTML report: readable per-quarter and per-half like-for-like blocks

Each quarter / half block of the Comparable (Like-for-Like) section now has its own heading
(`Q1 · Like-for-like`, `H1 · Like-for-like`) and is separated from the next by a rule and extra
spacing. Table cells stay center-aligned.

**Affected:** `kpi_pipeline/html_report.py`

**Date:** 2026-10-01

#### Comparable pairs: `yoy` and `quarter` kinds, `grain` toggle

`comparable_pairs.kinds` (default `["ytd"]`) extends like-for-like comparisons beyond YTD-only:
`yoy` recomputes metrics over pairs present in every year of the run window on the full window
year (not just the YTD-elapsed subset), chained across every consecutive year pair; `quarter` is
computed independently per quarter number, restricted to years where that specific quarter has
fully elapsed. `comparable_pairs.grain` (default `"product_store"`, existing behavior) adds a
`"product"` option that restricts the same-pairs population to `product_id` alone, letting every
store of a qualifying product through, independent of `defined_scope.grain`.

**Affected:** `config.py`, `kpi_pipeline/comparable.py`, `kpi_pipeline/context.py`,
`kpi_pipeline/io.py`, `kpi_pipeline/html_report.py`

**Date:** 2026-09-19

#### `item_family_rollup["defined_scope"]`

Fourth toggle (default `False`) alongside the existing `daily_data`/`lost_sales`/
`inventory_warehouse` rollups — rolls a raw scope source's `product_id` to parent id before use.
Off by default since a scope source may already be pre-rolled upstream; available as a safety net
for a client whose isn't.

**Affected:** `config.py`, `kpi_pipeline/scope.py`

**Date:** 2026-09-19

#### `defined_scope.backfill_leading_gap` (`product_store_week` grain)

If a pair's earliest recorded scope week starts later than the report window's own start,
backfills the leading gap by assuming the pair was in scope from the window's start (default
`True`) — same "min date in window" principle `dc_in_stock_rate`'s per-pair grid already uses for
DC. Only the leading gap is filled; later starts, mid-window gaps, and end dates are honoured
exactly as recorded. No effect under `product`/`product_store` grain.

**Affected:** `config.py`, `kpi_pipeline/scope.py`

**Date:** 2026-09-19

#### Complete-period filtering for the Quarter and Monthly trend tabs

New `fiscal.complete_fiscal_periods` drops any `(Year, Fiscal_Quarter)`/`(Year, Fiscal_Month)` row
from the value-trend tabs that hasn't fully elapsed within `[EFFECTIVE_REPORT_START_DATE,
REPORT_END_DATE]` — an in-progress trailing quarter/month (e.g. one week into a 13-week quarter)
no longer renders next to full prior periods. Purely additive on top of the existing
`REPORT_END_DATE` (last completed Saturday) — the Weekly tab is unaffected, since a partial week
never existed there. **Live-impact:** the already-deployed report's Quarter/Monthly tabs will stop
showing an in-progress trailing row once this ships.

**Affected:** `kpi_pipeline/fiscal.py`, `kpi_pipeline/kpi_long.py`, `kpi_pipeline/context.py`

**Date:** 2026-09-19

### 🔄 Changed

#### YTD's elapsed-window check switched from quarter-grain to month-grain

`available_fiscal_quarters` → `available_fiscal_months`: YTD now sums every fiscal month that has
fully elapsed for the latest year (applied identically to every year for an apples-to-apples
comparison), instead of only whole elapsed quarters. Quarter-grain understated YTD whenever the
"current" quarter was in progress but had one or more of its own months already closed — which is
virtually always true, since `REPORT_END_DATE` (a week boundary) essentially never lands on a
quarter boundary.

**Affected:** `kpi_pipeline/fiscal.py`, `kpi_pipeline/kpi_long.py`, `kpi_pipeline/context.py`

**Date:** 2026-09-19

### 🐛 Fixed

#### `defined_scope` NATIVE path bypassed the fiscal calendar regardless of `USE_FISCAL_CALENDAR`

The `year_col`/`week_col` NATIVE path (an alternative to `date_col` for `product_store_week`
grain) was chosen purely on `date_col is None`, unlike every other source's own native-week path
(`daily_data`, `lost_sales_source`), which is explicitly gated on `USE_FISCAL_CALENDAR=False`. A
fiscal-mode config with `date_col` unset would have silently trusted a possibly-mismatched week
numbering instead of reconciling against `fiscal_cal`/`fiscal_week`. Now raises a clear config
error if fiscal mode is on and `date_col` is unset.

**Affected:** `kpi_pipeline/scope.py`

**Date:** 2026-09-19

#### Quarter-completeness checks were reading from an already-clipped calendar

`ctx.fiscal_week` is clipped to `[EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]`, so both the
brand-new `quarter` comparable-pairs guard and the pre-existing, already-deployed
`available_fiscal_quarters` YTD mechanism were comparing week bounds that could never exceed the
window by construction — making their "is this period complete" checks trivially true regardless
of whether the period was really complete. The in-progress "current" quarter was always silently
treated as fully elapsed. Fixed by re-reading the fiscal calendar unclipped specifically for this
check (`fiscal.complete_fiscal_periods`), and rewiring both call sites onto it. Caught by an
adversarial review pass before the `quarter` comparable kind shipped; the pre-existing YTD bug was
found as a direct consequence while fixing it.

**Affected:** `kpi_pipeline/fiscal.py`, `kpi_pipeline/comparable.py`

**Date:** 2026-09-19

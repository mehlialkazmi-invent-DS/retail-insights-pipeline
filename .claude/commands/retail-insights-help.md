---
name: retail-insights-help
description: >-
  Operate, configure, extend, troubleshoot, and answer questions about the
  retail-insights-pipeline — a PySpark retail KPI pipeline for Databricks.
  Covers onboarding, config editing, scope modes, metric computation, the
  roots/cuts report structure (dimension_sources as named population tabs
  like NVROUT/COMP, slices as breakdowns within every root), fiscal calendar
  column mapping, operation scope, blocked scope (per-metric blocked_scope.metrics),
  the in-stock method (instock.method: daily / weekly_source / lost_sales_source),
  gated goods in transit (goods_in_transit),
  report_end=complete_month / latest_day, half periods, HTML report, output saves,
  comparable pairs, performance patterns, and how to add/change anything.
  Use when a user asks to: set up, modify, run, debug, explain, or extend this
  KPI pipeline; configure it with AI; onboard to it quickly; or understand what
  a specific metric means.
---

# Retail Insights Pipeline — Operate, Configure, Extend

Toolkit root: `retail-insights-pipeline/`  
**Always edit `config.py` first. Never change pipeline internals to alter behaviour.**

---

## 1. Quick onboarding (5-minute start)

**`config.py` in this repo is a generic reference template, not any specific customer's deployed config** — every optional feature (`scope_adjustments`, `dimension_sources`, `lost_sales_ensemble`, `instock` methods `daily` / `weekly_source`, `comparable_pairs`, `dc_instock`, `blocked_scope`, `goods_in_transit`) ships disabled with a placeholder example. Every config keeps ALL sections and keys, switched off when unused; the merged sections `instock`, `goods_in_transit` and `blocked_scope.metrics` replace the former per-feature keys (old key -> new key table in the CHANGELOG). Onboarding a customer: copy its structure, replace every placeholder with that customer's own values — don't assume any of them are safe defaults. `tbretail_config.py` (repo root's parent directory) is a real, fully-wired-up deployed config for tbretail specifically — diff against it to see what an actual customization looks like.

If you are a new DS picking this up for the first time:

1. **Upload to Databricks** — same folder: `main.ipynb`, `config.py`, entire `kpi_pipeline/`.
2. **Open `config.py`** and change at minimum:
   - `reporting_window.as_of_date` → today or the last Sunday
   - `path_segments.defined_scope` → path to your instock scope table
   - `defined_scope.*_col` → column names in that table
3. **Run `main.ipynb`** top to bottom. Cell 1 prints resolved settings. Cell 2 previews raw inputs. The **Scope debug** cell reports distinct product/store counts per slice. Cell 3 runs the pipeline.
4. **Check the HTML report** written next to the notebook (Cell 6). It has a Metric Details tab explaining every metric.
5. **If something looks wrong**, check the Troubleshooting section (§8) or ask me what each config key does.

---

## 2. Architecture at a glance

```
config.py          → materialize(fund.paste) → settings dict
main.ipynb         Cell 1: config summary
                   Cell 2: input previews (defined_scope or operation scope, lost_sales, daily_data)
                   (before Cell 3): scope debug — distinct product/store counts per slice
                   Cell 3: runner.run() — full pipeline (+ scope adjustment logs)
                   (after Cell 3): scope summary display
                   Cell 4: save plan preview (when save_outputs=True)
                   Cell 5: Delta write
                   (unnumbered): kpi_long sample, YoY / YTD comparisons,
                                 comparable pairs (ytd/yoy/quarter/half), scope diff
                   Cell 6: HTML report

kpi_pipeline/
  context.py       KPIContext dataclass — shared state
  runner.py        KPIRunner: build_dimensions → build_scopes → build_kpis →
                              build_comparisons → build_comparable_pairs →
                              build_scope_comparison → build_html_report
                   run.mode=html_only → apply_report_end_mode + load_saved_outputs +
                   build_fiscal_week_only (html_only also re-infers cut_dimensions/
                   root_definitions from the loaded kpi_long's own columns, since
                   fiscal.build_fiscal_and_products does not run)
  fiscal.py        fiscal_cal + fiscal_week frames; product attributes + cut dimensions;
                   dimension_sources left-join → ROOT columns (not cuts, see §3.4);
                   _resolve_root_definitions: root_values (named) or auto-discovery
                   (one root per distinct value) from real data;
                   fiscal_calendar.column_map (quarter_col/month_col/month_name_col) —
                   each optional/auto-detected, derived when a client's fiscal_cal lacks it;
                   available_fiscal_months (elapsed-month set for YTD, from the latest year);
                   complete_fiscal_periods (per-(Year,period) fully-elapsed set for the Quarter/
                   Half/Monthly trend tabs, dict keyed by Fiscal_Quarter/Fiscal_Half (only when
                   half_periods)/Fiscal_Month — see §3.1);
                   apply_report_end_mode (report_end=complete_month cut, run first in
                   build_dimensions / run_html_only — §3.1a; "as_of" and "latest_day" return
                   unchanged);
                   build_latest_day_windows (report_end=latest_day: K = fiscal day of
                   REPORT_END_DATE, ctx.ytd_years, ctx.ytd_lost_sales_last_week, ctx.day_calendar
                   — §3.1b); complete_fiscal_periods also at "Year" / "Week" grain under
                   latest_day (LATEST_DAY_COMPLETE_PERIOD_COLUMNS);
                   require_complete_time_grain (fail-loud: every date in the window must be in the
                   calendar)
  inputs.py        cached Delta reads (daily_data_raw, lost_sales_weekly_base) + input_filters;
                   prints the source date range for daily_data/lost_sales on every read
  scope.py         defined scope, operation scope (scope_source), score scope (hybrid), manual
                   adjustments, build_blocked_days (blocked_scope) — §3.2a/b
  scope_debug.py   scope_universe_counts: pre-flight distinct product/store counts per slice
                   (all active_slice_dimensions, root-defining columns included — this is a
                   raw diagnostic, unaware of the root/cut split the KPI step applies)
  pipeline.py      build_pipeline_frames: scoped_daily, inst_data, lost_base, dc_daily, dc_inst
                   per scope (dc_inst is dc_in_stock_rate's expanded inventory grid, gated by
                   dc_instock.enabled -- see README's "dc_instock" config reference);
                   build_instock_daily builds inst_data from noob/daily-data when
                   instock.method='daily' (§3.2c); build_scoped_daily flags blocked days (is_blocked)
                   and, with a store metric in goods_in_transit.inventory_metrics, full-outer-joins
                   store goods in transit to the daily rows first (has_daily_row / git_quantity —
                   §3.2d);
                   build_dc_daily does the same for DC goods in transit (has_inventory_row);
                   _goods_in_transit_quantity is the shared GIT reader (the in-stock
                   _goods_in_transit_days derive from it); under report_end=latest_day the
                   week containing day K is split into two parts (_fiscal_week_parts,
                   last_day_index) and lost_base keeps weeks up to the last Saturday
  metrics.py       compute_kpis: sales, WOS, mean_stock, instock, weighted_instock_rate, WOS_DC/WOS_TOTAL, dc_mean_stock/total_mean_stock, dc_in_stock_rate; each group on its own, real rows (has_daily_row / has_inventory_row) on-hand unless it is in goods_in_transit.inventory_metrics; each metric reads or drops blocked rows per blocked_scope.metrics (conditional aggregation, §3.2b)
  kpi_long.py      build_kpi_long: loops root × cut × annual/ytd/quarter/half/monthly/weekly →
                   pandas. Roots = "overall" + ctx.root_definitions; cuts = "overall" +
                   ctx.cut_dimensions, applied identically within every root. Reuses
                   _filter_frames_for_dimension for BOTH root population restriction and a
                   cut's own value_filters. kpi_long gains a "root" column (see §3.4c).
                   trim_periods_to_recent: trims each period type to N most recent.
                   _period_frames under report_end=latest_day: Annual / Weekly keep complete
                   periods only, YTD = days 1..K of ctx.ytd_years (_ytd_latest_day_frames)
  comparisons.py   YoY / YTD + build_scope_diff, root × cut aware throughout (comparison_yoy/ytd
                   carry a "root" column). YTD compares the SAME elapsed-window across years
                   (not sequential), chained across every consecutive year pair present — see
                   _consecutive_year_pairs. No QoQ/MoM/WoW comparison table exists — the
                   Quarter/Monthly/Weekly period tabs show value trends only.
  comparable.py    build_comparable_pairs: like-for-like ytd/yoy/quarter/half, root × cut aware.
                   ONE pair universe PER KIND (ytd/yoy: intersection across EVERY year in the
                   window; quarter/half: per quarter/half number, across years where that period is fully
                   elapsed — NOT restricted per root, computed once against the overall
                   population), shared by every consecutive-year link within that kind — see
                   rebuild_comparable_kind_from_saved_rows. Store-side grain (product_store vs
                   product) is comparable_pairs.grain, independent of defined_scope.grain — §3.6
  io.py            incremental Delta saves, save plan, load_saved_outputs (html_only),
                   recompute comparisons from merged kpi_long history. TABLE_ROW_KEYS now
                   includes "root" everywhere dimension/dimension_value appears. Under
                   report_end=latest_day an existing "ytd" row of kpi_long / comparable_kpi_long
                   is always replaced on merge (merge_table_incremental).
  html_report.py   standalone HTML renderer — root → period → dimension → value tabs when
                   more than one root exists (Metric Details becomes a peer of the root tabs);
                   a single root (no root-producing dimension_sources) renders exactly as
                   before, period → dimension → value, Metric Details a peer of period tabs.
                   Dimensions inferred from kpi_long via ctx.cut_dimensions. All table cells
                   centered; html_report.root_labels renames root tabs and
                   html_report.dimension_labels renames dimension tabs (display only);
                   _tab_label capitalizes every tab label; Metric Details text for in_stock_rate / blocked days is built
                   from settings (_settings_metric_definitions).
```

---

## 3. Configure with AI — decision guide

When a user wants to configure the toolkit, ask them (or read from their message):

### 3.1 Reporting window

```python
"reporting_window": {
    "as_of_date": "YYYY-MM-DD",   # run anchor → last completed Saturday on or before this date
    "run_min_date": "YYYY-MM-DD", # optional narrow start (Sunday-aligned); null/"" = full YTD from Jan 1
    "report_end": "as_of",        # "as_of" (default) | "complete_month" | "latest_day" — see §3.1a / §3.1b (env KPI_REPORT_END)
}
```

**Common setups:**
- Full YTD: `run_min_date: null`
- Last week only: `run_min_date` = this week's Sunday
- Multi-year backfill: `run_min_date` = Jan 1 of earliest year

**Fiscal calendar vs native time grain** (`fiscal_calendar.use_fiscal_calendar`):
- `True` (default): Year/Week/Quarter/Month come from `one_time_uploads/fiscal_cal`, via `fiscal_calendar.column_map`:
  ```python
  "column_map": {
      "quarter_col": "Quarter",       # "Q1" -> Fiscal_Quarter. Absent -> ceil(Fiscal_Month/3).
      "month_col": "Month",           # "M01" -> Fiscal_Month. Absent -> calendar month of week start.
      "month_name_col": "month_name", # display label e.g. "August", shown verbatim on Monthly tab.
                                       # Absent -> derived (see below).
  }
  ```
  Each entry is independently optional/auto-detected: read when the column exists on `fiscal_cal`, derived when absent or set to `None`. This matters because **a client's fiscal month/quarter number does not necessarily match the real calendar month/quarter** — e.g. tbretail's fiscal year runs Feb–Jan, so fiscal month 07 has been observed spanning real 8/2–8/29. Feeding that number straight into a month-name lookup (`calendar.month_abbr[7]` → "Jul") would be **wrong** whenever a client's fiscal calendar is offset like this. So when `month_name_col` is absent, the Monthly tab instead derives its display label from the **majority real calendar month by day count** across each fiscal month's actual dates (`html_report._build_month_display_labels` / `fiscal._build_fiscal_week_frame`) — correct regardless of the offset, and a no-op (reduces to the trivial case) on a calendar-aligned fiscal year.
- `False`: derived from `noob/daily-data` (`fiscal_calendar.daily_time_columns`, which has only `date`/`week` keys), read via `get_daily_data_raw` — the same cached, config-filtered read every other daily_data consumer uses, so `input_filters.daily_data` applies here too. **Year is the calendar year of `date`** (`F.year(date)`); **Week is the native fiscal week column**; Quarter/Month are both derived from the real calendar month the same way as the fiscal-calendar fallback above (they ARE the real calendar values here, so this is exact, not approximate). Year is deliberately never read from a raw source year column — it can carry the ISO week-year, mislabeling late-December weeks as the next year (e.g. Dec 2025 shown as `2026`), which previously mismatched the Quarter/Month derived from `date` (Dec 2025 → "Q4 2026"). Caveat: a fiscal week straddling Jan 1 now appears as two partial weeks (one per calendar year) in the Weekly view; quarter/month/annual rollups stay correct.

**Complete periods only — Quarter/Monthly trend tabs (`REPORT_END_DATE` itself is UNCHANGED, this is additive).** The Quarter/Monthly `kpi_long` `period_type` rows (value-trend tabs, not YTD — see below) previously showed whatever data existed up to `REPORT_END_DATE`, including an in-progress trailing quarter/month (a 1-week-old "quarter" plotted next to full 13-week ones). Now a `(Year, Fiscal_Quarter)`/`(Year, Fiscal_Month)` only appears once **fully elapsed on both edges** — not truncated at the trailing edge, and not truncated at the window's own start either (matters when `run_min_date` doesn't land on a period boundary). Mechanics: `fiscal.complete_fiscal_periods(ctx, period_col)` (cached per `Fiscal_Quarter`/`Fiscal_Month` on `ctx.complete_fiscal_periods`, set in `fiscal.build_fiscal_and_products`) is semi-joined onto every metric frame by `kpi_long._drop_incomplete_periods`, called from both `"quarter"`/`"monthly"` branches of `_period_frames`. Weekly needs no equivalent — `REPORT_END_DATE` already guarantees a whole trailing week.

**Real pre-existing bug this fixed — plus a follow-up grain fix.** `fiscal._compute_available_fiscal_months` (YTD's own "which periods count" set, in production for a while as quarter-grain, since switched to month-grain) previously read week bounds from `ctx.fiscal_week`, which is itself clipped to `[EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]` — so its completeness check was trivially true for any period present in the window at all, and never actually excluded an in-progress period. Now fixed: it delegates to the same unclipped `complete_fiscal_periods`. (`comparable.py`'s `_complete_period_years` — §3.6 — already did this correctly and was the template for the fix.) **Separately, the grain itself moved from quarter to month** (`available_fiscal_quarters` → `available_fiscal_months`; `kpi_long._with_ytd_filter` now filters `Fiscal_Month` membership, not `Fiscal_Quarter`) — quarter-grain understated YTD whenever the "current" quarter was in progress but had one or more of its own months already closed (virtually always, since `REPORT_END_DATE` essentially never lands on a quarter boundary). **Live impact:** both changes affect what the already-deployed report shows, not just future runs — a currently-visible partial trailing quarter/month row disappears from a freshly computed run, and YTD's own elapsed-period selection may shift since it's now genuinely checking completeness at the finer month grain. Under `save_mode="incremental"` + default `allow_overwrite_existing=False`, a stale partial-period row saved by a run *before* this fix stays in the merged Delta history (the new run doesn't reproduce that merge key, so nothing appends over or removes it) — `allow_overwrite_existing=True` or `full_refresh` to clear it. Whether a given run's own output changes at all depends on whether `reporting_window.as_of_date` happens to land on a fiscal period boundary in that customer's `fiscal_cal` upload — not something config alone can answer.

### 3.1a `report_end`, half periods, calendar checks

**`reporting_window.report_end`** — `"as_of"` (default, today's behaviour) keeps `REPORT_END_DATE` at the last completed Saturday. `"complete_month"` cuts it back to the last day of the most recent fully elapsed month on or before that Saturday (`fiscal.apply_report_end_mode`, first call in `KPIRunner.build_dimensions` and `run_html_only`; it overwrites `settings["REPORT_END_DATE"]`, so scope, daily in-stock, blocked days, comparisons, comparable, HTML header all follow). YTD sums the complete months of every year; annual is year-to-date through the cut. `OUTPUT_RUN_DATE` still follows `as_of_date`.
- **Fiscal calendar** (`use_fiscal_calendar=True`): month = fiscal month from the **unclipped** `fiscal_cal` upload. The upload must extend **past** the as-of Saturday or the run raises (`fiscal_cal upload ends X; it must extend past REPORT_END_DATE ...`), and `run_min_date` must reach the start of the cut month or it raises (`no month ends between ...` when none ends in the window; `the cut month ... starts before the report start` when the cut month begins before `EFFECTIVE_REPORT_START_DATE` — fiscal: the month's `period_start`, civil: the 1st) — so the weekly-refresh pattern (narrow `run_min_date`) does not work with it.
- **No fiscal calendar** (`use_fiscal_calendar=False`): calendar month. The cut is usually mid-week; `kpi_long._drop_partial_trailing_week` drops the clipped trailing Weekly column. Months are bucketed by each week's start date, so it is a calendar-month **cut**, not exact calendar-month totals.
- Incremental saves: with `incremental` + `allow_overwrite_existing=False`, switching from `as_of` to `complete_month` keeps the `as_of` versions of the current annual row and later weekly rows in history — use `full_refresh` for the switch (tbretail does).
- HTML filename: `html_report.filename` is a template (`{customer}`, `{report_end}` only; anything else raises in `materialize()`), kept raw in `HTML_REPORT_FILENAME_TEMPLATE` and formatted by `KPIRunner.build_html_report` with the cut date; `HTML_REPORT_OUTPUT_DIR` is the optional datastore folder (from `output_path_segments`).

### 3.1b `report_end = "latest_day"` (YTD to the latest day, complete periods elsewhere)

tbretail's mode (generic default stays `"as_of"`). `REPORT_END_DATE` is `as_of_date` itself — set in `materialize()` (`_resolve_report_window`), so scopes, blocked days, the daily in-stock read, the daily / DC reads and the HTML filename all see it; `fiscal.apply_report_end_mode` returns unchanged. Set `as_of_date` to a day daily-data has reached (the daily in-stock latest-date check stays); the `fiscal_cal` upload must extend past it. **Requires `instock.method='daily'`** (`materialize()` raises otherwise: the YTD cut splits a fiscal week of the daily in-stock frame, which weekly in-stock sources cannot do).
- **YTD** = days 1..K of every fiscal year in `ctx.ytd_years`, the same fiscal day for every year, K = day of the fiscal year of `REPORT_END_DATE` (1-based; fiscal: first date of each year from the UNCLIPPED `fiscal_cal` upload; civil: day of the calendar year). A year qualifies only when its first date is inside the window (`run_min_date` must reach the start of the first fiscal year to show; the run prints `YTD years` and the ones left out).
- **Annual** = complete fiscal years only (the current year is in YTD only), so YoY compares complete years. **Quarter / Half / Monthly** = complete periods as before. **Weekly** = whole weeks only (the trailing partial week is dropped and takes no `weekly_display_weeks` slot). Mechanism: `complete_fiscal_periods` at `"Year"` / `"Week"` grain, applied by `kpi_long._drop_incomplete_periods` in `_period_frames` (fiscal: unclipped bounds; civil: Jan 1–Dec 31 and week start + 6).
- **Lost sales** only reaches the last Saturday on or before `REPORT_END_DATE`: `build_pipeline_frames` keeps `lost_base` weeks with `week_end_date <= that Saturday` in every view; YTD uses weeks 1..`ctx.ytd_lost_sales_last_week` (fiscal week of that Saturday, 0 when it is in the previous fiscal year) for every year. Numerator and denominator both come from `lost_base`.
- **YTD cut mechanics** (`fiscal.build_latest_day_windows`): `ctx.ytd_through_day` (K), `ctx.ytd_years`, `ctx.day_calendar` (date, Year, Week, `day_index`, `last_day_index`). `scoped_daily` / `dc_daily` carry `day_index` → YTD `day_index <= K`. The pair-week frames `inst_data` / `dc_inst` carry `last_day_index`: the fiscal week containing day K is split into the days `<= K` and the days after (`pipeline._fiscal_week_parts`, `build_dc_inst` grouping on `last_day_index`), each part's stocked / available / blocked / unusable days counted exactly, so YTD takes `last_day_index <= K` and every other view sums both parts. WOS's part week: `scoped_daily` also carries `week_days` (7 outside this mode) and the YTD frames replace it with the days up to K, so `compute_kpis` weights that week's average inventory by `week_days / 7` (§6.1); `build_scope_diff`'s Annual table goes through `_period_frames(..., "annual")` too, so it holds complete fiscal years like the Annual tab.
- Comparisons (`comparisons.py`, `comparable.py`) read these frames / kpi_long rows, so YoY = complete fiscal years, YTD = same-fiscal-day windows (comparable `ytd` years = `ctx.ytd_years`), quarter / half = complete periods.
- Incremental saves: `ytd` rows of `kpi_long` / `comparable_kpi_long` are always replaced on merge (their window moves with `as_of_date`); other period types merge as usual. After switching a deployment to `latest_day` run one `full_refresh` (old partial-year Annual rows would otherwise stay in history).
- HTML: header gets a **Period basis** card; the Lost Sales % definition states the last-Saturday basis, and the WOS family definitions state that YTD's last week counts only its elapsed days.

**`fiscal_calendar.half_periods`** (default `False`; tbretail `True`; env `KPI_HALF_PERIODS`): adds `Fiscal_Half` (H1 = fiscal quarters 1-2, H2 = 3-4) on the fiscal week frame and every metric frame, a `half` `kpi_long` period type labelled `2025-H1` (complete halves only, same unclipped rule as quarters), a **Half** tab (`html_report.half_display_halves`, default 4; env `KPI_HTML_HALF_HALVES`) and the `half` comparable kind (§3.6). The Fiscal_Half completeness set is only computed when it is on; `comparable_pairs.kinds` containing `"half"` without it is rejected.

**Calendar completeness (fail-loud).** `fiscal.require_complete_time_grain` raises `time grain (...) is missing N date(s) between START and END: [first 20]` when any date in the window is absent from the calendar. On the civil path the calendar is only the dates present in `noob/daily-data` (after `input_filters.daily_data`), so a gap means missing source data.

### 3.2 Scope mode

```python
"scope": {
    "use_hybrid_scope": True,   # True = defined + score backfill; False = defined only
    "run_scope_diff": False,    # True = compute score scope and defined-vs-score annual diff
}
```

Use `use_hybrid_scope=True` (hybrid) unless the client has pristine defined scope coverage.

**`run_scope_diff`** (default `False`): when `False`, score scope is **not** computed unless hybrid backfill needs it (`use_hybrid_scope=True`). The notebook scope-diff cell and `scope_diff` Delta output are skipped. Set `True` to run the defined-vs-score annual KPI comparison (sanity check; its years are the Annual tab's, so with `report_end="latest_day"` complete fiscal years only).

Score backfill parameters (hybrid or scope diff):
```python
"score_scope": {
    "min_percentile": 0.2,         # keep weeks where weekly_sales ≥ p20 AND inventory ≥ p20 per pair
    "min_weeks_for_filter": 2,     # skip filter for pairs with ≤ this many weeks (avoids over-filtering new items)
}
```
Inventory for the score filter is the **last available daily snapshot in the fiscal week** (`max_by(inventory, date)`), not Saturday-only — avoids false zero when `week_end_date` is missing from daily data.

### 3.2a `scope_source` (operation scope)

```python
"scope_source": {
    "mode": "defined_scope",   # "defined_scope" (default) | "operation_scope"
    "solution_id": 21,         # also the solution of the blocked_scope snapshot (int, not bool)
    "run_date": None,          # Sunday "YYYY-MM-DD"; None = latest Sunday on or before TODAY
    "roll_to_family_main": True,
    "instock_main_eligible_only": False,  # True: in-stock only where the main itself is eligible
    "instock_exclude_unsuperseded_sizes": False,  # True: in-stock leaves out sizes not in a supersession
    "active_only": True,
}
```

**`instock_main_eligible_only`** (tbretail `True`, generic `False`): in-stock (and weighted in-stock, which uses the same frame) counts only the stores where the main item itself is eligible — a store where only a superseded (sub) item is eligible was intentionally not assorted the new item, so it leaves in-stock (client rule). Those stores stay in every other metric (sales, revenue, inventory, WOS, turnover, lost sales), so all volume and inventory is captured. The pair's start is still the earliest start of the main and sub rows there. Built from `ctx.operation_scope_pairs.main_eligible` (`scope._scope_start_pairs`) and applied in `pipeline.build_instock_daily`; scope additions are not operation-scope pairs and are kept. Requires `mode = "operation_scope"`, `roll_to_family_main = True` and `instock.method = "daily"`.

**`instock_exclude_unsuperseded_sizes`** (tbretail `True`, generic `False`): in-stock leaves out the sizes "not created in the supersession" — a product in no `item_family` row whose class color (`products.option_code`) has at least one size in `item_family` (as main or sub). The client treats these like NGF: out of in-stock, still in sales, revenue, inventory, WOS, turnover and lost sales (`pipeline._unsuperseded_sizes`, applied in `build_instock_daily`). Requires `instock.method = "daily"`.
`operation_scope` builds the scope ONCE (`scope._operation_scope_pairs`) from `path_segments.scope` (`operation/scope`): the solution's rows for one `run_date` still open on it (`end_date` null or `>= run_date`), reduced to `(product_id, store_id)` (`scope._scope_start_pairs`, kept on `ctx.operation_scope_pairs`): with `roll_to_family_main`, every row is rolled to its family main (`coalesce(parent_id, product_id)`; a product without a main keeps its own id), so a store where only a sub-item is in scope gets the main, and each pair keeps its **earliest** `start_date` as `scope_start`; `active_only` keeps `is_active = true`. Every metric uses this one scope. Grain `product` or `product_store` only (`product_store_week` rejected; daily in-stock and blocked days need `product_store`); the `defined_scope` table is not read.
- `scope_adjustments`, `input_filters.daily_data` and `metrics.population_filters` still apply; **`input_filters.defined_scope` does NOT** (it is only read in `defined_scope` mode).
- `run_date=None` uses today's Sunday, not the as-of date — a backdated run is not reproducible; set `run_date` for backfills.
- **Scope additions are not operation-scope pairs:** they skip the family roll-up / active filter, have no `scope_start`, receive no blocks, and a product-only addition (`store_col=None`) becomes every store with a daily-data row in the window. This widens every metric: additions are part of the one scope all metrics use, in-stock included (counted from their first daily row, with no family roll-up, active or blocked-scope filter).
- tbretail: `operation_scope`, solution 21, rolled to the family main, active only, its additions kept on (user decision).

### 3.2b `blocked_scope`

```python
"blocked_scope": {
    "ui_parameters_path": None,        # path under the datastore root; None = OFF, set = ON
    "rule": "after_scope_start",       # or "all"
    "dc_solution_id": None,            # int (not bool, e.g. 22; tbretail 22): DC blocks of that solution
    "metrics": "all",                  # "all" or a list from METRICS_ALL: the metrics that DROP blocked days
}
```
Reads `{ui_parameters_path}/blocked_scope/{product,product_destination,destination}` (parquet; `destination_id` = store) for `scope_source.solution_id`; requires `scope_source.mode="operation_scope"`. Rule `after_scope_start`: a block applies to a pair only if `block.start_date >= scope_start` (same day applies; an earlier block is ignored — the pair was set up again after it); `all` applies every matched block. An applied block covers the pair's days from `start_date` to `end_date` (null = open-ended), clipped to the report window (`ctx.blocked_days`, `scope.build_blocked_days`). Block `product_id`s are NOT rolled to the family main: only blocks on the main's own `product_id` apply, as in the client reference script (`scope._applied_block_days`, shared with the DC blocks — see §6.1b). A missing `blocked_scope/<kind>` folder fails the run. Pairs added by `scope_adjustments` are never blocked. Always set `ui_parameters_path` explicitly (the newest snapshot may hold no blocks for the solution).
`dc_solution_id` (None = no DC blocks) does the same for `{ui_parameters_path}/dc_blocked_scope/<kind>` of that solution (`destination_id` = warehouse); each DC pair's `scope_start` comes from `operation/scope` of that solution (same `run_date`, family roll-up, earliest start and `active_only` as `scope_source`); DC pairs outside it get no blocks (`ctx.dc_blocked_days`, `scope.build_dc_blocked_days`). Requires `ui_parameters_path`.

**`metrics` is the per-metric gate** (indexed directly; unknown name raises; `settings["BLOCKED_SCOPE"]["metrics"]` is the resolved list in `METRICS_ALL` order). Blocked days are FLAGGED, not removed: `build_scoped_daily` / `build_dc_daily` add `is_blocked` by one left join of the blocked days. A metric NAMED in `metrics` reads only unblocked rows; a metric not named reads blocked and unblocked rows alike. `"all"` (generic default) = exactly the old behaviour of turning blocked scope on. Per family:
- sales group (`total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `distinct_*_count`): each metric its own gate on the real daily rows (`AUR`/`AUC`: numerator and units both);
- `total_inventory`, `mean_stock` (+ retail / cost), `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate` (its sales units and its mean stock): each its own gate (WOS-type metrics also on the sales they divide by); the store part of `total_mean_stock` / `WOS_TOTAL` by that metric's own gate; a blocked day drops its goods in transit too (GIT is joined before the flag);
- DC: `dc_mean_stock`, `WOS_DC` and the DC part of `total_mean_stock` / `WOS_TOTAL` drop DC blocked days by that metric's gate;
- `in_stock_rate`: `build_instock_daily` leaves blocked store-days, on-hand days and GIT days out only when named; `weighted_instock_rate` uses that same frame and its sales weights drop blocked days when it is named;
- `dc_in_stock_rate`: `build_dc_inst` leaves DC blocked days out of stocked / available days only when named (`dc_unblocked_days` keeps the unblocked count for `comparable.py`);
- `lost_sales_pct`: `daily_for_lost` (the sales half of its denominator) drops blocked days only when named; the weekly lost-sales numerator never changes.
`metrics.compute_kpis` keeps ONE aggregation pass per frame and family with conditional aggregation (`_reads` / `_read_only`: `F.when(~is_blocked, x)` inside sum / avg / countDistinct; `_day_stock` per-day `<metric>_day` / `<metric>_has` columns so a day whose rows are all blocked is not a day of a gated average). If no sales metric reads blocked rows (all named) they are left out of the sales rows, so a period × slice with blocked days only gets no output row, as before. The comparable-pairs universe stays real, unblocked rows whatever the gate; `scope_diff` and the scope debug are unchanged. The HTML Metric Details note about blocked days appears only on named metrics. **Not filterable per day:** weekly sources (`lost_sales_source`, `instock.method='weekly_source'`).
- tbretail: store solution 21, DC solution 22, rule `after_scope_start`; `metrics` = `in_stock_rate`, `weighted_instock_rate`, `dc_in_stock_rate`, `total_inventory`, `mean_stock` (+ retail / cost), `dc_mean_stock`, `total_mean_stock`, `WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`, `inventory_turnover_rate`; sales units / revenue, `AUR`, `AUC`, the distinct counts and `lost_sales_pct` keep blocked days (a blocked pair can still sell its existing stock).

### 3.2c `instock` (where `in_stock_rate` comes from)

```python
"instock": {
    "method": "lost_sales_source",   # "daily" | "weekly_source" | "lost_sales_source" (tbretail: "daily")
    "daily": {                       # read only for method "daily"
        "count_start": "first_daily_row",   # generic default; "scope_start" | "earliest" need operation scope
        "require_daily_data": True,    # drop pairs with no daily row (required by first_daily_row)
        "history_start": None,         # first day searched for a pair's first daily row; None = window start
        "usable_only": True,           # unusable (usable != 1) days leave numerator AND denominator, once
        "input_filters": [],           # Spark SQL on product_id / store_id only, narrows the in-stock pair universe
    },
    "weekly_source": {               # read only for method "weekly_source"; same keys as lost_sales_source + path_segments
        "path_segments": None, "week_col": "week_start_date", "product_col": "product_id", "store_col": "store_id",
        "in_stock_col": "in_stock", "total_days_col": "total_days", "product_agg_level_col": None,
        "fallback_sources": [],
    },
}
```
Both sub-sections are in every config (all keys present, indexed directly in `materialize()`); only the one `method` names is used. `settings["INSTOCK_METHOD"]` (env `KPI_INSTOCK_METHOD`), `INSTOCK_DAILY["enabled"]`, `INSTOCK_SOURCE_ENABLED`. `materialize()` raises: unknown `method` (lists the allowed); `weekly_source.path_segments` empty under `"weekly_source"`; `"daily"` with `defined_scope.grain="product"`; `"daily"` / `"weekly_source"` together with `lost_sales_ensemble.enabled`; `daily.count_start` outside its three values (checked whatever the method), `scope_start` / `earliest` without operation scope, `first_daily_row` without `require_daily_data`; `history_start` after the window start; `report_end="latest_day"` unless `"daily"`; `goods_in_transit.store_instock` unless `"daily"`.
- **`"daily"`** (`pipeline.build_instock_daily`), per scope pair, every scoped pair counting (scope additions included: no `scope_start`, so they count from their first daily row, never blocked): (1) `daily.input_filters` narrow the pair universe; (2) daily-data read WITHOUT `input_filters.daily_data` (that list usually holds `usable = 1`), filtered on the raw date column from `history_start` to `REPORT_END_DATE` (so Delta file pruning applies), not family-rolled here (noob/daily-data is already rolled upstream), restricted to the pairs with blocked days removed ONLY when `in_stock_rate` is in `blocked_scope.metrics`, then cached; the run raises unless daily-data's latest date for those pairs reaches `REPORT_END_DATE`; (3) count start per `count_start`, clipped to the window start, `require_daily_data` drops pairs without a daily row; (4) store-days = count start → `REPORT_END_DATE`, a day without a row is out of stock, minus blocked (when removed) and (with `usable_only`) unusable days; (5) in-stock day = `inventory > 0` on a usable day, OR (with `goods_in_transit.store_instock`) a day with store goods-in-transit (`operation/goods_in_transit`, `destination_type=0`, `quantity>0`, rolled to the family main; snapshot dated D+1 = end of day D, shifted by `goods_in_transit.date_shift_days`) — a union, never a sum. Output has the old weekly `inst_data` shape (`stocked_pairs` / `available_days`), so metrics, `population_filters` and comparable work unchanged. Requires a store-level scope grain. The HTML Metric Details text for In-Stock Rate is generated from these settings.
- **`"weekly_source"`**: read in-stock from a separate table (see §3.9). **`"lost_sales_source"`**: `lost_sales_source`'s own `in_stock_col` / `total_days_col`.
- tbretail: `"daily"`, `count_start="earliest"`, `history_start="2024-01-21"`, ECOM stores 829/639/917 excluded from in-stock (`daily.input_filters`; and, via `lost_sales_source.sales_filter` and the report_dfu model, lost sales — nowhere else), NON-COMP excluded from in-stock only (`population_filters.in_stock_rate`), scope additions (JAB, NGF products, `nvrout_scope_backfill`) counted like every other pair; `weekly_source` (report_dfu) stays configured for a switch back; `dc_instock` off; `wos_revenue`, `weighted_instock_rate`, `dc_in_stock_rate` removed from `metric_cols`.

### 3.2d `goods_in_transit`

```python
"goods_in_transit": {
    "date_shift_days": None,        # None = GIT off everywhere; int (not bool): snapshot dated D+1 = end of day D (tbretail -1)
    "roll_to_family_main": True,    # roll every GIT read to the family main
    "store_instock": False,         # daily in-stock day = on-hand > 0 or store GIT > 0 (needs instock.method "daily")
    "dc_instock": False,            # DC in-stock grid day also counts DC GIT
    "inventory_metrics": [],        # any of INVENTORY_GIT_METRICS_ALL (config.py), each on its own
}
```
All five keys required. `materialize()` raises on: non-int / bool `date_shift_days`; `date_shift_days=None` with `store_instock` / `dc_instock` True or a non-empty `inventory_metrics`; `store_instock` without `instock.method="daily"`; an unknown name in `inventory_metrics`; a non-empty `inventory_metrics` without `use_fiscal_calendar=True`. Settings key `GOODS_IN_TRANSIT` (same five keys, `inventory_metrics` in canonical order); the name sets live in `context.STORE_GIT_METRICS` / `DC_GIT_METRICS`. `roll_to_family_main` (default `True`) rolls every `operation/goods_in_transit` read to the family main in `pipeline._goods_in_transit_quantity`; set `False` only for a source already rolled upstream.
Gated and per metric: a metric named in `inventory_metrics` uses on-hand + goods in transit (retail = units × `price_without_tax`, cost = units × `cogs`, rounded like `inventory_retail` / `inventory_cost`); every other metric keeps on-hand only and its exact previous value. Available (`INVENTORY_GIT_METRICS_ALL`): `total_inventory`, `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `dc_mean_stock`, `total_mean_stock`, `WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`, `inventory_turnover_rate`; pick any subset. Store metrics count store GIT, `dc_mean_stock` / `WOS_DC` DC GIT, `total_mean_stock` / `WOS_TOTAL` both; `inventory_turnover_rate` counts it in its mean stock. Report metrics are picked in `metrics.metric_cols` from `METRICS_ALL` (unknown names raise).
- **Store side** (`pipeline.build_scoped_daily` → `_join_store_goods_in_transit`, only when a metric in `context.STORE_GIT_METRICS` is named; otherwise no GIT read and `has_daily_row=True`, `git_quantity=0`). Order: daily data (window-filtered) → store GIT quantity (`_goods_in_transit_quantity`: `destination_type=0`, `quantity>0`, shifted, summed per product×store×day, rolled to the family main, window, scoped pairs) → full outer join on `(product_id, store_id, date)` (daily rows first summed per pair-day so the quantity attaches once; a GIT-only day gets sales / on-hand 0 and `has_daily_row=False`) → drop GIT-only days whose daily row `input_filters.daily_data` removed (`inputs.get_daily_data_excluded_days`: built once per run from the raw table, cached on `ctx.daily_data_excluded_days`, left-joined onto each scope variant's GIT days) → blocked-day flag (flags both kinds of rows) → scope product-week semi-join → calendar / product attributes. The scoped-pair semi-join runs on the daily rows first (it commutes with the blocked-day flag and keeps the GIT join to scoped pairs). No GIT frame is cached: it is used once per scope variant.
- **DC side** (`pipeline.build_dc_daily`, only when a metric in `context.DC_GIT_METRICS` is named): DC GIT (`destination_type=1`) full-outer-joined to the rolled `inventory_warehouse` rows; GIT-only day → inventory 0, `has_inventory_row=False`; then DC blocked days (`blocked_scope.dc_solution_id`) flagged, then Year/Week + `scope_core` product-week restriction as before. `dc_instock` feeds `build_dc_inst`'s stocked days instead.
- **Metrics** (`metrics.compute_kpis`): sales, AUR/AUC, distinct counts and `weighted_instock_rate`'s sales weights read `has_daily_row` rows only; each inventory group (WOS, mean stock, total mean stock, turnover, DC) builds its own frame in one pass (`_day_stock` / `_day_avg`): a named metric averages `inventory + git_quantity` over every day it has a row on, a metric not named averages on-hand over the days with a real row (its exact previous value); joined on the period keys as before. `population_filters` still apply per group.
- **Never changed:** sales, lost sales (`daily_for_lost` filters `has_daily_row`); the daily in-stock and DC in-stock have their own `store_instock` / `dc_instock` day unions. `comparable.py` builds the pair universe from real rows only. The HTML Metric Details of a named metric says it counts store / DC goods in transit.
- tbretail: shift `-1`, `roll_to_family_main`, `store_instock`, `dc_instock` on, every inventory metric in `inventory_metrics`.

### 3.3 Defined scope column mapping

```python
"defined_scope": {
    "product_col": "product_id",
    "store_col": "store_id",       # set None for product-week scope (no store grain)
    "date_col": "week_start_date", # DATE path: date → fiscal_cal → Year/Week
    "year_col": None,              # NATIVE path: set year_col + week_col instead of date_col
    "week_col": None,
    "backfill_leading_gap": True,  # product_store_week only, see below
}
```

**⚠️ NATIVE path (`year_col`/`week_col`) risk — and now enforced.** Unlike `date_col`, the native path takes `Year`/`Week` verbatim from the source table — never reconciled with `fiscal_cal`/`fiscal_week`. If `year_col` follows ISO week-year numbering instead (late-December rows carrying the next year), the join silently mismatches and those weeks vanish from scope with no error. **`build_defined_scope` now raises loudly if `use_fiscal_calendar=True` and `date_col` is unset** — the NATIVE path is only valid under `use_fiscal_calendar=False`, matching how `daily_data`/`lost_sales_source` already gate their own native-week paths on the same setting (`defined_scope` was the one source that didn't check it, until this fix). Only use the NATIVE path when the source has no date column at all AND fiscal mode is off, and verify `year_col` is a true calendar year first. Same risk applies to `year_col`/`week_col` in `scope_adjustments` entries (not gated the same way — verify manually there).

**`backfill_leading_gap` (`product_store_week` grain only, default `True`).** If the scope source's own earliest available week (across every pair) starts later than the report window's own start, that gap is a data-availability limit of the source itself, not a per-pair signal — only the pairs tied to that earliest week are backfilled, assumed in scope from the window's start, same "min date in window" principle `dc_in_stock_rate`'s per-pair grid uses (§6.1b) — never a hardcoded floor date. A pair whose own first-seen week is later still (later than the source's earliest week, not merely later than the window start) is left untouched — a real new store/product, not a leading-gap artifact. Only the LEADING gap is filled; later starts, mid-window gaps, and end dates are honoured exactly as recorded. `product`/`product_store` grains are unaffected (they already apply every pair to the whole window). Set `False` only for a deployment with **existing** `product_store_week` history saved before this option existed — turning it on there mixes two scope definitions in one incrementally-merged table.

**`item_family_rollup.defined_scope`** (default `False`, see §12 constraint list): rolls a raw scope source's `product_id` to parent id before use, same mechanism as `daily_data`/`inventory_warehouse`'s own rollup. OFF by default since a scope source may already be pre-rolled upstream (tbretail's is) — opt-in for a client whose isn't.

Goods in transit has its own roll-up toggle, `goods_in_transit.roll_to_family_main` (§3.2d).

### 3.3a Scope debug (pre-flight product/store counts)

Before Cell 3's full run, the **Scope debug** cell in `main.ipynb` sanity-checks scope size and per-slice coverage — read-only, distinct from `runner.run()`:

```python
runner.build_dimensions()
runner.build_scopes(fund_paste=fund.paste)
display(runner.scope_debug_summary())
```

`scope_debug_summary()` → `kpi_pipeline.scope_debug.scope_universe_counts(ctx)` returns distinct `product_id`, `store_id`, and pair counts for the **final scope** (after hybrid backfill + adjustments): one `overall` row plus one row per active slice dimension value (`slices`, `derived_dimensions`, enabled `dimension_sources`). It applies the same `value_filters` as the KPI step, so counts match `kpi_long` per slice. Product-week scope (no `store_col`) shows only `distinct_product_count`. NULL slice values show as `"NULL"` here vs blank/None in `kpi_long`. `build_dimensions`/`build_scopes` are idempotent; Cell 3 rebuilds the same scope. Skipped in `html_only` mode (no scope is built).

### 3.4 Slice dimensions → roots and cuts

Every report row belongs to a **root** (which population) and a **cut** (how that population is broken down). There are two config surfaces, and which one you use depends on **where the column lives**, not on whether you want a root or a cut — `slices` is always cuts, `dimension_sources` is always roots:

#### 3.4a — `slices` (columns from master-data/products) → CUTS

Use this when the column already exists on (or is derivable from) the products table. Applied identically **within every root**, including the always-present `"overall"` root.

```python
"slices": {
    "dimensions": ["brand"],          # existing column names in master-data/products
                                      # add multiple: ["brand", "category"]
    "derived_dimensions": {           # Spark SQL expressions against the products schema
        "price_tier": "CASE WHEN price_without_tax < 50 THEN 'budget' ELSE 'premium' END"
    },
    "value_filters": {},              # restrict which values of a cut appear in its own breakdown
}
```

- Derived expressions are validated at runtime; failures are **skipped with a warning** (unlike dimension sources, which fail loudly).
- `value_filters`: applied only to that cut's own breakdown — Overall and other cuts are unaffected. Accepts a LIST (include-only: `[]` = non-null, `["A"]` = only A) or a DICT (`{"include": [...]}`, `{"exclude": [...]}` keeps the rest incl NULL, optional `keep_null`).
  - dim omitted → keep all values, including `NULL` (default)
  - `[]` → keep all non-null values (drops only the `NULL` bucket)
  - `["A", "B"]` → keep only those values (drops `NULL` and unlisted values)
  - Example: `{"brand": ["NIKE", "ADIDAS"]}` or `{"brand": []}` to drop a `NULL` brand bucket.

#### 3.4b — `dimension_sources` (columns from other tables) → ROOTS

Use this when a breakdown column does **not** live on the products table (e.g. NVROUT from `operation/extended_product`). Each enabled source is left-joined onto the product attribute projection, and **every one of its columns becomes a root** — a fully-broken-out population, like kpi-skill-toolkit's NVROUT/COMP major tabs — not a flat cut.

```python
"dimension_sources": [
    {
        "enabled": True,
        "label": "extended_product",
        "source": "delta",                    # "delta" | "csv"
        "path_segments": ["operation", "extended_product"],
        "join_key": "product_id",             # must be a column on products
        "columns": [],                        # raw source columns to carry over
        "derived": {
            # Spark SQL over the SOURCE table's columns → new ROOT dimension(s)
            "is_nvrout": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END",
        },
        "root_values": {"is_nvrout": {"yes": "nvrout"}},  # root "nvrout" = is_nvrout=='yes' only
    },
    # Add more sources as needed — one dict per external table
    {
        "enabled": False,
        "label": "another_table",
        "source": "delta",
        "path_segments": ["operation", "another_table"],
        "join_key": "product_id",
        "columns": ["some_flag"],
        "derived": {},
    },
]
```

**`root_values`: `{dim_col: {raw_value: root_name}}`**
- Omit a `dim_col` (or omit `root_values` entirely) → **auto-discovery**: one root per distinct value actually found in the data, named after the value itself (resolved at runtime in `fiscal._resolve_root_definitions`, since it needs real data — not something `materialize()` alone can determine).
- `{"yes": "nvrout"}` → exactly one root, named `"nvrout"`, restricted to rows where `is_nvrout=='yes'`. `'no'` and `NULL` are **not** their own root and only appear under `"overall"`.
- `{"yes": "a", "no": "b"}` → two named roots, one per listed value.
- A misconfigured column (not among that source's own `columns`/`derived`) fails loudly in `materialize()`.

**Key rules:**
- Do NOT also list a `dimension_sources` column in `slices.dimensions` — it becomes a root automatically, and roots and cuts are mutually exclusive by design.
- The source is deduplicated to **one row per join_key** before the join — pre-aggregate your source if the raw table has multiple rows per product.
- **Enabled sources fail loudly** on bad path / missing column / bad expression (by design — a silently dropped segment would misreport).
- **NULL behaviour**: products absent from the source get `NULL` (left join). For a clean yes/no split, ensure the source covers the full product universe, use `fillna`, or accept that `NULL` products never get their own root value regardless.
- **Overlapping segments** (e.g. COMP includes NVROUT): model as independent dimension columns (`is_nvrout`, `is_comp`), each with its own `root_values` entry — a product can belong to both roots.
- **`value_filters` on `dimension_sources` no longer exists** — it was superseded by `root_values`. A dimension_source column is never a cut, so a value_filter on it could never fire.
- CSV sources honour the same `location` (`datastore` / `workspace`) and `csv_options` as scope adjustments.

#### 3.4c — Roots and cuts: the full model

- **Roots** = `"overall"` (always, unrestricted) + one per configured `root_values` entry (e.g. `"nvrout"`, `"comp"`). A client with no root-producing `dimension_sources` gets a single `"overall"` root — the report looks exactly as it did before roots existed.
- **Cuts** = `"overall"` (the root's own total, no further breakdown) + each `slices.dimensions`/`derived_dimensions` entry (e.g. `brand`, `SMW`) — applied identically within every root.

With `root_values: {"is_nvrout": {"yes": "nvrout"}}` and `slices.dimensions: ["brand"]`, `kpi_long` has: `overall`×`overall` (grand total), `overall`×`brand` (brand across everything), `nvrout`×`overall` (NVROUT total), `nvrout`×`brand` (brand within NVROUT only) — mirroring kpi-skill-toolkit's `overall_annual_segment` / `nvr_all_annual_brand` outputs. `kpi_long`, comparisons (`comparison_yoy`/`comparison_ytd`/every `comparable_comparison_{ytd,yoy,quarter}` all carry a `"root"` column), and the HTML report (root becomes an outer tab when more than one root exists) are all root × cut aware. See §7 for the exact loop and §9 for `ctx.cut_dimensions`/`ctx.root_definitions`.

**Scope vs. roots vs. cuts vs. population filters — four different knobs:**
| Need | Use | Effect |
|------|-----|--------|
| Include/exclude which (product, store, week) rows enter KPIs at all | `scope_adjustments` | Changes scope **membership** |
| A named, fully-broken-out population tab (NVROUT vs COMP) | `dimension_sources[].root_values` | Adds a **root** |
| Break any root's population out by a dimension (brand, SMW) | `slices` | Adds a **cut**, applied within every root |
| Narrow ONE metric's own population, inside every root/cut | `metrics.population_filters` | Restricts that **metric only** (see §3.4d) |

#### 3.4d — Metric population filters (restrict ONE metric's own population)

`metrics.population_filters` narrows a single metric's product population without touching scope, roots, or any other metric — e.g. "in-stock rate should never count NON-COMP products, even inside `overall`" or "WOS without NVROUT." Applied **on top of** whatever root/cut is already in effect — a no-op inside a root that already restricts to the same value (e.g. filtering `IS_COMP` inside the `comp` root), the thing that actually changes a number inside `overall` (which applies no restriction of its own).

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

Shape: `{metric_col: {dim_col: value_filter_spec}}` — `dim_col` is any `dimension_sources`/`slices` column already joined onto products; `value_filter_spec` is the exact same list/dict shape as §3.4a's `slices.value_filters`.

**Constraint — metric_cols computed in one shared aggregation pass (`kpi_pipeline/metrics.py`) can only be filtered TOGETHER**, per `kpi_pipeline/filters.py`'s `METRIC_FILTER_GROUPS`:

| Group | metric_cols |
|-------|-------------|
| `sales` | `total_sales_quantity`, `total_sales_revenue`, `total_inventory`, `AUR`, `AUC`, `distinct_product_count`, `distinct_store_count`, `distinct_pair_count` |
| `wos` | `WOS`, `wos_revenue`, `wos_cost` |
| `wos_dc_total` | `WOS_DC`, `WOS_TOTAL` |
| `mean_stock` | `mean_stock`, `mean_stock_retail`, `mean_stock_cost` |
| `dc_inventory` | `dc_mean_stock`, `total_mean_stock` |
| `turnover` | `inventory_turnover_rate` |
| `instock` | `in_stock_rate` |
| `weighted_instock` | `weighted_instock_rate` |
| `lost_sales` | `lost_sales_pct` |

An entry on any one column in a group applies to the whole group; conflicting specs on two columns in the same group fail loudly at run time. A metric with no entry is unchanged — fully additive/opt-in. Validated in `materialize()` (unknown `metric_col`, or a malformed spec, fails loudly — same check `slices.value_filters` gets).

### 3.5 Output saves

```python
"output": {
    "save_outputs": False,          # True to write Delta tables
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,               # null = reporting_window.as_of_date → run_date=YYYY-MM-DD partition
    "save_mode": "incremental",     # initial | incremental | full_refresh
    "allow_overwrite_existing": False,
    "recompute_comparisons_from_history": True,  # incremental: recompute YoY/YTD from full merged kpi_long
}
```

**Path:** `{bucket}/{path_segments}/{table_name}/run_date={run_date}/` — e.g. `analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`.

`run_date` defaults to `as_of_date`. Each run date gets its own Delta partition; incremental merge reads the **latest existing partition on or before** the current run_date and accumulates onto it.

**Tables saved:**

| Table | Contents |
|-------|----------|
| `kpi_long` | All metrics × periods (annual/ytd/quarter/monthly/weekly) × **root** × cut (see §3.4c) |
| `comparison_yoy` | YoY comparison rows, per root × cut |
| `comparison_ytd` | YTD comparison rows, per root × cut (one row set per consecutive-year pair, elapsed-window sums) |
| `scope_diff` | Defined vs score annual diff (only when `scope.run_scope_diff=True`) |
| `comparable_kpi_long` | ONE shared table across every enabled `comparable_pairs.kinds` entry, tagged by `comparison_type` (`"ytd"`/`"yoy"`/`"quarter"`/`"half"`) + `comparable_pair_count` + `link_prior_year`/`link_current_year` (+ plain `quarter_number` / `half_number` for those rows), per root × cut (only when `comparable_pairs.enabled=True`) |
| `comparable_comparison_ytd` / `_yoy` / `_quarter` / `_half` | One comparison table per enabled kind, per root × cut (`_quarter` additionally keys on `quarter_number`, `_half` on `half_number`) |

No `comparison_qoq`/`comparison_mom`/`comparison_wow` table exists — those aren't comparison kinds at all (only `yoy`/`ytd` are, see §3.5.1); comparable pairs is a separate feature with its own `ytd`/`yoy`/`quarter` kinds (§3.6).

**Merge keys (incremental only)** — defined in `io.py` `TABLE_ROW_KEYS`. **`root` is now part of every key that has `dimension`/`dimension_value`** — without it, the same cut (e.g. `brand`="KNG") under two different roots would collide as if it were one row. Saved history from before this existed lacks the `root` column entirely; the merge fails loudly on that missing key column (by design) rather than silently corrupting old history — re-run and re-save if you hit this.

| Table | Keys |
|-------|------|
| `kpi_long` | `period_type`, `period`, `root`, `dimension`, `dimension_value` |
| `comparison_yoy` / `comparison_ytd` | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `scope_diff` | `Year`, `metric` |
| `comparable_kpi_long` | `comparison_type`, `period_type`, `period`, `root`, `dimension`, `dimension_value`, `link_prior_year`, `link_current_year` (the link tag is required — the same year appears once per adjacent link it participates in, even though every link within a kind shares one all-years-restricted population) |
| `comparable_comparison_ytd` / `_yoy` | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` (same shape as `comparison_yoy`/`comparison_ytd` — no link tag needed here: `current_period` already embeds `current_year`, and each consecutive-year link has a distinct `current_year`, so it can't collide across links) |
| `comparable_comparison_quarter` / `_half` | Same as above **plus `quarter_number` / `half_number`** — needed because a given `current_period` can recur across different quarter numbers' independent year-sets |

**Save modes:**

| save_mode | Behaviour | When to use |
|-----------|-----------|-------------|
| `initial` | Write all rows; **fail** if output tables already exist | First-ever backfill only |
| `incremental` | Load latest prior partition, append new merge keys, skip overlaps (unless `allow_overwrite_existing=True`), write merged result to this run's partition | Weekly refresh — history accumulates |
| `full_refresh` | Overwrite each table entirely with **this run's output only** (no merge with prior history) | Rebuild saved tables for current run window |

**Incremental — how history accumulates:**
- Each weekly run loads the **latest existing `run_date` partition on or before** the current run, appends only new merge keys, and writes the full merged result to this run's `run_date` partition.
- Each `run_date` partition is therefore a self-contained snapshot of the full merged history as of that run.
- Comparison tables (`comparison_yoy`/`comparison_ytd`) are **recomputed from the merged `kpi_long` history** after the kpi_long save, then overwritten wholesale — so a single-week refresh can still produce YoY vs last year. Disable with `recompute_comparisons_from_history: False`.
- With `report_end="latest_day"`, existing `ytd` rows of `kpi_long` / `comparable_kpi_long` are **always replaced** on an incremental merge (their window moves with every `as_of_date`; the save plan counts them as overwrite), whatever `allow_overwrite_existing` says (`io.merge_table_incremental`, `_always_overwrite_period_types`); other period types merge as usual. Switching an existing deployment to `latest_day` needs one `full_refresh` (§3.1b).
- `comparable_kpi_long` is merged incrementally like `kpi_long`; each enabled kind's `comparable_comparison_{ytd,yoy,quarter,half}` is then recomputed from the merged `comparable_kpi_long` (grouped by `link_prior_year`/`link_current_year`, and additionally `quarter_number` / `half_number` for `quarter` / `half` — see `rebuild_comparable_kind_from_saved_rows`). A single-week refresh can still produce a comparable comparison for any enabled kind relative to prior saved history.

**Full refresh details:**
- Replaces the **entire** Delta table — not "update 2026 inside a multi-year table."
- If the run window is 2026 only, saved tables contain 2026 data only; older years are dropped unless included in this run.

**Metadata:** every row gets `_run_as_of` (run's `as_of_date`) and `_saved_at` (UTC write time). On incremental saves, untouched existing rows keep their original metadata.

**Notebook flow:** Cell 3 `run(save=False)` → Cell 4 `preview_save_plan()` → Cell 5 `save_outputs()`. Review the save plan before writing.

**Typical workflows:**

```python
# First backfill
"output": {"save_outputs": True, "save_mode": "initial", "allow_overwrite_existing": False}

# Weekly append — history accumulates via incremental merge
"output": {"save_outputs": True, "save_mode": "incremental", "allow_overwrite_existing": False}

# Replace a re-run week
"output": {"save_outputs": True, "save_mode": "incremental", "allow_overwrite_existing": True}

# Rebuild all saved tables from a full-history run
"output": {"save_outputs": True, "save_mode": "full_refresh", "allow_overwrite_existing": False}
```

### 3.5.1 Selecting which comparisons to run

`comparisons.enabled` chooses which period-over-period comparisons are computed, printed, saved, and rendered — any subset of `yoy`/`ytd` (default: both). There is no `qoq`/`mom`/`wow` comparison kind — dropped for simplicity; the Quarter/Monthly/Weekly period tabs already show recent-period value trends (`kpi_long`, always built in full) without a delta table.

```python
"comparisons": {
    "enabled": ["yoy"],   # only YoY; ytd skipped entirely
}
```

- **`yoy`** — last two full calendar/fiscal years (with `report_end="latest_day"`: the last two **complete** fiscal years — the Annual tab only holds complete years, §3.1b).
- **`ytd`** — each year's **elapsed window** (only the fiscal months fully closed as of `as_of_date` for the latest year — `fiscal.available_fiscal_months` — applied to every year; with `report_end="latest_day"` instead the same fiscal day of every year, days 1..K, §3.1b) vs the prior year's same window, chained across consecutive years. Use instead of `yoy` once the current year is only partially reported, so a partial current year isn't compared against a full prior year.
- Gates the `comparison_{kind}` Delta tables + HTML comparison columns only. `kpi_long` is always built in full, including a `"ytd"` `period_type`. Comparable pairs' own `comparable_comparison_{ytd,yoy,quarter}` tables are gated **independently**, purely by `comparable_pairs.enabled` + `comparable_pairs.kinds` — not coupled to this `comparisons.enabled` selection at all (see §3.6).
- HTML rendering: `ytd`, if more than one consecutive-year pair exists, renders as several stacked mini comparison tables in one panel (one per year-pair) instead of a single table. `yoy` always renders as a single table.
- A latest-week run can still produce e.g. YoY: with `save_mode="incremental"` + `recompute_comparisons_from_history=True`, selected comparisons are rebuilt from the full merged `kpi_long` (this run unioned onto prior saved runs). Needs a prior saved partition.
- Invalid/empty selection fails loudly in `materialize()`. Env override: `KPI_COMPARISONS="yoy,ytd"`.
- Need a quarter-over-quarter or month-over-month percentage change? Compute it from consecutive `kpi_long` rows (`period_type="quarter"`/`"monthly"`) directly — there's no built-in comparison table for it.

### 3.6 Comparable pairs (like-for-like: ytd / yoy / quarter / half)

**Gated, opt-in** (default off). Metrics are recomputed over **only the pairs present in EVERY qualifying year**, then compared. Isolates like-for-like movement from mix shifts caused by new/closed pairs. Four independent kinds, selected via `comparable_pairs.kinds`:

- **`ytd`** — pairs present in every window year, on each year's elapsed (fully-closed-months) window (with `report_end="latest_day"`: days 1..K of the fiscal year, over `ctx.ytd_years` — the years whose days 1..K are all in the window). Chains every consecutive year pair.
- **`yoy`** — pairs present in every window year, on the FULL window year (not the YTD-elapsed subset). Chains every consecutive year pair too (not just the latest two, unlike the regular non-comparable YoY). Window-boundary years can themselves be partial — same accepted behaviour as the regular Annual/YoY tab, not something this corrects for (not under `report_end="latest_day"`: the Annual frames only hold complete fiscal years there).
- **`quarter`** — computed INDEPENDENTLY per quarter number. For quarter Q, only years where Q falls **entirely inside the report window** count (`_complete_period_years` in `comparable.py`) — `REPORT_END_DATE` is a week boundary, never quarter-aligned, so the in-progress "current" quarter would otherwise be silently compared as if complete against a full prior-year quarter. Mirrors the same "fully elapsed" guard `ytd`'s own elapsed-period check already uses (`fiscal.py`'s `available_fiscal_months` — same helper, but at MONTH grain, not quarter grain), generalized here to check both window boundaries for an arbitrary quarter and year. A pair common across years for Q1 says nothing about Q2 — fully independent populations.
- **`half`** — the same as `quarter` per half number (H1/H2); needs `fiscal_calendar.half_periods=True` (rejected otherwise). A pair counts when scoped daily rows exist for it in every qualifying year after all scope steps. Both quarter and half read `ctx.complete_fiscal_periods`.

There is no comparable QoQ/MoM/WoW — those aren't comparison kinds at all (see §3.5.1).

**Pair-level under every `defined_scope.grain`, `"product"` included.** `scoped_daily` carries `store_id` straight from daily-data whatever the scope grain, so comparable always requires the *same product-store pairs* in every year — it does not degrade to a product-only match when the report's own scope is store-agnostic just because `defined_scope.grain="product"`. Don't "restore" a grain-conditional key here: a comparable universe that silently weakens with the SCOPE configuration is the bug this replaced. (That's separate from `comparable_pairs.grain` below, which changes this deliberately, by its own config key.)

**`comparable_pairs.grain` — what "same pair" means, independent of `defined_scope.grain`:**
- `"product_store"` (default) — population is `(product_id, store_id)` pairs present in every qualifying year (the behaviour described above).
- `"product"` — population is `product_id` values present in every qualifying year; every store of a qualifying product is then kept. Store-estate churn is NOT isolated. Use only when a pair-level intersection leaves too small a population.

Only the store-side frames (`scoped_daily`/`inst_data`/`lost_base`/`scope_pairs`/`scope_pair_weeks`) are affected — `dc_daily`/`dc_inst` always keep their own `(product_id, warehouse_id)` universe regardless (no store dimension to drop).

```python
"comparable_pairs": {
    "enabled": True,                       # default False
    "kinds": ["ytd", "yoy", "quarter", "half"],    # default ["ytd"]
    "grain": "product_store",              # default; "product" = same-pair population is product_id alone
    "pair_days": "unblocked",              # or "all": blocked days also make a pair present
}
```

**`pair_days`** (`"unblocked"` default | `"all"`): which days make a pair "present" in a year when the same-pair universe is built. `"unblocked"` counts only real daily-data rows outside blocked days (and DC rows outside DC blocked days); `"all"` counts blocked days too. Goods-in-transit-only days never count. Either way each metric then applies its own `blocked_scope.metrics` gate on the comparable pairs, exactly as on the other tabs. tbretail: `"unblocked"`.

**Gated independently of `comparisons.enabled`** — the old coupling ("comparable requires `ytd` in `comparisons.enabled`") is gone. Each kind's own save/recompute is driven purely by `comparable_pairs.kinds`.

**How it works — one fixed universe per kind, shared by every link within it:**
For `ytd`/`yoy`, the pair universe is computed ONCE, as the intersection across every qualifying year (not per link). With 2024/2025/2026 all present: only pairs present in **2024 AND 2025 AND 2026** count — a pair present in 2025+2026 but missing from 2024 is excluded entirely, from every link. `quarter` applies this same rule **per quarter number independently** (Q1's population only considers years where Q1 is complete; Q2's is separate). The single population is then reused for every consecutive-year link within that kind, so **a given year carries the same metric value in every link it appears in** within that kind. `comparable_kpi_long` rows still carry `link_prior_year`/`link_current_year` (see §3.5 merge keys) purely so incremental save's merge key doesn't collide across links — not because the values themselves differ by link. `quarter` rows additionally carry a plain `quarter_number` column (not a key column — `period_type`/`period` already disambiguate quarter/year combinations).

All metric frames are restricted to this one all-years pair set and metrics recomputed for Overall and every slice (since slice dims are product attributes, no extra per-slice intersections needed).

The restriction key is chosen **per frame**, from the columns that frame actually carries — not once globally:
- `scoped_daily` → `comparable_pairs.grain`'s own key columns: `(product_id, store_id)` under `"product_store"` (default), `product_id` alone under `"product"`.
- `inst_data` / `lost_base` / `scope_pairs` / `scope_pair_weeks` → `(product_id, store_id)` when they have a store dimension (native `lost_sales_source.store_col`, or — for `scope_pairs`/`scope_pair_weeks` — a store-grain scope) **and** `comparable_pairs.grain="product_store"`; otherwise the pair universe's **distinct products**. Collapsing to distinct products first is load-bearing: joining a store-less frame straight onto pair keys would fan its rows out one-per-store and multiply lost sales. Those frames keep product-level values; only their product universe is made like-for-like.
- `dc_daily` / `dc_inst` → each gets its OWN independent `(product_id, warehouse_id)` all-years intersection (two separate universes, not one shared). DC/warehouse inventory has no store dimension, so neither can ever share the store-side keys. Both are item-family-rolled to parent `product_id` (same id space as `scope_core`) and both come from `inventory_warehouse`, but `dc_inst` still doesn't reuse `dc_daily`'s universe: `dc_inst` 0-fills every day from a pair's first stocked day to the end of the report window, so a pair that stopped being stocked partway through still has rows — all stockouts — in later years where `dc_daily` has none. Its per-year universe is a superset of `dc_daily`'s, and reusing that intersection would delete exactly the sustained stockouts `dc_in_stock_rate` exists to surface. See README's "dc_instock" section.

**Outputs:**
- `comparable_kpi_long` — ONE shared table across every enabled kind, tagged by `comparison_type` (`"ytd"`/`"yoy"`/`"quarter"`/`"half"`), + `comparable_pair_count` + `link_prior_year`/`link_current_year` (+ `quarter_number` / `half_number` for those rows).
- `comparable_comparison_ytd` / `_yoy` / `_quarter` / `_half` — one comparison table per enabled kind (same schema as `comparison_ytd`/`comparison_yoy`; the quarter table also keys on `quarter_number`, the half table on `half_number`).
- HTML report — a "Comparable (Like-for-Like)" section, visually divided from the regular comparison table above it: one consolidated wide table on the Annual (`yoy`) and YTD (`ytd`) tabs; **one narrow block per quarter number** on the Quarter tab and **per half number** on the Half tab (a single table mixing them would need too many value/delta columns to stay readable); each block has a heading (`Q1 · Like-for-like`, `H1 · Like-for-like`) and a rule between blocks.
- Notebook — a "Comparable pairs (like-for-like)" cell.

**Single-week run and history:** `comparable_kpi_long` is merged incrementally (same as `kpi_long`). A single-week refresh can still produce a comparable comparison for any enabled kind **relative to prior saved `comparable_kpi_long` history** — even if the current window only spans one week. Recomputation from saved history (`rebuild_comparable_kind_from_saved_rows`) is pure pandas grouped by `link_prior_year`/`link_current_year` (and `quarter_number` for `quarter`) — no Spark recomputation needed, since each link's rows already carry that link's own pair-restricted values. A given kind's comparison is skipped for the current run only when fewer than 2 qualifying years are present *and* there is no saved history covering more (`quarter`: fewer than 2 years where that quarter number is fully elapsed).

### 3.7 Run mode

```python
"run": {
    "mode": "full",   # full = compute from source Delta; html_only = load saved outputs + render HTML
}
```

| mode | Behaviour |
|------|-----------|
| `full` (default) | Full pipeline: scope → KPIs → comparisons → comparable → optional save → HTML |
| `html_only` | Load `kpi_long` and comparison tables from `.../outputs/{table}/run_date={OUTPUT_RUN_DATE}/`; render HTML only — no pipeline compute |

**html_only requirements:**
- Saved outputs must exist at the `run_date` partition under `PATH_OUTPUT_ROOT` (from a prior `save_outputs=True` run).
- `OUTPUT_RUN_DATE` = `output.run_date` or `as_of_date` — set explicitly to load a different snapshot.
- `fund.paste` is still required to resolve output paths.
- Fiscal week frame is loaded for weekly column ordering in the HTML report.
- Slice dimensions are inferred from `kpi_long` (configured order first, then any extras in data).

**Notebook:** Cell 3 calls `runner.run()` which branches automatically. Input preview and save cells are not needed in `html_only` mode.

Environment override: `KPI_RUN_MODE=html_only`

### 3.8 HTML report

```python
"html_report": {
    "enabled": True,                                              # False = skip
    "filename": "kpi_report_{customer}_{report_end}.html",        # template: ONLY {customer} and {report_end}; formatted at write time with the final REPORT_END_DATE
    "report_title": None,                                         # None = "<CUSTOMER> KPI Report"
    "output_path_segments": None,                                 # None = local only; or the datastore FOLDER segments (filename is appended)
    "metric_definitions": {},                                     # override any entry in DEFAULT_METRIC_DEFINITIONS
    "weekly_display_weeks": 5,         # Weekly tab: most recent N weeks; null = all
    "monthly_display_months": 5,       # Monthly tab: most recent N months; null = all
    "quarterly_display_quarters": 5,   # Quarter tab: most recent N quarters; null = all
    "half_display_halves": 4,          # Half tab (only with half_periods): most recent N halves; null = all
    "yearly_display_years": 5,         # Annual tab: most recent N years; null = all
    "root_labels": {},                 # root id -> tab label, e.g. {"comp": "LFL", "nvrout": "NVROUT"}
    "dimension_labels": {},            # slice dimension name -> tab label (display only), e.g. {"brand": "Banner"}; dict of str -> str or materialize() raises
    # No display-count trim setting for the YTD tab yet — it always shows every year present.
}
```

**Root tab (only rendered when more than one root exists — see §3.4c).** With a single root (`"overall"` only, the common case with no root-producing `dimension_sources`), the report renders exactly as it always has — no extra tab layer, period tabs are the outermost level. With more than one root, an outer tab bar appears (one tab per root, e.g. Overall / NVROUT / COMP), each containing its own complete period-tab set below; **Metric Details** moves to be a peer of the root tabs instead of the period tabs in this case (it's root-independent — just metric definitions — so it's never duplicated per root).

Each root's period tabs (seven top-level tabs when it's the only/outermost level and half_periods is on, six without; one fewer plus the shared Metric Details when nested under a root tab):
- **Annual** — KPI table by year + YoY comparison (+ comparable `yoy` when enabled — see §3.6)
- **YTD** — KPI table by year over each year's elapsed (fully-closed-months) window + YTD comparison, stacked one mini-table per consecutive-year pair (+ comparable `ytd` when enabled)
- **Quarter** — KPI table by fiscal quarter, **value trend only** (most recent N quarters, default 5, and only fully-elapsed quarters — see §3.1) — no regular comparison table (+ comparable `quarter`, one narrow table per quarter number, when enabled)
- **Half** (only with `fiscal_calendar.half_periods`) — KPI table by half, **value trend only** (most recent N halves, default 4, complete halves only) (+ comparable `half`, one block per half number, when enabled)
- **Monthly** — KPI table by month, **value trend only** (most recent N months, default 5, and only fully-elapsed months — see §3.1) — no comparison table. Column header text is the real calendar month (e.g. "2026-Aug"), which can differ from the fiscal month *number* underlying the grouping — see §3.1's fiscal-calendar note.
- **Weekly** — KPI table for the **most recent N fiscal weeks** (default 5; sorted by `week_start_date`), **value trend only** — no comparison table
- **Metric Details** — plain-English definition, store scope, and formula for every active metric (single root only; a peer of the root tabs, not the period tabs, when there's more than one root)

Within each period tab, navigation is three levels:
1. **Period** (horizontal) — Annual / YTD / Quarter / Half / Monthly / Weekly
2. **Cut dimension** (horizontal pills) — Overall + every `ctx.cut_dimensions` column present in `kpi_long` for that root (inferred automatically; not hard-coded to brand) — root-defining columns never appear here, only `slices` columns
3. **Cut value** (vertical sidebar) — one clickable tab per value (e.g. each brand); Overall shows a single panel

The executive header shows client, reporting window, scope mode (Hybrid / Operation scope / Defined only), cut dimensions, and generated timestamp.

**Style (global, every client):** all table cells are centered; every tab label has its all-lowercase words capitalized by `_tab_label` (`annual` -> `Annual`, a value tab `jab` -> `Jab`; words with capitals such as `YTD`/`SMW` are kept), so a root without a `root_labels` entry (e.g. `nvrout`) shows as `Nvrout`. `html_report.root_labels` renames root tabs (tbretail: `comp` -> `LFL`, `nvrout` -> `NVROUT`). `html_report.dimension_labels` renames a slice dimension wherever its name is shown (the dimension tabs and the header's slice-dimensions card; tbretail: `brand` -> `Banner`), still capitalized by `_tab_label`; it is display only, so `kpi_long` and the saved outputs keep the raw dimension key (`brand`).

**Metric Details are partly settings-driven:** with `instock.method='daily'` the In-Stock Rate row describes the daily method (on-hand or goods-in-transit, count start, blocked days only when `blocked_scope` is on and `in_stock_rate` is in `blocked_scope.metrics`, plus unusable days) and shows `instock.daily.input_filters` as store scope; with `blocked_scope` on, the sales / inventory / WOS / turnover rows note that blocked days are excluded, only on the metrics named in `blocked_scope.metrics`; a metric named in `goods_in_transit.inventory_metrics` states that its store / DC part counts goods in transit on top of on-hand (`html_report._GIT_METRIC_NOTES`); under `report_end="latest_day"` the Lost Sales % row states the last-Saturday basis, the WOS rows (`WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`) state that YTD's last week counts only its elapsed days, and the header gets a **Period basis** card. `html_report.metric_definitions` still overrides any row.

`kpi_long` itself is never trimmed (see §12.14) — only `ctx.kpi_long_display`, used solely for HTML rendering, is trimmed per the `*_display_*` settings; the saved Delta `kpi_long` always holds the full computed window.

Metric definitions can be customised per-client:
```python
"metric_definitions": {
    "total_sales_revenue": {
        "definition": "Net sales excluding returns, in local currency.",
        "store_scope": "All scoped stores",
        "formula": "Σ(daily_net_sales_revenue)",
    },
}
```

### 3.9 Lost-sales & instock column mapping

Map raw lost-sales and instock table columns to canonical names, or read in-stock from a separate source.

**`lost_sales_source`** — renames lost-sales columns without code changes:
- `week_col`, `product_col`, `store_col`, `lost_sales_col`, `in_stock_col`, `total_days_col` — all default to tbretail's current schema.
- Downstream code always sees canonical names (`week_start_date`, `product_id`, `store_id`, `lost_sales`, `in_stock`, `total_days`).
- Supports dotted nested-struct paths (e.g. `total_days_col: "details.total_days"`).
- `product_col` / `product_agg_level_col` — **configure exactly one, never both.** Native `product_id`-level column present → set `product_col` to it. Keyed by planning/DFU level instead → set `product_col: None` and set `product_agg_level_col` — left-joins `path_segments.product_planning_level` to backfill `product_id`. **`product_col` always wins when set and present on the source**, regardless of whether `product_agg_level_col` is also configured; `product_col: None` with no `product_agg_level_col` fails loudly at read time.
- `store_col: None` (optional): for a store-less source. The scope join **collapses to the source's grain** — `store_id` drops out of the join keys and the deduplicated scope is semi-joined on `(product_id, Year, Week)` — so one product-week stays one row. Required because `lost_sales` is an ABSOLUTE count, not a ratio: fanning it out per scoped store would inflate every later cross-store sum by the store-count factor. `lost_base`/`inst_data` then carry no `store_id`, and `lost_sales_pct` is taken against the product-week sales total so both sides share a population. Residual limit no join can fix: the value spans the product's whole store footprint, possibly wider than a `product_store` scope. Prefer a genuinely per-store source when one exists.
- `sales_filter` (optional, list of Spark SQL expressions): narrows ONLY the daily-data sales forming the other half of `lost_sales_pct`'s denominator (`lost_sales / (sales + lost_sales)`); nothing else changes. Use it when the lost-sales table covers a NARROWER population than `daily_data` — classically an ecom-excluding model (tbretail's `model_id=top_down_excluding_ecom`, direct or via `report_dfu`): the numerator has no ecom lost sales but `daily_data`'s sales still do, inflating the denominator so `lost_sales_pct` reads LOW, worse the bigger the ecom share. E.g. `"sales_filter": ["store_id NOT IN (9001, 9002)"]`. Applied to `scoped_daily` in `build_pipeline_frames` before the weekly rollup; expressions are ANDed; empty (default) is byte-identical to previous behaviour. **Not** interchangeable with the other two knobs: `input_filters.daily_data` fixes the ratio but strips ecom out of `total_sales_quantity`/`mean_stock`/`WOS` too, and `metrics.population_filters` can't reach it at all (applied to `lost_base`, already product-week grain with store sales summed in, and it takes dim/value specs not SQL).
- Defaults produce no behaviour change.

**`instock`** — where `in_stock_rate` comes from (`method`: `"daily"` | `"weekly_source"` | `"lost_sales_source"`, §3.2c). `instock.weekly_source` reads in-stock from a separate table (when your in-stock rate is calculated separately from lost-sales); both sub-sections are in every config and only the one the method names is used:
- `method: "lost_sales_source"` (generic default) — instock/total-days come from `lost_sales_source` (a null `total_days` falls back to the fiscal week's day count).
- `method: "weekly_source"` — reads the `weekly_source.path_segments` table on its own (`read_instock_weekly`) and semi-joins it to scope at its own grain, independent of lost sales. Every in-scope pair-week the in-stock table has counts, even with no lost-sales row; the two only meet at the final per-period aggregate join. A null/zero `total_days` is dropped, never padded. Blocked days cannot be removed (no per-day grain).
- `weekly_source.product_col` / `product_agg_level_col`: same "exactly one, `product_col` wins if present" rule as `lost_sales_source`, above.
- `weekly_source.store_col: None`: restricted to scope at product × week, never fanned out across stores; independent of `lost_sales_source`'s own store grain. Verified example: `reporting_inv_fc_dfu/report_dfu` (product × week only; uses the actual `TY_total_days_instock`/`TY_total_day`, not the simulated `sim_*` columns).
- `weekly_source.fallback_sources` (optional): additional column-sets read from the SAME table, appended in order to fill weeks the primary column-set doesn't have (e.g. `report_dfu`'s `TY_` window only reaches back its own trailing build horizon; `LY_`/`LLY_` carry the identical formula 52/104 weeks earlier and backfill older history). `read_instock_source` left-anti-joins each fallback against everything already covered before unioning — a fallback fills gaps, never overrides a week the primary (or an earlier fallback) already has. Each entry needs its own `week_col`/`in_stock_col`/`total_days_col`; `product_col`/`store_col`/`product_agg_level_col` inherit from the parent block unless overridden.
- `"daily"` and `"weekly_source"` are each incompatible with `lost_sales_ensemble.enabled=True` — the ensemble branch selects in-stock/total-days from the chosen model and uses its `total_days` to decide row existence, neither of which is computed then; `materialize()` fails on the combination.

---

## 4. Editing config.py with AI

**Pattern: read → edit → verify.**

1. Read the current `config.py` value.
2. Propose the change.
3. Apply via StrReplace.
4. Confirm the output of `materialize()` by reading the key in `settings`.

**What to edit → where in CONFIG:**

| Goal | CONFIG key |
|------|-----------|
| Change date window | `reporting_window` |
| Switch hybrid ↔ defined | `scope.use_hybrid_scope` |
| Enable defined-vs-score diff | `scope.run_scope_diff` |
| Change scope table path | `path_segments.defined_scope` |
| Change scope column names | `defined_scope.*_col` |
| Add a brand/category cut (products column) | `slices.dimensions` |
| Add a derived cut (products SQL expression) | `slices.derived_dimensions` |
| Add a named root population (e.g. NVROUT/COMP tab) from another table | `dimension_sources[].root_values` |
| Add multiple external dimension sources / roots | add another dict to `dimension_sources` list |
| Restrict a cut's values / drop NULL bucket | `slices.value_filters` |
| Restrict/rename which values become their own root | `dimension_sources[].root_values` (omit for auto-discovery — see §3.4b) |
| Narrow one metric's own population (e.g. instock without NON-COMP, WOS without NVROUT) | `metrics.population_filters` — see §3.4d |
| Change a client's fiscal_cal upload column names | `fiscal_calendar.column_map` (`quarter_col`/`month_col`/`month_name_col`) |
| Filter inputs | `input_filters.{defined_scope,lost_sales,daily_data}` |
| Blend fast/slow-mover lost-sales models by product velocity | `lost_sales_ensemble.enabled: True` (+ `slow_path_segments`, `speed_cluster_path_segments`, `speed_cluster_format`, `speed_cluster_attribute_name`/`speed_cluster_value_col`, `fast_mover_clusters`) |
| Map lost-sales table columns to different names | `lost_sales_source` (week_col, product_col, store_col, lost_sales_col, in_stock_col, total_days_col) |
| Match `lost_sales_pct`'s sales denominator to an ecom-excluding lost-sales source | `lost_sales_source.sales_filter` — see §8 |
| Read in-stock rate from a separate table | `instock.method: "weekly_source"` (+ `instock.weekly_source.path_segments` + column keys) |
| Build in-stock from daily-data | `instock.method: "daily"` (+ `instock.daily.*`, §3.2c) |
| Choose which metrics drop UI-blocked days | `blocked_scope.metrics` (`"all"` or a list, §3.2b) |
| Count goods in transit on inventory metrics / in-stock days | `goods_in_transit` (`date_shift_days`, `inventory_metrics`, `store_instock`, `dc_instock`, §3.2d) |
| Add a scope addition from Delta | `scope_adjustments.additions` (multiple entries supported) |
| Add a scope removal from CSV/Delta | `scope_adjustments.removals` (multiple entries supported) |
| Enable comparable (like-for-like) pairs | `comparable_pairs.enabled: True` |
| Enable DC in-stock rate (requires `path_segments.item_family`) | `dc_instock.enabled: True` — see §6.1b / README's "dc_instock" |
| Enable output saves | `output.save_outputs: True` |
| Change output path | `output.path_segments` |
| Change run_date partition | `output.run_date` (default `as_of_date`) |
| Control comparison history recompute | `output.recompute_comparisons_from_history` (default `True`) |
| Toggle HTML report | `html_report.enabled` |
| HTML only from saved data | `run.mode: "html_only"` |
| Change HTML title | `html_report.report_title` |
| Override a metric definition | `html_report.metric_definitions` |
| Weekly columns shown in HTML | `html_report.weekly_display_weeks` (default 5; `null` = all) |
| Monthly columns shown in HTML | `html_report.monthly_display_months` (default 5; `null` = all) |
| Quarterly columns shown in HTML | `html_report.quarterly_display_quarters` (default 5; `null` = all) |
| Yearly columns shown in HTML | `html_report.yearly_display_years` (default 5; `null` = all) |

---

## 5. Extending the pipeline

### Add a metric

1. Add Spark aggregation to `compute_kpis` in `kpi_pipeline/metrics.py` (join to existing `keys`).
2. Add to `CONFIG["metrics"]["metric_cols"]` and `CONFIG["metrics"]["labels"]`.
3. Optionally add to `scope_diff_metrics` (scope diff) and `pp_change_metrics` (pp change instead of %) — mirror what an existing similar metric already does, don't add speculatively.
4. Optionally add a definition to `CONFIG["html_report"]["metric_definitions"]` for the Metric Details tab (or `html_report.py`'s `DEFAULT_METRIC_DEFINITIONS`/`_CAT`/`_fmt` if it's a repo-level default metric, not a per-customer override).
5. If the new metric shares an aggregation pass with existing columns, add it to the right group in `kpi_pipeline/filters.py`'s `METRIC_FILTER_GROUPS` (or a NEW group if it shouldn't inherit an existing group's population-filter config) so `metrics.population_filters` can target it.
6. Update `comparisons.py`'s `_format_metric_value`/`_format_change` if the metric needs formatting different from the fallback (`f"{value:,.2f}"`).

### Add a new output table

1. Add a pandas DataFrame field to `KPIContext` in `context.py`.
2. Populate it in `runner.py` (e.g. inside `build_kpis`).
3. Add the table name + row key columns to `TABLE_ROW_KEYS` in `io.py`.
4. Add it to `_output_frames` in `io.py`.

### Add a new scope source

Add a path to `path_segments` in config, read in `fiscal.py` or a new module, and merge into `hybrid_scope_keys` in `scope.py` with a distinct `scope_origin` label.

### Add a new input frame

For a genuinely new source table (not just a new column off an existing frame) that needs its own scope restriction and its own metrics — e.g. `dc_daily` (DC/warehouse inventory) added for `dc_mean_stock`/`total_mean_stock`/`WOS_DC`/`WOS_TOTAL`:

1. **`context.py`**: add a cached-raw-read field, e.g. `my_source_raw: Optional[DataFrame] = None`, mirroring `daily_data_raw`. Reset it in `runner.py`'s `_reset_run_caches`.
2. **`inputs.py`**: add `read_my_source(...)` (Delta read + `_input_filters` + `_print_date_range`) and a cached accessor `get_my_source_raw(ctx)` mirroring `read_daily_data_source`/`get_daily_data_raw`.
3. **`config.py`**: add a `path_segments` entry, resolve its `PATH_...` in `materialize()`'s `paths = {...}` dict, and add an `input_filters` entry.
4. **`pipeline.py`**: add a `build_my_frame(ctx, scope_core, ...)` function that restricts the raw read to the SAME in-scope population as everything else for that scope/root (left-semi against `scope_core` or a projection of it — never an independently-scoped universe), joins `ctx.fiscal_cal`/`ctx.fiscal_week` for time-grain columns and `ctx.product_dims`/`ctx.products_attr` for slice dimensions. Call it from `build_pipeline_frames` and add the result to the returned dict under a new key.

   **If the source has no store dimension (like `inventory_warehouse`), don't collapse the semi-join key to `product_id` alone.** `scope_core` always carries `Year`/`Week` (`ctx.scope_keys` includes them for every grain), and under `product_store_week` grain, scope membership genuinely varies by week — a product can be in scope for some weeks and not others. Attach `Year`/`Week` to your new frame's rows (via the fiscal calendar join) **before** the scope semi-join, and restrict on `(product_id, Year, Week)`, not `product_id` alone — otherwise the new frame's data leaks into weeks the product was already out of scope. This exact bug existed in `build_dc_daily` (fixed 2026-09-16) — see §12.19.
5. **`kpi_long.py`**: add the new key to `_period_frames`'s branches (quarter/half/monthly/ytd, and `_PERIOD_METRIC_FRAMES` for the complete-period and weekly-drop filters) and to `_VALUE_FILTERED_FRAMES` so root/cut/slice filtering reaches it too.
6. **`metrics.py`**: `compute_kpis`/`build_kpi_table` pass frames as explicit positional args (not a generic passthrough) — add a new explicit parameter to `compute_kpis` and thread `frames["my_frame"]` through `build_kpi_table`'s call to it.

---

## 6. Metric reference

| Metric | Label | Scope | Notes |
|--------|-------|-------|-------|
| `total_sales_revenue` | Sales Revenue | All stores | Sum of daily sales revenue |
| `total_sales_quantity` | Sales Units | All stores | Sum of daily sales quantity |
| `AUR` | AUR | All stores | Revenue ÷ Units |
| `AUC` | AUC | All stores | Cost ÷ Units |
| `total_inventory` | Total Inventory | All stores | Sum of daily inventory units across the period (+ goods in transit when `total_inventory` is in `goods_in_transit.inventory_metrics`, §3.2d) |
| `distinct_product_count` | Distinct Products | All stores | COUNT DISTINCT product_id |
| `distinct_store_count` | Distinct Stores | All stores | COUNT DISTINCT store_id |
| `distinct_pair_count` | Distinct Pairs | All stores | COUNT DISTINCT (product_id, store_id) |
| `mean_stock` | Daily Stock Avg (units) | All stores | AVG of daily summed inventory (+ goods in transit when gated by `mean_stock`, also on `_retail` / `_cost`) |
| `mean_stock_retail` | Daily Stock Avg Retail | All stores | AVG of daily summed inventory at retail |
| `mean_stock_cost` | Daily Stock Avg Cost | All stores | AVG of daily summed inventory at cost |
| `WOS` | WOS (units) | All stores | product × fiscal week; all scoped stores aggregated; sales-weighted weekly→period rollup (inventory + goods in transit when gated by `wos`, also on `wos_revenue` / `wos_cost`) |
| `wos_revenue` | WOS Revenue | All stores | product × fiscal week; revenue-based rollup |
| `wos_cost` | WOS Cost | All stores | product × fiscal week; cost-based rollup |
| `inventory_turnover_rate` | Inventory Turnover Rate | All stores | Sales Units ÷ Mean Stock for the period; HTML shows tab-appropriate label (mean stock includes goods in transit when gated by `inventory_turnover_rate`) |
| `in_stock_rate` | In-Stock Rate | All stores | Σ(in_stock_days) ÷ Σ(available_days); pp-change in comparisons |
| `weighted_instock_rate` | Weighted In-Stock Rate | All stores | Sales-weighted average of weekly in-stock rates; pp-change in comparisons |
| `lost_sales_pct` | Lost Sales % | All stores | 100×Σ(lost_sales)÷Σ(floor(sales+lost_sales)); pp-change. Numerator from `lost_sales_source`, `sales` from `daily_data` — narrow the sales half with `lost_sales_source.sales_filter` when the source covers a narrower population (e.g. ecom-excluding) |
| `dc_mean_stock` | Daily DC Stock Avg (units) | DC/warehouse only | AVG of daily summed DC inventory, in-scope product population (+ DC goods in transit when gated by `dc_mean_stock`) |
| `total_mean_stock` | Daily Total Stock Avg (units) | All stores + DC | AVG of daily summed (store + DC) inventory; store part follows the `mean_stock` gate, DC part the `dc_mean_stock` gate |
| `WOS_DC` | WOS (DC) | DC/warehouse only | product × fiscal week; DC-inventory-based rollup, same grain/weighting as WOS (+ DC goods in transit when gated by `wos_dc`) |
| `WOS_TOTAL` | WOS (Total) | All stores + DC | product × fiscal week; (store + DC)-inventory-based rollup, same grain/weighting as WOS; store part follows the `wos` gate, DC part the `wos_dc` gate |
| `dc_in_stock_rate` | DC In-Stock Rate | DC/warehouse only | Gated (`dc_instock.enabled`); Σ(dc_stocked_days) ÷ Σ(dc_available_days) over an EXPANDED `inventory_warehouse` grid (product×warehouse×date, item-family-rolled to parent), each pair running from its own first stocked day to `REPORT_END_DATE` with gaps 0-filled as stockouts — so a pair that stops being stocked keeps accruing stockout days, unlike `dc_mean_stock`/`WOS_DC`; a pair never stocked in the window is absent; `null` column when disabled |

**One population per source (never break):** every daily-data metric — sales, `total_inventory`, `mean_stock`, `WOS`, turnover, weighted-instock's sales weights — is computed from a **single** `daily_scoped` frame in `compute_kpis`, under every `defined_scope.grain`, so two metrics in one output row never describe different populations, except per `blocked_scope.metrics` (a metric not in the list also reads the blocked days of the same rows). `compute_kpis` previously built a second, narrower frame for the stock metrics (a leftover of a removed service-store exclusion); don't reintroduce one. Lost sales and in-stock come from their own source and are restricted separately at that source's grain; DC (`dc_daily`/`dc_inst`, backing `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL`/`dc_in_stock_rate`) comes from `inventory_warehouse`, item-family-rolled onto parent `product_id` (via the shared `pipeline._get_inventory_warehouse_parent_rolled` helper) BEFORE restriction — same id space as `scope_core`, which is already parent-rolled at its own source. Per-metric `population_filters` are the only sanctioned exception. With goods in transit on, `scoped_daily` / `dc_daily` also hold goods-in-transit-only days (`has_daily_row` / `has_inventory_row` False): sales, distinct counts, weighted-instock's sales weights and every metric NOT in `goods_in_transit.inventory_metrics` read the real rows only (so they are what they were without the feature), while a gated metric reads all rows with on-hand + `git_quantity` (§3.2d). With blocked scope on, the same frames also keep their blocked days (`is_blocked`): each metric reads or drops them per `blocked_scope.metrics`, inside the same single frame (§3.2b). Never let a goods-in-transit-only row into sales, in-stock, lost sales or the comparable pair universe, and never let a blocked day into the comparable pair universe.

**Critical formula constraints (never break):**
- WOS grain is **product × fiscal week**, not product×store×week. Three steps: (1) sum daily inventory/sales across all scoped stores → product×date, (2) weekly WOS = `avg_daily_inventory / weekly_sales` at product×fiscal week, (3) sales-weighted rollup to the reporting period. Never divide period totals directly.
- In-Stock Rate uses `available_days` from lost-sales output, not from daily data.
- Lost Sales % denominator = corrected demand (sales + imputed lost), not sales alone.
- `mean_stock` = average of daily totals (not average of weekly averages).
- `in_stock_rate` null handling: `F.greatest(F.lit(0.0), ratio)` floors to 0 when `available_days=0` — never 1.0.

### 6.1 WOS computation grain (important)

Scope is product×store×week, but WOS in `metrics.py` is **not** computed at that grain. Implementation (`metrics.py`, `compute_kpis`):

1. **product × date** — `groupBy(product_id, Year, Week, date)` sums inventory and sales across all scoped stores for each product-day.
2. **product × fiscal week** — within each week, take `avg(daily_total_inventory)` and `sum(daily_sales)`; weekly WOS = `avg_daily_inventory × week_days/7 / weekly_sales` (units, revenue, or cost variant). `week_days` is the calendar days of that fiscal week in the view (`scoped_daily.week_days`, `pipeline._with_week_days`): 7 for a whole week, so the factor is 1 and nothing changes — except under `report_end="latest_day"`, where YTD ends on day K and its last week has 1-6 days (`kpi_long._ytd_latest_day_frames` swaps in `fiscal.week_day_counts`' `ytd_week_days`); that part week then counts as the fraction of a week of inventory its days cover against the sales of the same days. Without the factor it would add a full week of inventory to the numerator for a part week of sales (about +3% on a 26-week YTD whose last week has one day, more early in the year). The same factor applies to `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`.
3. **period rollup** — sales-weighted average of weekly WOS values: `Σ(weekly_wos × weekly_sales) ÷ Σ(weekly_sales)` (= `Σ(avg_daily_inventory × week_days/7) ÷ Σ(weekly_sales)`).

Do not confuse scope grain (product×store×week) with WOS computation grain (product×fiscal week after store aggregation).

### 6.1a WOS (DC) / WOS (Total)

`WOS_DC` and `WOS_TOTAL` are twins of `WOS` (units) and follow the **identical** product-week-first, sales-weighted-rollup grain — this is deliberate, so nobody re-introduces the "sum everything into one group-total before dividing" bug a sibling repo had (see the comment above the `WOS_DC`/`WOS_TOTAL` block in `metrics.py`). Implementation (`metrics.py`, `compute_kpis`):

1. Reuses the SAME `daily_data_week` frame and `weekly_sales_units` column the store-side WOS already built — no separate sales recomputation.
2. A new `dc_daily` frame (from `pipeline.build_dc_daily`, restricted to the same in-scope `(product_id, Year, Week)` population as every other frame) is aggregated to the identical `week_keys + period_extra` grain: `avg_daily_dc_inventory`.
3. Left-joined onto `daily_data_week`; weeks with no DC record get `avg_daily_dc_inventory = 0` (filled, not dropped).
4. `wos_dc = avg_daily_dc_inventory / weekly_sales_units`; `wos_total = (avg_daily_total_inventory + avg_daily_dc_inventory) / weekly_sales_units` — same `F.when(weekly_sales_units > 0, ...).otherwise(None)` null guard as `wos_units`.
5. Period rollup: `Σ(wos_dc × weekly_sales_units) ÷ Σ(weekly_sales_units)` and the same shape for `wos_total` — sales-weighted, never a direct period-level division.

`dc_mean_stock`/`total_mean_stock` are NOT WOS ratios — they mirror `mean_stock`'s plain per-day-average shape instead (see `_mean_stock_frame`), just built from `dc_daily` (+ store daily inventory, for the total variant).

### 6.1b DC In-Stock Rate (`dc_in_stock_rate`)

Gated by `dc_instock.enabled` (default `False`) — see README's "dc_instock" config reference for the full column-mapping/config surface. Unlike every other DC metric above (`dc_mean_stock`, `WOS_DC`, `WOS_TOTAL`), which average whatever `inventory_warehouse` rows exist, this metric **expands** those rows into a continuous per-pair daily grid (`pipeline.build_dc_inst`) and counts the gaps as stockouts.

1. Take rolled-up `inventory_warehouse` (`pipeline._get_inventory_warehouse_parent_rolled`, shared with `build_dc_daily` — no second Delta scan, already filtered to the report window) and reduce it to one `first_stocked_date = MIN(date)` per `(product_id, warehouse_id)`.
2. `F.explode(F.sequence(first_stocked_date, REPORT_END_DATE))` for that pair's own row space, then join the fiscal calendar for `Year`/`Week`. The inventory frame is window-filtered upstream, so `first_stocked_date` can never precede `EFFECTIVE_REPORT_START_DATE` — no further clamping needed.
3. Restrict to `scope_core`'s in-scope `(product_id, Year, Week)` — the identical left-semi `build_dc_daily` already applies. Before the inventory join, so that join only touches in-scope rows.
4. Left-join the same rolled-up `inventory_warehouse` back onto the grid, `F.coalesce(inventory, 0)` — a grid day with no inventory row is a genuine stockout, not a row to drop.
5. With `blocked_scope.dc_solution_id` (int, e.g. 22), flag `ctx.dc_blocked_days` on the grid (`is_blocked`; see §3.2b: the same DC blocked days leave `dc_mean_stock` / `WOS_DC` / the DC part of `WOS_TOTAL` and `total_mean_stock` per their own `blocked_scope.metrics` entries). With `goods_in_transit.dc_instock`, left-join DC goods-in-transit days (`destination_type=1`, `quantity>0`, rolled to the family main, snapshot D+1 = end of day D, shifted by `goods_in_transit.date_shift_days`).
6. Aggregate to `(product_id, warehouse_id, Year, Week)`: `dc_stocked_days = COUNT(inventory > stock_threshold OR goods in transit)`, `dc_available_days = COUNT(*)`, both over the unblocked days only when `dc_in_stock_rate` is in `blocked_scope.metrics` (rows left with no available day are dropped), plus `dc_unblocked_days` (the unblocked days either way; `comparable.py` reads it).

**Window bound — why `inventory_warehouse`'s own `MIN(date)`.** `inventory_warehouse` carries a row whenever a pair holds stock, making its first row the same "first day this pair has any history" signal `daily_data_expanded` uses to bound the **store-level** in-stock denominator (`customer-analysis-tbretail`'s `05_future_visibility_data_prep.py`) — so both in-stock series rest on one definition. Trade-off: a pair ranged at a DC but never once stocked in the window has no row to anchor to and is **absent** rather than reading 0%. A pair that stops being stocked mid-window is still covered — its grid runs to `REPORT_END_DATE` and every later day is a stockout.

`dc_in_stock_rate = F.greatest(0.0, Σ(dc_stocked_days) ÷ Σ(dc_available_days))` — mirrors the `in_stock_rate` block exactly (direct period-grain sum/sum, no sales-weighted rollup like WOS/weighted-instock). When `dc_instock.enabled=False`, `build_dc_inst` returns a correctly-shaped but empty frame, so the column is always present as a literal `null` rather than requiring special-casing downstream.

**Also changes `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` (non-gated, always-on metrics), not just `dc_in_stock_rate`.** `build_dc_daily`'s `inventory_warehouse` read goes through the SAME rolled helper `build_dc_inst` uses (`pipeline._get_inventory_warehouse_parent_rolled` → `_roll_to_item_family_parent`, unconditionally — NOT gated by `dc_instock.enabled`), fixing a real bug where DC inventory sitting on a superseded/child `product_id` was silently dropped by the old raw-id semi-join against `scope_core` (which was already parent-rolled). This means `path_segments.item_family` must point at a real table any time `path_segments.inventory_warehouse` does, and `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` will show a step change for any product family with DC inventory split across old and current item codes — that's expected, not a data problem. See README's "dc_instock" / "inventory_warehouse" sections.

### 6.2 Weighted In-Stock Rate grain

`weighted_instock_rate` is computed at `Year×Week + group_keys` (product-week grain after store aggregation), **not** product×store×week:

1. Weekly in-stock computed at `Year×Week + group_keys` from `inst_data`.
2. Joined with weekly sales for weighting.
3. Sales-weighted rollup: `Σ(weekly_instock × weekly_sales) ÷ Σ(weekly_sales)` to the reporting period.
4. Null guard: `F.greatest(F.lit(0.0), ratio)` — zero-sales weeks yield 0.0, not null.

---

## 7. Data flow (quick reference)

```
daily_data_raw (cached Delta) — prints its source date range on read
  └─ equi-join fiscal_cal on date → score scope (build_weekly_scope) when hybrid or run_scope_diff
  └─ build_scoped_daily → scoped_daily (fiscal + products joined). With a store metric in
     goods_in_transit.inventory_metrics: window-filtered daily rows (summed per pair-day) ⟕⟖ store goods
     in transit (rolled to the family main) on (product_id, store_id, date) → drop GIT-only days
     input_filters.daily_data removed → blocked days flagged (is_blocked, left join) → scope semi-joins
     (has_daily_row / git_quantity / is_blocked columns, §3.2b / §3.2d)

goods_in_transit (operation/goods_in_transit snapshots; destination_type 0 = store, 1 = warehouse)
  └─ pipeline._goods_in_transit_quantity (quantity > 0, shifted, summed per day, family main, window)
     ├─ goods_in_transit.inventory_metrics: store quantity → build_scoped_daily; DC quantity → build_dc_daily
     └─ goods_in_transit.store_instock / dc_instock: _goods_in_transit_days (the days of it) → build_instock_daily / build_dc_inst

lost_sales_source (cached as lost_sales_weekly_base) — prints its source date range on read
  └─ lost_sales_ensemble.enabled=False (default): single fast-mover model (PATH_LOST_SALES)
  └─ lost_sales_ensemble.enabled=True: fast + slow models full-outer joined on
     (product_id, store_id, week_start_date), left-joined to the speed-cluster source
     (PATH_SPEED_CLUSTER; long- or wide-shaped per speed_cluster_format) → one shared
     boolean picks lost_sales/in_stock_days/total_days together per row from whichever
     model matches the product's cluster
  └─ scoped to hybrid_scope_keys → lost_sales_weekly → inst_data, lost_base

inventory_warehouse_raw (cached Delta) — prints its source date range on read
  └─ item_family_raw (cached Delta, read UNCONDITIONALLY whenever inventory_warehouse is
     configured -- NOT gated by dc_instock.enabled) → _roll_to_item_family_parent maps
     product_id -> coalesce(parent_id, product_id), then re-aggregated to
     (product_id, warehouse_id, date): F.sum("inventory") after the mapping
     (_get_inventory_warehouse_parent_rolled, shared with build_dc_inst below)
  └─ build_dc_daily → dc_daily (with a DC metric in goods_in_transit.inventory_metrics: DC goods in transit
     full-outer-joined first, has_inventory_row / git_quantity; DC blocked days flagged is_blocked; left-semi restricted to scope_core's in-scope
     (product_id, Year, Week), no store dimension; fiscal + product_dims joined). Now in the
     SAME parent-id space as scope_core -- previously restricted RAW product_id against an
     already-parent-rolled scope_core, silently dropping DC inventory on superseded/child ids
     (fixed 2026-09-17; see §12).

build_dc_inst (gated by dc_instock.enabled; reads no Delta table of its own -- reuses the same
cached rolled inventory frame as build_dc_daily)
  └─ rolled inventory_warehouse → MIN(date) per (product_id, warehouse_id) = first_stocked_date
     → explode(sequence(first_stocked_date, REPORT_END_DATE)) for that pair's own row space (NOT
     a flat window shared by every pair) → join the fiscal calendar for Year/Week → left-semi
     restrict to scope_core (same population as dc_daily, applied before the inventory join) →
     left-join the rolled inventory frame back on (coalesce 0 = stockout day) → aggregate to
     (product_id, warehouse_id, Year, Week): dc_stocked_days/dc_available_days → dc_inst (fiscal +
     product_dims joined, same shape as dc_daily). A pair never stocked inside the window has no
     first_stocked_date and is absent entirely. When enabled=False: a cheap empty-but-correctly-
     shaped frame, no expansion at all -- dc_in_stock_rate ends up a literal null column
     (item_family_raw is STILL read for dc_daily above regardless of this flag).

build_pipeline_frames(scope) → {scoped_daily, inst_data, lost_base, dc_daily, dc_inst, ...}
  └─ build_kpi_table(period, group_keys) → pandas
       └─ compute_kpis (real rows on-hand, or all rows + git_quantity per goods_in_transit.inventory_metrics;
                        unblocked rows only per blocked_scope.metrics):
                        sales | WOS (product×week, stores aggregated) | mean_stock | instock
                        | WOS_DC/WOS_TOTAL (dc_daily left-joined onto the same WOS grain)
                        | dc_mean_stock/total_mean_stock (mean_stock-shaped, dc_daily-based)
                        | dc_in_stock_rate (dc_inst, mirrors the instock block — direct
                          sum(dc_stocked_days)/sum(dc_available_days) at period grain)
       └─ sort in pandas (.sort_values), not Spark orderBy

build_kpi_long → kpi_long (period_type|period|root|dimension|dimension_value|metrics)
               → annual / ytd / quarter / monthly / weekly × root (overall + ctx.root_definitions)
                 × cut (overall + ctx.cut_dimensions) — every root gets every cut (see §3.4c)
trim_periods_to_recent → produces ctx.kpi_long_display (HTML rendering ONLY) trimmed to N most
                          recent per period_type; ctx.kpi_long itself is NEVER trimmed — it's
                          always the full computed window, which is what gets saved to Delta
                          (no trim setting for ytd — always shows every year present)
build_comparisons → yoy/ytd pandas tables, per root × cut (ytd: one row set per year-pair,
                     chained across consecutive years). No qoq/mom/wow comparison table exists.
build_comparable_pairs → comparable_kpi_long + comparable_comparison_{ytd,yoy,quarter} (one per
                          enabled comparable_pairs.kinds entry), per root × cut (when enabled; ONE
                          all-years pair restriction PER KIND shared by every link within it,
                          computed once against the overall population, not per root; store-side
                          grain is comparable_pairs.grain — see §3.6)
build_scope_diff → scope_diff pandas table (defined vs score; only when run_scope_diff=True)
save_outputs → kpi_long (incremental) → recompute comparisons from merged history → save all
render_kpi_html → standalone HTML file

report_end="latest_day" adds (once per run, fiscal.build_latest_day_windows): K / ctx.ytd_years /
ctx.ytd_lost_sales_last_week / ctx.day_calendar; build_instock_daily + build_dc_inst split the fiscal
week containing day K (last_day_index); kpi_long._period_frames keeps complete Annual / Weekly periods
and YTD = days 1..K; lost_base keeps weeks up to the last Saturday (§3.1b).
```

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `initial save blocked` | Output tables already exist — switch to `incremental` or `full_refresh` |
| Overlapping periods skipped | Expected with `incremental`; set `allow_overwrite_existing=True` to replace (see §3.5) |
| Saved Delta stale vs notebook | Incremental skip left old rows on disk; enable overwrite or use `full_refresh` |
| Comparisons skipped | Need ≥2 years present in the run window (both YoY and YTD) |
| Ensemble read fails on `attribute_name` not found | Speed-cluster table is **wide**-shaped (cluster already its own column) but `speed_cluster_format` is still `"long"` (default) — set `speed_cluster_format: "wide"` + `speed_cluster_value_col` |
| Comparisons only show current run period | `recompute_comparisons_from_history` may be False or no prior partition found; ensure incremental save ran first |
| Empty cut dimension | Column missing from products table or derived SQL failed validation |
| A NVROUT/COMP-style breakdown is missing, or shows as a flat cut instead of its own tab | Use `dimension_sources[].root_values` — it becomes a **root**, not a `slices.dimensions` cut, when the column lives on another table (see §3.4b/c) |
| Dimension source errors on read | An **enabled** dimension source fails loudly on bad path / missing column / bad expression — fix the source or set `enabled: False` |
| Cut value count looks doubled | A `dimension_source` table has multiple rows per `join_key` — pre-aggregate to one row per product (toolkit keeps arbitrary row) |
| Only want one root value (e.g. only NVROUT products), or an expected root is missing | Set `root_values` on that `dimension_sources` entry: `{"yes": "nvrout"}` makes exactly one root; omit it to auto-discover one root per distinct value instead. `NULL` never gets its own root. |
| A cut dimension shows a NULL bucket, or you want only one value | `slices.value_filters`: `["yes"]` keeps only `yes`; `[]` drops the NULL bucket; `{"exclude": ["nfg"]}` drops a set but keeps the rest incl NULL — see §3.4a. (For a `dimension_sources` column's own NULL bucket, use `fillna` on that source instead — it's a root, not a cut.) |
| Scope debug counts don't match `kpi_long` per cut | The debug cell recomputes scope independently — re-run it after any config change (scope mode, adjustments, `value_filters`) so it matches Cell 3. NULL values show as `"NULL"` here vs blank/None in `kpi_long` — see §3.3a. Note scope debug reports all `active_slice_dimensions` (root columns included), not root-restricted like `kpi_long`. |
| A root you expect is missing from the HTML report, or the root tab layer doesn't appear at all | The root tab only renders when more than one root exists in `kpi_long`. With a single root (`"overall"`, no root-producing `dimension_sources` enabled), the report renders exactly as before — this is expected, not a bug. Check `ctx.root_definitions` (or the "ROOTS:" print in the fiscal log) to confirm what was actually resolved. |
| A metric looks right for `"overall"` but wrong/missing within a specific root | Confirm the root's `dim_col` actually has non-null values matching its `root_values` for the products you expect — a product missing that dimension_source's join key entirely gets `NULL` and never appears in any named root, only `"overall"` |
| A metric (e.g. instock) is right in a named root but wrong in `"overall"` specifically | `"overall"` applies no population restriction of its own — if a segment (e.g. NON-COMP) should never count toward that metric even in `overall`, add a `metrics.population_filters` entry for it (see §3.4d) rather than trying to fix it via scope or roots |
| `metrics.population_filters` entry silently doesn't seem to apply / applies to a sibling metric too | Check `METRIC_FILTER_GROUPS` in `kpi_pipeline/filters.py` (also §3.4d) — several metric_cols share one aggregation pass and can only be filtered together; setting it on any one column in the group is enough, but two columns in the same group with different specs fail loudly at run time |
| `time grain (...) is missing N date(s) between ...` | The calendar (fiscal_cal upload, or on the civil path the dates present in daily-data after `input_filters.daily_data`) lacks days inside the window — fix the source data or narrow `run_min_date` (§3.1a) |
| `report_end='complete_month': the fiscal_cal upload ends X; it must extend past REPORT_END_DATE` | Extend the `fiscal_cal` upload beyond the as-of Saturday so the month containing it can be classified |
| `report_end='complete_month': no month ends between START and END` / `the cut month X..Y starts before the report start` | `run_min_date` does not reach the start of the cut month (also hit by the weekly-refresh pattern) — widen it or use `as_of` |
| `reporting_window.report_end='latest_day' requires instock.method='daily'` | The YTD cut splits a fiscal week of the daily in-stock frame, which weekly in-stock sources cannot do — set `instock.method="daily"` or use `as_of` / `complete_month` (§3.1b) |
| Current fiscal year missing from Annual / YoY under `latest_day`, or a year missing from YTD | By design: Annual = complete fiscal years only (the current year is in YTD). A year is left out of YTD when its first date is before `EFFECTIVE_REPORT_START_DATE` — move `run_min_date` back to that fiscal year's start; the run prints `YTD years` and the years left out |
| Old partial-year Annual / Monthly / Weekly rows still in saved `kpi_long` after switching to `latest_day` | Incremental merge keeps old keys (only `ytd` rows are always replaced) — run one `full_refresh` (§3.1b) |
| `lost_sales_pct` covers fewer days than the other metrics under `latest_day` | By design: lost sales only reaches the last Saturday on or before `REPORT_END_DATE`; every view uses whole weeks up to it, YTD uses weeks 1..`ctx.ytd_lost_sales_last_week` for every year |
| YTD `WOS` (and `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`) under `latest_day` differs from the previous mode's YTD | By design: YTD ends mid-week, and that week's average inventory is weighted by elapsed days / 7 (`week_days`), so a part week is not a full week of inventory against a part week of sales (§6.1). Whole weeks, and every other view, are unchanged. |
| `goods_in_transit...` `ValueError` at materialize time | Unknown name in `inventory_metrics`; `date_shift_days` is `None` while `store_instock` / `dc_instock` / `inventory_metrics` ask for GIT; non-int / bool `date_shift_days`; `store_instock` without `instock.method="daily"`; or `use_fiscal_calendar=False` with a non-empty `inventory_metrics` (§3.2d) |
| `blocked_scope.metrics...` `ValueError` at materialize time | Not `"all"` or a list, or a name outside `METRICS_ALL` (the message lists the allowed names) (§3.2b) |
| A metric still counts blocked days | It is not named in `blocked_scope.metrics` — a metric not named reads blocked and unblocked rows alike (§3.2b) |
| A gated inventory metric rose after adding it to `goods_in_transit.inventory_metrics` | Expected: it now adds goods in transit to on-hand, and a day with only goods in transit adds a zero-on-hand day to its averages. Drop the name from `inventory_metrics` to return it to on-hand; ungated metrics never change |
| No goods in transit on the last day (`date_shift_days=-1`) | The snapshot dated `REPORT_END_DATE + 1` is not in `goods_in_transit` yet |
| `instock.method='daily': daily-data latest date X is before the report end Y` | daily-data has not reached `REPORT_END_DATE` yet (days after it would count as out of stock) — wait for the load or set `reporting_window.as_of_date` earlier |
| `html_report.dimension_labels must be a dict ...` | `dimension_labels` must map slice dimension name -> label, both strings |
| `scope_source.mode ... ` / `blocked_scope.ui_parameters_path requires scope_source.mode='operation_scope'` / `instock.daily.count_start=... needs the operation scope start date` | Blocks and `scope_start`-based count starts need the operation scope (§3.2a-c); or use `count_start='first_daily_row'` |
| `No operation scope rows for solution_id=... run_date=...` | Wrong `solution_id`, or no snapshot for that Sunday — set `scope_source.run_date` explicitly |
| `KeyError` on `SCOPE_SOURCE` / `BLOCKED_SCOPE` / `INSTOCK_DAILY` / `HTML_REPORT_FILENAME_TEMPLATE` at materialize time | The customer config predates these keys — copy them from the current `config.py` / `tbretail_config.py` (every key is required, no silent defaults) |
| `html_report.filename ... may only use the placeholders {customer} and {report_end}` | Remove other `{...}` placeholders from the filename template |
| In-stock differs from the client reference script for tbretail | Check that the scope additions count in in-stock (the reference script leaves them out), `count_start`, ECOM `input_filters`, and that block product_ids are rolled to the family main (§3.2b) |
| Blocked product still shows sales at some stores | Sales units / revenue may not be in `blocked_scope.metrics` (tbretail leaves them out on purpose: a blocked pair can still sell stock), scope additions are never blocked, and `lost_sales_source` (weekly, no store) cannot be filtered per day (§3.2a-b) |
| Weekly tab lacks the last (partial) week under `complete_month` without a fiscal calendar | Expected — the clipped trailing week is dropped (§3.1a) |
| Score backfills all weeks | Defined scope path wrong or defined scope table empty for the window |
| `kpi_long is empty — run pipeline first` | Called `build_html_report` before `runner.run()`, or saved outputs missing in `html_only` mode |
| HTML file not generated | `html_report.enabled` is False, or check the Cell 6 output for errors |
| `html_only` fails on load | Output tables not at `.../run_date={OUTPUT_RUN_DATE}/` — run full save first or set `output.run_date` |
| Weekly tab looks wrong with sparse weeks | The display trim (`weekly_display_weeks`) shows the N most recent weeks present in `kpi_long`, not necessarily consecutive fiscal weeks. There's no WoW comparison table to be affected by this — QoQ/MoM/WoW aren't comparison kinds (see §3.5.1). |
| YTD shows nothing for a slice | The whole run has fewer than 2 years present — degrades to "no comparison," not an error; the `"ytd"` period_type rows in `kpi_long` still show whatever data exists |
| A comparable kind (`ytd`/`yoy`/`quarter`) is skipped | Fewer than 2 qualifying years available in the current run window AND no saved `comparable_kpi_long` history covering them yet (`quarter`: fewer than 2 years where that quarter number is fully elapsed). NOT gated by `comparisons.enabled` — that coupling is gone; only `comparable_pairs.enabled` + `comparable_pairs.kinds` control it (§3.6). |
| Comparable pairs population smaller than expected, or `comparable_pair_count` low | Check `comparable_pairs.grain` — under default `"product_store"`, a product opening/closing at even one store between compared years drops that store's pair from the WHOLE population for that kind. Set `grain: "product"` to compare at product level instead (store-estate churn then isn't isolated) — see §3.6. |
| Quarter/Monthly trend tab's most recent row disappeared after upgrading, no config change | Expected, one-time — that period hadn't actually fully elapsed; now correctly excluded instead of showing a partial quarter/month beside full ones. See §3.1 "Complete periods only." Re-run with `allow_overwrite_existing=True` (or `full_refresh`) if a stale partial-period row from before the fix is still saved. |
| `ctx.available_fiscal_months` (YTD's elapsed-period selection) changed after upgrading, no config change | Expected, one-time — two stacked fixes: the "is this period fully elapsed" check was previously trivially true for any period present in the window (a real bug, now fixed), and the grain itself moved from quarter to month (a quarter in progress can still have already-closed months). See §3.1 "Complete periods only." |
| Monthly tab missing from HTML | Monthly period type may not be present in `kpi_long` — check `Fiscal_Month` is derived (requires fiscal calendar upload or daily data with civil month fallback) |
| YTD tab missing or empty from HTML | No `"ytd"` `period_type` rows in `kpi_long` — under `latest_day` check `ctx.ytd_years` isn't empty; otherwise check `ctx.available_fiscal_months` isn't empty (would mean even the latest year's first fiscal month hasn't fully closed as of `as_of_date`) |
| HTML report header shows wrong reporting-window start date | Header was reading `REPORT_START_DATE` (raw Jan-1-anchored) instead of `EFFECTIVE_REPORT_START_DATE` (incorporating `run_min_date`) — now fixed. If you set `run_min_date` to narrow the window, the header now correctly reflects the effective start. |
| Monthly tab empty or stops earlier than Quarterly/Annual tabs | Fiscal weeks in the reporting window have null `Fiscal_Month` in the fiscal calendar upload while `Fiscal_Quarter` and `Fiscal_Year` are complete — now raises a validation error listing affected weeks. Previously these dropped silently. Fix the fiscal calendar upload or use a narrower window. |
| Unexpected extra year in Annual/YTD view (only sometimes) | When `use_fiscal_calendar=True`, `Year` comes directly from the fiscal calendar's own `Year` column. If the customer's fiscal year rolls over in late January/early February (not Jan 1), a `run_min_date` early in a calendar year can legitimately span two fiscal years per their calendar — this is correct. |
| Saved `kpi_long` has far fewer periods than the run actually computed | Fixed — `ctx.kpi_long` was previously overwritten with the HTML-display-trimmed frame *before* `save_outputs()` ran (notably in the Cell 3 `runner.run(save=False)` → Cell 5 `save_outputs(ctx, ...)` workflow, where the trim happened inside Cell 3). Now trimming only ever writes to `ctx.kpi_long_display` (used solely by `render_kpi_html`); `ctx.kpi_long` — and therefore the Delta save — always stays the full computed window. See §9. |
| `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` shifted after upgrading, no config change made | Expected, one-time — `build_dc_daily` now item-family-rolls `inventory_warehouse` to parent `product_id` before restricting to `scope_core` (previously raw `product_id`, silently dropping DC inventory on a superseded/child item code). See §6.1b / §12.20. Also means `path_segments.item_family` must now be a real table any time `path_segments.inventory_warehouse` is. |
| A `(product_id, warehouse_id)` pair you expect is missing from `dc_in_stock_rate` | Expected — each pair's grid is anchored to its own first `inventory_warehouse` row, so a pair ranged at a DC but never once stocked inside the report window has nothing to anchor to and is absent rather than reading 0%. See §6.1b / §12.21. |
| `dc_in_stock_rate` keeps falling for a product long after it stopped being ordered | Expected — a pair's grid runs to `REPORT_END_DATE` once started, so every day past its last stocked day is a stockout. `dc_mean_stock`/`WOS_DC` go quiet for the same pair instead. See §6.1b / §12.21. |

---

## 9. KPIContext fields (for debugging)

| Field | What it holds |
|-------|--------------|
| `fiscal_cal` | date → Year/Week lookup |
| `fiscal_week` | Year/Week → week_start/end/Fiscal_Quarter/Fiscal_Month |
| `available_fiscal_months` | fiscal-MONTH numbers fully closed as of `REPORT_END_DATE` for the latest year — the YTD elapsed-window set, applied to every year. Delegates to `complete_fiscal_periods` below at `Fiscal_Month` grain. Two stacked fixes this session: (1) previously read clipped `ctx.fiscal_week` bounds, so the "closed" check was trivially always true (now delegates to the unclipped helper); (2) the grain itself moved from quarter to month — `available_fiscal_quarters` was renamed/replaced, since a quarter in progress can still have already-closed months. See §3.1. |
| `complete_fiscal_periods` | `{"Fiscal_Quarter": df, "Fiscal_Month": df}` (plus `"Fiscal_Half"` only when `half_periods`) — the `(Year, period)` pairs fully elapsed on BOTH window edges (unclipped fiscal calendar), from `fiscal.complete_fiscal_periods`. Semi-joined onto every metric frame by `kpi_long._drop_incomplete_periods` for the Quarter/Monthly trend tabs (§3.1). `None` in `html_only` mode (`fiscal.build_fiscal_and_products` does not run). |
| `ytd_through_day` / `ytd_years` / `ytd_lost_sales_last_week` / `day_calendar` | `report_end="latest_day"` only (else `None`; `fiscal.build_latest_day_windows`): K = day of the fiscal year of `REPORT_END_DATE`; the years whose days 1..K are inside the window (YTD tab / comparable `ytd` kind only use these); fiscal week number of the last Saturday on or before `REPORT_END_DATE` (0 when it is in the previous fiscal year) for the lost-sales YTD weeks; the window's `(date, Year, Week, day_index, last_day_index)` calendar (the week containing day K is split into the days `<= K` and the days after; `fiscal.week_day_counts` derives each week's `week_days` / `ytd_week_days` from it) |
| `products_attr` | broadcast: product_id, cogs, price, ALL dimension columns (cuts + root-defining) |
| `active_slice_dimensions` | every validated dimension column (slices + dimension_sources) — includes root-defining columns; used to build `products_attr`/`product_dims` and by `scope_debug.py`. NOT what the KPI step iterates for cuts — see `cut_dimensions`. |
| `cut_dimensions` | `active_slice_dimensions` minus root-defining columns — what `kpi_long`/comparisons/HTML actually iterate as cuts within every root (§3.4c) |
| `root_definitions` | resolved roots (excluding the implicit `"overall"`): `[{"root": name, "dim_col": ..., "value": ...}, ...]`, from `fiscal._resolve_root_definitions`. In `html_only` mode, `dim_col`/`value` are `None` (re-inferred from a loaded `kpi_long`'s own `root` column — only the name is needed to render) |
| `operation_scope_pairs` | operation-scope mode only: cached `(product_id, store_id, scope_start)` after the family roll-up (earliest start) / active filter |
| `dc_blocked_days` | `blocked_scope.dc_solution_id` set only: cached `(product_id, warehouse_id, date)` blocked days, flagged `is_blocked` on `dc_daily` and removed from the DC metrics named in `blocked_scope.metrics` (DC in-stock grid included); `None` otherwise |
| `blocked_days` | blocked_scope on only: cached `(product_id, store_id, date)` blocked days, flagged `is_blocked` on `scoped_daily` and removed from the metrics named in `blocked_scope.metrics` (daily in-stock included); `None` when off |
| `defined_scope_keys` | product×[store×]Year×Week keys from defined scope |
| `hybrid_scope_keys` | final scope (defined + adjustments + score backfill) |
| `score_only_scope_keys` | score-filter scope (set when `use_hybrid_scope=True` or `run_scope_diff=True`) |
| `daily_data_raw` | cached daily Delta read |
| `lost_sales_weekly_base` | cached weekly lost-sales aggregates |
| `kpi_long` | primary pandas output — FULL computed/loaded history, never trimmed; includes `period_type="ytd"` rows. This is what `save_outputs()` persists to Delta. |
| `kpi_long_display` | trimmed-to-recent copy of `kpi_long` (per `html_report.*_display_*` settings), used ONLY by `render_kpi_html`. Set by `runner.build_comparisons()` / `run_html_only()` / `_recompute_comparisons_from_saved_history`; `render_kpi_html` falls back to `ctx.kpi_long` if this is `None`. |
| `comparison_yoy/ytd` | full comparison long-format DataFrames, per root × cut (ytd can carry multiple year-pair rows per metric). No qoq/mom/wow — not comparison kinds. |
| `yoy_display/ytd_display` | display-format DataFrames (overall); for ytd this is just the **latest** consecutive-year pair — see `comparison_ytd` for the full multi-pair detail |
| `scope_diff` | defined vs score annual diff (when `run_scope_diff=True`) |
| `comparable_kpi_long` | like-for-like per-link rows for EVERY enabled `comparable_pairs.kinds` entry, tagged by `comparison_type` (`"ytd"`/`"yoy"`/`"quarter"`/`"half"`) + `link_prior_year`/`link_current_year` (+ `quarter_number` / `half_number` for those rows) (when `comparable_pairs.enabled=True`) |
| `comparable_comparison_ytd` / `_yoy` / `_quarter` / `_half` | one comparison DataFrame per enabled kind (same shape as `comparison_ytd`/`comparison_yoy`; `quarter` additionally keyed on `quarter_number`). Populated only for kinds in `comparable_pairs.kinds`; the others stay `None`. |
| `comparable_ytd_display` / `comparable_yoy_display` / `comparable_quarter_display` | display DataFrame per enabled kind (ytd/yoy: latest link; quarter: latest link per quarter number) |
| `save_plan` | SavePlan from last save_outputs call |

For a quick distinct product/store count of the final scope (overall + per slice) without running the KPI step, use `runner.scope_debug_summary()` — see §3.3a.

---

## 10. Performance rules (preserve these)

- **Products**: `cache()` then `broadcast()` — never add joins to products Delta without caching.
- **Daily data**: always via `get_daily_data_raw(ctx)` — cached per run.
- **Lost sales**: always via `read_lost_sales_weekly(ctx)` — cached per run.
- **Score scope equi-join**: equi-join on `date` (fiscal_cal), never a `BETWEEN week_start/week_end` range join.
- **Score scope inventory**: `max_by(inventory, date)` per fiscal week — last in-week snapshot, not Saturday-only.
- **HTML weekly column order**: sort by `week_start_date` from `ctx.fiscal_week`, not `Year_Week` string order.
- **Pair stats via window**: `Window.partitionBy` for percentile_approx — avoids a second groupBy+join shuffle.
- **Pandas sort after collect**: `toPandas().sort_values(keys)` not `orderBy(*keys).toPandas()`.
- **Trim at data level**: period trimming (`trim_periods_to_recent`) runs before saves and HTML — never in the HTML renderer itself.

---

## 11. Environment variable overrides

| Variable | Config key |
|----------|-----------|
| `KPI_BUCKET` | `/mnt/invent-{customer}-datastore` |
| `KPI_CUSTOMER` | `customer` |
| `KPI_AS_OF_DATE` | `reporting_window.as_of_date` |
| `KPI_RUN_MIN_DATE` | `reporting_window.run_min_date` |
| `KPI_REPORT_END` | `reporting_window.report_end` (`as_of` / `complete_month` / `latest_day`) |
| `KPI_HALF_PERIODS` | `fiscal_calendar.half_periods` (true/false) |
| `KPI_USE_HYBRID_SCOPE` | `scope.use_hybrid_scope` |
| `KPI_RUN_SCOPE_DIFF` | `scope.run_scope_diff` |
| `KPI_COMPARABLE_PAIRS` | `comparable_pairs.enabled` |
| `KPI_COMPARISONS` | `comparisons.enabled` (comma-separated subset of `yoy,ytd`) |
| `KPI_LOST_SALES_ENSEMBLE` | `lost_sales_ensemble.enabled` |
| `KPI_LOST_SALES_SLOW_PATH` | `lost_sales_ensemble.slow_path_segments` (comma-separated) |
| `KPI_SPEED_CLUSTER_PATH` | `lost_sales_ensemble.speed_cluster_path_segments` (comma-separated) |
| `KPI_SPEED_CLUSTER_FORMAT` | `lost_sales_ensemble.speed_cluster_format` (`long` or `wide`) |
| `KPI_SPEED_CLUSTER_ATTRIBUTE` | `lost_sales_ensemble.speed_cluster_attribute_name` (format=`long`) |
| `KPI_SPEED_CLUSTER_VALUE_COL` | `lost_sales_ensemble.speed_cluster_value_col` (format=`wide`) |
| `KPI_FAST_MOVER_CLUSTERS` | `lost_sales_ensemble.fast_mover_clusters` (comma-separated ints) |
| `KPI_LOST_SALES_WEEK_COL` | `lost_sales_source.week_col` (default `week_start_date`) |
| `KPI_LOST_SALES_PRODUCT_COL` | `lost_sales_source.product_col` (default `product_id`) |
| `KPI_LOST_SALES_STORE_COL` | `lost_sales_source.store_col` (default `store_id`) |
| `KPI_LOST_SALES_COL` | `lost_sales_source.lost_sales_col` (default `lost_sales`) |
| `KPI_LOST_SALES_IN_STOCK_COL` | `lost_sales_source.in_stock_col` (default `in_stock`) |
| `KPI_LOST_SALES_TOTAL_DAYS_COL` | `lost_sales_source.total_days_col` (default `details.total_days`) |
| `KPI_INSTOCK_METHOD` | `instock.method` (`daily` / `weekly_source` / `lost_sales_source`) |
| `KPI_INSTOCK_SOURCE_PATH` | `instock.weekly_source.path_segments` (comma-separated) |
| `KPI_INSTOCK_WEEK_COL` | `instock.weekly_source.week_col` (default `week_start_date`) |
| `KPI_INSTOCK_PRODUCT_COL` | `instock.weekly_source.product_col` (default `product_id`) |
| `KPI_INSTOCK_STORE_COL` | `instock.weekly_source.store_col` (default `store_id`) |
| `KPI_INSTOCK_IN_STOCK_COL` | `instock.weekly_source.in_stock_col` (default `in_stock`) |
| `KPI_INSTOCK_TOTAL_DAYS_COL` | `instock.weekly_source.total_days_col` (default `total_days`) |
| `KPI_USE_FISCAL_CALENDAR` | `fiscal_calendar.use_fiscal_calendar` |
| `KPI_SCOPE_MIN_PERCENTILE` | `score_scope.min_percentile` |
| `KPI_SCOPE_MIN_WEEKS_FOR_FILTER` | `score_scope.min_weeks_for_filter` |
| `KPI_SLICE_DIMENSIONS` | `slices.dimensions` (comma-separated) |
| `KPI_SAVE_OUTPUTS` | `output.save_outputs` |
| `KPI_OUTPUT_SAVE_MODE` | `output.save_mode` |
| `KPI_ALLOW_OVERWRITE_EXISTING` | `output.allow_overwrite_existing` |
| `KPI_RECOMPUTE_COMPARISONS` | `output.recompute_comparisons_from_history` |
| `KPI_OUTPUT_PATH` | `output.path_segments` (comma-separated) |
| `KPI_OUTPUT_RUN_DATE` | `output.run_date` partition key |
| `KPI_HTML_ENABLED` | `html_report.enabled` |
| `KPI_HTML_FILENAME` | `html_report.filename` |
| `KPI_HTML_TITLE` | `html_report.report_title` |
| `KPI_HTML_OUTPUT_PATH` | `html_report.output_path_segments` (comma-separated) |
| `KPI_HTML_WEEKLY_WEEKS` | `html_report.weekly_display_weeks` (`null`/empty = all) |
| `KPI_HTML_MONTHLY_MONTHS` | `html_report.monthly_display_months` (`null`/empty = all) |
| `KPI_HTML_QUARTERLY_QUARTERS` | `html_report.quarterly_display_quarters` (`null`/empty = all) |
| `KPI_HTML_HALF_HALVES` | `html_report.half_display_halves` (`null`/empty = all) |
| `KPI_HTML_YEARLY_YEARS` | `html_report.yearly_display_years` (`null`/empty = all) |
| `KPI_RUN_MODE` | `run.mode` (`full` or `html_only`) |

---

## 12. Key design constraints (never violate)

1. **Report end = last completed Saturday** — prevents partial-week instock asymmetry. The exceptions are `report_end="complete_month"`, which cuts it back to a month end (a Saturday with a fiscal calendar; mid-week without, where the partial trailing week is dropped from the Weekly tab), and `report_end="latest_day"`, where it is `as_of_date` itself: YTD runs to it, every other view shows complete periods only, and lost sales is held to the last Saturday (§3.1b).
2. **Defined scope uses fiscal-week overlap** — a week whose Sunday precedes `run_min_date` is still included if any day overlaps the window.
3. **Score thresholds over the full window** — not just the backfill window.
4. **No pair pre-filter in product-week mode** — when `store_col=None`, `build_scoped_daily` must not pre-filter by lost-sales pairs.
5. **Scope adjustment data quality is caller's responsibility** — the toolkit maps columns, never validates or cleans input files.
6. **Scope diff is optional and pre-adjustment** — `scope_diff` runs only when `scope.run_scope_diff=True`; compares defined-only vs score-only scope before manual additions/removals.
7. **Weekly tab display trim uses the last N weeks present in `kpi_long`**, not necessarily consecutive fiscal weeks when coverage is sparse. There is no WoW comparison table — QoQ/MoM/WoW are not comparison kinds at all (only `yoy`/`ytd` exist).
8. **Incremental merge reads the latest prior partition** — not the current run's partition. History accumulates across runs as `run_date` advances with `as_of_date`.
9. **YTD is same-period-across-years, not sequential** — `ytd` compares a given year's elapsed window against the prior year's same window, chained across every consecutive year pair present. `yoy` is the last two full years. Neither has a sequential-quarter/month equivalent — that need is served by the Quarter/Monthly period tabs' plain value trends, not a comparison table.
10. **YTD's elapsed window is fixed once per run, from the latest year** — `available_fiscal_months` (month-grain, not quarter-grain — a quarter in progress can still have already-closed months, see §24) only looks at whether each month's weeks are within `REPORT_END_DATE` for the latest year; it is not recomputed per year being compared, so every year sums the same month-set.
11. **Speed-cluster table shape is config, not auto-detected** — `speed_cluster_format` must match the actual source table (`"long"` attribute_name/attribute_value vs `"wide"` a direct cluster column); pointing at the wrong shape fails loudly on read rather than silently returning nulls.
12. **Comparisons recomputed from merged history** — under incremental, `comparison_*` tables reflect the full saved `kpi_long` history, not just the current run window.
13. **`ctx.kpi_long` is never trimmed; only `ctx.kpi_long_display` is** — the HTML display trim (`*_display_*` settings) must only ever write to `kpi_long_display`. Trimming `ctx.kpi_long` itself would silently truncate what `save_outputs()` persists, since `main.ipynb` calls `runner.run(save=False)` then `save_outputs(ctx, ...)` separately in a later cell — this was a real, previously-shipped bug.
14. **Comparable-pairs restriction is a single universe PER KIND, fixed across that kind's own qualifying years, shared by every consecutive-year link within it** — for `ytd`/`yoy` a pair/product must be present in EVERY year in the run window to count at all; for `quarter`, independently per quarter number, in every year where that quarter is fully elapsed. Each kind computes this once (not per link), so the same year's metric value is identical across every link it participates in **within that kind** — `yoy`/`quarter` and `ytd` do NOT share one universe with each other. `comparable_kpi_long` rows still carry `link_prior_year`/`link_current_year` (+ `quarter_number` for `quarter`), but only so incremental merge doesn't collide two links' rows for the same year under one key, not because the values differ by link. The store-side population's own grain (`(product_id, store_id)` vs `product_id` alone) is `comparable_pairs.grain`, independent of `defined_scope.grain` — see §3.6/§22.
15. **Dimension sources fail loudly** — unlike `slices.derived_dimensions` (skipped on error), an enabled `dimension_sources` entry always raises on bad path/column/expression.
16. **`dimension_sources` columns are ALWAYS roots, never cuts** — mutually exclusive with `slices` by design. A dimension_source column is unconditionally excluded from `ctx.cut_dimensions` even if nothing lists it as a root explicitly (auto-discovery still applies); do not expect it to show up as a flat breakdown alongside brand/SMW.
17. **A fiscal calendar's month/quarter NUMBER is not assumed to equal the real calendar month/quarter** — `fiscal_calendar.column_map` reads a client's own quarter/month columns when present, but the Monthly tab's *display label* is never derived by feeding a fiscal month number into a month-name table (a client's fiscal year can be offset from the civil calendar, e.g. tbretail's Feb–Jan year, so fiscal month 07 can span real August). The label is instead derived from the majority real calendar month by day count across each fiscal month's actual dates when `month_name_col` isn't configured — see §3.1.
18. **No global store-exclusion config key** — every metric uses all scoped stores. If a store should never contribute (e.g. e-com fulfillment), filter it via `input_filters.daily_data` (everything read from `daily_data`; with `instock.method="daily"` also add it to `instock.daily.input_filters`, since the daily in-stock read skips `input_filters.daily_data`), or rely on `lost_sales_source`/`instock.weekly_source` tables that already exclude it upstream. The per-metric exceptions are explicit: `instock.daily.input_filters` leaves stores out of in-stock (tbretail: ECOM 829/639/917), `lost_sales_source.sales_filter` out of `lost_sales_pct`, and `metrics.population_filters` narrows a metric's products (tbretail: NON-COMP out of `in_stock_rate` only) — so on every root tab, period type, comparison, comparable table and in the HTML, NON-COMP is removed from in-stock only and ECOM from in-stock and lost sales only (the LFL root is `IS_COMP == "yes"` by definition, so NON-COMP is absent from every metric there).
19. **DC/warehouse scope restriction must include the week dimension, not just product_id** — `build_dc_daily` restricts on `(product_id, Year, Week)` against `scope_core`, attaching `Year`/`Week` to DC rows (via the fiscal calendar join) **before** the scope semi-join. Under `product_store_week` grain, scope membership varies by week; restricting on `product_id` alone (dropping `Year`/`Week` first) previously let a product's DC inventory leak into weeks it had fallen out of scope — inflating `WOS_DC`/`WOS_TOTAL`/`dc_mean_stock`/`total_mean_stock` for those weeks. Fixed 2026-09-16. No effect under `"product"`/`"product_store"` grain (scope is uniform across every week there already). Any new store-less input frame (§5 "Add a new input frame") must follow the same pattern.
20. **DC/warehouse frames restrict against `scope_core` in the SAME id space they were built in — item-family-rolled to parent `product_id`, not raw.** `scope_core`/`defined_scope` is already parent-rolled at its own source (its producer maps `product_id -> coalesce(parent_id, product_id)` via `item_family`'s `is_main=false` rows before writing the scope table). `build_dc_daily` and `build_dc_inst` both read `inventory_warehouse` through the same shared `pipeline._get_inventory_warehouse_parent_rolled` helper (which applies `_roll_to_item_family_parent`) before restricting to `scope_core` — fixed 2026-09-17. Previously `build_dc_daily` restricted RAW `product_id` against the parent-rolled `scope_core`, silently dropping any DC inventory sitting on a superseded/child `product_id`. This is unconditional (NOT gated by `dc_instock.enabled`): `path_segments.item_family` must point at a real table any time `path_segments.inventory_warehouse` does, and `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` will shift for any family with inventory split across old/current item codes. Any new DC-adjacent input frame (§5) must roll to parent `product_id` the same way before restricting to `scope_core`.
21. **`dc_in_stock_rate` bounds each pair's grid by that pair's own first `inventory_warehouse` row — never by a scope table's `start_date`, and never by a flat window shared across pairs.** This deliberately matches how `daily_data_expanded` bounds the store-level in-stock denominator, keeping both in-stock series on one definition. Consequences, both expected: a pair ranged at a DC but never once stocked inside the window is **absent** from the metric rather than reading 0%; a pair that stops being stocked mid-window keeps accruing stockout days through `REPORT_END_DATE`. Changed 2026-09-18 — the previous scope-derived grid (`operation/scope` + a go-live-floor rewrite) was removed entirely, along with `path_segments.dc_scope`, `input_filters.dc_scope` and `dc_scope_source`. See README's "dc_instock" section.
22. **`comparable_pairs.grain` is a SEPARATE grain from `defined_scope.grain`, never conflated with it.** `defined_scope.grain` decides scope membership (which rows are in the report at all); `comparable_pairs.grain` decides what "the same pair across years" means for the like-for-like population only, and only when comparable pairs is enabled. `"product_store"` (default) intersects `(product_id, store_id)`; `"product"` intersects `product_id` alone, keeping every store of a qualifying product. Resolved once via `comparable.py`'s `_GRAIN_PAIR_KEYS` and threaded through `_build_comparable_kind`/`_restrict_frames`. Only the store-side frames are affected (`scoped_daily`/`inst_data`/`lost_base`/`scope_pairs`/`scope_pair_weeks`) — `dc_daily`/`dc_inst` always keep their own `(product_id, warehouse_id)` universe regardless, since DC/warehouse has no store dimension to drop in the first place. See §3.6.
23. **Quarter/Monthly `kpi_long` rows only exist for FULLY ELAPSED periods — checked on both window edges, via the unclipped fiscal calendar, not `ctx.fiscal_week`.** `fiscal.complete_fiscal_periods(ctx, period_col)` re-reads the fiscal calendar without the `[EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]` clip `ctx.fiscal_week` carries, then `kpi_long._drop_incomplete_periods` semi-joins it onto every metric frame in `_period_frames`'s `"quarter"`/`"monthly"` branches — so an in-progress trailing quarter/month (or one truncated by a `run_min_date` not on a period boundary) never enters `kpi_long`, and therefore never gets saved. `REPORT_END_DATE` itself is unchanged; this is additive. Do NOT read period bounds from `ctx.fiscal_week` for any future completeness check — that was the exact bug in `fiscal._compute_available_fiscal_months` (YTD's own elapsed-period check) fixed this session: `ctx.fiscal_week` is already window-clipped, so any bounds check against it is trivially true for every period present at all. `comparable.py`'s `_complete_period_years` got this right from the start and was the template for the fix. See §3.1.
24. **YTD's own elapsed-window check runs at MONTH grain (`available_fiscal_months`), not quarter grain — do not "simplify" it back to quarter.** `fiscal._compute_available_fiscal_months` finds which fiscal MONTH numbers are fully elapsed for the latest year (delegating to `complete_fiscal_periods(ctx, "Fiscal_Month")`, same unclipped mechanism as #23), and `kpi_long._with_ytd_filter` filters on `Fiscal_Month` membership. This replaced an earlier quarter-grain version (`available_fiscal_quarters` / `Fiscal_Quarter`) fixed this session: quarter-grain understated YTD whenever the "current" quarter was in progress but had one or more of its own months already closed — virtually always true, since `REPORT_END_DATE` (a week boundary) essentially never lands on a quarter boundary. Month-grain picks up those already-complete months immediately while keeping the identical apples-to-apples mechanism (the same period-number set is summed for every year). Unrelated to the comparable-pairs `quarter` kind (§3.6), which is intentionally quarter-grain and untouched by this.
25. **Operation scope is built once and every metric uses it** — `scope_source.mode="operation_scope"` (§3.2a); scope additions are the only pairs outside the operation scope, they get no blocks, and they count in every metric including in-stock (from their first daily row).
26. **Blocked days leave exactly the metrics named in `blocked_scope.metrics`, once** — `build_scoped_daily` / `build_dc_daily` only FLAG them (`is_blocked`); `metrics.compute_kpis` reads unblocked rows for a named metric and every row for the others, in one conditional-aggregation pass per frame and family; `build_instock_daily` / `build_dc_inst` / `daily_for_lost` drop them at build time only when `in_stock_rate` / `dc_in_stock_rate` / `lost_sales_pct` are named (blocked and unusable days are subtracted without double counting); the comparable pair universe always uses unblocked real rows; weekly no-store sources (`lost_sales_source`) are documented as not filterable (§3.2b). Do not add an anti-join of blocked days back into a frame builder.
27. **`instock.method='daily'` counts only days from each pair's count start; a missing day is out of stock** — and its union of on-hand and goods-in-transit days is never summed.
28. **The calendar must be gap-free over the window and `complete_month` must fail loudly** — `require_complete_time_grain` and the `fiscal_cal` upload-end check in `apply_report_end_mode`; do not add silent fallbacks.
29. **New settings are indexed directly in `materialize()`** — no `.get(key, default)` that duplicates `CONFIG`. `config.py` and `tbretail_config.py` each vendor their own `materialize()`; a new setting or validation goes into both identically. They still differ in pre-existing places: `config.py` has the `KPI_ITEM_FAMILY_ROLLUP_*` env overrides (`tbretail_config.py` has none), different wording of the `comparable_pairs.grain` validation and some comments, and the `INSTOCK_SOURCE_*` settings sit at a different position in the returned dict.
30. **`goods_in_transit.inventory_metrics` never leaks goods in transit into sales, lost sales or the comparable pair universe, and an ungated metric is on-hand only (the in-stock frames have their own `store_instock` / `dc_instock` unions).** Goods-in-transit-only days exist only in `scoped_daily` (`has_daily_row=False`) and `dc_daily` (`has_inventory_row=False`); `compute_kpis` reads real rows for sales, distinct counts, weighted-instock's weights and every metric not named in `goods_in_transit.inventory_metrics`; `build_pipeline_frames` filters `has_daily_row` for `daily_for_lost`; `comparable.py` takes the pair / year universe from real rows. The store GIT must be joined to the daily rows BEFORE blocked days are removed, summed to one row per pair-day (a child + parent daily row must not double its quantity), and GIT-only days that `input_filters.daily_data` removed must be dropped (§3.2d). Store and in-stock GIT share `pipeline._goods_in_transit_quantity` / `_goods_in_transit_days`.
31. **`report_end="latest_day"` cuts YTD at the same fiscal day for every year and splits only the week containing day K.** `fiscal.build_latest_day_windows` is the only place K / `ytd_years` / the day calendar are computed (from the UNCLIPPED fiscal calendar — never `ctx.fiscal_week`, see #23); the pair-week frames (`inst_data`, `dc_inst`) are cut by `last_day_index`, the daily frames by `day_index`, lost sales by whole weeks up to the last Saturday; Annual and Weekly keep complete periods via `complete_fiscal_periods` at `"Year"` / `"Week"` grain. It requires `instock.method='daily'` (weekly in-stock sources cannot be split). `ytd` rows of the saved tables are always replaced on an incremental merge (§3.1b).

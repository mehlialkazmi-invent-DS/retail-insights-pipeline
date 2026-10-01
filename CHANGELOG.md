# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

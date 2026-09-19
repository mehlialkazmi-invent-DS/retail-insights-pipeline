# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Released]

### ✨ Added

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

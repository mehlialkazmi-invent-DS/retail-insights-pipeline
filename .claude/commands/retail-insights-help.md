---
name: retail-insights-help
description: >-
  Operate, configure, extend, troubleshoot, and answer questions about the
  retail-insights-pipeline — a PySpark retail KPI pipeline for Databricks.
  Covers onboarding, config editing, scope modes, metric computation, HTML
  report, output saves, comparable pairs, dimension sources, performance
  patterns, and how to add/change anything.
  Use when a user asks to: set up, modify, run, debug, explain, or extend this
  KPI pipeline; configure it with AI; onboard to it quickly; or understand what
  a specific metric means.
---

# Retail Insights Pipeline — Operate, Configure, Extend

Toolkit root: `retail-insights-pipeline/`  
**Always edit `config.py` first. Never change pipeline internals to alter behaviour.**

---

## 1. Quick onboarding (5-minute start)

If you are a new DS picking this up for the first time:

1. **Upload to Databricks** — same folder: `main.ipynb`, `config.py`, entire `kpi_pipeline/`.
2. **Open `config.py`** and change at minimum:
   - `reporting_window.as_of_date` → today or the last Sunday
   - `service_metrics.excluded_store_ids` → list of e-com store IDs
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
                   Cell 2: input previews (defined_scope, lost_sales, daily_data)
                   (before Cell 3): scope debug — distinct product/store counts per slice
                   Cell 3: runner.run() — full pipeline (+ scope adjustment logs)
                   (after Cell 3): scope summary display
                   Cell 4: save plan preview (when save_outputs=True)
                   Cell 5: Delta write
                   (unnumbered): kpi_long sample, YoY / QoQ / MoM / WoW / YTD comparisons,
                                 comparable pairs, scope diff
                   Cell 6: HTML report

kpi_pipeline/
  context.py       KPIContext dataclass — shared state
  runner.py        KPIRunner: build_dimensions → build_scopes → build_kpis →
                              build_comparisons → build_comparable_pairs →
                              build_scope_comparison → build_html_report
                   run.mode=html_only → load_saved_outputs + build_fiscal_week_only
  fiscal.py        fiscal_cal + fiscal_week frames; product attributes + slice dims;
                   dimension_sources left-join; Month column extraction;
                   available_fiscal_quarters (elapsed-quarter set for YTD, from the latest year)
  inputs.py        cached Delta reads (daily_data_raw, lost_sales_weekly_base) + input_filters;
                   prints the source date range for daily_data/lost_sales on every read
  scope.py         defined scope, score scope (hybrid), manual adjustments
  scope_debug.py   scope_universe_counts: pre-flight distinct product/store counts per slice
  pipeline.py      build_pipeline_frames: scoped_daily, inst_data, lost_base per scope
  metrics.py       compute_kpis: sales, WOS, mean_stock, instock, weighted_instock_rate
  kpi_long.py      build_kpi_long: loops annual/ytd/quarter/monthly/weekly × slices → pandas
                   trim_periods_to_recent: trims each period type to N most recent
  comparisons.py   YoY / QoQ / MoM / WoW / YTD + build_scope_diff. QoQ/MoM/YTD compare the SAME
                   quarter/month/elapsed-window across years (not sequential), chained across
                   every consecutive year pair present — see _consecutive_year_pairs
  comparable.py    build_comparable_pairs: like-for-like metrics; YoY/WoW over the last two
                   periods, QoQ/MoM/YTD over the intersection across EVERY year present
  io.py            incremental Delta saves, save plan, load_saved_outputs (html_only),
                   recompute comparisons from merged kpi_long history
  html_report.py   standalone HTML renderer — period → dimension → value tabs;
                   slices inferred from kpi_long; Annual/Quarter/Monthly/Weekly tabs
```

---

## 3. Configure with AI — decision guide

When a user wants to configure the toolkit, ask them (or read from their message):

### 3.1 Reporting window

```python
"reporting_window": {
    "as_of_date": "YYYY-MM-DD",   # run anchor → last completed Saturday on or before this date
    "run_min_date": "YYYY-MM-DD", # optional narrow start (Sunday-aligned); null/"" = full YTD from Jan 1
}
```

**Common setups:**
- Full YTD: `run_min_date: null`
- Last week only: `run_min_date` = this week's Sunday
- Multi-year backfill: `run_min_date` = Jan 1 of earliest year

**Fiscal calendar vs native time grain** (`fiscal_calendar.use_fiscal_calendar`):
- `True` (default): Year/Week/Quarter/Month come from `one_time_uploads/fiscal_cal`.
- `False`: derived from `noob/daily-data` (`fiscal_calendar.daily_time_columns`). **Year is the calendar year of `date`** (`F.year(date)`); **Week is the native fiscal week column**. The source `daily_time_columns.year` column is **not** used for Year — it can carry the ISO week-year, mislabeling late-December weeks as the next year (e.g. Dec 2025 shown as `2026`), which previously mismatched the Quarter/Month derived from `date` (Dec 2025 → "Q4 2026"). Caveat: a fiscal week straddling Jan 1 now appears as two partial weeks (one per calendar year) in the Weekly view; quarter/month/annual rollups stay correct.

### 3.2 Scope mode

```python
"scope": {
    "use_hybrid_scope": True,   # True = defined + score backfill; False = defined only
    "run_scope_diff": False,    # True = compute score scope and defined-vs-score annual diff
}
```

Use `use_hybrid_scope=True` (hybrid) unless the client has pristine defined scope coverage.

**`run_scope_diff`** (default `False`): when `False`, score scope is **not** computed unless hybrid backfill needs it (`use_hybrid_scope=True`). The notebook scope-diff cell and `scope_diff` Delta output are skipped. Set `True` to run the defined-vs-score annual KPI comparison (sanity check).

Score backfill parameters (hybrid or scope diff):
```python
"score_scope": {
    "min_percentile": 0.2,         # keep weeks where weekly_sales ≥ p20 AND inventory ≥ p20 per pair
    "min_weeks_for_filter": 2,     # skip filter for pairs with ≤ this many weeks (avoids over-filtering new items)
}
```
Inventory for the score filter is the **last available daily snapshot in the fiscal week** (`max_by(inventory, date)`), not Saturday-only — avoids false zero when `week_end_date` is missing from daily data.

### 3.3 Defined scope column mapping

```python
"defined_scope": {
    "product_col": "product_id",
    "store_col": "store_id",       # set None for product-week scope (no store grain)
    "date_col": "week_start_date", # DATE path: date → fiscal_cal → Year/Week
    "year_col": None,              # NATIVE path: set year_col + week_col instead of date_col
    "week_col": None,
}
```

**⚠️ NATIVE path (`year_col`/`week_col`) risk.** Unlike `date_col`, the native path takes `Year` verbatim from the source table — never reconciled with `fiscal_cal`. Scope is joined to daily/lost-sales by an exact match on `(product[, store], Year, Week)`, and when `use_fiscal_calendar=False` daily's `Year` is the calendar year of `date` (see §3.1). If `year_col` follows ISO week-year numbering instead (late-December rows carrying the next year), the join silently mismatches and those weeks vanish from scope with no error. Only use the NATIVE path when the source has no date column at all, and verify `year_col` is a true calendar year first. Same risk applies to `year_col`/`week_col` in `scope_adjustments` entries.

### 3.3a Scope debug (pre-flight product/store counts)

Before Cell 3's full run, the **Scope debug** cell in `main.ipynb` sanity-checks scope size and per-slice coverage — read-only, distinct from `runner.run()`:

```python
runner.build_dimensions()
runner.build_scopes(fund_paste=fund.paste)
display(runner.scope_debug_summary())
```

`scope_debug_summary()` → `kpi_pipeline.scope_debug.scope_universe_counts(ctx)` returns distinct `product_id`, `store_id`, and pair counts for the **final scope** (after hybrid backfill + adjustments): one `overall` row plus one row per active slice dimension value (`slices`, `derived_dimensions`, enabled `dimension_sources`). It applies the same `value_filters` as the KPI step, so counts match `kpi_long` per slice. Product-week scope (no `store_col`) shows only `distinct_product_count`. NULL slice values show as `"NULL"` here vs blank/None in `kpi_long`. `build_dimensions`/`build_scopes` are idempotent; Cell 3 rebuilds the same scope. Skipped in `html_only` mode (no scope is built).

### 3.4 Slice dimensions

There are two ways to add breakdown dimensions to the report. Pick based on **where the column lives**:

#### 3.4a — `slices` (columns from master-data/products)

Use this when the column already exists on (or is derivable from) the products table. Simpler, no join overhead.

```python
"slices": {
    "dimensions": ["brand"],          # existing column names in master-data/products
                                      # add multiple: ["brand", "category"]
    "derived_dimensions": {           # Spark SQL expressions against the products schema
        "price_tier": "CASE WHEN price_without_tax < 50 THEN 'budget' ELSE 'premium' END"
    },
    "value_filters": {},              # restrict which values of a dimension appear in its own breakdown
}
```

- Derived expressions are validated at runtime; failures are **skipped with a warning** (unlike dimension sources, which fail loudly).
- `value_filters` (also available on `dimension_sources`, see 3.4b): applied only to that dimension's own slice breakdown — Overall and other slices are unaffected. Accepts a LIST (include-only: `[]` = non-null, `["A"]` = only A) or a DICT (`{"include": [...]}`, `{"exclude": [...]}` keeps the rest incl NULL, optional `keep_null`).
  - dim omitted → keep all values, including `NULL` (default)
  - `[]` → keep all non-null values (drops only the `NULL` bucket)
  - `["A", "B"]` → keep only those values (drops `NULL` and unlisted values)
  - Example: `{"brand": ["NIKE", "ADIDAS"]}` or `{"brand": []}` to drop a `NULL` brand bucket.

#### 3.4b — `dimension_sources` (columns from other tables)

Use this **only** when a breakdown column does **not** live on the products table (e.g. NVROUT from `operation/extended_product`). Each enabled source is left-joined onto the product attribute projection.

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
            # Spark SQL over the SOURCE table's columns → new slice dimension(s)
            "is_nvrout": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END",
        },
        "value_filters": {"is_nvrout": ["yes"]},  # numbers only for the NVROUT universe
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

**Key rules:**
- Do NOT also list a `dimension_sources` column in `slices.dimensions` — it becomes a slice automatically.
- The source is deduplicated to **one row per join_key** before the join — pre-aggregate your source if the raw table has multiple rows per product.
- **Enabled sources fail loudly** on bad path / missing column / bad expression (by design — a silently dropped segment would misreport).
- **NULL behaviour**: products absent from the source get `NULL` (left join). For a clean yes/no split, ensure the source covers the full product universe, or use `ELSE 'no'` only works for rows that exist.
- **Overlapping segments** (e.g. COMP includes NVROUT): use independent boolean dimensions (`is_nvrout`, `is_comp`) — one product can be `yes` for both.
- **`value_filters`** (same semantics as `slices.value_filters`, see 3.4a): restricts that dimension's own breakdown only. LIST form (include-only): omit the dim for all values incl `NULL`, `[]` for all non-null, `["yes"]` for only that value. DICT form (NULL-aware): `{"include": ["yes"]}` keep only yes; `{"exclude": ["nfg"]}` keep everything except nfg **including NULL** (the way to drop an exclusion list and keep the remainder); optional `"keep_null": True/False`. `{"is_nvrout": ["yes"]}` gives numbers only for the NVROUT universe.
- CSV sources honour the same `location` (`datastore` / `workspace`) and `csv_options` as scope adjustments.

**Scope vs slices — two different machines:**
| Need | Use | Effect |
|------|-----|--------|
| Include/exclude which (product, store, week) rows enter KPIs | `scope_adjustments` | Changes scope membership; `scope_origin` is for reporting only |
| Break the report out by a segment | `slices` + `dimension_sources` | Adds a grouping dimension everywhere downstream |

### 3.5 Service store exclusions

```python
"service_metrics": {
    "excluded_store_ids": [],   # e-com store IDs excluded from WOS / instock / lost_sales_pct / mean_stock
}
```

All-store sales totals include these stores; only service-specific metrics exclude them. Scope is store-agnostic — e-com exclusion happens **only at metric compute**, not at scope level.

### 3.6 Output saves

```python
"output": {
    "save_outputs": False,          # True to write Delta tables
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,               # null = reporting_window.as_of_date → run_date=YYYY-MM-DD partition
    "save_mode": "incremental",     # initial | incremental | full_refresh
    "allow_overwrite_existing": False,
    "recompute_comparisons_from_history": True,  # incremental: recompute YoY/QoQ/MoM/WoW/YTD from full merged kpi_long
}
```

**Path:** `{bucket}/{path_segments}/{table_name}/run_date={run_date}/` — e.g. `analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`.

`run_date` defaults to `as_of_date`. Each run date gets its own Delta partition; incremental merge reads the **latest existing partition on or before** the current run_date and accumulates onto it.

**Tables saved:**

| Table | Contents |
|-------|----------|
| `kpi_long` | All metrics × periods (annual/ytd/quarter/monthly/weekly) × slices |
| `comparison_yoy` | YoY comparison rows |
| `comparison_qoq` | QoQ comparison rows (one row set per fiscal-quarter number × consecutive-year pair) |
| `comparison_mom` | MoM comparison rows (one row set per fiscal-month number × consecutive-year pair) |
| `comparison_wow` | WoW comparison rows |
| `comparison_ytd` | YTD comparison rows (one row set per consecutive-year pair, elapsed-window sums) |
| `scope_diff` | Defined vs score annual diff (only when `scope.run_scope_diff=True`) |
| `comparable_kpi_long` | Like-for-like per-period metrics + `comparable_pair_count` (only when `comparable_pairs.enabled=True`) |
| `comparable_comparison_{yoy,qoq,mom,wow,ytd}` | Like-for-like comparison rows (only when `comparable_pairs.enabled=True`) |

**Merge keys (incremental only)** — defined in `io.py` `TABLE_ROW_KEYS`:

| Table | Keys |
|-------|------|
| `kpi_long` | `period_type`, `period`, `dimension`, `dimension_value` |
| `comparison_*` | `comparison_type`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `scope_diff` | `Year`, `metric` |
| `comparable_kpi_long` | `comparison_type`, `period_type`, `period`, `dimension`, `dimension_value` |

**Save modes:**

| save_mode | Behaviour | When to use |
|-----------|-----------|-------------|
| `initial` | Write all rows; **fail** if output tables already exist | First-ever backfill only |
| `incremental` | Load latest prior partition, append new merge keys, skip overlaps (unless `allow_overwrite_existing=True`), write merged result to this run's partition | Weekly refresh — history accumulates |
| `full_refresh` | Overwrite each table entirely with **this run's output only** (no merge with prior history) | Rebuild saved tables for current run window |

**Incremental — how history accumulates:**
- Each weekly run loads the **latest existing `run_date` partition on or before** the current run, appends only new merge keys, and writes the full merged result to this run's `run_date` partition.
- Each `run_date` partition is therefore a self-contained snapshot of the full merged history as of that run.
- Comparison tables (`comparison_yoy/qoq/mom/wow/ytd`) are **recomputed from the merged `kpi_long` history** after the kpi_long save, then overwritten wholesale — so a single-week refresh can still produce YoY vs last year. Disable with `recompute_comparisons_from_history: False`.
- `comparable_kpi_long` is merged incrementally like `kpi_long`; comparable comparison tables are then recomputed from the merged `comparable_kpi_long`. A single-week refresh produces comparable YoY/QoQ/MoM/WoW/YTD relative to prior saved history.

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

### 3.6.1 Selecting which comparisons to run

`comparisons.enabled` chooses which period-over-period comparisons are computed, printed, saved, and rendered — any subset of `yoy`/`qoq`/`mom`/`wow`/`ytd` (default: all five).

```python
"comparisons": {
    "enabled": ["yoy"],   # only YoY; qoq/mom/wow/ytd skipped entirely
}
```

- **`yoy`** — last two full calendar/fiscal years (unchanged).
- **`qoq`** — the **same fiscal quarter across years** (e.g. `2026-Q1` vs `2025-Q1`), NOT the prior sequential quarter. One mini-table per fiscal-quarter number present, chained across every consecutive pair of years present for that quarter number (also `2025-Q1` vs `2024-Q1` if a 3rd year exists). A quarter number present in <2 years is skipped, not an error.
- **`mom`** — same pattern by fiscal-month number.
- **`ytd`** — each year's **elapsed window** (only the fiscal quarters fully closed as of `as_of_date` for the latest year — `fiscal.available_fiscal_quarters` — applied to every year) vs the prior year's same window, chained across consecutive years like `qoq`/`mom`. Use instead of `yoy` once the current year is only partially reported, so a partial current year isn't compared against a full prior year.
- **`wow`** — last two sequential weeks (unchanged).
- Gates the `comparison_{kind}` (and `comparable_comparison_{kind}`) Delta tables + HTML comparison columns only. `kpi_long` is always built in full, including a `"ytd"` `period_type`.
- HTML rendering: `qoq`/`mom`/`ytd` render as several stacked mini comparison tables in one panel (one per quarter/month number or year-pair) when more than one pair exists, instead of a single table.
- A latest-week run can still produce e.g. YoY: with `save_mode="incremental"` + `recompute_comparisons_from_history=True`, selected comparisons are rebuilt from the full merged `kpi_long` (this run unioned onto prior saved runs). Needs a prior saved partition.
- Invalid/empty selection fails loudly in `materialize()`. Env override: `KPI_COMPARISONS="yoy,mom"`.

### 3.7 Comparable pairs (like-for-like)

**Gated, opt-in** (default off). For each comparison the metrics are recomputed over **only the `(product_id, store_id)` pairs present across the periods that comparison touches**, then compared. Isolates like-for-like movement from mix shifts caused by new/closed pairs.

```python
"comparable_pairs": {
    "enabled": True,   # default False
}
```

**How it works — depends on the kind:**
- **yoy/wow** (unchanged): the two most recent periods in the run window; comparable universe = pairs present in `scoped_daily` for **both**.
- **qoq/mom/ytd**: comparable universe = pairs present in **every year** that has that quarter/month number (qoq/mom) or in every year's elapsed window (ytd) — not just the two years in one chain link, so the universe stays fixed across every consecutive-year link. Mirrors the reference implementation's `sameytd` ("same pairs across all the years specified").

In every case, all metric frames are restricted to that pair set and metrics recomputed for Overall and every slice (since slice dims are product attributes, no extra per-slice intersections needed), then the standard comparison logic diffs/chains as usual.

**Outputs:**
- `comparable_kpi_long` — per-period comparable metrics + `comparable_pair_count` (varies per quarter/month number for qoq/mom).
- `comparable_comparison_{yoy,qoq,mom,wow,ytd}` — comparison rows (same schema as regular tables).
- HTML report — a second "Comparable YoY/QoQ/MoM/WoW/YTD" table beneath each standard comparison, same stacking as the standard one.
- Notebook — a "Comparable pairs (like-for-like)" cell.

**Single-week run and history:** `comparable_kpi_long` is merged incrementally (same as `kpi_long`). A single-week refresh can still produce comparable YoY/QoQ/MoM/WoW/YTD **relative to prior saved `comparable_kpi_long` history** — even if the current window only spans one week. A comparable comparison is skipped for the current run only when there aren't enough periods (or years, for qoq/mom/ytd) *and* there is no saved history covering them yet.

### 3.8 Run mode

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

### 3.9 HTML report

```python
"html_report": {
    "enabled": True,                                              # False = skip
    "filename": "kpi_report_{customer}_{report_end}.html",        # {customer} and {report_end} interpolated
    "report_title": None,                                         # None = "<CUSTOMER> KPI Report"
    "output_path_segments": None,                                 # None = local only; or path segs to also save under datastore
    "metric_definitions": {},                                     # override any entry in DEFAULT_METRIC_DEFINITIONS
    "weekly_display_weeks": 5,         # Weekly tab: most recent N weeks; null = all
    "monthly_display_months": 5,       # Monthly tab: most recent N months; null = all
    "quarterly_display_quarters": 5,   # Quarter tab: most recent N quarters; null = all
    "yearly_display_years": 5,         # Annual tab: most recent N years; null = all
    # No display-count trim setting for the YTD tab yet — it always shows every year present.
}
```

The report has six top-level tabs:
- **Annual** — KPI table by year + YoY comparison (+ comparable YoY when enabled)
- **YTD** — KPI table by year over each year's elapsed (fully-closed-quarters) window + YTD comparison, stacked one mini-table per consecutive-year pair (+ comparable YTD when enabled)
- **Quarter** — KPI table by fiscal quarter + QoQ comparison, stacked one mini-table per fiscal-quarter number × consecutive-year pair (same-quarter-across-years, not sequential)
- **Monthly** — KPI table by month (`YYYY-MM`) + MoM comparison, same stacking as Quarter but by fiscal-month number
- **Weekly** — KPI table for the **most recent N fiscal weeks** (default 5; sorted by `week_start_date`) + WoW comparison
- **Metric Details** — plain-English definition, store scope, and formula for every active metric

Within each period tab, navigation is three levels:
1. **Period** (horizontal) — Annual / YTD / Quarter / Monthly / Weekly
2. **Slice dimension** (horizontal pills) — Overall + every slice column present in `kpi_long` (inferred automatically; not hard-coded to brand)
3. **Dimension value** (vertical sidebar) — one clickable tab per value (e.g. each brand); Overall shows a single panel

The executive header shows client, reporting window, scope mode, slice dimensions, and generated timestamp.

Period display is trimmed **at the data level** before saves and HTML render. All `*_display_*` settings affect both the HTML and the saved Delta tables.

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

### 3.10 Lost-sales & instock column mapping

Map raw lost-sales and instock table columns to canonical names, or read in-stock from a separate source.

**`lost_sales_source`** — renames lost-sales columns without code changes:
- `week_col`, `product_col`, `store_col`, `lost_sales_col`, `in_stock_col`, `total_days_col` — all default to tbretail's current schema.
- Downstream code always sees canonical names (`week_start_date`, `product_id`, `store_id`, `lost_sales`, `in_stock`, `total_days`).
- Supports dotted nested-struct paths (e.g. `total_days_col: "details.total_days"`).
- Defaults produce no behaviour change.

**`instock_source`** — optional; read in-stock from a separate table (when your in-stock rate is calculated separately from lost-sales):
- `enabled: False` by default — instock/total-days come from `lost_sales_source`.
- `enabled: True` — reads `path_segments` table, left-joins by `(product_col, store_col, week_col)`, overrides in-stock/total-days values in lost-sales pairs.
- Mutually exclusive with `lost_sales_ensemble.enabled=True` — ensemble's per-row model selection doesn't compose with a separate-table override; `materialize()` fails if both are True.

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
| Exclude e-com stores | `service_metrics.excluded_store_ids` |
| Add a brand/category slice (products column) | `slices.dimensions` |
| Add a derived slice (products SQL expression) | `slices.derived_dimensions` |
| Add a slice from another table (e.g. NVROUT) | `dimension_sources` |
| Add multiple external dimension sources | add another dict to `dimension_sources` list |
| Restrict a slice's values / drop NULL bucket | `slices.value_filters` or `dimension_sources[].value_filters` |
| Filter inputs | `input_filters.{defined_scope,lost_sales,daily_data}` |
| Blend fast/slow-mover lost-sales models by product velocity | `lost_sales_ensemble.enabled: True` (+ `slow_path_segments`, `speed_cluster_path_segments`, `speed_cluster_format`, `speed_cluster_attribute_name`/`speed_cluster_value_col`, `fast_mover_clusters`) |
| Map lost-sales table columns to different names | `lost_sales_source` (week_col, product_col, store_col, lost_sales_col, in_stock_col, total_days_col) |
| Read in-stock rate from a separate table | `instock_source.enabled: True` (+ `path_segments` + column keys) |
| Add a scope addition from Delta | `scope_adjustments.additions` (multiple entries supported) |
| Add a scope removal from CSV/Delta | `scope_adjustments.removals` (multiple entries supported) |
| Enable comparable (like-for-like) pairs | `comparable_pairs.enabled: True` |
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
3. Optionally add to `scope_diff_metrics` (scope diff) and `pp_change_metrics` (pp change instead of %).
4. Optionally add a definition to `CONFIG["html_report"]["metric_definitions"]` for the Metric Details tab.

### Add a new output table

1. Add a pandas DataFrame field to `KPIContext` in `context.py`.
2. Populate it in `runner.py` (e.g. inside `build_kpis`).
3. Add the table name + row key columns to `TABLE_ROW_KEYS` in `io.py`.
4. Add it to `_output_frames` in `io.py`.

### Add a new scope source

Add a path to `path_segments` in config, read in `fiscal.py` or a new module, and merge into `hybrid_scope_keys` in `scope.py` with a distinct `scope_origin` label.

---

## 6. Metric reference

| Metric | Label | Scope | Notes |
|--------|-------|-------|-------|
| `total_sales_revenue` | Sales Revenue | All stores | Sum of daily sales revenue |
| `total_sales_quantity` | Sales Units | All stores | Sum of daily sales quantity |
| `AUR` | AUR | All stores | Revenue ÷ Units |
| `AUC` | AUC | All stores | Cost ÷ Units |
| `total_inventory` | Total Inventory | All stores | Sum of daily inventory units across the period |
| `distinct_product_count` | Distinct Products | All stores | COUNT DISTINCT product_id |
| `distinct_store_count` | Distinct Stores | All stores | COUNT DISTINCT store_id |
| `distinct_pair_count` | Distinct Pairs | All stores | COUNT DISTINCT (product_id, store_id) |
| `mean_stock` | Daily Stock Avg (units) | Service only | AVG of daily summed inventory |
| `mean_stock_retail` | Daily Stock Avg Retail | Service only | AVG of daily summed inventory at retail |
| `mean_stock_cost` | Daily Stock Avg Cost | Service only | AVG of daily summed inventory at cost |
| `WOS` | WOS (units) | Service only | product × fiscal week; service stores aggregated; sales-weighted weekly→period rollup |
| `wos_revenue` | WOS Revenue | Service only | product × fiscal week; revenue-based rollup |
| `wos_cost` | WOS Cost | Service only | product × fiscal week; cost-based rollup |
| `inventory_turnover_rate` | Inventory Turnover Rate | Service only | Sales Units ÷ Mean Stock for the period; HTML shows tab-appropriate label |
| `in_stock_rate` | In-Stock Rate | Service only | Σ(in_stock_days) ÷ Σ(available_days); pp-change in comparisons |
| `weighted_instock_rate` | Weighted In-Stock Rate | Service only | Sales-weighted average of weekly in-stock rates; pp-change in comparisons |
| `lost_sales_pct` | Lost Sales % | Service only | 100×Σ(lost_sales)÷Σ(floor(sales+lost_sales)); pp-change |

**Critical formula constraints (never break):**
- WOS grain is **product × fiscal week**, not product×store×week. Three steps: (1) sum daily inventory/sales across service stores → product×date, (2) weekly WOS = `avg_daily_inventory / weekly_sales` at product×fiscal week, (3) sales-weighted rollup to the reporting period. Never divide period totals directly.
- In-Stock Rate uses `available_days` from lost-sales output, not from daily data.
- Lost Sales % denominator = corrected demand (sales + imputed lost), not sales alone.
- `mean_stock` = average of daily totals (not average of weekly averages).
- `in_stock_rate` null handling: `F.greatest(F.lit(0.0), ratio)` floors to 0 when `available_days=0` — never 1.0.

### 6.1 WOS computation grain (important)

Scope is product×store×week, but WOS in `metrics.py` is **not** computed at that grain. Implementation (`metrics.py` lines 62–105):

1. **product × date** — `groupBy(product_id, Year, Week, date)` sums inventory and sales across all service stores for each product-day.
2. **product × fiscal week** — within each week, take `avg(daily_total_inventory)` and `sum(daily_sales)`; weekly WOS = `avg_daily_inventory / weekly_sales` (units, revenue, or cost variant).
3. **period rollup** — sales-weighted average of weekly WOS values: `Σ(weekly_wos × weekly_sales) ÷ Σ(weekly_sales)`.

Do not confuse scope grain (product×store×week) with WOS computation grain (product×fiscal week after store aggregation).

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
  └─ build_scoped_daily → scoped_daily (fiscal + products joined)

lost_sales_source (cached as lost_sales_weekly_base) — prints its source date range on read
  └─ lost_sales_ensemble.enabled=False (default): single fast-mover model (PATH_LOST_SALES)
  └─ lost_sales_ensemble.enabled=True: fast + slow models full-outer joined on
     (product_id, store_id, week_start_date), left-joined to the speed-cluster source
     (PATH_SPEED_CLUSTER; long- or wide-shaped per speed_cluster_format) → one shared
     boolean picks lost_sales/in_stock_days/total_days together per row from whichever
     model matches the product's cluster
  └─ scoped to hybrid_scope_keys → lost_sales_weekly → inst_data, lost_base

build_pipeline_frames(scope) → {scoped_daily, inst_data, lost_base, ...}
  └─ build_kpi_table(period, group_keys) → pandas
       └─ compute_kpis: sales | WOS (product×week, stores aggregated) | mean_stock | instock
       └─ sort in pandas (.sort_values), not Spark orderBy

build_kpi_long → kpi_long (period_type|period|dimension|dimension_value|metrics)
               → annual / ytd / quarter / monthly / weekly × overall × slices
trim_periods_to_recent → trims each period_type to N most recent (applied before save and HTML;
                          no trim setting yet for ytd — always shows every year present)
build_comparisons → yoy/qoq/mom/wow/ytd pandas tables (qoq/mom/ytd: one row set per
                     quarter/month-number or year-pair, chained across consecutive years)
build_comparable_pairs → comparable_kpi_long + comparable_comparison_* (when enabled)
build_scope_diff → scope_diff pandas table (defined vs score; only when run_scope_diff=True)
save_outputs → kpi_long (incremental) → recompute comparisons from merged history → save all
render_kpi_html → standalone HTML file
```

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `initial save blocked` | Output tables already exist — switch to `incremental` or `full_refresh` |
| Overlapping periods skipped | Expected with `incremental`; set `allow_overwrite_existing=True` to replace (see §3.6) |
| Saved Delta stale vs notebook | Incremental skip left old rows on disk; enable overwrite or use `full_refresh` |
| Comparisons skipped | Need ≥2 years (YoY/YTD), ≥2 years present for a given fiscal-quarter/month number (QoQ/MoM), or ≥2 fiscal weeks (WoW) in window |
| Ensemble read fails on `attribute_name` not found | Speed-cluster table is **wide**-shaped (cluster already its own column) but `speed_cluster_format` is still `"long"` (default) — set `speed_cluster_format: "wide"` + `speed_cluster_value_col` |
| Comparisons only show current run period | `recompute_comparisons_from_history` may be False or no prior partition found; ensure incremental save ran first |
| Empty slice dimension | Column missing from products table or derived SQL failed validation |
| Slice from another table (NVROUT, etc.) missing | Use `dimension_sources` — not `slices.dimensions` — when the column lives on another table |
| Dimension source errors on read | An **enabled** dimension source fails loudly on bad path / missing column / bad expression — fix the source or set `enabled: False` |
| Slice value count looks doubled | A `dimension_source` table has multiple rows per `join_key` — pre-aggregate to one row per product (toolkit keeps arbitrary row) |
| `is_nvrout` (or another slice) shows a NULL bucket, or you want only one value (e.g. only NVROUT products) | Set `value_filters` for that dimension in `slices` or the `dimension_source`: `["yes"]` keeps only `yes`; `[]` drops the NULL bucket; `{"exclude": ["nfg"]}` drops a set but keeps the rest incl NULL — see §3.4a/§3.4b |
| Scope debug counts don't match `kpi_long` per slice | The debug cell recomputes scope independently — re-run it after any config change (scope mode, adjustments, `value_filters`) so it matches Cell 3. NULL values show as `"NULL"` here vs blank/None in `kpi_long` — see §3.3a |
| Score backfills all weeks | Defined scope path wrong or defined scope table empty for the window |
| WOS unexpectedly high/low | Check `excluded_store_ids` — missing e-com IDs inflate network inventory |
| `kpi_long is empty — run pipeline first` | Called `build_html_report` before `runner.run()`, or saved outputs missing in `html_only` mode |
| HTML file not generated | `html_report.enabled` is False, or check the Cell 6 output for errors |
| `html_only` fails on load | Output tables not at `.../run_date={OUTPUT_RUN_DATE}/` — run full save first or set `output.run_date` |
| WoW looks wrong with sparse weeks | WoW compares last two weeks in `kpi_long`, not necessarily consecutive fiscal weeks |
| QoQ/MoM/YTD show nothing for a slice | That quarter/month number (or the whole run) has fewer than 2 years present — degrades to "no comparison," not an error; the KPI value table still shows whatever data exists |
| Comparable pairs skipped for a kind | Fewer than 2 periods/years available for that kind in the current run window AND no saved `comparable_kpi_long` history covering them yet |
| Monthly tab missing from HTML | Monthly period type may not be present in `kpi_long` — check `Fiscal_Month` is derived (requires fiscal calendar upload or daily data with civil month fallback) |
| YTD tab missing or empty from HTML | No `"ytd"` `period_type` rows in `kpi_long` — check `ctx.available_fiscal_quarters` isn't empty (would mean even the latest year's Q1 hasn't fully closed as of `as_of_date`) |
| HTML report header shows wrong reporting-window start date | Header was reading `REPORT_START_DATE` (raw Jan-1-anchored) instead of `EFFECTIVE_REPORT_START_DATE` (incorporating `run_min_date`) — now fixed. If you set `run_min_date` to narrow the window, the header now correctly reflects the effective start. |
| Monthly tab empty or stops earlier than Quarterly/Annual tabs | Fiscal weeks in the reporting window have null `Fiscal_Month` in the fiscal calendar upload while `Fiscal_Quarter` and `Fiscal_Year` are complete — now raises a validation error listing affected weeks. Previously these dropped silently. Fix the fiscal calendar upload or use a narrower window. |
| Unexpected extra year in Annual/YTD view (only sometimes) | When `use_fiscal_calendar=True`, `Year` comes directly from the fiscal calendar's own `Year` column. If the customer's fiscal year rolls over in late January/early February (not Jan 1), a `run_min_date` early in a calendar year can legitimately span two fiscal years per their calendar — this is correct. |
| Saved `kpi_long` has far fewer periods than the run actually computed | Fixed — `ctx.kpi_long` was previously overwritten with the HTML-display-trimmed frame *before* `save_outputs()` ran (notably in the Cell 3 `runner.run(save=False)` → Cell 5 `save_outputs(ctx, ...)` workflow, where the trim happened inside Cell 3). Now trimming only ever writes to `ctx.kpi_long_display` (used solely by `render_kpi_html`); `ctx.kpi_long` — and therefore the Delta save — always stays the full computed window. See §9. |

---

## 9. KPIContext fields (for debugging)

| Field | What it holds |
|-------|--------------|
| `fiscal_cal` | date → Year/Week lookup |
| `fiscal_week` | Year/Week → week_start/end/Fiscal_Quarter/Fiscal_Month |
| `available_fiscal_quarters` | fiscal-quarter numbers fully closed as of `REPORT_END_DATE` for the latest year — the YTD elapsed-window set, applied to every year |
| `products_attr` | broadcast: product_id, cogs, price, slice dims |
| `active_slice_dimensions` | validated slice column names (from slices + dimension_sources) |
| `defined_scope_keys` | product×[store×]Year×Week keys from defined scope |
| `hybrid_scope_keys` | final scope (defined + adjustments + score backfill) |
| `score_only_scope_keys` | score-filter scope (set when `use_hybrid_scope=True` or `run_scope_diff=True`) |
| `daily_data_raw` | cached daily Delta read |
| `lost_sales_weekly_base` | cached weekly lost-sales aggregates |
| `kpi_long` | primary pandas output — FULL computed/loaded history, never trimmed; includes `period_type="ytd"` rows. This is what `save_outputs()` persists to Delta. |
| `kpi_long_display` | trimmed-to-recent copy of `kpi_long` (per `html_report.*_display_*` settings), used ONLY by `render_kpi_html`. Set by `runner.build_comparisons()` / `run_html_only()` / `_recompute_comparisons_from_saved_history`; `render_kpi_html` falls back to `ctx.kpi_long` if this is `None`. |
| `comparison_yoy/qoq/mom/wow/ytd` | full comparison long-format DataFrames (qoq/mom/ytd can carry multiple period-pair rows per metric) |
| `yoy_display/qoq_display/mom_display/wow_display/ytd_display` | display-format DataFrames (overall); for qoq/mom/ytd this is just the **latest** consecutive-year pair — see `comparison_{kind}` for the full multi-pair detail |
| `scope_diff` | defined vs score annual diff (when `run_scope_diff=True`) |
| `comparable_kpi_long` | like-for-like kpi_long rows (when `comparable_pairs.enabled=True`) |
| `comparable_comparison_yoy/qoq/mom/wow/ytd` | like-for-like comparison DataFrames |
| `comparable_yoy_display/…/comparable_ytd_display` | like-for-like display DataFrames |
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
| `KPI_USE_HYBRID_SCOPE` | `scope.use_hybrid_scope` |
| `KPI_RUN_SCOPE_DIFF` | `scope.run_scope_diff` |
| `KPI_COMPARABLE_PAIRS` | `comparable_pairs.enabled` |
| `KPI_COMPARISONS` | `comparisons.enabled` (comma-separated, e.g. `yoy,mom,ytd`) |
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
| `KPI_INSTOCK_SOURCE_ENABLED` | `instock_source.enabled` (true/false) |
| `KPI_INSTOCK_SOURCE_PATH` | `instock_source.path_segments` (comma-separated) |
| `KPI_INSTOCK_WEEK_COL` | `instock_source.week_col` (default `week_start_date`) |
| `KPI_INSTOCK_PRODUCT_COL` | `instock_source.product_col` (default `product_id`) |
| `KPI_INSTOCK_STORE_COL` | `instock_source.store_col` (default `store_id`) |
| `KPI_INSTOCK_IN_STOCK_COL` | `instock_source.in_stock_col` (default `in_stock`) |
| `KPI_INSTOCK_TOTAL_DAYS_COL` | `instock_source.total_days_col` (default `total_days`) |
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
| `KPI_HTML_YEARLY_YEARS` | `html_report.yearly_display_years` (`null`/empty = all) |
| `KPI_RUN_MODE` | `run.mode` (`full` or `html_only`) |

---

## 12. Key design constraints (never violate)

1. **Report end = last completed Saturday** — prevents partial-week instock asymmetry.
2. **Defined scope uses fiscal-week overlap** — a week whose Sunday precedes `run_min_date` is still included if any day overlaps the window.
3. **Score thresholds over the full window** — not just the backfill window.
4. **No pair pre-filter in product-week mode** — when `store_col=None`, `build_scoped_daily` must not pre-filter by lost-sales pairs.
5. **E-com exclusion at metric compute only** — WOS/instock/lost_sales_pct/mean_stock exclude `excluded_store_ids`; scope and total sales include them. Never add e-com exclusion to scope building.
6. **Scope adjustment data quality is caller's responsibility** — the toolkit maps columns, never validates or cleans input files.
7. **Scope diff is optional and pre-adjustment** — `scope_diff` runs only when `scope.run_scope_diff=True`; compares defined-only vs score-only scope before manual additions/removals.
8. **WoW with sparse weeks** — week-over-week uses the last two weeks present in `kpi_long`, not necessarily consecutive fiscal weeks when coverage is sparse.
9. **Incremental merge reads the latest prior partition** — not the current run's partition. History accumulates across runs as `run_date` advances with `as_of_date`.
10. **QoQ/MoM are same-period-across-years, not sequential** — `qoq`/`mom` compare a given fiscal quarter/month number against the same number in the prior year(s), chained across every consecutive year pair present. They are NOT the prior calendar quarter/month. Use `yoy`/`ytd` for full-year-scale comparisons.
11. **YTD's elapsed window is fixed once per run, from the latest year** — `available_fiscal_quarters` only looks at whether each quarter's weeks are within `REPORT_END_DATE` for the latest year; it is not recomputed per year being compared, so every year sums the same quarter-set.
12. **Speed-cluster table shape is config, not auto-detected** — `speed_cluster_format` must match the actual source table (`"long"` attribute_name/attribute_value vs `"wide"` a direct cluster column); pointing at the wrong shape fails loudly on read rather than silently returning nulls.
10. **Comparisons recomputed from merged history** — under incremental, `comparison_*` tables reflect the full saved `kpi_long` history, not just the current run window.
11. **Dimension sources fail loudly** — unlike `slices.derived_dimensions` (skipped on error), an enabled `dimension_sources` entry always raises on bad path/column/expression.

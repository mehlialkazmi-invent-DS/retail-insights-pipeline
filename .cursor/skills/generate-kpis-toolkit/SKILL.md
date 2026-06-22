---
name: generate-kpis-toolkit
description: >-
  Operate, configure, extend, troubleshoot, and answer questions about the
  generate-kpis-toolkit — a PySpark retail KPI pipeline for Databricks.
  Covers onboarding, config editing, scope modes, metric computation, HTML
  report, output saves, performance patterns, and how to add/change anything.
  Use when a user asks to: set up, modify, run, debug, explain, or extend this
  KPI pipeline; configure it with AI; onboard to it quickly; or understand what
  a specific metric means.
---

# Generate KPIs Toolkit — Operate, Configure, Extend

Toolkit root: `generate-kpis-toolkit/`  
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
3. **Run `main.ipynb`** top to bottom. Cell 1 prints resolved settings. Cell 2 previews raw inputs. Cell 3 runs the pipeline.
4. **Check the HTML report** written next to the notebook (Cell 6). It has a Metric Details tab explaining every metric.
5. **If something looks wrong**, check the Troubleshooting section (§8) or ask me what each config key does.

---

## 2. Architecture at a glance

```
config.py          → materialize(fund.paste) → settings dict
main.ipynb         Cell 1: config summary
                   Cell 2: input previews (defined_scope, lost_sales, daily_data)
                   Cell 3: runner.run() — full pipeline (+ scope adjustment logs)
                   (after Cell 3): scope summary display
                   Cell 4: save plan preview (when save_outputs=True)
                   Cell 5: Delta write
                   (unnumbered): kpi_long sample, YoY / QoQ / WoW comparisons, scope diff
                   Cell 6: HTML report

kpi_pipeline/
  context.py       KPIContext dataclass — shared state
  runner.py        KPIRunner: build_dimensions → build_scopes → build_kpis → build_comparisons → build_html_report
                   run.mode=html_only → load_saved_outputs + build_fiscal_week_only (skip pipeline)
  fiscal.py        fiscal_cal + fiscal_week frames; product attributes + slice dims
  inputs.py        cached Delta reads (daily_data_raw, lost_sales_weekly_base) + input_filters
  scope.py         defined scope, score scope (hybrid), manual adjustments
  pipeline.py      build_pipeline_frames: scoped_daily, inst_data, lost_base per scope
  metrics.py       compute_kpis: sales, WOS, mean_stock, instock
  kpi_long.py      build_kpi_long: loops periods × slices → pandas
  comparisons.py   YoY / QoQ / WoW + build_scope_diff
  io.py            incremental Delta saves, save plan, load_saved_outputs (html_only mode)
  html_report.py   standalone HTML renderer — period → dimension → value tabs; slices inferred from kpi_long
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

### 3.4 Slice dimensions

```python
"slices": {
    "dimensions": ["brand"],           # existing columns in master-data/products
    "derived_dimensions": {            # Spark SQL expressions evaluated against products schema
        "price_tier": "CASE WHEN price_without_tax < 50 THEN 'budget' ELSE 'premium' END"
    },
}
```

- Only add columns that actually exist in the products Delta table.
- Derived expressions are validated at runtime; failures are skipped with a warning.

### 3.5 Service store exclusions

```python
"service_metrics": {
    "excluded_store_ids": [],   # e-com store IDs excluded from WOS / instock / lost_sales_pct / mean_stock
}
```

All-store sales totals include these stores; only service-specific metrics exclude them.

### 3.6 Output saves

```python
"output": {
    "save_outputs": False,          # True to write Delta tables
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,               # null = reporting_window.as_of_date → run_date=YYYY-MM-DD partition
    "save_mode": "incremental",     # initial | incremental | full_refresh
    "allow_overwrite_existing": False,
}
```

**Path:** `{bucket}/{path_segments}/{table_name}/run_date={run_date}/` — e.g. `analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`.

`run_date` defaults to `as_of_date`. Each run date gets its own Delta partition; incremental merge applies **within** that partition only.

**Tables saved:** `kpi_long`, `comparison_yoy`, `comparison_qoq`, `comparison_wow`, `scope_diff`.

**Merge keys (incremental only)** — defined in `io.py` `TABLE_ROW_KEYS`:

| Table | Keys |
|-------|------|
| `kpi_long` | `period_type`, `period`, `dimension`, `dimension_value` |
| `comparison_*` | `comparison_type`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `scope_diff` | `Year`, `metric` |

**Save modes:**

| save_mode | Behaviour | When to use |
|-----------|-----------|-------------|
| `initial` | Write all rows; **fail** if output tables already exist | First-ever backfill only |
| `incremental` | Append rows with new merge keys; skip overlaps unless `allow_overwrite_existing=True` | Weekly refresh — add missing periods |
| `full_refresh` | Overwrite each table entirely with **this run's output only** (no merge with prior saved history) | Rebuild saved tables for the current run window |

**Incremental details:**
- Appends only merge keys not already on disk.
- Overlapping keys are **skipped** by default (existing values kept).
- Set `allow_overwrite_existing=True` to replace overlapping keys (re-run a corrected week).

**Full refresh details:**
- Replaces the **entire** Delta table — not "update 2026 inside a multi-year table."
- If the run window is 2026 only, saved tables contain 2026 data only; older years are dropped unless included in this run.

**Metadata:** every row gets `_run_as_of` (run's `as_of_date`) and `_saved_at` (UTC write time). On incremental saves, untouched existing rows keep their original metadata.

**Notebook flow:** Cell 3 `run(save=False)` → Cell 4 `preview_save_plan()` → Cell 5 `save_outputs()`. Review the save plan before writing.

**Typical workflows:**

```python
# First backfill
"output": {"save_outputs": True, "save_mode": "initial", "allow_overwrite_existing": False}

# Weekly append
"output": {"save_outputs": True, "save_mode": "incremental", "allow_overwrite_existing": False}

# Replace a re-run week
"output": {"save_outputs": True, "save_mode": "incremental", "allow_overwrite_existing": True}

# Rebuild all saved tables from a full-history run
"output": {"save_outputs": True, "save_mode": "full_refresh", "allow_overwrite_existing": False}
```

**Caveats:**
- Notebook `kpi_long` reflects the current run; saved Delta may be stale for skipped overlaps.
- Comparison tables are saved from the current run window, not recomputed from merged history.
- Empty tables (e.g. comparisons on a narrow window) are skipped on write — prior Delta data left unchanged.

### 3.7 Run mode

```python
"run": {
    "mode": "full",   # full = compute from source Delta; html_only = load saved outputs + render HTML
}
```

| mode | Behaviour |
|------|-----------|
| `full` (default) | Full pipeline: scope → KPIs → comparisons → optional save → HTML |
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
    "filename": "kpi_report_{customer}_{report_end}.html",        # {customer} and {report_end} interpolated
    "report_title": None,                                         # None = "<CUSTOMER> KPI Report"
    "output_path_segments": None,                                 # None = local only; or path segs to also save under datastore
    "metric_definitions": {},                                     # override any entry in DEFAULT_METRIC_DEFINITIONS
    "weekly_display_weeks": 5,                                      # Weekly tab: most recent N weeks; null = all
}
```

The report has four top-level tabs:
- **Annual** — KPI table by year + YoY comparison
- **Quarter** — KPI table by fiscal quarter + QoQ comparison
- **Weekly** — KPI table for the **most recent N fiscal weeks** (default 5; sorted by `week_start_date`) + WoW comparison
- **Metric Details** — plain-English definition, store scope, and formula for every active metric

Within each period tab, navigation is three levels:
1. **Period** (horizontal) — Annual / Quarter / Weekly
2. **Slice dimension** (horizontal pills) — Overall + every slice column present in `kpi_long` (inferred automatically; not hard-coded to brand)
3. **Dimension value** (vertical sidebar) — one clickable tab per value (e.g. each brand); Overall shows a single panel

The executive header shows client, reporting window, scope mode, slice dimensions, and generated timestamp.

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
| Add a brand/category slice | `slices.dimensions` |
| Add a derived slice | `slices.derived_dimensions` |
| Filter inputs | `input_filters.{defined_scope,lost_sales,daily_data}` |
| Enable output saves | `output.save_outputs: True` |
| Change output path | `output.path_segments` |
| Change run_date partition | `output.run_date` (default `as_of_date`) |
| Toggle HTML report | `html_report.enabled` |
| HTML only from saved data | `run.mode: "html_only"` |
| Change HTML title | `html_report.report_title` |
| Override a metric definition | `html_report.metric_definitions` |
| Weekly columns shown in HTML | `html_report.weekly_display_weeks` (default 5; `null` = all weeks) |
| Add a scope addition/removal | `scope_adjustments.additions` / `.removals` |

---

## 5. Extending the pipeline

### Add a metric

1. Add Spark aggregation to `compute_kpis` in `kpi_pipeline/metrics.py` (join to existing `keys`).
2. Add to `CONFIG["metrics"]["metric_cols"]` and `CONFIG["metrics"]["labels"]`.
3. Optionally add to `key_metrics` (scope diff) and `pp_change_metrics` (pp change instead of %).
4. Optionally add a definition to `CONFIG["html_report"]["metric_definitions"]` for the Metric Details tab.

### Add a new output table

1. Add a pandas DataFrame field to `KPIContext` in `context.py`.
2. Populate it in `runner.py` (e.g. inside `build_kpis`).
3. Add the table name + row key columns to `TABLE_ROW_KEYS` in `io.py`.
4. Add it to `outputs` dict in `build_save_plan` and `save_outputs` in `io.py`.

### Add a new scope source

Add a path to `path_segments` in config, read in `fiscal.py` or a new module, and merge into `hybrid_scope_psw` in `scope.py` with a distinct `scope_origin` label.

---

## 6. Metric reference

| Metric | Label | Scope | Notes |
|--------|-------|-------|-------|
| `total_sales_revenue` | Sales Revenue | All stores | Sum of daily sales revenue |
| `total_sales_quantity` | Sales Units | All stores | Sum of daily sales quantity |
| `AUR` | AUR | All stores | Revenue ÷ Units |
| `total_inventory` | Total Inventory | All stores | Sum of daily inventory units across the period |
| `distinct_product_count` | Distinct Products | All stores | COUNT DISTINCT product_id |
| `distinct_store_count` | Distinct Stores | All stores | COUNT DISTINCT store_id |
| `distinct_pair_count` | Distinct Pairs | All stores | COUNT DISTINCT (product_id, store_id) |
| `mean_stock` | Daily Stock Avg (units) | Service only | AVG of daily summed inventory |
| `mean_stock_retail` | Daily Stock Avg Retail | Service only | AVG of daily summed inventory at retail |
| `mean_stock_cost` | Daily Stock Avg Cost | Service only | AVG of daily summed inventory at cost |
| `WOS` | WOS (units) | Service only | product × fiscal week; service stores aggregated; sales-weighted weekly→period rollup |
| `wos_revenue` | WOS Revenue | Service only | product × fiscal week; service stores aggregated; revenue-based rollup |
| `wos_cost` | WOS Cost | Service only | product × fiscal week; service stores aggregated; cost-based rollup |
| `inventory_turnover_rate` | Inventory Turnover Rate | Service only | Sales Units ÷ Mean Stock; HTML shows **Annual** / **Quarterly** / **Weekly** label per tab |
| `in_stock_rate` | In-Stock Rate | Service only | Σ(in_stock_days) ÷ Σ(available_days); pp-change in comparisons |
| `lost_sales_pct` | Lost Sales % | Service only | 100×Σ(lost_sales)÷Σ(floor(sales+lost_sales)); pp-change |

**Critical formula constraints (never break):**
- WOS grain is **product × fiscal week**, not product×store×week. Three steps: (1) sum daily inventory/sales across service stores → product×date, (2) weekly WOS = `avg_daily_inventory / weekly_sales` at product×fiscal week, (3) sales-weighted rollup to the reporting period. Never divide period totals directly.
- In-Stock Rate uses `available_days` from lost-sales output, not from daily data.
- Lost Sales % denominator = corrected demand (sales + imputed lost), not sales alone.
- `mean_stock` = average of daily totals (not average of weekly averages).

### 6.1 WOS computation grain (important)

Scope is product×store×week, but WOS in `metrics.py` is **not** computed at that grain. Implementation (`metrics.py` lines 62–105):

1. **product × date** — `groupBy(product_id, Year, Week, date)` sums inventory and sales across all service stores for each product-day.
2. **product × fiscal week** — within each week, take `avg(daily_total_inventory)` and `sum(daily_sales)`; weekly WOS = `avg_daily_inventory / weekly_sales` (units, revenue, or cost variant).
3. **period rollup** — sales-weighted average of weekly WOS values: `Σ(weekly_wos × weekly_sales) ÷ Σ(weekly_sales)`.

Do not confuse scope grain (product×store×week) with WOS computation grain (product×fiscal week after store aggregation).

---

## 7. Data flow (quick reference)

```
daily_data_raw (cached Delta)
  └─ equi-join fiscal_cal on date → score scope (build_weekly_scope) when hybrid or run_scope_diff when hybrid or run_scope_diff
  └─ build_scoped_daily → scoped_daily (fiscal + products joined)

lost_sales_source (cached as lost_sales_weekly_base)
  └─ scoped to hybrid_scope_psw → lost_sales_weekly → inst_data, lost_base

build_pipeline_frames(scope) → {scoped_daily, inst_data, lost_base, ...}
  └─ build_kpi_table(period, group_keys) → pandas
       └─ compute_kpis: sales | WOS (product×week, stores aggregated) | mean_stock | instock (all joined on period keys)
       └─ sort in pandas (.sort_values), not Spark orderBy

build_kpi_long → kpi_long (period_type|period|dimension|dimension_value|metrics)
build_comparisons → yoy/qoq/wow pandas tables
build_scope_diff → scope_diff pandas table (defined vs score **before** manual adjustments; only when `scope.run_scope_diff=True`)
render_kpi_html → standalone HTML file
```

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `initial save blocked` | Output tables already exist — switch to `incremental` or `full_refresh` |
| Overlapping periods skipped | Expected with `incremental`; set `allow_overwrite_existing=True` to replace (see §3.6) |
| Saved Delta stale vs notebook | Incremental skip left old rows on disk; enable overwrite or use `full_refresh` |
| Comparisons skipped | Need ≥2 years (YoY), ≥2 quarters (QoQ), or ≥2 fiscal weeks (WoW) |
| Empty slice dimension | Column missing from products table or derived SQL failed validation |
| Score backfills all weeks | Defined scope path wrong or defined scope table empty for the window |
| WOS unexpectedly high/low | Check `excluded_store_ids` — missing e-com IDs inflate network inventory |
| `kpi_long is empty — run pipeline first` | Called `build_html_report` before `runner.run()`, or saved outputs missing in `html_only` mode |
| HTML file not generated | `html_report.enabled` is False, or check the Cell 6 output for errors |
| `html_only` fails on load | Output tables not at `.../run_date={OUTPUT_RUN_DATE}/` — run full save first or set `output.run_date` |
| WoW looks wrong with sparse weeks | WoW compares last two weeks in `kpi_long`, not necessarily consecutive fiscal weeks |

---

## 9. KPIContext fields (for debugging)

| Field | What it holds |
|-------|--------------|
| `fiscal_cal` | date → Year/Week lookup |
| `fiscal_week` | Year/Week → week_start/end/Fiscal_Quarter |
| `products_attr` | broadcast: product_id, cogs, price, slice dims |
| `active_slice_dimensions` | validated slice column names |
| `defined_scope_psw` | product×store×Year×Week keys from defined scope |
| `hybrid_scope_psw` | final scope (defined + adjustments + score backfill) |
| `daily_data_raw` | cached daily Delta read |
| `lost_sales_weekly_base` | cached weekly lost-sales aggregates |
| `kpi_long` | primary pandas output |
| `comparison_yoy/qoq/wow` | full comparison long-format DataFrames |
| `scope_diff` | defined vs score annual diff (when `run_scope_diff=True`) |

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
| `KPI_USE_FISCAL_CALENDAR` | `fiscal_calendar.use_fiscal_calendar` |
| `KPI_SCOPE_MIN_PERCENTILE` | `score_scope.min_percentile` |
| `KPI_SCOPE_MIN_WEEKS_FOR_FILTER` | `score_scope.min_weeks_for_filter` |
| `KPI_SLICE_DIMENSIONS` | `slices.dimensions` (comma-separated) |
| `KPI_SAVE_OUTPUTS` | `output.save_outputs` |
| `KPI_OUTPUT_SAVE_MODE` | `output.save_mode` |
| `KPI_ALLOW_OVERWRITE_EXISTING` | `output.allow_overwrite_existing` |
| `KPI_OUTPUT_PATH` | `output.path_segments` (comma-separated) |
| `KPI_OUTPUT_RUN_DATE` | `output.run_date` partition key |
| `KPI_HTML_ENABLED` | `html_report.enabled` |
| `KPI_HTML_FILENAME` | `html_report.filename` |
| `KPI_HTML_TITLE` | `html_report.report_title` |
| `KPI_HTML_OUTPUT_PATH` | `html_report.output_path_segments` (comma-separated) |
| `KPI_HTML_WEEKLY_WEEKS` | `html_report.weekly_display_weeks` (`null`/empty = all weeks) |
| `KPI_RUN_MODE` | `run.mode` (`full` or `html_only`) |

---

## 12. Key design constraints (never violate)

1. **Report end = last completed Saturday** — prevents partial-week instock asymmetry.
2. **Defined scope uses fiscal-week overlap** — a week whose Sunday precedes `run_min_date` is still included if any day overlaps the window.
3. **Score thresholds over the full window** — not just the backfill window.
4. **No pair pre-filter in product-week mode** — when `store_col=None`, `build_scoped_daily` must not pre-filter by lost-sales pairs.
5. **E-com exclusion from service metrics** — WOS/instock/lost_sales_pct/mean_stock exclude `excluded_store_ids`; total sales includes them.
6. **Scope adjustment data quality is caller's responsibility** — the toolkit maps columns, never validates or cleans input files.
7. **Scope diff is optional and pre-adjustment** — `scope_diff` runs only when `scope.run_scope_diff=True`; compares defined-only vs score-only scope before manual additions/removals (intentional sanity check).
8. **WoW with sparse weeks** — week-over-week uses the last two weeks present in `kpi_long`, not necessarily consecutive fiscal weeks when coverage is sparse.

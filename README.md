# Generate KPIs Toolkit

PySpark toolkit for weekly, quarterly, and annual retail KPIs with configurable scope (defined-only or hybrid with score backfill), optional manual scope adjustments, and incremental Delta output saves.

Designed to run on **Databricks** against the customer Delta datastore (`/mnt/invent-{customer}-datastore`).

## What it produces


| Output                       | Description                                                                                                                                            |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kpi_long`                   | One tidy table: `period_type`, `period`, `dimension`, `dimension_value`, plus all configured metrics. Filter this to reproduce any slice/period panel. |
| YoY / QoQ / WoW comparisons | Prior vs current period with formatted display columns (overall + each active slice dimension).                                                        |
| Scope diff                   | Side-by-side annual KPIs for **defined-only** vs **score-only** scope (optional sanity check). Only computed when `scope.run_scope_diff=True`. Compares scope **before** manual adjustments — intentional diagnostic of defined vs score coverage. |
| **HTML report**              | Standalone offline HTML with tabbed layout, Metric Details, and client/period info panel (see [HTML report](#html-report) section below).              |


Saved Delta tables live under `PATH_OUTPUT_ROOT`, partitioned by `run_date` per table:

```
{bucket}/{output.path_segments}/{table_name}/run_date={as_of_date}/
```

Default example: `.../analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`

## Repository layout

```
generate-kpis-toolkit/
├── README.md           # This file
├── config.py           # All user-editable settings (edit CONFIG, then %run in notebook)
├── main.ipynb          # Databricks runner — compute, preview save plan, write, HTML report
└── kpi_pipeline/       # Pipeline logic (imported by main.ipynb)
    ├── runner.py       # KPIRunner orchestrates the full run + HTML report generation
    ├── scope.py        # Defined scope, hybrid/score scope, manual adjustments
    ├── fiscal.py       # Fiscal calendar + product attributes / slice dims
    ├── inputs.py       # Cached Delta reads (daily_data, lost_sales) + input_filters
    ├── pipeline.py     # Scoped daily, lost sales, instock input frames per scope
    ├── metrics.py      # KPI aggregation (sales, WOS, instock, lost sales %, …)
    ├── kpi_long.py     # Long-format output across periods and slices
    ├── comparisons.py  # YoY / QoQ / WoW + defined vs score diff
    ├── io.py           # Incremental Delta saves + save plan preview
    ├── html_report.py  # Standalone HTML renderer (v4-style tabbed report)
    └── context.py      # Shared runtime state (KPIContext)
```

## Quick start (Databricks)

1. **Upload** to a Databricks workspace folder (same directory):
  - `main.ipynb`
  - `config.py`
  - the entire `kpi_pipeline/` folder
2. **Edit** `config.py` → `CONFIG` (at minimum):
  - `reporting_window.as_of_date` — anchor date for the run
  - `reporting_window.run_min_date` — optional narrow start (Sunday-aligned)
  - `scope.use_hybrid_scope` — `True` for hybrid, `False` for defined scope only
  - `defined_scope` — column mapping for your scope Delta table
  - `path_segments.defined_scope` — Delta path segments under the datastore bucket
  - `input_filters` — optional Spark SQL filters on defined scope, lost sales, daily data
  - `slices.dimensions` — product-master columns to slice by (e.g. `brand`)
  - `output.save_mode` — `initial`, `incremental`, or `full_refresh`
3. **Open** `main.ipynb` and **Run All** — Cell 2 previews inputs before the pipeline run in Cell 3.
4. **Review** the save plan cell before the write cell runs. Set `allow_overwrite_existing=True` if you intentionally want to replace overlapping periods.

### Prerequisites

- Databricks cluster with PySpark and access to `/mnt/invent-{customer}-datastore` (or override with `KPI_BUCKET`).
- `algo_helpers` available on the cluster (`from algo_helpers import fundamentals as fund`).
- Delta tables referenced in `config.py` must exist for the chosen date window.

## Scope modes

### Defined scope only

Set `scope.use_hybrid_scope = False`. KPIs use only rows from your configured defined scope table. Score scope is **not** computed unless `scope.run_scope_diff=True` or hybrid backfill is enabled.

### Hybrid scope (default)

1. **Defined scope** — Read from your configured Delta table at `product_id × store_id × Year × Week` grain (store optional; product-week fallback if no `store_col`). Weeks are matched by **calendar overlap** with the report window (fixes mid-week `run_min_date` false gaps).
2. **Gap detection** — Fiscal weeks in the report window with **no** defined-scope rows are treated as missing.
3. **Score backfill** — The score scope is computed once over the **full** report window: each `(product_id, store_id)` keeps the weeks whose weekly sales and **last available in-week inventory** clear a per-pair percentile threshold (thresholds use all the pair's weeks; ecom stores excluded). The backfill then adds only the weeks that fall in the **missing** weeks above. Because thresholds use the full window, the defined-vs-score diff mirrors exactly what the backfill contributes.
4. **Hybrid union** — Defined rows (`scope_origin=defined`) ∪ score backfill rows (`scope_origin=score`).

### Manual scope adjustments

After hybrid/defined scope is built, optional additions and removals are applied from **Delta tables or CSV files**.

**Additions** — union rows into scope with a custom `scope_origin` label (default `manual_add`).

**Removals** — anti-join rows out of scope.

**Sources**


| `source`          | How to point at data                                                                  |
| ----------------- | ------------------------------------------------------------------------------------- |
| `delta` (default) | `path_segments` under the datastore bucket, or full `path`                            |
| `csv`             | Full `path` to a `.csv` file (auto-detected from extension), or set `"source": "csv"` |


CSV options (optional): `"csv_options": {"header": True, "inferSchema": True}`

When adjustments run, the pipeline prints the scope **before**, after **each** addition/removal, and the **final** scope. The notebook scope summary cell displays before/after tables and a steps table.

Both support `join_keys` of:

- `["product_id"]` — all stores/weeks for that product (or specific weeks when `date_col` is set)
- `["store_id"]` — all products/weeks for that store
- `["product_id", "store_id"]` — specific pairs

**Data quality (your responsibility)**  
Scope adjustment files (CSV, Delta, or other sources) are **not validated or cleaned** by the toolkit. Messy input — duplicates, null keys, bad dates, wrong dtypes — is expected to be fixed **before** the run. The toolkit maps your columns via `product_col`, `store_col`, and `date_col`, but does not repair bad rows.

**Expected logical keys for adjustments**


| Key       | Config column                           | Required                                                               |
| --------- | --------------------------------------- | ---------------------------------------------------------------------- |
| Product   | `product_col` → `product_id`            | Yes (unless removing/adding by `store_id` only)                        |
| Store     | `store_col` → `store_id`                | Yes when using pair-level `join_keys`                                  |
| Week/date | `date_col` (or `year_col` + `week_col`) | Recommended; if omitted, keys expand to all weeks in the report window |


Time resolution on adjustment tables (same as defined scope):

- `date_col` → fiscal Year/Week
- `year_col` + `week_col` → native Year/Week
- neither → expand keys across **all fiscal weeks** in the report window

Example (Delta + CSV):

```python
"scope_adjustments": {
    "additions": [{
        "enabled": True,
        "label": "promo_add",
        "source": "csv",
        "path": "/mnt/invent-{customer}-datastore/analysis/kpi_reports/manual_additions.csv",
        "join_keys": ["product_id", "store_id"],
        "product_col": "product_id",
        "store_col": "store_id",
        "date_col": "week_start_date",
    }],
    "removals": [{
        "enabled": True,
        "source": "delta",
        "path_segments": ["analysis", "kpi_reports", "blocked_pairs"],
        "join_keys": ["product_id"],
        "product_col": "product_id",
        "date_col": None,  # removes product from all weeks in window
    }],
}
```

## Input previews and filters

The notebook reads **defined scope**, **lost sales**, and **daily data** separately before the pipeline run so you can inspect them. Optional config filters are applied automatically to both previews and the pipeline.

### `input_filters`

```python
"input_filters": {
    "defined_scope": [
        # "store_id NOT IN (829, 639, 917)",
    ],
    "lost_sales": [],
    "daily_data": [],
}
```

Each entry is a Spark SQL expression passed to `.filter()`. You can also filter ad hoc in the notebook preview cell (e.g. `.filter("brand = 'NIKE'")`).

Preview cells re-read the same tables the pipeline uses (with the same config filters). The pipeline caches daily data and lost-sales weekly aggregates within each run.

## Reporting window

Only two inputs in config:

- `as_of_date` — end anchor
- `run_min_date` — optional start narrow (aligned to Sunday)

Resolved automatically:

- **Start**: Sunday of Jan 1 (YTD) or Sunday of `run_min_date`
- **End**: **Last completed Saturday** on or before `as_of_date` (excludes the in-progress fiscal week)
- **Defined scope weeks**: any fiscal week whose Sun–Sat range **overlaps** the effective window

## Output saves

Persist pipeline results to Delta under a **single persistent root**:

```
{bucket}/{output.path_segments}/{table_name}/run_date={run_date}/
```

Default: `/mnt/invent-{customer}-datastore/analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`

`run_date` defaults to `reporting_window.as_of_date`. Override with `output.run_date` or `KPI_OUTPUT_RUN_DATE` when loading a specific snapshot (`html_only` mode uses the same partition).

Set `output.save_outputs: True` in `config.py` (default is `False`). The notebook runs with `save=False` by default — Cell 4 previews the save plan, Cell 5 writes.

### Tables written

| Table | Contents | Merge keys (incremental mode) |
| ----- | -------- | ----------------------------- |
| `kpi_long` | All metrics × periods × slices | `period_type`, `period`, `dimension`, `dimension_value` |
| `comparison_yoy` | YoY comparison rows | `comparison_type`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `comparison_qoq` | QoQ comparison rows | same as YoY |
| `comparison_wow` | WoW comparison rows | same as YoY |
| `scope_diff` | Defined vs score annual diff (when `run_scope_diff=True`) | `Year`, `metric` |

Merge keys are defined in `kpi_pipeline/io.py` (`TABLE_ROW_KEYS`). Incremental mode uses these keys to decide append vs skip vs overwrite.

### Save modes

| `save_mode` | What it does | When to use |
| ----------- | ------------ | ----------- |
| `initial` | Writes all rows from the current run. **Fails** if any output table already exists at the path. | First-ever backfill only (safety guard against accidental overwrite). |
| `incremental` | Loads existing Delta tables, **appends** rows whose merge keys are not yet saved, **skips** overlapping keys (unless overwrite allowed). | Weekly refresh — add new weeks/periods without touching history. |
| `full_refresh` | **Overwrites** each output table entirely with whatever the current run produced. No merge with prior saved data. | Rebuild outputs from scratch for the current run window. |

**Incremental — append missing keys only**

- **Yes:** Only rows with merge keys that do not already exist in the saved table are appended.
- **No (by default):** Rows for periods/slices already on disk are **not** updated — they are skipped.
- Set `allow_overwrite_existing: True` to replace overlapping keys with the new run's values.

Example: first run saves 2024–2026. A later weekly run with `run_min_date` = last Sunday appends only the new week (e.g. `2026-W25`). Existing weeks stay unchanged unless overwrite is enabled.

**Full refresh — replace entire table, not “update one year in history”**

- **Yes:** Each output Delta table is fully replaced (`overwrite`) with the current run's DataFrames.
- **No:** It does **not** update 2026 inside a table that still keeps 2024–2025. If the run window is 2026 YTD only, the saved table becomes **2026 data only** — older years disappear unless they are included in this run's output.

Use `full_refresh` when you want the saved tables to exactly match what this run computed — typically after re-running the full history window.

**Initial — first-time write only**

- **Yes:** Writes all rows; blocks if tables already exist (raises with a clear error).
- After the first successful initial save, switch to `incremental` (weekly append) or `full_refresh` (full replace).

### `allow_overwrite_existing`

| Value | Incremental behaviour on overlapping merge keys |
| ----- | ----------------------------------------------- |
| `False` (default) | Skip overlapping rows; existing saved values kept; save plan prints `skipped_rows` count and a warning. |
| `True` | Drop existing rows with matching keys, write new rows in their place; save plan prints `overwrite_rows` count. |

Use `True` when intentionally re-running a week or period that was already saved (e.g. scope fix, data correction).

### Run metadata columns

Every saved row includes:

| Column | Meaning |
| ------ | ------- |
| `_run_as_of` | `as_of_date` from the run that wrote (or last overwrote) the row |
| `_saved_at` | UTC timestamp when the row was written |

On **incremental** saves, only newly appended and overwritten rows get the current run's stamp. Previously saved rows that were not touched keep their original `_run_as_of` / `_saved_at`.

### Notebook workflow

1. **Cell 3** — `runner.run(fund_paste=fund.paste, save=False)` — compute KPIs; no write yet.
2. **Cell 4** — `runner.preview_save_plan(fund.paste)` — prints append / overwrite / skip counts per table **before** writing.
3. **Cell 5** — `save_outputs(ctx, fund.paste)` — executes the write (only when `save_outputs: True`).

Review Cell 4 output before Cell 5. If `skipped_rows > 0` and you intended to replace those periods, set `allow_overwrite_existing=True` and re-run.

### Typical workflows

**First-time backfill (2024 + 2025 + 2026)**

```python
"reporting_window": {"as_of_date": "2026-06-15", "run_min_date": "2024-01-01"},
"output": {"save_outputs": True, "save_mode": "initial", "allow_overwrite_existing": False},
```

**Weekly refresh (append latest week only)**

```python
"reporting_window": {"as_of_date": "2026-06-15", "run_min_date": "2026-06-08"},
"output": {"save_outputs": True, "save_mode": "incremental", "allow_overwrite_existing": False},
```

**Re-run a week that was already saved (replace overlapping keys)**

```python
"output": {"save_outputs": True, "save_mode": "incremental", "allow_overwrite_existing": True},
```

**Rebuild saved tables to match a full-history run**

```python
"reporting_window": {"as_of_date": "2026-06-15", "run_min_date": "2024-01-01"},
"output": {"save_outputs": True, "save_mode": "full_refresh", "allow_overwrite_existing": False},
```

### Important caveats

- **Notebook vs saved Delta can differ on incremental runs:** `ctx.kpi_long` and comparisons always reflect the **current run**. Saved Delta tables keep old rows for overlapping keys when `allow_overwrite_existing=False`. Downstream consumers reading Delta may see stale values until you append new keys or enable overwrite.
- **Comparisons saved per run window:** YoY/QoQ/WoW tables contain comparisons computed from the **current run's** `kpi_long`, not recomputed from the full merged saved history. Rebuild comparisons from merged `kpi_long` offline if you need full-history comparisons.
- **Empty outputs skipped:** If a table is empty for this run (e.g. comparisons on a very narrow window), the write for that table is skipped and prior Delta data is left unchanged — even under `full_refresh`.

### Config keys

```python
"output": {
    "save_outputs": False,          # True to write Delta tables
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,               # null = use reporting_window.as_of_date for run_date= partition
    "save_mode": "incremental",     # initial | incremental | full_refresh
    "allow_overwrite_existing": False,
}
```

Environment overrides: `KPI_SAVE_OUTPUTS`, `KPI_OUTPUT_SAVE_MODE`, `KPI_ALLOW_OVERWRITE_EXISTING`, `KPI_OUTPUT_PATH` (comma-separated path segments), `KPI_OUTPUT_RUN_DATE`.

## Config reference

### `input_filters`

See [Input previews and filters](#input-previews-and-filters) above.

### `scope`

```python
"scope": {
    "use_hybrid_scope": True,   # False = defined scope only
    "run_scope_diff": False,    # True = compute score scope and defined-vs-score annual diff
}
```

When `run_scope_diff=False` (default), the pipeline skips score-scope computation entirely unless `use_hybrid_scope=True` (hybrid backfill still needs score scope). The notebook scope-diff cell and `scope_diff` Delta output are omitted.

### `defined_scope`

DATE path (when your scope table has a date column):

```python
"defined_scope": {
    "product_col": "product_id",
    "store_col": "store_id",
    "date_col": "week_start_date",
    "year_col": None,
    "week_col": None,
}
```

### `score_scope`

Used when `use_hybrid_scope=True` or `run_scope_diff=True`.

```python
"score_scope": {
    "min_percentile": 0.2,
    "min_weeks_for_filter": 2,
}
```

### `output`

See [Output saves](#output-saves) for full mode behaviour, merge keys, workflows, and caveats.

```python
"output": {
    "save_outputs": True,
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,
    "save_mode": "incremental",
    "allow_overwrite_existing": False,
}
```

### `run`

```python
"run": {
    "mode": "full",   # full | html_only
}
```

See [HTML report — Run mode](#run-mode-html-only-from-saved-data).

### `html_report`

See [HTML report](#html-report) section.

## Environment variable overrides


| Variable                         | Effect                                                       |
| -------------------------------- | ------------------------------------------------------------ |
| `KPI_BUCKET`                     | Datastore mount (default `/mnt/invent-{customer}-datastore`) |
| `KPI_CUSTOMER`                   | Customer slug                                                |
| `KPI_AS_OF_DATE`                 | Overrides `as_of_date`                                       |
| `KPI_RUN_MIN_DATE`               | Overrides `run_min_date`                                     |
| `KPI_USE_HYBRID_SCOPE`           | `true`/`false`                                               |
| `KPI_RUN_SCOPE_DIFF`             | `true`/`false` — enable defined-vs-score scope diff          |
| `KPI_USE_FISCAL_CALENDAR`        | `true`/`false`                                               |
| `KPI_SCOPE_MIN_PERCENTILE`       | e.g. `20` or `0.2`                                           |
| `KPI_SCOPE_MIN_WEEKS_FOR_FILTER` | Integer                                                      |
| `KPI_SLICE_DIMENSIONS`           | Comma-separated column names                                 |
| `KPI_SAVE_OUTPUTS`               | `true`/`false`                                               |
| `KPI_OUTPUT_SAVE_MODE`           | `initial`, `incremental`, or `full_refresh`                  |
| `KPI_ALLOW_OVERWRITE_EXISTING`   | `true`/`false`                                               |
| `KPI_OUTPUT_PATH`                | Comma-separated path segments                                |
| `KPI_OUTPUT_RUN_DATE`            | `output.run_date` partition (default: `as_of_date`)          |
| `KPI_HTML_OUTPUT_PATH`           | Comma-separated path segments for `html_report.output_path_segments` |
| `KPI_HTML_WEEKLY_WEEKS`          | Recent fiscal weeks in Weekly tab (default 5; empty = all)       |
| `KPI_RUN_MODE`                     | `full` or `html_only` — skip pipeline and render HTML from saved outputs |


## Metrics

Default metrics (configurable in `CONFIG["metrics"]`):

- Sales / inventory (all scoped stores): `total_sales_quantity`, `total_sales_revenue`, `AUR`, `total_inventory`
- Coverage (all scoped stores): `distinct_product_count`, `distinct_store_count`, `distinct_pair_count`
- Service stores only: `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate`, `in_stock_rate`, `lost_sales_pct`

**Lost Sales %** = `100 × sum(lost_sales) / sum(floor(weekly_sales + lost_sales))` — denominator includes imputed lost demand.

**In-Stock Rate** = `sum(in_stock_days) / sum(available_days)` from top-down lost-sales output (service stores only).

**WOS** = per-product per-fiscal-week WOS after summing daily inventory/sales across service stores at product×date (`avg_daily_inventory / weekly_sales`), then rolled up to the reporting period using a sales-weighted average. Not computed at product×store×week grain.

**Inventory Turnover Rate** = Sales Units ÷ Mean Stock for the same period grain (service stores only). The HTML report labels it **Annual**, **Quarterly**, or **Weekly** Inventory Turnover Rate in the matching period tab.

## Programmatic use

```python
from kpi_pipeline import KPIRunner
from kpi_pipeline.io import save_outputs, load_saved_outputs

# Full run
runner = KPIRunner(spark, settings)
ctx = runner.run(fund_paste=fund.paste, save=False)
runner.preview_save_plan(fund.paste)
save_outputs(ctx, fund.paste)

# HTML only from saved outputs
# settings = materialize(fund.paste)  with run.mode = "html_only"
ctx = runner.run(fund_paste=fund.paste, save=False)  # loads saved Delta, skips pipeline
html_path = runner.build_html_report(local_dir=".")
```

## Performance notes

The pipeline is optimised for large retail datasets on Databricks. Key patterns to preserve if you modify the internals:

- **Products table**: read once, cached and broadcast — re-used by all scope variants without re-scanning Delta.
- **Daily data and lost sales**: cached once per run via `get_daily_data_raw` and `read_lost_sales_weekly` — the pipeline and the notebook preview cells share the same read.
- **Score scope join**: uses a fiscal-calendar equi-join (not a date-range join on week bounds) — avoids a nested-loop scan on the full daily frame.
- **Score scope inventory**: last available daily snapshot in the fiscal week (`max_by(inventory, date)` in `build_weekly_scope`), not Saturday-only.
- **HTML weekly columns**: sorted by `week_start_date` from `fiscal_week`, not lexicographic `Year_Week` strings.
- **KPI sort**: final sort happens in pandas after `toPandas()`, not via Spark `orderBy` — removes a shuffle stage from every aggregation call.

If you see slow runs, check: (1) scope table path is correct so defined scope is not empty, (2) `excluded_store_ids` is set correctly, (3) `run_min_date` is aligned to a Sunday (or left null for full YTD).

## Known limitations

- **Defined scope without store**: Falls back to product-week grain. Sales/inventory KPIs cover all stores selling the in-scope product-weeks; instock/lost-sales pairs are inferred from lost-sales weekly data.
- **Comparisons on incremental runs**: YoY/QoQ/WoW reflect the current run window only, not the full merged saved history (see [Output saves](#output-saves)).
- **Incremental skip vs notebook output**: Saved Delta can retain old values for overlapping period keys while the notebook shows fresh `kpi_long` — set `allow_overwrite_existing=True` to replace.
- **WoW with sparse weeks**: Week-over-week compares the last two weeks **present in `kpi_long`**, not necessarily consecutive fiscal weeks when weekly coverage is sparse (e.g. after a narrow `run_min_date` or partial backfill).

## Troubleshooting


| Symptom                     | Likely cause                                                                |
| --------------------------- | --------------------------------------------------------------------------- |
| `initial save blocked`      | Output tables already exist — switch to `incremental` or `full_refresh` (see [Output saves](#output-saves)) |
| Overlapping periods skipped | Expected with `incremental` + `allow_overwrite_existing=False` — set `True` to replace |
| Saved Delta stale vs notebook | Incremental skip kept old rows on disk while notebook shows fresh `kpi_long` — enable overwrite or use `full_refresh` |
| Comparisons skipped         | Need ≥2 years (YoY), ≥2 quarters (QoQ), or ≥2 fiscal weeks (WoW) in window  |
| Empty slice dimension       | Column missing from `master-data/products` or derived SQL failed validation |


## HTML report

Cell 6 of `main.ipynb` generates a standalone, offline HTML file after the pipeline run (or after loading saved outputs in `html_only` mode).  
Enabled by default (`html_report.enabled: True` in `config.py`).

### Run mode: HTML only from saved data

Set `run.mode: "html_only"` in `config.py` to skip the full pipeline and render the HTML report from previously saved Delta outputs:

```python
"run": {"mode": "html_only"},
"output": {"path_segments": ["analysis", "kpi_reports", "outputs"]},
```

Requires `kpi_long` and comparison tables at `{PATH_OUTPUT_ROOT}/{table}/run_date={OUTPUT_RUN_DATE}/`. Cell 3 loads them via `runner.run()`; Cell 6 renders HTML. Set `output.run_date` to load a different snapshot.

Environment override: `KPI_RUN_MODE=html_only`

### What the report contains

| Section | Description |
| ------- | ----------- |
| **Executive header** | Client, reporting window, as-of date, scope mode, slice dimensions, generated timestamp |
| **Period tabs** | Annual / Quarter / Weekly (horizontal) |
| **Slice dimension tabs** | Overall + every slice column in `kpi_long` (inferred automatically from data and config) |
| **Value tabs** | Vertical sidebar within each slice dimension — one panel per value (e.g. each brand) |
| **KPI tables** | Metrics as rows (colour-coded), periods as columns; inventory turnover is labelled **Annual** / **Quarterly** / **Weekly** per tab |
| **Comparison** | YoY / QoQ / WoW per value panel |
| **Metric Details tab** | Definition, store scope, and formula for every active metric |

Slice dimensions and values are **inferred from `kpi_long`** — if you configure `category` instead of `brand`, or add multiple slice columns, the report adapts without code changes.

### Config keys

```python
"html_report": {
    "enabled": True,
    "filename": "kpi_report_{customer}_{report_end}.html",
    "report_title": None,
    "output_path_segments": None,
    "metric_definitions": {},
    "weekly_display_weeks": 5,   # Weekly tab shows only the 5 most recent weeks; null = all weeks
}
```

### Environment variable overrides

| Variable | Effect |
| -------- | ------ |
| `KPI_HTML_ENABLED` | `true`/`false` |
| `KPI_HTML_FILENAME` | Output filename |
| `KPI_HTML_TITLE` | Report title |
| `KPI_HTML_OUTPUT_PATH` | Comma-separated path segments for datastore HTML copy |
| `KPI_HTML_WEEKLY_WEEKS` | Number of recent fiscal weeks in the Weekly tab (`null`/empty = all weeks) |

### Overriding metric definitions

Add entries to `html_report.metric_definitions` to customise the Metric Details tab:

```python
"html_report": {
    "enabled": True,
    "metric_definitions": {
        "total_sales_revenue": {
            "definition": "Net retail sales after returns, excluding VAT.",
            "store_scope": "All scoped stores",
            "formula": "Σ(daily_net_sales_revenue)",
        },
    },
}
```

Only the keys you provide are overridden; all other metrics keep their default definitions from `kpi_pipeline/html_report.py`.

### Programmatic use

```python
html_path = runner.build_html_report(local_dir=".")
```

Or call the renderer directly:

```python
from kpi_pipeline.html_report import render_kpi_html, DEFAULT_METRIC_DEFINITIONS
render_kpi_html(ctx, "/dbfs/mnt/.../report.html", report_title="My KPI Report")
```

## Related notes

Design background and scope concepts are documented in the `Notes/` folder in the workspace.
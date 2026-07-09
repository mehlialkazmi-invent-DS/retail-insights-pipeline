<p align="center">
  <img src="docs/images/readme-banner.png" alt="Retail KPI analytics — sales, inventory, in-stock rate, and weeks of supply" width="100%" />
</p>

# Generate KPIs Toolkit

PySpark toolkit for weekly, monthly, quarterly, and annual retail KPIs with configurable scope (defined-only or hybrid with score backfill), optional manual scope adjustments, comparable (like-for-like) pair analysis, and incremental Delta output saves.

Designed to run on **Databricks** against the customer Delta datastore (`/mnt/invent-{customer}-datastore`).

## What it produces


| Output                                  | Description                                                                                                                                            |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kpi_long`                              | One tidy table: `period_type`, `period`, `dimension`, `dimension_value`, plus all configured metrics. Filter this to reproduce any slice/period panel. |
| `comparison_yoy / qoq / mom / wow`      | Prior vs current period with formatted display columns (overall + each active slice dimension). YoY/QoQ/MoM/WoW are recomputed from the full merged kpi_long history on incremental saves. |
| `scope_diff`                            | Side-by-side annual KPIs for **defined-only** vs **score-only** scope (optional sanity check). Only computed when `scope.run_scope_diff=True`. Compares scope **before** manual adjustments — intentional diagnostic of defined vs score coverage. |
| `comparable_kpi_long` / `comparable_comparison_*` | Like-for-like metrics over only the pairs present in both compared periods. Gated on `comparable_pairs.enabled=True`. |
| **HTML report**                         | Standalone offline HTML with tabbed layout, Metric Details, and client/period info panel (see [HTML report](#html-report) section below).              |


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
    ├── scope_debug.py  # Pre-flight distinct product/store counts overall + per slice
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
  - `dimension_sources` — optional, gated: pull extra slice dimensions from tables other than products (e.g. NVROUT from `extended_product`) — see [Dimension sources](#dimension-sources-extra-slices-from-other-tables)
  - `output.save_mode` — `initial`, `incremental`, or `full_refresh`
3. **Open** `main.ipynb` and **Run All** — Cell 2 previews inputs, then the **Scope debug** cell reports distinct product/store counts per slice, before the full pipeline run in Cell 3.
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
3. **Score backfill** — The score scope is computed once over the **full** report window: each `(product_id, store_id)` keeps the weeks whose weekly sales and **last available in-week inventory** clear a per-pair percentile threshold (thresholds use all the pair's weeks; **all stores included** — scope membership is store-agnostic, e-com is excluded later only for service metrics). The backfill then adds only the weeks that fall in the **missing** weeks above. Because thresholds use the full window, the defined-vs-score diff mirrors exactly what the backfill contributes.
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

**CSV location** — `"location"` controls where a CSV physically lives (applies to scope adjustments *and* [dimension sources](#dimension-sources-extra-slices-from-other-tables)):

| `location` | Reads from | Notes |
| ---------- | ---------- | ----- |
| `datastore` (default) | A cloud / DBFS path under the datastore mount (`/mnt/invent-{customer}-datastore/...`) | Path used as-is by Spark |
| `workspace` | A Databricks **workspace** file (`/Workspace/Users/...`) | Read through the `file:` scheme — mirrors how the v4 script loaded CSVs next to the notebook |

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

**⚠️ `year_col`/`week_col` risk — prefer `date_col`.** Unlike `date_col`, the native path takes `Year` **verbatim** from your source table — it is never derived from a date or reconciled against `fiscal_cal`/`fiscal_week`. Scope is joined to daily/lost-sales by an **exact match** on `(product_id[, store_id], Year, Week)` (see `scope_keys` in `kpi_pipeline/scope.py`), and when `use_fiscal_calendar=False` those daily-side `Year` values are the **calendar year of `date`** (see "Fiscal calendar vs native time grain" below). If your `year_col` source instead follows ISO week-year numbering (late-December rows carrying next year's value), the join silently mismatches and those rows drop out of scope entirely — no error, just missing weeks. Only use `year_col`/`week_col` when the source table has no date column at all, and confirm its `year` column is a genuine calendar year before relying on it.

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

### Scope debug (product/store counts before the full run)

Before the heavy KPI computation, sanity-check scope with a lightweight, read-only pre-flight count. The **Scope debug** cell in `main.ipynb` (between the input previews and the pipeline run) calls:

```python
runner.build_dimensions()
runner.build_scopes(fund_paste=fund.paste)
display(runner.scope_debug_summary())
```

`scope_debug_summary()` returns a pandas DataFrame with distinct `product_id`, `store_id`, and pair counts for the **final scope** (after hybrid backfill and manual adjustments) — one `overall` row plus one row per **active slice dimension** value (`slices`, `derived_dimensions`, and any enabled `dimension_sources`). It applies the same `value_filters` the KPI step uses, so the counts match what `kpi_long` reports per slice. For a product-week scope (no `store_col`), only `distinct_product_count` is shown.

`build_dimensions()` and `build_scopes()` are idempotent; Cell 3's `runner.run()` rebuilds the same scope as part of the full pipeline. NULL slice values appear here as the string `"NULL"`; in `kpi_long` those rows carry an empty/None `dimension_value`.

## Dimension sources (extra slices from other tables)

**What it is.** By default, every slice dimension (the breakdown columns in the report) comes from the **products master** (`master-data/products`) — either a real column listed in `slices.dimensions` or a `slices.derived_dimensions` SQL expression over products columns. `dimension_sources` lets you pull a breakdown column from a **different table** when it does not live on products.

**Why it exists.** Some segmentations are not products attributes. The classic example is tbretail's **NVROUT** flag, which is derived from `program` on `operation/extended_product`, not `master-data/products`. Without this feature you could only slice by products columns; the segment would be invisible to the report. `dimension_sources` joins that external column onto the product attribute table *after* scope is built, so it becomes a normal slice dimension everywhere downstream (`kpi_long`, comparisons, HTML).

**It is gated and opt-in.** The default config ships one **disabled** example. With nothing enabled, behaviour is exactly as before — slices come from products only. A data scientist who does *not* need external dimensions never has to think about this block; one who does flips `enabled: True` on a source. **If your breakdown column already exists on (or is derivable from) the products table, use `slices` — not this.** Dimension sources are only for columns that genuinely live elsewhere.

### How it works

Each **enabled** source is **left-joined** onto the product attribute projection by `join_key` (normally `product_id`, and it must already be a products column). The source's raw `columns` and `derived` SQL expressions (evaluated against the **source** table) become slice dimensions automatically — you do **not** also need to list them in `slices.dimensions`. They appear under `ACTIVE_SLICE_DIMENSIONS` in the Cell 1 / fiscal log.

```python
"dimension_sources": [
    {
        "enabled": True,
        "label": "extended_product",
        "source": "delta",                              # "delta" | "csv"
        "path_segments": ["operation", "extended_product"],
        "join_key": "product_id",
        "columns": [],                                  # raw source columns to carry over
        "derived": {                                    # Spark SQL over the SOURCE table
            "is_nvrout": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END",
        },
    },
],
"slices": {"dimensions": ["brand"], "derived_dimensions": {}},
```

This produces an `is_nvrout` slice (`yes` / `no`) alongside `brand` — the `yes` panel is your NVROUT KPIs.

### Source-specific behaviour to know

- **One row per `join_key`.** The source is reduced with `dropDuplicates([join_key])` **before** the join so it can never fan out (duplicate) the product rows. If the raw table has several rows per product (e.g. `extended_product` with multiple `program` values), the surviving row is **arbitrary** — pre-aggregate the source to one row per product (or to a clean membership flag) before pointing the toolkit at it, exactly as the v4 script did with `.select(product_id).distinct()`. The toolkit does **not** clean the source (same data-quality contract as scope adjustments).
- **Mutually exclusive within one column.** A single dimension column gives each product one value. To represent **overlapping** segments (e.g. v4's "COMP includes NVROUT"), model them as **independent boolean dimensions** — `is_nvrout`, `is_comp` — each its own slice; a product can be `yes` under both. A single `segment` column cannot express the overlap.
- **Products missing from the source get NULL.** The join is a **left** join (it must keep every product). A product with no row in the source table gets `NULL` for the new dimension — *not* a default like `"no"`. A `CASE WHEN program LIKE '%NVROUT%' ... ELSE 'no'` expression only yields `"no"` for products that **have** a row in the source; products absent from `extended_product` are `NULL` (their own panel). Make the source cover the full product universe, use `fillna` (below), or accept the `NULL` bucket, if you want a clean `yes`/`no` split.
- **`fillna` — impute the NULL side of a partial source.** `dimension_sources[].fillna` is `{dim_name: default_value}`, applied **after** the join, coalescing NULLs (products absent from the source) to a literal. This is the direct fix for a source that intentionally lists only one side of a split (e.g. a CSV of just the NON-COMP product_ids) — instead of leaving the complement `NULL`, it reads as the value you choose:

  ```python
  "dimension_sources": [
      {
          "enabled": True,
          "label": "ngf_comp_split",
          "source": "csv",
          "path": "/Workspace/Users/you@invent.ai/lists/ngf_product_ids.csv",
          "location": "workspace",
          "join_key": "product_id",
          "derived": {"is_comp": "'no'"},      # only fires for rows present in the CSV (NGF items)
          "fillna": {"is_comp": "yes"},        # everyone else -> 'yes' instead of NULL
      },
  ],
  ```

  `fillna` keys must be among that source's own `columns`/`derived` dimensions — the toolkit fails loudly otherwise. Nothing is removed from scope; this only affects the `is_comp` slice breakdown, same as any other `dimension_sources` entry.
- **Enabled sources fail loudly.** Unlike `derived_dimensions` (best-effort, skipped on error), an **enabled** dimension source raises on a bad path, missing column, or unresolved expression — a silently dropped segment would misreport the very breakdown you added it to produce. Disable the source if you want it ignored.
- **CSV location.** Delta or CSV; CSV honours the same `location` (`datastore` / `workspace`) and `csv_options` as [scope adjustments](#manual-scope-adjustments).

### Value filters (restrict slice values / drop the NULL bucket)

`value_filters` restricts which values of a slice dimension appear **in that dimension's own report breakdown** — it never affects Overall or any other slice. It is config-only (no environment variable). Available on both `slices.value_filters` and each `dimension_sources[].value_filters`. Each entry accepts **two shapes**:

**LIST form** (include-only; the original, still supported):

- **Dim omitted** — keep **all** values, including `NULL` (default).
- **`[]` (empty list)** — keep all **non-null** values; drops only the `NULL` bucket.
- **`["v1", "v2", ...]`** — keep **only** those values; `NULL` and any unlisted value are dropped.

**DICT form** (include and/or exclude, NULL-aware):

- **`{"include": ["A", "B"]}`** — keep **only** `A` and `B` (same as the list form; `NULL` dropped).
- **`{"exclude": ["X", "Y"]}`** — keep **everything except** `X` and `Y`, **including `NULL`**. This is how you drop a set and keep the whole remainder.
- **`{"include": [...], "exclude": [...]}`** — apply the include set first, then remove the excludes.
- **`"keep_null": true｜false`** — optional; force-keep or force-drop the `NULL` bucket. Default: `NULL` is dropped when `include` is present, kept when only `exclude` is given.

```python
"dimension_sources": [
    {
        "enabled": True,
        "label": "extended_product",
        ...
        "derived": {"is_nvrout": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END"},
        "value_filters": {"is_nvrout": ["yes"]},   # numbers only for the NVROUT universe
    },
],
"slices": {
    "dimensions": ["brand"],
    "derived_dimensions": {},
    "value_filters": {},   # e.g. {"brand": ["NIKE", "ADIDAS"]} or {"brand": []} to drop a NULL brand bucket
},
```

`is_nvrout: ["yes"]` above means the `is_nvrout` breakdown in `kpi_long` reports **numbers only for the NVROUT universe** (the `no` and `NULL` panels are dropped); `brand`, Overall, and every other slice are untouched.

**Excluding a set but keeping the rest (e.g. a "not going forward" / NFG list).** When your source only lists the products to *drop* — so non-listed products come through as `NULL` — the list form cannot select the complement. Two options: give the complement a real label with `fillna` (see above — the breakdown then has clean named panels instead of a `NULL` one), or keep the `NULL` remainder and use `exclude`:

```python
"dimension_sources": [
    {
        "enabled": True,
        "label": "nfg_list",                    # a CSV/Delta list of the not-going-forward product_ids
        "source": "csv",
        "path": "/Workspace/Users/you@invent.ai/lists/nfg.csv",
        "location": "workspace",
        "join_key": "product_id",
        "derived": {"nfg": "CASE WHEN product_id IS NOT NULL THEN 'nfg' END"},  # 'nfg' for listed rows
        "value_filters": {"nfg": {"exclude": ["nfg"]}},   # breakdown = everyone EXCEPT NFG (NULL kept)
    },
],
```

The `nfg` breakdown then covers the going-forward (COMP) universe only, while Overall and `brand` still include the NFG products. (Rows kept by `exclude` here carry `nfg = NULL`, so they appear under the dimension's `NULL`/None panel — give the flag a full-universe `ELSE` value if you want a cleaner label.)

### Scope vs slices — two different machines

| Need | Use | Effect |
| ---- | --- | ------ |
| Include/exclude which (product, store, week) rows enter the KPIs (e.g. a JAB include list) | `scope_adjustments` | Changes scope **membership**; the addition's `scope_origin` label is for reporting only — it is **not** a report breakdown |
| Break the report out by a segment (NVROUT vs COMP, etc.) | `slices` + `dimension_sources` | Adds a **dimension** the report groups by |

`scope_adjustments` decide *which rows*; `slices` / `dimension_sources` decide *how rows are grouped*. A typical tbretail setup uses both: a JAB scope addition **and** an `is_nvrout` dimension source.

## Comparable pairs (like-for-like)

**Gated, opt-in** (default off). When enabled, for each comparison (YoY / QoQ / MoM / WoW) the metrics are recomputed over **only the `(product_id, store_id)` pairs present in both compared periods**, then compared. This isolates like-for-like movement (same pairs in both periods) from mix shifts caused by newly listed or closed pairs — the same idea as v4's `_pairs_same_calendar_years` same-pair YoY.

```python
"comparable_pairs": {
    "enabled": True,   # default False
},
```

Or set `KPI_COMPARABLE_PAIRS=true`.

**How it works**
1. For each comparison kind, the two most recent periods present in the run window are the compared pair (prior, current).
2. The comparable universe = pairs that appear in **both** periods (`scoped_daily` presence in each), intersected.
3. All metric frames (sales/inventory, service metrics, lost sales) are restricted to that pair set, and metrics are recomputed for both periods — for **Overall and every slice**. Because each slice dimension is a product attribute, the single overall intersection grouped by slice equals a per-slice intersection.
4. The standard comparison logic diffs the two periods on that fixed universe.

**Outputs**
- `comparable_kpi_long` — per-period comparable metrics, tagged with `comparison_type` and `comparable_pair_count` (universe size).
- `comparable_comparison_{yoy,qoq,mom,wow}` — comparison rows (same schema as the regular `comparison_*` tables).
- HTML report — a second **"Comparable YoY/QoQ/MoM/WoW"** comparison table beneath the standard one in every value panel.
- Notebook — a "Comparable pairs (like-for-like)" cell.

**Important**
- A comparable comparison is produced only when the **run window spans both compared periods** (e.g. a multi-year window for comparable YoY). When the run window is narrow (single-week refresh), comparable metrics for that kind will be skipped for that run — but any previously saved comparable_kpi_long history is preserved and comparisons are recomputed from it on the next full-window run.
- `comparable_kpi_long` is merged incrementally across runs (same as `kpi_long`), and comparable comparison tables are recomputed from the merged history — so a single-week refresh can still produce comparable YoY/QoQ/MoM/WoW relative to prior saved history (as long as comparable_kpi_long has data for both compared periods across runs).
- Each `run_date` partition of `comparable_kpi_long` is a self-contained snapshot of the full merged history for comparable pairs as of that run.

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

### Fiscal calendar vs native time grain

Controlled by `fiscal_calendar.use_fiscal_calendar`:

- **`True` (default)** — Year/Week/Quarter/Month come from the uploaded `one_time_uploads/fiscal_cal` table.
- **`False`** — the time grain is derived from `noob/daily-data` (`fiscal_calendar.daily_time_columns`). **Year is the CALENDAR year of `date`** (`F.year(date)`), and **Week is the native fiscal week column** (`daily_time_columns.week`). Quarter/Month are also derived from `date`.

  The source `year` column (`daily_time_columns.year`) is **not** used for the reporting year — it can carry the **ISO week-year**, which labels late-December weeks as the *following* year (e.g. a week starting Dec 29, 2025 shown as `year=2026`). Using it directly mislabelled December as `Q4 2026` instead of `Q4 2025`. Deriving Year from `date` fixes this.

  **Caveat**: a native fiscal week that straddles Jan 1 is split into two partial weeks in the Weekly view — one dated in the old calendar year, one in the new — instead of being a single week. Quarter, Month, and annual rollups are unaffected and remain correct.

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
| `kpi_long` | All metrics × periods × slices (annual / quarter / monthly / weekly) | `period_type`, `period`, `dimension`, `dimension_value` |
| `comparison_yoy` | YoY comparison rows | `comparison_type`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `comparison_qoq` | QoQ comparison rows | same as YoY |
| `comparison_mom` | MoM comparison rows | same as YoY |
| `comparison_wow` | WoW comparison rows | same as YoY |
| `scope_diff` | Defined vs score annual diff (when `run_scope_diff=True`) | `Year`, `metric` |
| `comparable_kpi_long` | Comparable (like-for-like) per-period metrics + `comparable_pair_count` (when `comparable_pairs.enabled=True`) | `comparison_type`, `period_type`, `period`, `dimension`, `dimension_value` |
| `comparable_comparison_{yoy,qoq,mom,wow}` | Comparable comparison rows (when `comparable_pairs.enabled=True`) | recomputed from merged `comparable_kpi_long` — overwrites the partition |

Merge keys are defined in `kpi_pipeline/io.py` (`TABLE_ROW_KEYS`). Incremental mode uses these keys to decide append vs skip vs overwrite. `comparison_*` tables are recomputed from the merged `kpi_long` and overwritten wholesale; `comparable_kpi_long` is merged incrementally like `kpi_long` and `comparable_comparison_*` tables are then recomputed from the merged `comparable_kpi_long` (see [Comparable pairs](#comparable-pairs-like-for-like)).

### Save modes

| `save_mode` | What it does | When to use |
| ----------- | ------------ | ----------- |
| `initial` | Writes all rows from the current run. **Fails** if any output table already exists at the path. | First-ever backfill only (safety guard against accidental overwrite). |
| `incremental` | Loads the **latest existing `run_date` partition on or before** this run, **appends** rows whose merge keys are not yet saved, **skips** overlapping keys (unless overwrite allowed), and writes the merged result to this run's `run_date` partition. | Weekly refresh — add new weeks/periods; history accumulates even as `run_date` advances. |
| `full_refresh` | **Overwrites** each output table entirely with whatever the current run produced. No merge with prior saved data. | Rebuild outputs from scratch for the current run window. |

**Incremental — append missing keys only**

- **Yes:** Only rows with merge keys that do not already exist in the saved table are appended.
- **No (by default):** Rows for periods/slices already on disk are **not** updated — they are skipped.
- Set `allow_overwrite_existing: True` to replace overlapping keys with the new run's values.

Example: first run (`as_of_date=2026-06-15`) saves 2024–2026 into `run_date=2026-06-15`. A later weekly run (`as_of_date=2026-06-22`, `run_min_date`=last Sunday) loads that prior partition, appends only the new week (e.g. `2026-W25`), and writes the full merged result into `run_date=2026-06-22`. Existing weeks stay unchanged unless overwrite is enabled. Because the merge reads the **latest** prior partition, history accumulates across runs even though each `run_date` advances.

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

- **Incremental accumulates onto the latest partition:** under `incremental`, `kpi_long` is merged onto the **latest existing `run_date` partition on or before** the current run (not the partition being written), so weekly runs whose `run_date` advances with `as_of_date` build up history instead of writing isolated single-week snapshots. Each `run_date` partition is a self-contained snapshot of the full merged history as of that run.
- **Comparisons recomputed from merged history (incremental):** after merging `kpi_long`, the toolkit re-reads the merged partition and recomputes YoY/QoQ/MoM/WoW from the **full saved history**, then overwrites the comparison tables in that partition. A single-week refresh can therefore still produce a YoY vs last year. The same applies to comparable (like-for-like) tables: `comparable_kpi_long` is merged incrementally and comparable comparison tables are recomputed from it. `ctx.comparison_*` / `ctx.comparable_comparison_*` and the notebook/HTML displays reflect the merged history after save. Disable with `output.recompute_comparisons_from_history=False` (or `KPI_RECOMPUTE_COMPARISONS=false`) to keep comparisons scoped to the current run.
- **Notebook vs saved Delta on overlapping period values:** under `allow_overwrite_existing=False`, overlapping `kpi_long` keys keep the prior saved values (e.g. a stale partial-year annual total is not replaced by a narrower re-run). Enable overwrite or use `full_refresh` to replace them.
- **Empty outputs skipped:** If a table is empty for this run (e.g. comparisons on a very narrow window), the write for that table is skipped and prior Delta data is left unchanged — even under `full_refresh`.

### Selecting which comparisons to run

`comparisons.enabled` chooses which period-over-period comparisons are **computed, printed, saved, and rendered**. Pick any subset of `"yoy"` (year-over-year), `"qoq"` (quarter-over-quarter), `"mom"` (month-over-month), `"wow"` (week-over-week):

```python
"comparisons": {
    "enabled": ["yoy"],          # only year-over-year; others are skipped entirely
},
```

- Only the selected kinds produce `comparison_{kind}` (and, when `comparable_pairs.enabled=True`, `comparable_comparison_{kind}`) Delta tables and HTML comparison columns. Unselected kinds are never computed or written.
- `kpi_long` (the raw per-period metrics) is **always** produced in full — this setting only gates the *comparison* tables, not the underlying period data.
- **Reading a comparison from saved history:** a single latest-week run can still produce e.g. YoY. With `save_mode="incremental"` and `recompute_comparisons_from_history=True`, the selected comparisons are rebuilt from the **full merged `kpi_long`** (this run's window unioned onto prior saved runs) — so `["yoy"]` on a one-week run compares the current (partial) year against last year's saved annual total. Requires prior saved history at an earlier `run_date` partition.
- Invalid or empty selections fail loudly at config `materialize()`. Environment override: `KPI_COMPARISONS="yoy,mom"` (comma-separated).

### Config keys

```python
"output": {
    "save_outputs": False,          # True to write Delta tables
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,               # null = use reporting_window.as_of_date for run_date= partition
    "save_mode": "incremental",     # initial | incremental | full_refresh
    "allow_overwrite_existing": False,
    "recompute_comparisons_from_history": True,  # incremental: recompute YoY/QoQ/MoM/WoW from merged kpi_long
}
```

Environment overrides: `KPI_SAVE_OUTPUTS`, `KPI_OUTPUT_SAVE_MODE`, `KPI_ALLOW_OVERWRITE_EXISTING`, `KPI_OUTPUT_PATH` (comma-separated path segments), `KPI_OUTPUT_RUN_DATE`, `KPI_RECOMPUTE_COMPARISONS`.

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

NATIVE path (set `date_col: None` and both `year_col`/`week_col`) — only use this when the scope table has **no date column at all**. It takes `Year` verbatim from your source, bypassing `fiscal_cal` entirely, so it must already be a genuine calendar year — see the ⚠️ warning under [Manual scope adjustments](#manual-scope-adjustments) for why an ISO week-year column here would silently drop late-December weeks from scope.

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


| Variable                          | Effect                                                       |
| --------------------------------- | ------------------------------------------------------------ |
| `KPI_BUCKET`                      | Datastore mount (default `/mnt/invent-{customer}-datastore`) |
| `KPI_CUSTOMER`                    | Customer slug                                                |
| `KPI_AS_OF_DATE`                  | Overrides `as_of_date`                                       |
| `KPI_RUN_MIN_DATE`                | Overrides `run_min_date`                                     |
| `KPI_USE_HYBRID_SCOPE`            | `true`/`false`                                               |
| `KPI_RUN_SCOPE_DIFF`              | `true`/`false` — enable defined-vs-score scope diff          |
| `KPI_COMPARABLE_PAIRS`            | `true`/`false` — enable comparable (like-for-like) pairs     |
| `KPI_RECOMPUTE_COMPARISONS`       | `true`/`false` — recompute comparisons from merged history (default `true` under incremental) |
| `KPI_USE_FISCAL_CALENDAR`         | `true`/`false`                                               |
| `KPI_SCOPE_MIN_PERCENTILE`        | e.g. `20` or `0.2`                                           |
| `KPI_SCOPE_MIN_WEEKS_FOR_FILTER`  | Integer                                                      |
| `KPI_SLICE_DIMENSIONS`            | Comma-separated column names                                 |
| `KPI_SAVE_OUTPUTS`                | `true`/`false`                                               |
| `KPI_OUTPUT_SAVE_MODE`            | `initial`, `incremental`, or `full_refresh`                  |
| `KPI_ALLOW_OVERWRITE_EXISTING`    | `true`/`false`                                               |
| `KPI_OUTPUT_PATH`                 | Comma-separated path segments                                |
| `KPI_OUTPUT_RUN_DATE`             | `output.run_date` partition (default: `as_of_date`)          |
| `KPI_HTML_ENABLED`                | `true`/`false`                                               |
| `KPI_HTML_FILENAME`               | Output filename                                              |
| `KPI_HTML_TITLE`                  | Report title                                                 |
| `KPI_HTML_OUTPUT_PATH`            | Comma-separated path segments for `html_report.output_path_segments` |
| `KPI_HTML_WEEKLY_WEEKS`           | Recent fiscal weeks in Weekly tab (default 5; empty = all)   |
| `KPI_HTML_MONTHLY_MONTHS`         | Recent months in Monthly tab (default 5; empty = all)        |
| `KPI_HTML_QUARTERLY_QUARTERS`     | Recent quarters in Quarter tab (default 5; empty = all)      |
| `KPI_HTML_YEARLY_YEARS`           | Recent years in Annual tab (default 5; empty = all)          |
| `KPI_RUN_MODE`                    | `full` or `html_only` — skip pipeline and render HTML from saved outputs |


## Metrics

Default metrics (configurable in `CONFIG["metrics"]`):

- Sales / inventory (all scoped stores): `total_sales_quantity`, `total_sales_revenue`, `AUR`, `total_inventory`
- Coverage (all scoped stores): `distinct_product_count`, `distinct_store_count`, `distinct_pair_count`
- Service stores only: `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate`, `in_stock_rate`, `weighted_instock_rate`, `lost_sales_pct`

**Lost Sales %** = `100 × sum(lost_sales) / sum(floor(weekly_sales + lost_sales))` — denominator includes imputed lost demand.

**In-Stock Rate** = `sum(in_stock_days) / sum(available_days)` from top-down lost-sales output (service stores only).

**Weighted In-Stock Rate** = sales-weighted average of weekly in-stock rates: each fiscal week's in-stock rate is weighted by that week's sales volume when rolling up to the reporting period. Weeks with higher sales carry more weight. Reported as pp-change in comparisons (service stores only).

**WOS** = per-product per-fiscal-week WOS after summing daily inventory/sales across service stores at product×date (`avg_daily_inventory / weekly_sales`), then rolled up to the reporting period using a sales-weighted average. Not computed at product×store×week grain.

**Inventory Turnover Rate** = Sales Units ÷ Mean Stock for the same period grain (service stores only). The HTML report labels it **Annual**, **Quarterly**, or **Weekly** Inventory Turnover Rate in the matching period tab.

## Programmatic use

```python
from kpi_pipeline import KPIRunner
from kpi_pipeline.io import save_outputs, load_saved_outputs

# Pre-flight scope debug (distinct product/store counts overall + per slice)
runner = KPIRunner(spark, settings)
runner.build_dimensions()
runner.build_scopes(fund_paste=fund.paste)
print(runner.scope_debug_summary())

# Full run
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
- **Incremental skip vs notebook output**: Saved Delta can retain old values for overlapping period keys while the notebook shows fresh `kpi_long` — set `allow_overwrite_existing=True` to replace.
- **WoW with sparse weeks**: Week-over-week compares the last two weeks **present in `kpi_long`**, not necessarily consecutive fiscal weeks when weekly coverage is sparse (e.g. after a narrow `run_min_date` or partial backfill).

## Troubleshooting


| Symptom                     | Likely cause                                                                |
| --------------------------- | --------------------------------------------------------------------------- |
| `initial save blocked`      | Output tables already exist — switch to `incremental` or `full_refresh` (see [Output saves](#output-saves)) |
| Overlapping periods skipped | Expected with `incremental` + `allow_overwrite_existing=False` — set `True` to replace |
| Saved Delta stale vs notebook | Incremental skip kept old rows on disk while notebook shows fresh `kpi_long` — enable overwrite or use `full_refresh` |
| Comparisons skipped         | Need ≥2 years (YoY), ≥2 quarters (QoQ), or ≥2 fiscal weeks (WoW) in window  |
| Empty slice dimension       | Column missing from `master-data/products` or derived SQL failed validation — if it lives on another table, add a [dimension source](#dimension-sources-extra-slices-from-other-tables) |
| Dimension source errors on read | An **enabled** dimension source fails loudly on bad path / missing column / bad expression (by design) — fix the source or set `enabled: False` |
| Slice value count looks doubled | A `dimension_source` table has multiple rows per `join_key` — pre-aggregate to one row per product (toolkit keeps an arbitrary row, see [Dimension sources](#dimension-sources-extra-slices-from-other-tables)) |
| `is_nvrout` (or another slice) shows a NULL bucket, or you want only one value (e.g. only NVROUT products) | set `value_filters` for that dimension in `slices` or the `dimension_source`: `["yes"]` keeps only `yes`; `[]` drops the NULL bucket; see [Value filters](#value-filters-restrict-slice-values--drop-the-null-bucket) |
| A `dimension_source` NULL bucket should really read as a real value (e.g. "everyone not on this list is comp") | add `fillna: {dim_name: default}` on that `dimension_sources` entry — coalesces the NULL left by the join to your literal; see [Dimension sources](#dimension-sources-extra-slices-from-other-tables) |
| Scope debug counts don't match `kpi_long` per slice | The debug cell recomputes scope independently — re-run it after any `config.py` change (scope mode, adjustments, `value_filters`) so it reflects the same scope Cell 3 builds. NULL slice values show as `"NULL"` here but as blank/None in `kpi_long` |


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
| **Period tabs** | Annual / Quarter / **Monthly** / Weekly (horizontal) |
| **Slice dimension tabs** | Overall + every slice column in `kpi_long` (inferred automatically from data and config) |
| **Value tabs** | Vertical sidebar within each slice dimension — one panel per value (e.g. each brand) |
| **KPI tables** | Metrics as rows (colour-coded), periods as columns; inventory turnover is labelled **Annual** / **Quarterly** / **Monthly** / **Weekly** per tab |
| **Comparison** | YoY / QoQ / MoM / WoW per value panel |
| **Comparable comparison** | Second comparison table per panel (when `comparable_pairs.enabled=True`) |
| **Metric Details tab** | Definition, store scope, and formula for every active metric |

Slice dimensions and values are **inferred from `kpi_long`** — if you configure `category` instead of `brand`, or add multiple slice columns, the report adapts without code changes.

### Config keys

```python
"html_report": {
    "enabled": True,
    "filename": "kpi_report_{customer}_{report_end}.html",
    "report_title": None,                # None = "<CUSTOMER> KPI Report"
    "output_path_segments": None,        # None = local only; or path segments to also save to datastore
    "metric_definitions": {},            # override DEFAULT_METRIC_DEFINITIONS entries
    "weekly_display_weeks": 5,           # Weekly tab: N most recent weeks; null = all
    "monthly_display_months": 5,         # Monthly tab: N most recent months; null = all
    "quarterly_display_quarters": 5,     # Quarter tab: N most recent quarters; null = all
    "yearly_display_years": 5,           # Annual tab: N most recent years; null = all
}
```

### Environment variable overrides

| Variable | Effect |
| -------- | ------ |
| `KPI_HTML_ENABLED` | `true`/`false` |
| `KPI_HTML_FILENAME` | Output filename |
| `KPI_HTML_TITLE` | Report title |
| `KPI_HTML_OUTPUT_PATH` | Comma-separated path segments for datastore HTML copy |
| `KPI_HTML_WEEKLY_WEEKS` | Recent fiscal weeks in Weekly tab (default 5; empty = all) |
| `KPI_HTML_MONTHLY_MONTHS` | Recent months in Monthly tab (default 5; empty = all) |
| `KPI_HTML_QUARTERLY_QUARTERS` | Recent quarters in Quarter tab (default 5; empty = all) |
| `KPI_HTML_YEARLY_YEARS` | Recent years in Annual tab (default 5; empty = all) |

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

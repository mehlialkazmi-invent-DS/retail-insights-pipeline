<p align="center">
  <img src="docs/images/readme-banner.png" alt="Retail KPI analytics — sales, inventory, in-stock rate, and weeks of supply" width="100%" />
</p>

# Retail Insights Pipeline

PySpark toolkit for weekly, monthly, quarterly, annual, and YTD retail KPIs with configurable scope (defined-only or hybrid with score backfill), optional manual scope adjustments, comparable (like-for-like) pair analysis, and incremental Delta output saves.

Designed to run on **Databricks** against the customer Delta datastore (`/mnt/invent-{customer}-datastore`).

## What it produces


| Output                                  | Description                                                                                                                                            |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kpi_long`                              | One tidy table: `period_type` (annual / **ytd** / quarter / monthly / weekly), `period`, `root`, `dimension`, `dimension_value`, plus all configured metrics. `root` is `"overall"` plus one per configured [dimension_source root](#roots-and-cuts-report-structure); `dimension`/`dimension_value` is the cut within that root. Filter this to reproduce any root/cut/period panel. |
| `comparison_yoy / ytd`                  | Prior vs current period with formatted display columns, per root × cut. YoY is the last two annual periods; YTD compares the **same** elapsed-window **across years**, chained across every consecutive year pair present (see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)). There is no separate QoQ/MoM/WoW comparison table — see the Quarter/Monthly/Weekly `kpi_long` period_type rows for recent-period value trends. Both are recomputed from the full merged kpi_long history on incremental saves. |
| `scope_diff`                            | Side-by-side annual KPIs for **defined-only** vs **score-only** scope (optional sanity check). Only computed when `scope.run_scope_diff=True`. Compares scope **before** manual adjustments — intentional diagnostic of defined vs score coverage. |
| `comparable_kpi_long` / `comparable_comparison_ytd` | Like-for-like YTD metrics over only the pairs present in both years of each consecutive-year link, per root × cut. Gated on `comparable_pairs.enabled=True`. |
| **HTML report**                         | Standalone offline HTML with tabbed layout (an outer root tab when more than one root exists), Metric Details, and client/period info panel (see [HTML report](#html-report) section below). |


Saved Delta tables live under `PATH_OUTPUT_ROOT`, partitioned by `run_date` per table:

```
{bucket}/{output.path_segments}/{table_name}/run_date={as_of_date}/
```

Default example: `.../analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`

## Repository layout

```
retail-insights-pipeline/
├── README.md           # This file
├── config.py           # Generic reference template -- copy + customize per client (see below)
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
    ├── comparisons.py  # YoY / YTD + defined vs score diff
    ├── comparable.py   # Gated like-for-like YTD comparison (per-link pair restriction)
    ├── io.py           # Incremental Delta saves + save plan preview
    ├── html_report.py  # Standalone HTML renderer (offline, tabbed report)
    └── context.py      # Shared runtime state (KPIContext)
```

## Quick start (Databricks)

**`config.py` is a generic reference template, not any specific customer's deployed config.** Every optional feature (`scope_adjustments`, `dimension_sources`, `lost_sales_ensemble`, `instock_source`, `comparable_pairs`) ships disabled with an illustrative placeholder example — copy this file per client, then replace `customer`, every `path_segments` entry, `defined_scope`'s column names, and any business rules (scope adjustments, dimension sources) with that client's own values. A real, fully-wired-up deployed config for one customer (tbretail) lives as `tbretail_config.py`, a sibling file one directory up from this repo — diff against it to see what a genuinely customized config looks like in practice (it currently reads `lost_sales_source`/`instock_source` directly from a customer-specific reporting table instead of running `lost_sales_ensemble`, and has real `scope_adjustments`/`dimension_sources` business rules wired in).

1. **Upload** to a Databricks workspace folder (same directory):
  - `main.ipynb`
  - `config.py`
  - the entire `kpi_pipeline/` folder
2. **Edit** `config.py` → `CONFIG` (at minimum):
  - `reporting_window.as_of_date` — anchor date for the run
  - `reporting_window.run_min_date` — optional narrow start (Sunday-aligned)
  - `scope.use_hybrid_scope` — `False` (default) for week-agnostic defined scope, `True` for hybrid (covered weeks + score backfill on missing weeks)
  - `defined_scope` — column mapping for your scope Delta table
  - `path_segments.defined_scope` — Delta path segments under the datastore bucket
  - `input_filters` — optional Spark SQL filters on defined scope, lost sales, daily data
  - `slices.dimensions` — product-master columns to cut by (e.g. `brand`), applied within every root
  - `dimension_sources` — optional, gated: each column becomes a root (a named, fully-broken-out population, e.g. NVROUT from `extended_product`) — see [Dimension sources → roots](#dimension-sources--roots-population-tabs-from-other-tables) and [Roots and cuts](#roots-and-cuts-report-structure)
  - `output.save_mode` — `initial`, `incremental`, or `full_refresh`
3. **Open** `main.ipynb` and **Run All** — Cell 2 previews inputs, then the **Scope debug** cell reports distinct product/store counts per slice, before the full pipeline run in Cell 3.
4. **Review** the save plan cell before the write cell runs. Set `allow_overwrite_existing=True` if you intentionally want to replace overlapping periods.

### Prerequisites

- Databricks cluster with PySpark and access to `/mnt/invent-{customer}-datastore` (or override with `KPI_BUCKET`).
- `algo_helpers` available on the cluster (`from algo_helpers import fundamentals as fund`).
- Delta tables referenced in `config.py` must exist for the chosen date window.

## Scope modes

`defined_scope.grain` controls how the scope table defines KPI membership — three values:

| `grain` | Universe | Week behaviour |
| ------- | -------- | -------------- |
| `"product"` | distinct `product_id` | store- and week-agnostic: every store, every window week, for each in-scope product |
| `"product_store"` (default) | distinct `(product_id, store_id)` | week-agnostic: every window week, for each in-scope pair |
| `"product_store_week"` | the scope table's own `(product_id, store_id, week)` rows | honoured (strict) — weeks come from `date_col` (or `year_col`/`week_col`) |

The two week-agnostic grains (`product`, `product_store`) flatten scope down to ids: a pair (or product) scoped in **any** period is scoped for **every** week in the report window — this is the key behaviour that keeps a pair that has **dropped out of the current-year scope weeks yet still transacts** in the window, instead of being silently dropped and undercounted. `product_store_week` is stricter: only the scope table's own weeks count.

Downstream always consumes `ctx.scope_keys` — `[product_id, Year, Week]` for `"product"`, `[product_id, store_id, Year, Week]` for `"product_store"`/`"product_store_week"`.

### Defined scope only (default: `use_hybrid_scope = False`)

Final scope = `defined_scope_keys` as built at the configured grain. For `product`/`product_store` that means every scope pair (or product) **× every week in the report window** — the scope table's own weeks are ignored entirely (`date_col`/`year_col`/`week_col` are not read). For `product_store_week`, only the scope table's own (product, store, week) rows survive, window-filtered. Score scope is **not** computed unless `scope.run_scope_diff=True`.

### Hybrid scope (`use_hybrid_scope = True`)

Hybrid = defined scope (at the configured grain) **+ score backfill on the window weeks the defined scope does not cover**:

1. **Covered weeks** — fiscal weeks in the window present in `defined_scope_keys`.
2. **Missing weeks** — fiscal weeks in the window with no rows in `defined_scope_keys`. These are backfilled from **score scope** (`scope_origin=score`): computed once over the **full** window, each `(product_id, store_id)` keeps the weeks whose weekly sales and **last available in-week inventory** clear a per-pair percentile threshold (**all stores included** — scope membership is store-agnostic; e-com is excluded later only for service metrics), restricted to the missing weeks.
3. **Hybrid union** — defined rows (`scope_origin=defined`) ∪ missing-week backfill rows (`scope_origin=score`).

For the week-agnostic grains (`product`, `product_store`) the defined scope already covers **every** window week, so there are no missing weeks and the backfill is a **no-op**. Hybrid backfill is only meaningful for `product_store_week`, whose covered weeks are exactly those present in the scope table.

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
| `workspace` | A Databricks **workspace** file (`/Workspace/Users/...`) | Read through the `file:` scheme, for CSVs kept alongside the notebook |

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

`scope_debug_summary()` returns a pandas DataFrame with distinct `product_id`, `store_id`, and pair counts for the **final scope** (after hybrid backfill and manual adjustments) — one `overall` row plus one row per **active slice dimension** value (`slices`, `derived_dimensions`, and any enabled `dimension_sources`). It applies the same `value_filters` the KPI step uses, so the counts match what `kpi_long` reports per slice. For `defined_scope.grain = "product"` (no store grain), only `distinct_product_count` is shown.

`build_dimensions()` and `build_scopes()` are idempotent; Cell 3's `runner.run()` rebuilds the same scope as part of the full pipeline. NULL slice values appear here as the string `"NULL"`; in `kpi_long` those rows carry an empty/None `dimension_value`.

## Dimension sources → roots (population tabs from other tables)

**What it is.** `dimension_sources` pulls a breakdown column from a table other than `master-data/products` (e.g. a program/channel flag) and joins it onto the product attribute projection. If your column already exists on (or derives from) products, use `slices` instead — this is only for columns that live elsewhere.

**Every dimension_source column is a ROOT, not a flat cut.** A root is a fully-broken-out population — like kpi-skill-toolkit's NVROUT/COMP major tabs — not just another breakdown column. `root_values` (`{dim_col: {raw_value: root_name}}`) controls which value(s) become a root and what to name them; omit it to auto-discover one root per distinct value found in the data. Every root additionally gets its own total plus a breakdown by each `slices` **cut** (brand, SMW, ...) — see [Roots and cuts](#roots-and-cuts-report-structure) below for the full model.

Gated/opt-in: the default config ships one disabled example; with nothing enabled, only the implicit `"overall"` root exists and the report looks exactly as before.

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
        "root_values": {"is_nvrout": {"yes": "nvrout"}},  # root "nvrout" = is_nvrout=='yes' only
    },
],
"slices": {"dimensions": ["brand"], "derived_dimensions": {}},
```

This produces an `"nvrout"` root (alongside `"overall"`), each broken out by `brand`.

### Source-specific behaviour to know

- **One row per `join_key`.** The source is `dropDuplicates([join_key])`'d before the join so it can't fan out product rows. If the raw table has several rows per product, the surviving row is arbitrary — pre-aggregate the source first. The toolkit doesn't clean the source (same contract as scope adjustments).
- **Left join → missing products get NULL**, not a default like `"no"`. A `CASE WHEN ... ELSE 'no'` expression only fires for products that have a row in the source at all; products absent from it are `NULL`. Use `fillna` (below) for a clean two-value split.
- **`fillna: {dim_name: default}`** coalesces NULLs (products absent from the source) to a literal, after the join. Fixes a source that only lists one side of a split (e.g. a CSV of NON-COMP ids):

  ```python
  "dimension_sources": [
      {
          "enabled": True, "label": "ngf_comp_split", "source": "csv",
          "path": "/Workspace/Users/you@invent.ai/lists/ngf_product_ids.csv",
          "location": "workspace", "join_key": "product_id",
          "derived": {"is_comp": "'no'"},   # only fires for CSV rows (NGF items)
          "fillna": {"is_comp": "yes"},     # everyone else -> 'yes' instead of NULL
          "root_values": {"is_comp": {"yes": "comp"}},
      },
  ],
  ```

  `fillna` keys must be among that source's own dimensions — fails loudly otherwise.
- **Enabled sources fail loudly** on a bad path, missing column, or unresolved expression (unlike `derived_dimensions`, which is best-effort). Disable a source to have it ignored.
- **CSV location**: same `location`/`csv_options` convention as [scope adjustments](#manual-scope-adjustments).

## Roots and cuts (report structure)

Every report row belongs to a **root** (which population) and a **cut** (how that population is broken down):

- **Roots** = `"overall"` (always, unrestricted) plus one per configured `dimension_sources[].root_values` entry (e.g. `"nvrout"`, `"comp"`) — each restricts the population to rows matching that value. A client with no root-producing `dimension_sources` gets a single `"overall"` root and the report looks exactly as it did before this existed.
- **Cuts** = `"overall"` (the root's own total, no further breakdown) plus each `slices.dimensions`/`derived_dimensions` entry (e.g. `brand`, `SMW`) — applied identically **within every root**, including `"overall"`.

So with `root_values: {"is_nvrout": {"yes": "nvrout"}}` and `slices.dimensions: ["brand"]`, the report has: `overall` root × `overall` cut (grand total), `overall` × `brand` (brand breakdown across everything), `nvrout` × `overall` (NVROUT total), `nvrout` × `brand` (brand breakdown within NVROUT only) — mirroring kpi-skill-toolkit's `overall_annual_segment` / `nvr_all_annual_brand` style outputs. `kpi_long`, the HTML report (root becomes an outer tab when there's more than one), and comparisons (`root` column added to `comparison_yoy`/`comparison_ytd`/`comparable_comparison_ytd`) are all root × cut aware.

### Value filters (restrict cut values / drop the NULL bucket)

`slices.value_filters` restricts which values of a **cut** dimension appear in that cut's own breakdown — never Overall or any other cut. Config-only, two shapes:

**LIST** (include-only): omit → keep all incl. `NULL` (default) · `[]` → drop only `NULL` · `["v1","v2"]` → keep only those, drop everything else incl. `NULL`.

**DICT** (include/exclude, NULL-aware): `{"include": [...]}` → same as LIST · `{"exclude": [...]}` → keep everything except those, **including `NULL`** · both together → include then remove excludes · `"keep_null": true/false` → force the NULL bucket either way (default: dropped when `include` is set, kept otherwise).

```python
"slices": {
    "dimensions": ["brand"],
    "derived_dimensions": {},
    "value_filters": {},   # e.g. {"brand": ["NIKE", "ADIDAS"]} or {"brand": []} to drop a NULL bucket
},
```

To exclude a set but keep the rest (e.g. a "not going forward" list that only lists what to drop, so everyone else is `NULL`): either give the complement a real label via `fillna`, or use `{"exclude": [...]}` to keep the `NULL` remainder.

### Scope vs. roots/cuts — three different knobs

| Need | Use | Effect |
| ---- | --- | ------ |
| Include/exclude which (product, store, week) rows enter the KPIs at all | `scope_adjustments` | Changes scope **membership** |
| A named, fully-broken-out population tab (NVROUT vs COMP) | `dimension_sources[].root_values` | Adds a **root** |
| Break any root's population out by a dimension (brand, SMW) | `slices` | Adds a **cut**, applied within every root |

`scope_adjustments` decide *which rows exist at all*; roots decide *which population a report view covers*; cuts decide *how a population is broken down*. A typical setup uses all three: a scope addition, a root for a segment flag, and a cut for brand.

## Comparable pairs (like-for-like, YTD-only)

**Gated, opt-in** (default off). When enabled, YTD metrics are recomputed over **only the `(product_id, store_id)` pairs present in both years of each consecutive-year link**, then compared. This isolates like-for-like movement from mix shifts caused by newly listed or closed pairs. There is no comparable YoY (or QoQ/MoM/WoW, which don't exist as comparison kinds at all — see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)).

```python
"comparable_pairs": {
    "enabled": True,   # default False
},
```

Or set `KPI_COMPARABLE_PAIRS=true`. Requires `"ytd"` to also be in `comparisons.enabled` — otherwise comparable pairs is skipped with a log message, since there's nothing to restrict.

**How it works — per link, not a fixed universe across the whole window**

Each consecutive-year link gets its **own** pair universe, computed from just that link's two years — not the intersection across every year in the run window. Concretely, with years 2024/2025/2026 all present:
- The **2025-vs-2026** link's universe = pairs present in **both 2025 and 2026** (2024 is irrelevant to this link).
- The **2024-vs-2025** link's universe = pairs present in **both 2024 and 2025** (2026 is irrelevant to this link).

A pair present in 2025 and 2026 but *not* 2024 still counts for the 2025-vs-2026 link, even though it would fail a "present in every year" test. Because each link's restriction is independent, the **same year's metric value can differ depending on which link it's paired with** — 2025's YTD revenue as the "current" value in the 2024-vs-2025 link (restricted to 2024∩2025 pairs) is generally not the same number as 2025's YTD revenue as the "prior" value in the 2025-vs-2026 link (restricted to 2025∩2026 pairs). This is expected, not a bug.

All metric frames (sales/inventory, service metrics, lost sales) are restricted to a link's pair set before metrics are computed, for **Overall and every slice**. Because each slice dimension is a product attribute, the single overall intersection grouped by slice equals a per-slice intersection.

**Outputs**
- `comparable_kpi_long` — per-link YTD metrics, tagged with `comparison_type="ytd"`, `comparable_pair_count` (that link's universe size), and `link_prior_year`/`link_current_year` (which link a row belongs to — needed because, as above, the same year can appear more than once with different values).
- `comparable_comparison_ytd` — comparison rows (same schema as the regular `comparison_ytd` table).
- HTML report — a second **"Comparable YTD"** comparison table beneath the standard one on the YTD panel only, stacked one mini-table per consecutive-year link when more than one exists.
- Notebook — a "Comparable pairs (like-for-like, YTD-only)" cell.

**Important**
- A comparable comparison is produced only when the run window spans at least 2 years. When the run window is narrow (single-week refresh), comparable YTD will be skipped for that run — but any previously saved `comparable_kpi_long` history is preserved and the comparison is recomputed from it on the next full-window run.
- `comparable_kpi_long` is merged incrementally across runs (same as `kpi_long`), and `comparable_comparison_ytd` is recomputed from the merged history — so a single-week refresh can still produce a comparable YTD comparison relative to prior saved history. The merge key includes `link_prior_year`/`link_current_year` precisely so that two different links' rows for the same year never collide.
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

**Ensemble note:** when `lost_sales_ensemble.enabled=True`, the `input_filters.lost_sales` list is applied to **both** the fast and slow lost-sales sources (same schema). Cell 2 also previews the slow source and the speed-cluster table in addition to the fast source — see [`lost_sales_ensemble`](#lost_sales_ensemble).

## Reporting window

Only two inputs in config:

- `as_of_date` — end anchor
- `run_min_date` — optional start narrow (aligned to Sunday)

Resolved automatically:

- **Start**: Sunday of Jan 1 (YTD) or Sunday of `run_min_date`
- **End**: **Last completed Saturday** on or before `as_of_date` (excludes the in-progress fiscal week)
- **Scope weeks**: any fiscal week whose Sun–Sat range **overlaps** the effective window is a window week; scope pairs are applied across all of them (and, under hybrid, covered vs missing weeks are split within this set)

**Input date ranges printed at read time**: to make it obvious what a run actually consumed (and catch stale/incomplete source data early), the two main time-series inputs always print the date span present in the source, independent of the usual verbose/quiet read logging:
- `daily_data` — min/max of its date column, printed once (the read is cached per run).
- `lost_sales` — min/max of `week_start_date`, printed once per source read (twice when `lost_sales_ensemble.enabled=True`: once for the fast/120-day source, once for the slow/365-day source).

These reflect what's actually in the source table (before the report-window filter is applied), so a max date short of the expected `REPORT_END_DATE` is a sign the source data is behind.

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

**`kpi_long` is saved in full, never trimmed.** The HTML report's period-display limits (`weekly_display_weeks`, `monthly_display_months`, etc. — see [HTML report](#html-report)) only narrow a separate in-memory `ctx.kpi_long_display` copy used for rendering; `ctx.kpi_long` itself — and therefore what Cell 5 / `save_outputs()` persists to Delta — always holds every period actually computed for the run window, regardless of the display limits. (Fixed: an earlier version trimmed `ctx.kpi_long` itself before the save step, so the notebook's Cell 3 → Cell 5 workflow — `runner.run(save=False)` then a separate `save_outputs()` call — silently persisted only the most recent ~5 weeks/months/quarters instead of the full computed window.)

### Tables written

| Table | Contents | Merge keys (incremental mode) |
| ----- | -------- | ----------------------------- |
| `kpi_long` | All metrics × periods × slices (annual / ytd / quarter / monthly / weekly) | `period_type`, `period`, `dimension`, `dimension_value` |
| `comparison_yoy` | YoY comparison rows | `comparison_type`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `comparison_ytd` | YTD comparison rows (one row set per consecutive-year pair, elapsed-window sums) | same as YoY |
| `scope_diff` | Defined vs score annual diff (when `run_scope_diff=True`) | `Year`, `metric` |
| `comparable_kpi_long` | Comparable (like-for-like) per-link YTD metrics + `comparable_pair_count` (when `comparable_pairs.enabled=True`) | `comparison_type`, `period_type`, `period`, `dimension`, `dimension_value`, `link_prior_year`, `link_current_year` |
| `comparable_comparison_ytd` | Comparable YTD comparison rows (when `comparable_pairs.enabled=True`) | recomputed from merged `comparable_kpi_long` — overwrites the partition |

There is no `comparison_qoq`/`comparison_mom`/`comparison_wow` table — see [Selecting which comparisons to run](#selecting-which-comparisons-to-run).

Merge keys are defined in `kpi_pipeline/io.py` (`TABLE_ROW_KEYS`). Incremental mode uses these keys to decide append vs skip vs overwrite. `comparison_*` tables are recomputed from the merged `kpi_long` and overwritten wholesale; `comparable_kpi_long` is merged incrementally like `kpi_long` and `comparable_comparison_ytd` is then recomputed from the merged `comparable_kpi_long` (see [Comparable pairs](#comparable-pairs-like-for-like-ytd-only)).

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
- **Comparisons recomputed from merged history (incremental):** after merging `kpi_long`, the toolkit re-reads the merged partition and recomputes YoY/YTD from the **full saved history**, then overwrites the comparison tables in that partition. A single-week refresh can therefore still produce a YoY vs last year. The same applies to the comparable (like-for-like) YTD table: `comparable_kpi_long` is merged incrementally and `comparable_comparison_ytd` is recomputed from it. `ctx.comparison_*` / `ctx.comparable_comparison_ytd` and the notebook/HTML displays reflect the merged history after save. Disable with `output.recompute_comparisons_from_history=False` (or `KPI_RECOMPUTE_COMPARISONS=false`) to keep comparisons scoped to the current run.
- **Notebook vs saved Delta on overlapping period values:** under `allow_overwrite_existing=False`, overlapping `kpi_long` keys keep the prior saved values (e.g. a stale partial-year annual total is not replaced by a narrower re-run). Enable overwrite or use `full_refresh` to replace them.
- **Empty outputs skipped:** If a table is empty for this run (e.g. comparisons on a very narrow window), the write for that table is skipped and prior Delta data is left unchanged — even under `full_refresh`.

### Selecting which comparisons to run

`comparisons.enabled` chooses which period-over-period comparisons are **computed, printed, saved, and rendered**. Pick any subset of `"yoy"`, `"ytd"` — these are the only two comparison kinds:

```python
"comparisons": {
    "enabled": ["yoy"],          # only year-over-year; ytd is skipped entirely
},
```

- **`yoy`** — full calendar/fiscal year vs the prior full year (last two annual periods).
- **`ytd`** — each year's **elapsed window** (only the fiscal quarters fully closed as of `as_of_date` for the latest year — see below) vs the prior year's same window, chained across consecutive years (e.g. `2026 YTD` vs `2025 YTD`, and `2025 YTD` vs `2024 YTD`). Use this instead of `yoy` once the current year is only partially reported — `yoy` would otherwise compare a partial current year against a full prior year, which reads as a much bigger drop/gain than what actually happened.
- **There is no separate QoQ/MoM/WoW comparison table.** The Quarter/Monthly/Weekly period tabs in the HTML report (and the corresponding `period_type` rows in `kpi_long`, always produced in full) show recent-quarter/month/week **value trends** — see the display-trim settings under [HTML report](#html-report) — without a delta/comparison table. If you need a quarter-over-quarter or month-over-month percentage change, compute it from consecutive `kpi_long` rows directly.
- Only the selected kinds produce `comparison_{kind}` (and, when `comparable_pairs.enabled=True` and `"ytd"` is selected, `comparable_comparison_ytd`) Delta tables and HTML comparison columns. Unselected kinds are never computed or written.
- `kpi_long` (the raw per-period metrics) is **always** produced in full, including a `"ytd"` `period_type` — this setting only gates the *comparison* tables, not the underlying period data.
- **YTD's elapsed window**: computed once per run from the **latest year present** — a fiscal quarter counts as "closed" only if every one of its weeks ends on or before `REPORT_END_DATE`. That same set of quarter numbers (e.g. just Q1, or Q1+Q2) is then summed for **every** year, so the comparison stays apples-to-apples even mid-year. A single-quarter or single-year window degrades gracefully: YTD still sums whatever quarters exist, and the comparison itself is simply absent if only one year is present.
- **HTML rendering**: `ytd`, if more than one consecutive-year pair exists, renders as several stacked mini comparison tables in one panel — one per year-pair, each labeled with its own period pair — rather than a single table. `yoy` always renders as a single table (exactly one pair).
- **Reading a comparison from saved history:** a single latest-week run can still produce e.g. YoY. With `save_mode="incremental"` and `recompute_comparisons_from_history=True`, the selected comparisons are rebuilt from the **full merged `kpi_long`** (this run's window unioned onto prior saved runs) — so `["yoy"]` on a one-week run compares the current (partial) year against last year's saved annual total. Requires prior saved history at an earlier `run_date` partition.
- Invalid or empty selections fail loudly at config `materialize()`. Environment override: `KPI_COMPARISONS="yoy,ytd"` (comma-separated).

### Config keys

```python
"output": {
    "save_outputs": False,          # True to write Delta tables
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,               # null = use reporting_window.as_of_date for run_date= partition
    "save_mode": "incremental",     # initial | incremental | full_refresh
    "allow_overwrite_existing": False,
    "recompute_comparisons_from_history": True,  # incremental: recompute YoY/YTD from merged kpi_long
}
```

Environment overrides: `KPI_SAVE_OUTPUTS`, `KPI_OUTPUT_SAVE_MODE`, `KPI_ALLOW_OVERWRITE_EXISTING`, `KPI_OUTPUT_PATH` (comma-separated path segments), `KPI_OUTPUT_RUN_DATE`, `KPI_RECOMPUTE_COMPARISONS`.

## Config reference

### `input_filters`

See [Input previews and filters](#input-previews-and-filters) above.

### `scope`

```python
"scope": {
    "use_hybrid_scope": False,  # False (default) = week-agnostic defined scope; True = hybrid
    "run_scope_diff": False,    # True = compute score scope and defined-vs-score annual diff
}
```

When `run_scope_diff=False` (default), the pipeline skips score-scope computation entirely unless `use_hybrid_scope=True` (hybrid backfill still needs score scope). The notebook scope-diff cell and `scope_diff` Delta output are omitted.

### `defined_scope`

```python
"defined_scope": {
    "grain": "product_store",   # "product" | "product_store" (default) | "product_store_week"
    "product_col": "product_id",
    "store_col": "store_id",
    "date_col": "week_start_date",
    "year_col": None,
    "week_col": None,
}
```

`grain` selects the scope universe (see [Scope modes](#scope-modes) above):

- `"product"` — distinct `product_id`; `store_col` is not required.
- `"product_store"` (default) — distinct `(product_id, store_id)`; **`store_col` is required**.
- `"product_store_week"` — the scope table's own `(product_id, store_id, week)` rows; **`store_col` is required**, and `date_col` (or `year_col`+`week_col`) is required to resolve weeks.

`date_col`/`year_col`/`week_col` are read **only for the `product_store_week` grain** (to resolve the scope table's own weeks) — for `product_store_week` this applies both to defined scope and, under hybrid, to finding covered vs missing weeks. For `"product"`/`"product_store"` they are ignored entirely.

DATE path (preferred, when your scope table has a date column):

```python
"date_col": "week_start_date",
"year_col": None,
"week_col": None,
```

NATIVE path (set `date_col: None` and both `year_col`/`week_col`) — only use this when the scope table has **no date column at all**. It takes `Year` verbatim from your source, bypassing `fiscal_cal` entirely, so it must already be a genuine calendar year — see the ⚠️ warning under [Manual scope adjustments](#manual-scope-adjustments) for why an ISO week-year column here would silently mislabel late-December weeks.

`materialize()` fails loudly if `grain` is invalid, if `store_col` is missing for `product_store`/`product_store_week`, or if `product_store_week` is missing both `date_col` and `year_col`/`week_col`.

### `score_scope`

Used when `use_hybrid_scope=True` or `run_scope_diff=True`. Applies to the **missing** (uncovered) weeks under hybrid scope.

```python
"score_scope": {
    "min_percentile": 0.2,
    "min_weeks_for_filter": 2,
}
```

### `lost_sales_ensemble`

**Off by default.** When disabled, the pipeline reads the single fast-mover model at `path_segments.lost_sales` (the 120-day model) exactly as before — other customers are unaffected.

```python
"lost_sales_ensemble": {
    "enabled": False,
    "slow_path_segments": ["noob", "lost-sales", "model_id=top_down_excluding_ecom_365days"],
    "speed_cluster_path_segments": ["noob", "product-cluster-attributes-snapshot"],
    "speed_cluster_format": "long",                      # "long" (default) | "wide"
    "speed_cluster_attribute_name": "sales_speed",        # used when speed_cluster_format="long"
    "speed_cluster_value_col": "product_speed_cluster",   # used when speed_cluster_format="wide"
    "fast_mover_clusters": [1, 2, 3],
}
```

When `enabled=True`, the pipeline blends **two** lost-sales models by product sales-speed cluster:

- Products whose `sales_speed` cluster is in `fast_mover_clusters` use the **fast (120-day)** model at `path_segments.lost_sales`.
- Every other product — slower clusters **and** products with no/NULL cluster row in `speed_cluster_path_segments` — uses the **slow (365-day)** model at `slow_path_segments`.
- The three lost-sales aggregate fields (`lost_sales`, `in_stock_days`, `total_days`) for a given `(product_id, store_id, week_start_date)` always come from **one** model — never mixed — so downstream in-stock-rate and lost-sales-% math stays internally consistent.
- A pair-week is kept only if the **chosen** model has a row for it (same as the legacy single-model behaviour); if the chosen side is missing from the full-outer join, the row is dropped rather than coalesced to zero.

**Speed-cluster source shape** (`speed_cluster_format`) — the cluster table can be either shape, so any client can point at whichever one it actually has:
- `"long"` (default) — a long-format attributes table with one row per `(product_id, attribute_name)`; filtered to `speed_cluster_attribute_name`, `attribute_value` is the cluster (1=fastest .. 5=slowest). This is the platform's `noob/product-cluster-attributes-snapshot` shape.
- `"wide"` — the cluster is already its own column (`speed_cluster_value_col`) on a table with one row per `product_id` — e.g. an ad-hoc `product_speed_cluster` table with a `product_speed_cluster` column.

`materialize()` fails loudly when `enabled=True` and: `fast_mover_clusters` is empty/non-list/non-integer; `speed_cluster_format` is not `"long"`/`"wide"`; `speed_cluster_attribute_name` is blank under `"long"`; or `speed_cluster_value_col` is blank under `"wide"`.

### `lost_sales_source`

Maps raw lost-sales table columns to canonical names. Allows customers whose lost-sales source uses different column names (e.g. `week_date` instead of `week_start_date`, or `stock_days` instead of `in_stock`) to point the pipeline at their native schema without code changes.

```python
"lost_sales_source": {
    "week_col": "week_start_date",        # default; maps to canonical week_start_date
    "product_col": "product_id",          # default
    "store_col": "store_id",              # default; set None if the source has no per-store dimension
    "lost_sales_col": "lost_sales",       # default
    "in_stock_col": "in_stock",           # default
    "total_days_col": "details.total_days",  # default; supports dotted nested-struct paths
    "product_agg_level_col": None,        # set when the source is keyed by product_agg_level, not product_id
}
```

**Behaviour.** Downstream code always sees canonical column names (`week_start_date`, `product_id`, `store_id`, `lost_sales`, `in_stock`, `total_days`) regardless of the mapping. `week_col`/`product_col`/`store_col` are renamed to their canonical join-key names at read time in `kpi_pipeline/inputs.py`; `lost_sales_col`/`in_stock_col`/`total_days_col` are read under their configured names and aliased to the canonical output names during aggregation in `kpi_pipeline/pipeline.py` (`_aggregate_lost_sales_pairweek`). Defaults reproduce tbretail's current schema exactly, so enabling this block with all defaults produces no behaviour change for existing customers.

**`product_agg_level_col`** (optional): when a source is keyed by planning/DFU level instead of `product_id` (e.g. `reporting_inv_fc_dfu/report_dfu`), set this to that column's name. Auto-detected — only fires when `product_col` is absent from the source; a no-op otherwise, even if configured. Left-joins to `path_segments.product_planning_level` (renaming `planning_level_id` to this column) to backfill `product_id`, mirroring kpi-skill-toolkit's own fallback.

**`store_col: None`** (optional): for a source with no per-store dimension. `lost_sales` is an absolute count, not a ratio — a store-less value gets broadcast across every scoped store of that product, which **over-counts** if later summed across stores. This is safe for `instock_source`'s ratio fields (below), not safe here without an explicit per-store normalization — prefer a genuinely per-store source for `lost_sales_source` when one exists.

### `instock_source`

**Optional.** Reads in-stock rate and total-days from a separate table (e.g. because your in-stock rate is calculated from a different data pipeline than your lost-sales model), then left-joins onto the lost-sales pair-weeks to override the in-stock and total-days values.

```python
"instock_source": {
    "enabled": False,                     # default; set True to read from separate table
    "path_segments": None,                # required when enabled; path to the instock table under datastore bucket
    "week_col": "week_start_date",        # column name; default "week_start_date"
    "product_col": "product_id",          # column name; default "product_id"
    "store_col": "store_id",              # column name; default "store_id"; set None if store-less (see below)
    "in_stock_col": "in_stock",           # column name; default "in_stock"
    "total_days_col": "total_days",       # column name; default "total_days"; supports dotted nested-struct paths
    "product_agg_level_col": None,        # set when the source is keyed by product_agg_level, not product_id
    "fallback_sources": [],               # optional additional column-sets from the SAME table (see below)
}
```

**Behaviour.** When `enabled=False` (default), `in_stock`/`total_days` are aggregated from `lost_sales_source`'s table exactly as before — no change to existing pipelines. When `enabled=True`, `_aggregate_lost_sales_pairweek` stops aggregating `in_stock`/`total_days` from the lost-sales table entirely (only `lost_sales` is aggregated from it); the pipeline instead reads and aggregates the separate `instock_source` table and left-joins it onto the lost-sales weekly frame by `(product_col, store_col, week_col)`. A pair-week present in lost-sales with no matching row in `instock_source` gets `NULL` for `in_stock`/`total_days` — there is no fallback to a lost-sales-side value, since none is computed in this mode. Downstream metrics (`in_stock_rate`, `weighted_instock_rate`, `lost_sales_pct`) use whatever the join produces.

**`product_agg_level_col`**: same auto-detected fallback as `lost_sales_source` above.

**`store_col: None`** — for a source with no per-store dimension (e.g. `reporting_inv_fc_dfu/report_dfu`, aggregated to `product_agg_level` × week only). The join drops `store_id` from its condition and broadcasts the product-week value across every scoped store instead. This **is** safe here, unlike `lost_sales_source`: `in_stock_days`/`total_days` form a ratio, and summing the same broadcast value across a product's stores then dividing reproduces the original ratio exactly (numerator and denominator scale identically) — you just lose real per-store variation, which a store-less source never had anyway. A verified example for tbretail:

```python
"instock_source": {
    "enabled": True,
    "path_segments": ["reporting", "future_visibility", "reporting_inv_fc_dfu", "report_dfu"],
    "week_col": "TY_week_start_date",
    "in_stock_col": "TY_total_days_instock",   # actual, NOT the raw sim_instock_days
    "total_days_col": "TY_total_day",          # actual, NOT the raw sim_total_days
    "product_agg_level_col": "product_agg_level",
    "store_col": None,
}
```

`sim_instock_days`/`sim_total_days` on this table are the future-visibility *simulation's* projected values, not observed actuals — they're only blended into `TY_total_days_instock`/`TY_total_day` for weeks on or after the simulation's own run week (i.e. current/future weeks with no real history yet). Reading the raw `sim_` columns directly would report simulated numbers even for historical periods.

**`fallback_sources`** (optional): a list of additional column-sets, read from the SAME `path_segments` table, appended after the primary column-set to fill in weeks it doesn't have. Each entry needs its own `week_col`/`in_stock_col`/`total_days_col`; `product_col`/`store_col`/`product_agg_level_col` are inherited from the parent `instock_source` block unless overridden. A fallback never overrides a `(product[, store], week)` the primary — or an earlier fallback — already covered; it only fills genuinely missing weeks (`read_instock_source`'s `left_anti` + `unionByName`). This is safe because `in_stock`/`total_days` is a ratio and both column-sets compute it the same way for any real week they both happen to cover.

`report_dfu`'s own `TY_` window only reaches back `path_segments.reporting_inv_fc_dfu`'s own trailing build horizon (tens of weeks, not years) — `LY_`/`LLY_` carry the identical formula for the calendar week exactly 52/104 weeks before each row's own `TY_` week, so they backfill older history `TY_` alone can't reach:

```python
"instock_source": {
    "enabled": True,
    "path_segments": ["reporting", "future_visibility", "reporting_inv_fc_dfu", "report_dfu"],
    "week_col": "TY_week_start_date",
    "in_stock_col": "TY_total_days_instock",
    "total_days_col": "TY_total_day",
    "product_agg_level_col": "product_agg_level",
    "store_col": None,
    "fallback_sources": [
        {"week_col": "LY_week_start_date", "in_stock_col": "LY_total_days_instock", "total_days_col": "LY_total_day"},
        {"week_col": "LLY_week_start_date", "in_stock_col": "LLY_total_days_instock", "total_days_col": "LLY_total_day"},
    ],
}
```

**Mutually exclusive with `lost_sales_ensemble.enabled=True`.** When `lost_sales_ensemble.enabled=True`, the ensemble blends two lost-sales models and picks in-stock/total-days per row from whichever model was selected — this does not compose with `instock_source`'s separate-table override, which assumes a single lost-sales source. Only one of the two may be active; `materialize()` raises a `ValueError` if both are `True`.

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
| `KPI_COMPARISONS`                 | Comma-separated subset of `yoy,ytd` — selects which comparisons to compute |
| `KPI_RECOMPUTE_COMPARISONS`       | `true`/`false` — recompute comparisons from merged history (default `true` under incremental) |
| `KPI_LOST_SALES_ENSEMBLE`         | `true`/`false` — blend fast (120d) + slow (365d) lost-sales models by speed cluster |
| `KPI_LOST_SALES_SLOW_PATH`        | Comma-separated path segments for the 365-day model          |
| `KPI_SPEED_CLUSTER_PATH`          | Comma-separated path segments for the product speed-cluster attributes table |
| `KPI_SPEED_CLUSTER_FORMAT`        | `long` (default) or `wide` — speed-cluster source table shape |
| `KPI_SPEED_CLUSTER_ATTRIBUTE`     | `attribute_name` value selecting the speed cluster (default `sales_speed`, format=`long`) |
| `KPI_SPEED_CLUSTER_VALUE_COL`     | Column already holding the numeric cluster (default `product_speed_cluster`, format=`wide`) |
| `KPI_FAST_MOVER_CLUSTERS`         | Comma-separated cluster ints taking the fast model (default `1,2,3`) |
| `KPI_LOST_SALES_WEEK_COL`         | Overrides `lost_sales_source.week_col` (default `week_start_date`) |
| `KPI_LOST_SALES_PRODUCT_COL`      | Overrides `lost_sales_source.product_col` (default `product_id`) |
| `KPI_LOST_SALES_STORE_COL`        | Overrides `lost_sales_source.store_col` (default `store_id`) |
| `KPI_LOST_SALES_COL`              | Overrides `lost_sales_source.lost_sales_col` (default `lost_sales`) |
| `KPI_LOST_SALES_IN_STOCK_COL`     | Overrides `lost_sales_source.in_stock_col` (default `in_stock`) |
| `KPI_LOST_SALES_TOTAL_DAYS_COL`   | Overrides `lost_sales_source.total_days_col` (default `details.total_days`) |
| `KPI_INSTOCK_SOURCE_ENABLED`      | `true`/`false` — enable separate instock source |
| `KPI_INSTOCK_SOURCE_PATH`         | Comma-separated path segments for `instock_source.path_segments` |
| `KPI_INSTOCK_WEEK_COL`            | Overrides `instock_source.week_col` (default `week_start_date`) |
| `KPI_INSTOCK_PRODUCT_COL`         | Overrides `instock_source.product_col` (default `product_id`) |
| `KPI_INSTOCK_STORE_COL`           | Overrides `instock_source.store_col` (default `store_id`) |
| `KPI_INSTOCK_IN_STOCK_COL`        | Overrides `instock_source.in_stock_col` (default `in_stock`) |
| `KPI_INSTOCK_TOTAL_DAYS_COL`      | Overrides `instock_source.total_days_col` (default `total_days`) |
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

- Sales / inventory (all scoped stores): `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `total_inventory`
- Coverage (all scoped stores): `distinct_product_count`, `distinct_store_count`, `distinct_pair_count`
- Service stores only: `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate`, `in_stock_rate`, `weighted_instock_rate`, `lost_sales_pct`

**Lost Sales %** = `100 × sum(lost_sales) / sum(floor(weekly_sales + lost_sales))` — denominator includes imputed lost demand.

**In-Stock Rate** = `sum(in_stock_days) / sum(available_days)` from top-down lost-sales output (service stores only).

**Weighted In-Stock Rate** = sales-weighted average of weekly in-stock rates: each fiscal week's in-stock rate is weighted by that week's sales volume when rolling up to the reporting period. Weeks with higher sales carry more weight. Reported as pp-change in comparisons (service stores only).

**WOS** = per-product per-fiscal-week WOS after summing daily inventory/sales across service stores at product×date (`avg_daily_inventory / weekly_sales`), then rolled up to the reporting period using a sales-weighted average. Not computed at product×store×week grain.

**Inventory Turnover Rate** = Sales Units ÷ Mean Stock for the same period grain (service stores only). The HTML report labels it per-tab: **Annual**, **YTD**, **Quarterly**, **Monthly**, or **Weekly** Inventory Turnover Rate.

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

- **`defined_scope.grain = "product"`**: the scope universe is `distinct(product_id)` and applies to every store selling the in-scope products across the window; instock/lost-sales pairs are inferred from lost-sales weekly data.
- **Incremental skip vs notebook output**: Saved Delta can retain old values for overlapping period keys while the notebook shows fresh `kpi_long` — set `allow_overwrite_existing=True` to replace.
- **YTD with a single year**: degrades gracefully to "no comparison" rather than erroring — the `"ytd"` period_type rows in `kpi_long` still show whatever data is present.
- **Weekly tab with sparse weeks**: the Weekly period tab's display trim (`weekly_display_weeks`) shows the N most recent weeks **present in `kpi_long`**, not necessarily consecutive fiscal weeks when weekly coverage is sparse (e.g. after a narrow `run_min_date` or partial backfill). There is no WoW comparison table to be affected by this — see [Selecting which comparisons to run](#selecting-which-comparisons-to-run).

## Troubleshooting


| Symptom                     | Likely cause                                                                |
| --------------------------- | --------------------------------------------------------------------------- |
| `initial save blocked`      | Output tables already exist — switch to `incremental` or `full_refresh` (see [Output saves](#output-saves)) |
| Overlapping periods skipped | Expected with `incremental` + `allow_overwrite_existing=False` — set `True` to replace |
| Saved Delta stale vs notebook | Incremental skip kept old rows on disk while notebook shows fresh `kpi_long` — enable overwrite or use `full_refresh` |
| Saved `kpi_long` had far fewer periods than the run actually computed (e.g. only the last 5 weeks/months) | Fixed — `ctx.kpi_long` was previously trimmed to the HTML display window *before* the save step; now only a separate `ctx.kpi_long_display` copy is trimmed, and `ctx.kpi_long`/the Delta save always hold the full computed window. Re-run and re-save if you saved under the old behavior. |
| Comparisons skipped         | Need ≥2 years present in the run window (both YoY and YTD) |
| Empty cut dimension       | Column missing from `master-data/products` or derived SQL failed validation — if it lives on another table, add a [dimension source](#dimension-sources--roots-population-tabs-from-other-tables) (it becomes a root, not a cut) |
| Dimension source errors on read | An **enabled** dimension source fails loudly on bad path / missing column / bad expression (by design) — fix the source or set `enabled: False` |
| Cut value count looks doubled | A `dimension_source` table has multiple rows per `join_key` — pre-aggregate to one row per product (toolkit keeps an arbitrary row, see [Dimension sources](#dimension-sources--roots-population-tabs-from-other-tables)) |
| Only want one root value (e.g. only NVROUT products), or a root you expected is missing | Set `root_values` on that `dimension_sources` entry: `{"yes": "nvrout"}` makes exactly one root from `is_nvrout=='yes'`; omit it to auto-discover one root per distinct value instead. `NULL` never gets its own root. |
| A cut dimension's NULL bucket shows and you want it excluded/renamed | `slices.value_filters` for cuts (`["yes"]` keeps only `yes`; `[]` drops NULL) — see [Value filters](#value-filters-restrict-cut-values--drop-the-null-bucket). For a `dimension_source`'s own NULL bucket, use `fillna: {dim_name: default}` on that source instead (coalesces the join's NULL to a literal) |
| Scope debug counts don't match `kpi_long` per cut | The debug cell recomputes scope independently — re-run it after any `config.py` change (scope mode, adjustments, `value_filters`) so it reflects the same scope Cell 3 builds. NULL values show as `"NULL"` here but as blank/None in `kpi_long` |
| Lost-sales / in-stock numbers change only for slower products after enabling ensemble | Expected — clusters not in `fast_mover_clusters` (and products with no/NULL cluster) now use the 365-day model |
| Slice/pair counts for the speed cluster look doubled | `product-cluster-attributes-snapshot` has >1 `sales_speed` row per product — reader dedupes on `product_id`; verify upstream data |
| Ensemble run fails loudly on read | An enabled ensemble source path (`slow_path_segments` / `speed_cluster_path_segments`) is wrong, or the attributes table lacks the `attribute_name` value — fix path/attribute or set `enabled: False`. If the speed-cluster table is **wide**-shaped (the cluster is already its own column, no `attribute_name`/`attribute_value` columns), set `speed_cluster_format: "wide"` and `speed_cluster_value_col` to that column name instead |
| Some fast-mover pair-weeks missing under ensemble | Expected — a pair-week is kept only if the *chosen* model has a row; the 120-day model simply had no record for it (same as legacy single-model behaviour) |
| HTML report header shows wrong reporting-window start date | Header was reading `REPORT_START_DATE` (raw Jan-1-anchored) instead of `EFFECTIVE_REPORT_START_DATE` (incorporating `run_min_date`) — now fixed in `kpi_pipeline/html_report.py`. If you set `run_min_date` to narrow the window, the header now correctly reflects the effective start. |
| Monthly tab empty or stops at an earlier date than Quarterly/Annual | One or more fiscal weeks in the reporting window have a null `Fiscal_Month` in the `one_time_uploads/fiscal_cal` table, while `Fiscal_Quarter` and `Fiscal_Year` are complete — now raises a validation error in `kpi_pipeline/fiscal.py` listing the affected weeks. Previously these weeks silently dropped from the Monthly rollup, making the tab truncated. Fix the fiscal calendar upload or run the report with a narrower window inside the covered months. |
| Unexpected extra year appears in Annual/YTD view but only sometimes | When `use_fiscal_calendar=True`, `Year` comes directly from the fiscal calendar's own `Year` column, not recalculated from date. If the customer's fiscal year rolls over in late January/early February (not Jan 1), a `run_min_date` early in a calendar year can legitimately fall in the tail of the prior fiscal year per their calendar — so the report window spans parts of two fiscal years even though it's a single calendar year. This is correct; it reflects the actual fiscal calendar. |


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
| **Period tabs** | Annual / **YTD** / Quarter / Monthly / Weekly (horizontal) |
| **Slice dimension tabs** | Overall + every slice column in `kpi_long` (inferred automatically from data and config) |
| **Value tabs** | Vertical sidebar within each slice dimension — one panel per value (e.g. each brand) |
| **KPI tables** | Metrics as rows (colour-coded), periods as columns; inventory turnover is labelled **Annual** / **YTD** / **Quarterly** / **Monthly** / **Weekly** per tab |
| **Comparison** | YoY / YTD per value panel, shown only on the Annual/YTD tabs — YTD renders as one stacked mini-table per consecutive-year pair, each with its own period labels, when more than one is present. Quarter/Monthly/Weekly tabs show value trends only, no comparison table. |
| **Comparable comparison** | Second comparison table per panel (when `comparable_pairs.enabled=True`), same stacking behavior |
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

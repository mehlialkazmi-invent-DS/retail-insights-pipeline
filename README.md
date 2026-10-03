<p align="center">
  <img src="docs/images/readme-banner.png" alt="Retail KPI analytics — sales, inventory, in-stock rate, and weeks of supply" width="100%" />
</p>

# Retail Insights Pipeline

PySpark toolkit for weekly, monthly, quarterly, annual, and YTD retail KPIs with configurable scope (one scope table, alone or hybrid with score backfill), comparable (like-for-like) pair analysis, and incremental Delta output saves.

Designed to run on **Databricks** against the customer Delta datastore (`/mnt/invent-{customer}-datastore`).

## What it produces

| Output                                  | Description                                                                                                                                            |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kpi_long`                              | One tidy table: `period_type` (annual / **ytd** / quarter / half / monthly / weekly; `half` only with `fiscal_calendar.half_periods`), `period`, `root`, `dimension`, `dimension_value`, plus all configured metrics. `root` is `"overall"` plus one per [dimension_source root](#roots-and-cuts-report-structure); `dimension`/`dimension_value` is the cut within that root. Filter it to reproduce any root/cut/period panel. |
| `comparison_yoy / ytd`                  | Prior vs current period with formatted display columns, per root × cut. YoY = last two annual periods; YTD = the **same** elapsed window **across years**, chained across every consecutive year pair (see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)). With `report_end = "latest_day"` annual periods are complete fiscal years only and YTD is the same fiscal day of every year ([Latest-day report end](#latest-day-report-end-report_end--latest_day)). No QoQ/MoM/WoW table; use the Quarter/Half/Monthly/Weekly `kpi_long` rows for trends. Recomputed from the full merged kpi_long on incremental saves. |
| `scope_diff`                            | Side-by-side annual KPIs for **scope-table-only** vs **score-only** scope (sanity check, only with `scope.run_scope_diff=True`), computed on the scope-table and score scopes; its columns are `Year, metric, scope, score, abs_diff, pct_diff`. Its years are the Annual tab's. |
| `comparable_kpi_long` / `comparable_comparison_{ytd,yoy,quarter}` | Like-for-like metrics over only the pairs present in every qualifying year, per root × cut. Gated on `comparable_pairs.enabled=True`; kinds via `comparable_pairs.kinds` (default `["ytd"]`). See [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half). |
| **HTML report**                         | Standalone offline tabbed HTML (outer root tab when more than one root), Metric Details, client/period info panel ([HTML report](#html-report)). |

Delta tables are saved under `PATH_OUTPUT_ROOT`, partitioned by `run_date` per table:

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

## Quick start (Databricks)

**`config.py` is a generic reference template, not any customer's deployed config.** Every optional feature ships disabled with a placeholder; copy it per client and replace `customer`, every `path_segments` entry, `scope.columns` and the business rules (dimension sources, population filters). tbretail's deployed config is `./tbretail_config.py`; the copies one directory up (`../tbretail_config.py`, `../tbretail_config_2.py`) are older versions and must be replaced by it before a run. It reads `lost_sales_source` from a customer-specific reporting table instead of `lost_sales_ensemble`, uses `instock.method = "daily"`, and has real `dimension_sources` (see [tbretail setup](#tbretail-setup)). Every config keeps **all** sections and keys, switched off when unused.

1. **Upload** `main.ipynb`, `config.py` and the entire `kpi_pipeline/` folder to one Databricks workspace folder.
2. **Edit** `config.py` → `CONFIG` (at minimum):
  - `reporting_window.as_of_date` — anchor date; `reporting_window.run_min_date` — optional narrow start (Sunday-aligned)
  - `scope.use_hybrid_scope` — `False` (default) for the scope table alone, `True` for hybrid (covered weeks + score backfill on missing weeks)
  - `scope` — `time`, `grain` and `columns` (the column mapping) for your scope Delta table; `path_segments.scope` — its path segments under the datastore bucket
  - `input_filters` — optional Spark SQL filters on the scope table, lost sales, daily data
  - `slices.dimensions` — product-master columns to cut by (e.g. `brand`), applied within every root
  - `dimension_sources` — optional: each column becomes a root, e.g. NVROUT from `extended_product` (see [Dimension sources → roots](#dimension-sources--roots-population-tabs-from-other-tables), [Roots and cuts](#roots-and-cuts-report-structure))
  - `instock.method`, `blocked_scope.metrics`, `goods_in_transit` — all gated, see [`instock`](#instock), [`blocked_scope`](#blocked_scope), [`goods_in_transit`](#goods_in_transit)
  - `output.save_mode` — `initial`, `incremental`, or `full_refresh`
3. **Open** `main.ipynb` and **Run All** — Cell 2 previews inputs, the **Scope debug** cell reports distinct product/store counts per slice, then Cell 3 runs the pipeline. `KPIRunner.run` displays results inline as each step finishes (see [Programmatic use](#programmatic-use)).
4. **Review** the save plan cell before the write cell runs. Set `allow_overwrite_existing=True` to intentionally replace overlapping periods.

### Prerequisites

- Databricks cluster with PySpark and access to `/mnt/invent-{customer}-datastore` (or override with `KPI_BUCKET`).
- `algo_helpers` available on the cluster (`from algo_helpers import fundamentals as fund`).
- Delta tables referenced in `config.py` must exist for the chosen date window.

## Scope modes

One `scope` section defines the scope, from one table (`path_segments.scope`), whatever its origin (a client-built table or the platform `operation/scope`). Two settings shape it:

`scope.grain` decides what a scope row identifies:

| `grain` | Universe |
| ------- | -------- |
| `"product"` | distinct `product_id`: store-agnostic, every store of an in-scope product |
| `"product_store"` (default) | distinct `(product_id, store_id)` |

`scope.time` decides how the rows relate to weeks:

| `time` | Week behaviour |
| ------ | -------------- |
| `"daily"` (default) | week-agnostic: a pair (or product) scoped in **any** row counts on **every** day / week of the report window. With `columns.start` / `columns.end` the rows still open on the run date are used and a pair's `scope_start` is its earliest start |
| `"weekly"` | the scope table's own `(product[, store], week)` rows (strict): weeks come from `columns.date` (or `columns.year` + `columns.week`) |

The `daily` time flattens scope down to ids, which keeps a pair that has **dropped out of the current-year scope rows yet still transacts** from being silently dropped and undercounted. `weekly` is stricter: only the scope table's own weeks count.

Roll-up to the family main, `active_only`, blocked scope and the in-stock client rules apply the same way to any source; a rule that needs a column that is not configured fails in `materialize()` (see [`scope`](#scope)).

Downstream always consumes `ctx.scope_keys`: `[product_id, Year, Week]` for `"product"`, `[product_id, store_id, Year, Week]` for `"product_store"`.

### Scope table only (default: `use_hybrid_scope = False`)

Final scope = `ctx.scope_table_keys` as built at the configured grain and time. For `daily` that is every scope pair (or product) **x every week in the report window**; for `weekly`, only the scope table's own (product[, store], week) rows survive, window-filtered. Score scope is **not** computed unless `scope.run_scope_diff=True`.

### Hybrid scope (`use_hybrid_scope = True`)

Hybrid = scope table (at the configured grain and time) **+ score backfill on the window weeks the scope table does not cover**:

1. **Covered weeks**: fiscal weeks in the window present in `scope_table_keys`.
2. **Missing weeks**: fiscal weeks in the window with no rows in `scope_table_keys`. These are backfilled from **score scope** (`scope_origin=score`): computed once over the **full** window, each `(product_id, store_id)` keeps the weeks whose weekly sales and **last available in-week inventory** clear a per-pair percentile threshold (**all stores included**; scope membership is store-agnostic), restricted to the missing weeks.
3. **Hybrid union**: scope-table rows (`scope_origin=scope`) U missing-week backfill rows (`scope_origin=score`).

For `time="daily"` the scope table already covers **every** window week, so the backfill is a **no-op**. Hybrid backfill only matters for `time="weekly"`.

### Scope debug (product/store counts before the full run)

A lightweight, read-only pre-flight count sanity-checks scope before the heavy computation. The **Scope debug** cell in `main.ipynb` (between the input previews and the pipeline run) calls:

```python
runner.prepare_scopes()
display(runner.scope_debug_summary())
```

`scope_debug_summary()` returns a DataFrame of distinct `product_id`, `store_id` and pair counts at each stage of removal, one column set per stage (`{stage}_product_count`, `{stage}_store_count`, `{stage}_pair_count`): an `overall` row plus one row per **active slice dimension** value (`slices`, `derived_dimensions`, enabled `dimension_sources`), using the KPI step's `value_filters`. Stages, in order:

- `scope` — the final scope (scope source + hybrid backfill).
- `unblocked` (blocked scope on) — scope pairs with at least one unblocked in-window scope day; a pair blocked on every day drops out of every metric in `blocked_scope.metrics`.
- `instock` (`instock.method = "daily"`) — the pairs the daily in-stock rate counts: after `instock.daily.input_filters`, `scope.instock_main_eligible_only` and `scope.instock_exclude_unsuperseded_sizes`, starting from `unblocked` when `in_stock_rate` is in `blocked_scope.metrics`. `require_daily_data` is not applied (it needs the run's daily-data scan).

Each stage is the population of the metrics it names, not of every metric: blocked days and the in-stock rules stay per metric inside `runner.run()`. The removal sets (`ctx.fully_blocked_pairs`, `ctx.instock_sub_only_pairs`, `ctx.instock_unsuperseded_products`) are built once by `scope.build_scope_removals` at the end of `build_scopes()`; this summary and the daily in-stock frame both read them. With `grain = "product"` only `scope_product_count` is shown. `prepare_scopes()` builds the scope once and `runner.run()` reuses it. NULL slice values show as `"NULL"` here, empty/None in `kpi_long`.

## Dimension sources → roots (population tabs from other tables)

**What it is.** `dimension_sources` pulls a breakdown column from a table other than `master-data/products` (e.g. a program/channel flag) and joins it onto the product attribute projection. If the column is on (or derives from) products, use `slices` instead.

**Every dimension_source column is a ROOT, not a flat cut**: a fully-broken-out population like kpi-skill-toolkit's NVROUT/COMP tabs. `root_values` (`{dim_col: {raw_value: root_name}}`) picks which values become roots and names them; omit it to auto-discover one root per distinct value. Each root also gets its own total plus a breakdown by every `slices` **cut** ([Roots and cuts](#roots-and-cuts-report-structure)). Gated/opt-in: with nothing enabled only the implicit `"overall"` root exists.

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

- **One row per `join_key`.** The source is `dropDuplicates([join_key])`'d so it can't fan out product rows; with several rows per product the survivor is arbitrary, so pre-aggregate. The toolkit doesn't clean the source.
- **Left join: missing products get NULL**, not a `CASE ... ELSE 'no'` default (that only fires for products with a source row). Use `fillna` for a clean two-value split.
- **`fillna: {dim_name: default}`** coalesces NULLs to a literal after the join, for a source that lists only one side of a split (e.g. a CSV of NON-COMP ids). Keys must be among that source's own dimensions, else it fails loudly:

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
- **Enabled sources fail loudly** on a bad path, missing column or unresolved expression (unlike best-effort `derived_dimensions`); disable a source to ignore it.
- **CSV location** (`location`): `datastore` (default) reads a cloud / DBFS path under the datastore mount (`/mnt/invent-{customer}-datastore/...`) as-is; `workspace` reads a Databricks workspace file (`/Workspace/Users/...`) through the `file:` scheme. `csv_options` (e.g. `{"header": True, "inferSchema": True}`) are passed to the Spark CSV reader.

## Roots and cuts (report structure)

Every report row has a **root** (which population) and a **cut** (how it is broken down):

- **Roots** = `"overall"` (always, unrestricted) plus one per `dimension_sources[].root_values` entry (e.g. `"nvrout"`, `"comp"`), each restricting to rows matching that value.
- **Cuts** = `"overall"` (the root's own total) plus each `slices.dimensions`/`derived_dimensions` entry (e.g. `brand`, `SMW`), applied identically **within every root**.

With `root_values: {"is_nvrout": {"yes": "nvrout"}}` and `slices.dimensions: ["brand"]` the report has `overall`×`overall` (grand total), `overall`×`brand`, `nvrout`×`overall`, `nvrout`×`brand` (brands within NVROUT only), mirroring kpi-skill-toolkit's `overall_annual_segment` / `nvr_all_annual_brand`. `kpi_long`, the HTML report (root = outer tab when there is more than one) and comparisons (`root` column on `comparison_yoy`/`comparison_ytd`/every `comparable_comparison_{kind}`) are root × cut aware.

### Value filters (restrict cut values / drop the NULL bucket)

`slices.value_filters` restricts which values of a **cut** dimension appear in that cut's own breakdown (never Overall or other cuts). Two shapes:

**LIST** (include-only): omit → keep all incl. `NULL` (default) · `[]` → drop only `NULL` · `["v1","v2"]` → keep only those, drop everything else incl. `NULL`.

**DICT**: `{"include": [...]}` → same as LIST · `{"exclude": [...]}` → keep everything except those, **including `NULL`** · both → include then remove excludes · `"keep_null": true/false` → force the NULL bucket either way (default: dropped when `include` is set, kept otherwise).

```python
"slices": {
    "dimensions": ["brand"],
    "derived_dimensions": {},
    "value_filters": {},   # e.g. {"brand": ["NIKE", "ADIDAS"]} or {"brand": []} to drop a NULL bucket
},
```

To exclude a list but keep the rest (a "not going forward" list leaves everyone else `NULL`): give the complement a label via `fillna`, or use `{"exclude": [...]}` to keep the `NULL` remainder.

### Scope vs. roots/cuts — three different knobs

| Need | Use | Effect |
| ---- | --- | ------ |
| Which (product, store, week) rows enter the KPIs at all | The scope table (`path_segments.scope`) | Scope **membership** comes only from there |
| A named, fully-broken-out population tab (NVROUT vs COMP) | `dimension_sources[].root_values` | Adds a **root** |
| Break any root's population out by a dimension (brand, SMW) | `slices` | Adds a **cut**, applied within every root |
| Narrow ONE metric's own population, inside every root/cut | `metrics.population_filters` | Restricts that **metric only** (see [Metrics](#metrics)) |

To leave a labelled group out of one metric, use `metrics.population_filters`; to report it separately, give it a root. A typical setup uses a root for a segment flag, a cut for brand and a population filter for one metric.

## Comparable pairs (like-for-like: ytd / yoy / quarter / half)

**Gated, opt-in** (default off; or `KPI_COMPARABLE_PAIRS=true`). Metrics are recomputed over **only the `(product_id, store_id)` pairs present in EVERY qualifying year**, isolating like-for-like movement from newly listed or closed pairs. `comparable_pairs.kinds` selects among four kinds; each kind is gated by that list only, not by `comparisons.enabled`. There is no comparable QoQ/MoM/WoW.

| Kind | Population | Chained links |
|---|---|---|
| `ytd` | Pairs present in every year of the run window, on each year's elapsed (fully-closed-months) window; under `report_end = "latest_day"` the **same fiscal day** of every year (days 1..K) over `ctx.ytd_years` | Every consecutive year pair |
| `yoy` | Pairs present in every year, on the **full window year** (under `latest_day`: complete fiscal years only) | Every consecutive year pair (not just the latest two) |
| `quarter` | **Per quarter number**: for quarter Q only years where Q lies **entirely inside the report window** count, and a pair must be present in Q of all of them | Consecutive years within that quarter's year-set |
| `half` | Same as `quarter` per half number (H1 = fiscal quarters 1-2, H2 = 3-4). Needs `fiscal_calendar.half_periods = True`. | Consecutive years within that half's year-set |

- `yoy`'s window-boundary years can be partial, as on the regular Annual/YoY tab, when `run_min_date`/`as_of_date` don't land on Jan 1 / Dec 31. Under `latest_day` only complete years are compared ([Latest-day report end](#latest-day-report-end-report_end--latest_day)).
- `quarter`/`half` never compare a partial period to a full one: `REPORT_END_DATE` is never quarter-aligned, so the latest in-progress quarter is excluded. A year counts only if that quarter's fiscal-calendar weeks fall entirely inside `[EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]` (`comparable.py`'s `_complete_period_years`; `ytd` uses `fiscal.complete_fiscal_periods` at month grain, see [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)).
- The universe is pair-level under every `scope.grain`, including `"product"`, because `scoped_daily` carries `store_id` from `noob/daily-data`.

```python
"comparable_pairs": {
    "enabled": True,     # default False
    "kinds": ["ytd", "yoy", "quarter", "half"],   # default ["ytd"] -- any subset of COMPARABLE_KINDS_ALL
    "grain": "product_store",   # default; "product" = same-pair population is product_id alone
    "pair_days": "unblocked",   # or "all": blocked days also make a pair present
},
```

- **`grain`** (independent of `scope.grain`): `"product_store"` (default) = distinct pairs present in every qualifying year; `"product"` = distinct `product_id`s, keeping every store of a qualifying product. `"product"` does **not** isolate store-estate churn (a product that opened/closed a store still moves the metrics); use it only when the pair intersection is too small. It affects only the store-side frames; `dc_daily`/`dc_inst` keep their own `(product_id, warehouse_id)` universe.
- **`pair_days`** (`"unblocked"` default | `"all"`): which days make a pair "present" in a year. `"unblocked"` counts only real daily-data rows outside blocked days (and DC rows outside DC blocked days); `"all"` counts blocked days too. Goods-in-transit-only days never count. Each metric then applies its own `blocked_scope.metrics` gate. tbretail: `"unblocked"`.

**One fixed universe per kind, shared by every link.** For `ytd`/`yoy` it is the intersection across **every** qualifying year, computed once: with 2024/2025/2026 only pairs present in all three count, so a pair in 2025 and 2026 but not 2024 is excluded from every link. For `quarter`/`half` it is built **per number independently**. The same year therefore carries the same value in every link it appears in. The universe uses **real** daily-data / `inventory_warehouse` rows only: goods-in-transit-only days ([`goods_in_transit`](#goods_in_transit)) and blocked days ([`blocked_scope`](#blocked_scope), whatever `metrics` says) never make a pair present (their rows are still restricted to the common pairs afterwards). All metric frames are restricted before metrics are computed, for Overall and every slice (slice dimensions are product attributes, so one overall intersection equals per-slice ones). The key is chosen per frame:

| Frame | Restricted on |
|---|---|
| `scoped_daily` | `comparable_pairs.grain`'s key: `(product_id, store_id)` or `product_id` |
| `inst_data`, `lost_base`, `scope_pairs`, `scope_pair_weeks` | `(product_id, store_id)` when the frame has a store dimension **and** `grain="product_store"`; otherwise the universe's **distinct products** |
| `dc_daily`, `dc_inst` | `(product_id, warehouse_id)`, **one independent universe each** (see [`dc_instock`](#dc_instock)) |

In-stock and lost-sales frames carry `store_id` only when `lost_sales_source.store_col` is set. For a store-less source they are matched on distinct products (collapsed first so the join can't fan out per store) and stay product-level totals. `dc_inst` does **not** reuse `dc_daily`'s universe: it 0-fills every day from a pair's first stocked day to the report end, so a pair that stopped being stocked still has stockout rows in later years where `dc_daily` has none, and reusing `dc_daily`'s intersection would delete exactly the sustained stockouts `dc_in_stock_rate` exists to show.

**Outputs**
- `comparable_kpi_long`: one table across every enabled kind with `comparison_type`, `comparable_pair_count` (that kind's universe size), `link_prior_year`/`link_current_year` (in the merge key, since a year can appear in two links) and, for `quarter` / `half` rows, `quarter_number` / `half_number` (not in the key; `period` such as `2025-Q1` already disambiguates).
- `comparable_comparison_ytd` / `_yoy` / `_quarter` / `_half`: one table per enabled kind, same schema as the regular comparison tables (quarter also keys on `quarter_number`, half on `half_number`).
- HTML: a **"Comparable (Like-for-Like)"** section under the regular comparison table: one wide value+delta table for `ytd` (YTD tab) and `yoy` (Annual tab), one narrow block per quarter / half number (headed `Q1 · Like-for-like` / `H1 · Like-for-like`) on the Quarter / Half tab. Notebook: a "Comparable pairs (like-for-like)" cell.

A kind's comparison needs at least 2 qualifying years; on a narrow window (single-week refresh) it is skipped for that run but saved `comparable_kpi_long` history is kept. That table is merged incrementally like `kpi_long` and each `comparable_comparison_{kind}` is recomputed from the merged history; each `run_date` partition is a self-contained snapshot.

## Input previews and filters

The notebook reads the **scope table**, **lost sales**, and **daily data** separately before the pipeline run so you can inspect them. Config filters are applied to both previews and the pipeline.

### `input_filters`

```python
"input_filters": {
    "scope": [  # also applied to the DC scope read
        # "store_id NOT IN (829, 639, 917)",
    ],
    "lost_sales": [],
    "daily_data": [],
}
```

Each entry is a Spark SQL expression passed to `.filter()`. You can also filter ad hoc in the notebook preview cell (e.g. `.filter("brand = 'NIKE'")`).

Preview cells re-read the same tables with the same config filters. The pipeline caches daily data and lost-sales weekly aggregates within each run.

**Ensemble note:** when `lost_sales_ensemble.enabled=True`, `input_filters.lost_sales` applies to **both** the fast and slow lost-sales sources (same schema). Cell 2 also previews the slow source and the speed-cluster table; see [`lost_sales_ensemble`](#lost_sales_ensemble).

## Reporting window

Inputs (`reporting_window`): `as_of_date` (end anchor), `run_min_date` (optional start narrow, aligned to Sunday), `report_end` (`"as_of"` default, `"complete_month"` or `"latest_day"`; env `KPI_REPORT_END`).

Resolved automatically:

- **Start**: Sunday of Jan 1 (YTD) or Sunday of `run_min_date`
- **End**: the **last completed Saturday** on or before `as_of_date`; `complete_month` cuts back further, `latest_day` uses `as_of_date` itself (see below)
- **Scope weeks**: any fiscal week whose Sun–Sat range **overlaps** the effective window is a window week; scope pairs apply across all of them (under hybrid, covered vs missing weeks are split within this set)

**Input date ranges printed at read time** (independent of verbose/quiet read logging): `daily_data` min/max of its date column (once, the read is cached per run); `lost_sales` min/max of `week_start_date` (once per source read, so twice with `lost_sales_ensemble.enabled=True`). They show the source before the window filter, so a max date short of the expected `REPORT_END_DATE` means the source is behind.

### Complete-month report cutoff (`report_end`)

`report_end = "as_of"` (default, generic `config.py`) keeps `REPORT_END_DATE` at the last completed Saturday. `report_end = "complete_month"` cuts it back to the **last day of the most recent fully elapsed month** on or before that Saturday, so no view shows a part-month (monthly, quarter, half, YTD of the complete months, annual through that month, weekly, regular and comparable comparisons).

The cut runs at run time (month ends live in the `fiscal_cal` upload): `fiscal.apply_report_end_mode`, at the start of `KPIRunner.build_dimensions` (and `run_html_only`), overwrites `settings["REPORT_END_DATE"]`, which everything downstream reads. It is idempotent.

- **With a fiscal calendar** the month is a fiscal month read from the **unclipped** upload (`fiscal._fiscal_period_bounds`). The upload must extend **past** the as-of Saturday, otherwise the run raises, and `run_min_date` must reach the start of the cut month, otherwise it raises (`no month ends between ...`, or `the cut month ... starts before the report start`). On the civil path the cut month starts on the 1st and is checked the same way. A fiscal month ends on a Saturday (whole weeks).
- **Without one** (`use_fiscal_calendar=False`) the month is a calendar month: the cut is the last day of the previous calendar month (or `REPORT_END_DATE` if that is a month end). That is usually not a Saturday, so the clipped week is dropped from the Weekly tab by `kpi_long._drop_partial_trailing_week`. Months are bucketed by each week's start date (`Fiscal_Month` = month of `week_start_date`), so the result is a calendar-month **cut**, not exact calendar-month totals.

`OUTPUT_RUN_DATE` (the saved `run_date=` partition) still follows `as_of_date`.


**Incremental saves.** Narrowing `run_min_date` for a weekly refresh does not work with `complete_month` (the window must reach the start of the cut month). Switching a client from `as_of` to `complete_month` under `incremental` with `allow_overwrite_existing = False` keeps the `as_of` versions of the current annual row and later weekly rows; use `full_refresh` for that switch (tbretail does).

### Latest-day report end (`report_end = "latest_day"`)

`report_end = "latest_day"` (`tbretail_config.py`; generic `config.py` keeps `"as_of"`) makes **YTD run to the latest day** while every other view shows **complete periods only**. `REPORT_END_DATE` is `as_of_date` itself (`materialize()` resolves it via `_resolve_report_window`), so scopes, blocked days, the daily in-stock and DC reads and the HTML filename see the same date, and `fiscal.apply_report_end_mode` leaves it unchanged. Set `as_of_date` to a day `noob/daily-data` has reached (the daily in-stock check raises otherwise); the `fiscal_cal` upload must extend past it. `KPI_REPORT_END=latest_day` works as for the other modes. The mode requires `instock.method = "daily"` (`materialize()` raises otherwise): the YTD cut splits a fiscal week of the daily in-stock frame, which a weekly source cannot do.

| View | What it shows |
|---|---|
| **YTD** | Days 1..**K** of every fiscal year, the **same fiscal day** for every year, where K = day of the fiscal year (1-based) of `REPORT_END_DATE`. Fiscal calendar: each year's first date comes from the **unclipped** `fiscal_cal` upload; civil calendar: day of the calendar year. Only years whose days 1..K lie inside the report window are shown (`ctx.ytd_years`), so `run_min_date` must reach the start of the first fiscal year you want in YTD. |
| **Annual** | **Complete** fiscal years only (real start `>= EFFECTIVE_REPORT_START_DATE`, real end `<= REPORT_END_DATE`); the current year appears in YTD only. YoY compares the latest two complete years. |
| **Quarter / Half / Monthly** | Complete periods only ([Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)). |
| **Weekly** | Whole weeks only: the trailing partial week is dropped (and takes none of the `weekly_display_weeks` slots). |
| **Lost Sales %** | Lost sales only has data through the **last Saturday on or before `REPORT_END_DATE`**, so every view counts only weeks ending on or before it (`pipeline.build_pipeline_frames` filters `lost_base` on `week_end_date`), and **YTD uses weeks 1..(fiscal week of that Saturday) for every year** (no week at all when that Saturday is in the previous fiscal year). Numerator and denominator both come from `lost_base`, so the missing days never dilute `lost_sales_pct` or misalign YoY YTD. |

Example: fiscal years starting Sunday 2025-02-02 and 2026-02-01, `REPORT_END_DATE` 2026-08-02 → K = 183: YTD 2026 is 2026-02-01..2026-08-02, YTD 2025 its days 1..183 (2025-02-02..2025-08-03). Lost sales ends 2026-08-01 (fiscal week 26), so YTD lost sales is weeks 1..26 of both years.

**How the YTD cut is built** (`fiscal.build_latest_day_windows`, once per run from `build_fiscal_and_products`): `ctx.ytd_through_day` (K), `ctx.ytd_years`, `ctx.ytd_lost_sales_last_week` and `ctx.day_calendar` (`(date, Year, Week)` plus `day_index`, `last_day_index`). Daily frames (`scoped_daily`, `dc_daily`) carry `day_index` and YTD keeps `day_index <= K` of the YTD years (`kpi_long._ytd_latest_day_frames`). Pair-week in-stock frames can't be cut by date, so `build_instock_daily` / `build_dc_inst` **split the fiscal week containing day K** into days `<= K` and after (two rows per pair, own `stocked_pairs` / `available_days`, each with a `last_day_index`); YTD keeps `last_day_index <= K`, every other view sums both parts. `ytd` rows of `lost_base` keep `Week <= ctx.ytd_lost_sales_last_week`.

**WOS and the part week.** The fiscal week containing K enters the WOS family (`WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`) with only its days up to K: `scoped_daily` carries `week_days` (calendar days of the row's fiscal week in the window, `pipeline._with_week_days`; 7 otherwise), YTD frames replace it with days up to K (`fiscal.week_day_counts`), and `metrics.compute_kpis` weights each product-week's average daily inventory by `week_days / 7`. Without it the part week would add a full week of inventory against a few days of sales (about +3% on a 26-week YTD ending one day into a week, more earlier in the year).

**Incremental saves.** A YTD window moves with every `as_of_date`, so under `incremental` an existing `ytd` row of `kpi_long` / `comparable_kpi_long` is **always replaced** (`io.merge_table_incremental`, whatever `allow_overwrite_existing` says); other period types are complete and merge as usual. Comparison tables are recomputed from the merged history. When switching an existing deployment from `as_of` / `complete_month`, run `full_refresh` once, otherwise rows of the old modes (e.g. the partial current-year Annual row) stay in the merged history.

### Time grain completeness

After the calendar is built, `fiscal.require_complete_time_grain` checks that **every date** from `EFFECTIVE_REPORT_START_DATE` to `REPORT_END_DATE` is in it, raising `time grain (...) is missing N date(s) between ... : [first 20 dates]` otherwise. On the civil path (`use_fiscal_calendar=False`) the calendar is only the dates in `noob/daily-data` (after `input_filters.daily_data`), so a gap means missing source data (a missing date would drop out of every daily metric and shift week bounds). The check also covers a `fiscal_cal` upload with holes.

### Fiscal calendar vs native time grain

`fiscal_calendar.use_fiscal_calendar`:

- **`True` (default)**: Year/Week/Quarter/Month come from the `one_time_uploads/fiscal_cal` table.
- **`False`**: the time grain is derived from `noob/daily-data` (`fiscal_calendar.daily_time_columns`, only `date`/`week` keys) through the same config-filtered `daily_data` read as every consumer (so `input_filters.daily_data` applies). **Year is the CALENDAR year of `date`** (`F.year(date)`), **Week is the native fiscal week column** (`daily_time_columns.week`), Quarter/Month derive from `date`. Year is never read from a raw `year` column because it can carry the **ISO week-year** (a week starting Dec 29, 2025 labelled `year=2026`, mislabelling December as `Q4 2026`). **Caveat**: a native fiscal week straddling Jan 1 is split into two partial weeks in the Weekly view; Quarter, Month and annual rollups are unaffected.

### Half periods (H1 / H2)

`fiscal_calendar.half_periods` (default `False`; tbretail `True`; env `KPI_HALF_PERIODS`) adds a `half` period type next to `quarter`. `Fiscal_Half` is derived from `Fiscal_Quarter` (1-2 -> H1, 3-4 -> H2) on both the `fiscal_cal` and civil paths and rides on every metric frame. With it on:

- `kpi_long` gets `period_type = "half"` rows (`2025-H1`), complete halves only ([Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)).
- The HTML report gets a **Half** tab (last `html_report.half_display_halves` halves, default 4).
- `comparable_pairs.kinds` may include `"half"` (rejected when `half_periods` is off): the same half of the prior year, with a universe per half number.

There is no *regular* half comparison table, as for quarters ([Selecting which comparisons to run](#selecting-which-comparisons-to-run)); the comparable `half` kind covers half-over-half-of-last-year.

### Complete periods only (Quarter, Half & Monthly trend tabs)


The Quarter, Half and Monthly `kpi_long` rows (the value-trend tabs, not YTD) include a `(Year, Fiscal_Quarter)` / `(Year, Fiscal_Half)` / `(Year, Fiscal_Month)` only once it has **fully elapsed on both edges**: not truncated at the trailing edge, nor at the window's own start (which matters when `run_min_date` isn't on a period boundary). The set is computed once per run (`fiscal.complete_fiscal_periods`, cached on `ctx.complete_fiscal_periods`; `Fiscal_Half` only when `half_periods` is on) and semi-joined onto every metric frame before the rollup (`kpi_long._drop_incomplete_periods`).

With `report_end = "latest_day"` the same completeness set is also computed at **Year** and **Week** grain (`fiscal.LATEST_DAY_COMPLETE_PERIOD_COLUMNS`) and applied to the Annual and Weekly tabs; YTD uses its own same-fiscal-day window there (see [Latest-day report end](#latest-day-report-end-report_end--latest_day)). Otherwise the Weekly tab needs no guard while `REPORT_END_DATE` is a Saturday; the exception is `complete_month` without a fiscal calendar, where the clipped partial week is dropped explicitly (see [Complete-month report cutoff](#complete-month-report-cutoff-report_end)).

**YTD's own mechanism.** The YTD tab sums only the fiscal **months** fully closed for the latest year (`fiscal._compute_available_fiscal_months`, `ctx.available_fiscal_months`), applied to every year, so YTD stays apples-to-apples (see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)). The check uses the same unclipped `complete_fiscal_periods`, and `kpi_long._with_ytd_filter` filters on `Fiscal_Month` membership. Month grain picks up already-closed months of an in-progress quarter immediately, which matters because `REPORT_END_DATE` essentially never lands on a quarter boundary. Whether a run's output changes depends on whether `as_of_date` lands on a fiscal period boundary of the `fiscal_cal` upload (boundaries live in that table, not in code).

**Incremental-save caveat.** The exclusion is applied when `kpi_long` is *computed*: an incomplete period never enters `ctx.kpi_long` or the save. But under `incremental` with `allow_overwrite_existing=False`, a partial-period row saved by an earlier run stays in the merged history (the new run doesn't reproduce that merge key, so nothing replaces it). The HTML report built from in-memory `ctx.kpi_long` won't show it; the saved Delta still carries it until you re-run with `allow_overwrite_existing=True` or `full_refresh`.

## Output saves

Results persist to Delta under one root:

```
{bucket}/{output.path_segments}/{table_name}/run_date={run_date}/
```

Default: `/mnt/invent-{customer}-datastore/analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`. `run_date` defaults to `reporting_window.as_of_date`; override with `output.run_date` / `KPI_OUTPUT_RUN_DATE` to load a specific snapshot (`html_only` uses the same partition). `output.save_outputs` defaults to `False`; the notebook runs with `save=False`, Cell 4 previews the save plan and Cell 5 writes.

**`kpi_long` is saved in full, never trimmed.** The HTML display limits (`weekly_display_weeks` etc., see [HTML report](#html-report)) only narrow a separate in-memory `ctx.kpi_long_display`; `ctx.kpi_long` and the save always hold every computed period.

### Tables written

| Table | Contents | Merge keys (incremental mode) |
| ----- | -------- | ----------------------------- |
| `kpi_long` | All metrics × periods × slices (annual / ytd / quarter / half / monthly / weekly) | `period_type`, `period`, `root`, `dimension`, `dimension_value` |
| `comparison_yoy` | YoY comparison rows | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `comparison_ytd` | YTD comparison rows (one row set per consecutive-year pair, elapsed-window sums) | same as YoY |
| `scope_diff` | Scope vs score annual diff (when `run_scope_diff=True`) | `Year`, `metric` |
| `comparable_kpi_long` | Comparable (like-for-like) per-link metrics for every enabled kind + `comparable_pair_count` + `comparison_type` (when `comparable_pairs.enabled=True`) | `comparison_type`, `period_type`, `period`, `root`, `dimension`, `dimension_value`, `link_prior_year`, `link_current_year` |
| `comparable_comparison_ytd` / `_yoy` / `_quarter` / `_half` | Comparable comparison rows for that kind (only when `comparable_pairs.enabled=True` and the kind is in `comparable_pairs.kinds`) | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` (same shape as YoY/YTD; `_quarter` additionally keys on `quarter_number`, `_half` on `half_number`); usually recomputed from merged `comparable_kpi_long` and overwritten wholesale |


Merge keys are defined in `kpi_pipeline/io.py` (`TABLE_ROW_KEYS`). `comparison_*` tables are recomputed from the merged `kpi_long` and overwritten wholesale; `comparable_kpi_long` is merged like `kpi_long` and each enabled kind's `comparable_comparison_{kind}` is recomputed from it ([Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half)). There is no `comparison_qoq`/`_mom`/`_wow` table.

### Save modes

| `save_mode` | What it does | When to use |
| ----------- | ------------ | ----------- |
| `initial` | Writes all rows; **fails** if any output table already exists at the path. | First-ever backfill; then switch to `incremental` or `full_refresh`. |
| `incremental` | Loads the **latest existing `run_date` partition on or before** this run, **appends** rows whose merge keys are not yet saved, **skips** overlapping keys (unless overwrite allowed), writes the merged result to this run's partition. | Weekly refresh; history accumulates as `run_date` advances. |
| `full_refresh` | **Overwrites** each output table with what this run produced, no merge. | Rebuild from scratch for the run window. |

Example: run 1 (`as_of_date=2026-06-15`) saves 2024–2026 into `run_date=2026-06-15`; a weekly run (`as_of_date=2026-06-22`, `run_min_date`=last Sunday) loads that partition, appends the new week and writes the merged result to `run_date=2026-06-22`. Overlapping keys are skipped unless `allow_overwrite_existing: True`.

`full_refresh` replaces whole tables: a 2026-YTD-only run leaves **2026 data only**. Use it when saved tables should exactly match this run.

### `allow_overwrite_existing`

| Value | Incremental behaviour on overlapping merge keys |
| ----- | ----------------------------------------------- |
| `False` (default) | Skip overlapping rows, keep saved values; the save plan prints `skipped_rows` and a warning. |
| `True` | Drop existing rows with matching keys and write the new ones; the save plan prints `overwrite_rows`. |

Use `True` to re-run an already saved week or period (scope fix, data correction).

### Run metadata columns

Every saved row has `_run_as_of` (`as_of_date` of the run that wrote or last overwrote it) and `_saved_at` (UTC write timestamp). On **incremental** saves only appended and overwritten rows get the current stamp.

### Notebook workflow

1. **Cell 3** — `runner.run(fund_paste=fund.paste, save=False)` computes KPIs, no write (results display inline as steps finish, see [Programmatic use](#programmatic-use)).
2. **Cell 4** — `runner.preview_save_plan(fund.paste)` prints append / overwrite / skip counts per table **before** writing.
3. **Cell 5** — `save_outputs(ctx, fund.paste)` writes (only when `save_outputs: True`).

If Cell 4 shows `skipped_rows > 0` and you meant to replace those periods, set `allow_overwrite_existing=True` and re-run.

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

- **Incremental accumulates onto the latest partition** on or before the current run, so weekly runs build history instead of single-week snapshots.
- **Comparisons are recomputed from merged history (incremental):** after merging `kpi_long` the toolkit re-reads the merged partition, recomputes YoY/YTD (and every comparable table) from the **full saved history** and overwrites the comparison tables, so a single-week refresh can still produce a YoY vs last year. `ctx.comparison_*` / `ctx.comparable_comparison_*` and the displays reflect the merged history. Disable with `output.recompute_comparisons_from_history=False` (or `KPI_RECOMPUTE_COMPARISONS=false`).
- **Notebook vs saved Delta:** under `allow_overwrite_existing=False`, overlapping `kpi_long` keys keep prior saved values (e.g. a stale partial-year annual total survives a narrower re-run). Enable overwrite or use `full_refresh`.
- **`report_end = "latest_day"`:** `ytd` rows are always replaced on incremental merge and counted as `overwrite` in the plan ([Latest-day report end](#latest-day-report-end-report_end--latest_day)).
- **Empty outputs are skipped:** an empty table (e.g. comparisons on a very narrow window) is not written and prior Delta data stays, even under `full_refresh`.

### Selecting which comparisons to run

`comparisons.enabled` chooses which comparisons are **computed, printed, saved, and rendered**: any subset of `"yoy"`, `"ytd"` (the only two kinds).

```python
"comparisons": {
    "enabled": ["yoy"],          # only year-over-year; ytd is skipped entirely
},
```

- **`yoy`** — full year vs the prior full year (last two annual periods; under `latest_day` the latest two **complete** fiscal years).
- **`ytd`** — each year's **elapsed window** vs the prior year's same window, chained across consecutive years (`2026 YTD` vs `2025 YTD`, `2025 YTD` vs `2024 YTD`). Under `latest_day` the window is the **same fiscal day** of every year (days 1..K, see [Latest-day report end](#latest-day-report-end-report_end--latest_day)); otherwise it is the fiscal **months** fully closed for the latest year (every week ends on or before `REPORT_END_DATE`, checked against the unclipped calendar, see [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)), the same month set summed for **every** year. Use it instead of `yoy` while the current year is partial. A single-month or single-year window still sums what exists; the comparison is absent with one year.
- **No QoQ/MoM/WoW comparison table.** The Quarter/Monthly/Weekly tabs (and `kpi_long` `period_type` rows, always produced in full) show value trends; compute quarter-over-quarter or month-over-month changes from consecutive `kpi_long` rows.
- Only selected kinds produce `comparison_{kind}` tables and HTML columns. `kpi_long` is **always** produced in full, including `"ytd"` rows. Comparable pairs is gated independently via `comparable_pairs`.
- **HTML**: `ytd` with several year-pairs renders as stacked mini tables, one per pair; `yoy` as a single table.
- **From saved history:** with `save_mode="incremental"` and `recompute_comparisons_from_history=True` comparisons are rebuilt from the **full merged `kpi_long`**, so `["yoy"]` on a one-week run compares the current partial year with last year's saved annual total (needs saved history at an earlier `run_date`).
- Invalid or empty selections fail loudly at `materialize()`. Env: `KPI_COMPARISONS="yoy,ytd"`.

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

Env: `KPI_SAVE_OUTPUTS`, `KPI_OUTPUT_SAVE_MODE`, `KPI_ALLOW_OVERWRITE_EXISTING`, `KPI_OUTPUT_PATH` (comma-separated segments), `KPI_OUTPUT_RUN_DATE`, `KPI_RECOMPUTE_COMPARISONS`.

## Config reference

### `input_filters`

See [Input previews and filters](#input-previews-and-filters).

### `scope`

One section for the whole scope definition (tbretail's deployed values shown):

```python
"scope": {
    "time": "daily",             # "daily" | "weekly" (see Scope modes)
    "grain": "product_store",    # "product" | "product_store"
    "columns": {                 # column names of path_segments.scope; None = column not used
        "product": "product_id",
        "store": "location_id",  # required for product_store; for dc_solution_id it holds the warehouse
        "start": "start_date",   # daily only
        "end": "end_date",       # daily only
        "solution": "solution_id",
        "run_date": "run_date",
        "date": None,            # weekly only: date OR year + week
        "year": None,
        "week": None,
    },
    "solution_id": 21,           # store scope solution(s), int or list (blocks: blocked_scope.solution_id)
    "dc_solution_id": 22,        # DC (network) scope, int or list: DC metrics' warehouse pairs among store-scope products; DC blocks
    "run_date": None,            # Sunday "YYYY-MM-DD"; None = latest Sunday on or before today
    "roll_to_family_main": True,
    "active_only": True,
    "instock_main_eligible_only": True,             # in-stock only where the main itself is eligible
    "instock_exclude_unsuperseded_sizes": True,     # in-stock leaves out sizes not in a supersession
    "backfill_leading_gap": False,                  # weekly only
    "use_hybrid_scope": False,   # False = scope table alone; True = hybrid (covered weeks + score backfill)
    "run_scope_diff": False,     # True = compute score scope and the scope-vs-score annual diff
}
```

The generic `config.py` reads a client table: `columns` = `product_id` / `store_id` only, `solution_id` 21 unused (no `columns.solution`), `dc_solution_id` / run date `None`, `roll_to_family_main` and `active_only` `False`, both in-stock rules `False`, `backfill_leading_gap` `True`.

**Reading the table** (`inputs.read_scope_source`): rows of `solution_id` (when `columns.solution` is set) for `run_date` (when `columns.run_date` is set) that are still open (`columns.end` null or `>= run_date`) are kept, then `input_filters.scope` applies; the DC read does the same with `dc_solution_id`, `columns.store` holding the warehouse. A solution / run-date filter that leaves no row fails the run: a wrong `solution_id` or `run_date` must not give an empty report. `run_date` defaults to the latest Sunday on or before **today** (`inputs.scope_run_date`), not the report's as-of date, so a backdated run or `KPI_AS_OF_DATE` still reads today's scope; set it for a reproducible backfill.

**`time = "daily"`** (`scope._scope_pairs`): rows reduce to `(product_id[, store_id])` pairs. With `roll_to_family_main` every row is rolled to its family main (`coalesce(parent_id, product_id)`; a product without a main keeps its own id), so a store where only a sub-item is in scope gets the main. With `columns.start` each pair keeps its **earliest** start as `scope_start` and `main_eligible` (the main's own id is among its rows), cached as `ctx.scope_pairs`; a row without a start fails. `active_only` keeps `is_active = true` products. The scope is built once for every metric; `input_filters.daily_data` and population filters apply on top.

**`time = "weekly"`** (`scope._weekly_scope_keys`): `grain` `"product"` needs no `columns.store`; weeks come from `columns.date` via `fiscal_cal`, or from native `columns.year` / `columns.week` (valid only when `fiscal_calendar.use_fiscal_calendar=False`; it takes `Year` / `Week` verbatim, so `Year` must be a genuine calendar year: scope is joined to daily and lost-sales data by exact match on `(product_id[, store_id], Year, Week)`, and an ISO week-year in the source breaks that match). With `use_fiscal_calendar=True`, `columns.date` is required: `build_scope` raises rather than trust week numbering that may not match `fiscal_cal`. `columns.start` / `end` are invalid with `weekly`.

**`backfill_leading_gap`** (`weekly` only): if the source's earliest week (across all pairs) starts after the window start, that is a data-availability limit, so only the pairs tied to that earliest week are assumed in scope back to the window start (same "min date in window" principle as [`dc_instock`](#dc_instock), no hardcoded floor). A pair whose own first week is later than the source's earliest week is left alone (a real new store/product). Only the leading gap is filled. Set `False` for a deployment with **existing** weekly history saved without this option, to avoid mixing two scope definitions in one incrementally merged table.

**Not the same knob as `comparable_pairs.grain`.** `scope.grain` decides scope *membership*; [`comparable_pairs.grain`](#comparable-pairs-like-for-like-ytd--yoy--quarter--half) decides what "the same pair across years" means for that feature only.

`materialize()` fails loudly when: `time` / `grain` is invalid; `columns.product` is unset, or `columns.store` for `product_store`; `weekly` has neither `columns.date` nor `year` + `week`, or has `start` / `end`; `daily` has `date` / `year` / `week`; `columns.solution` is set without `solution_id`; `run_date` is not a Sunday; and for a rule whose column is missing: `dc_solution_id` needs `columns.solution`, `start` and `store`; `blocked_scope.ui_parameters_path` needs `columns.start` and `store`; `instock.daily.count_start` other than `first_daily_row` needs `columns.start`; `instock_main_eligible_only` needs `columns.start`, `roll_to_family_main` and `instock.method = "daily"`; `instock_exclude_unsuperseded_sizes` needs `instock.method = "daily"`.

When `run_scope_diff=False` (default), score-scope computation is skipped unless `use_hybrid_scope=True` (hybrid backfill needs it). The notebook scope-diff cell and `scope_diff` Delta output are omitted.

- **`instock_main_eligible_only`** (tbretail `True`, generic `False`): in-stock (and weighted in-stock) counts only stores where the main item itself is eligible; a store where only a superseded (sub) item is eligible was intentionally not assorted the new item (client rule). Those stores stay in every other metric. The pair's start is still the earliest of the main and sub rows. Built from `ctx.scope_pairs.main_eligible`, applied in `pipeline.build_instock_daily`.
- **`instock_exclude_unsuperseded_sizes`** (tbretail `True`, generic `False`): in-stock leaves out sizes "not created in the supersession": a product in no `item_family` row whose class color (`products.option_code`) has at least one size in `item_family` (main or sub). Treated like NGF: out of in-stock, still in every other metric (`pipeline._unsuperseded_sizes`).

Block product_ids are **not** rolled: only blocks on the main's own `product_id` apply, as in the client reference script.

### `score_scope`

Used when `scope.use_hybrid_scope=True` or `scope.run_scope_diff=True`. Applies to the **missing** (uncovered) weeks under hybrid scope.

```python
"score_scope": {
    "min_percentile": 0.2,
    "min_weeks_for_filter": 2,
}
```

### `lost_sales_ensemble`

**Off by default**: the pipeline reads the single fast-mover (120-day) model at `path_segments.lost_sales`.

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

When enabled, two lost-sales models are blended by product sales-speed cluster:

- Products whose cluster is in `fast_mover_clusters` use the **fast (120-day)** model at `path_segments.lost_sales`; every other product (slower clusters **and** products with no/NULL cluster row) uses the **slow (365-day)** model at `slow_path_segments`.
- `lost_sales`, `in_stock_days` and `total_days` for a `(product_id, store_id, week_start_date)` always come from **one** model, never mixed.
- A pair-week is kept only if the **chosen** model has a row for it; if the chosen side is missing from the full-outer join the row is dropped, not zero-filled.

`speed_cluster_format`: `"long"` = one row per `(product_id, attribute_name)`, filtered to `speed_cluster_attribute_name`, `attribute_value` is the cluster (1=fastest .. 5=slowest; the platform's `noob/product-cluster-attributes-snapshot` shape); `"wide"` = the cluster is its own column (`speed_cluster_value_col`) on a one-row-per-`product_id` table.

`materialize()` fails loudly when enabled and: `fast_mover_clusters` is empty/non-list/non-integer; `speed_cluster_format` is not `"long"`/`"wide"`; `speed_cluster_attribute_name` is blank under `"long"`; `speed_cluster_value_col` is blank under `"wide"`.

### `lost_sales_source`

Maps raw lost-sales columns to canonical names, so a source with different column names (e.g. `week_date` instead of `week_start_date`, `stock_days` instead of `in_stock`) works without code changes.

```python
"lost_sales_source": {
    "week_col": "week_start_date",        # default; maps to canonical week_start_date
    "product_col": "product_id",          # default
    "store_col": "store_id",              # default; set None if the source has no per-store dimension
    "lost_sales_col": "lost_sales",       # default
    "in_stock_col": "in_stock",           # default
    "total_days_col": "details.total_days",  # default; supports dotted nested-struct paths
    "product_agg_level_col": None,        # set when the source is keyed by product_agg_level, not product_id
    "sales_filter": [],                   # narrows ONLY lost_sales_pct's sales denominator (see below)
}
```

Downstream code always sees canonical names (`week_start_date`, `product_id`, `store_id`, `lost_sales`, `in_stock`, `total_days`): the first three are renamed at read time (`kpi_pipeline/inputs.py`), the others aliased in `pipeline._aggregate_lost_sales_pairweek`. Defaults reproduce tbretail's schema.

- **`product_col` / `product_agg_level_col` — configure exactly one.** If the source has a `product_id`-level column set `product_col`; if it is keyed by planning/DFU level (e.g. `reporting_inv_fc_dfu/report_dfu`) set `product_col: None` and `product_agg_level_col`, which is left-joined to `path_segments.product_planning_level` (renaming `planning_level_id`) to backfill `product_id`, as in kpi-skill-toolkit. A `product_col` that is set AND present on the source always wins. `product_col: None` without `product_agg_level_col` fails at read time.
- **`store_col: None`** (source with no per-store dimension): the scope join **collapses to the source's grain** (`store_id` leaves the join keys, scope is semi-joined on `(product_id, Year, Week)`), so a product-week keeps one row; fanning the absolute `lost_sales` count out per scoped store would inflate sums by the store count. `lost_base`/`inst_data` then carry no `store_id`, and `lost_sales_pct` is computed against product-week sales summed across scoped stores. Residual limit: the source's value covers the product's **entire** store footprint, possibly wider than a partial-store scope; prefer a per-store source when one exists.
- **`sales_filter`** (list of Spark SQL expressions, ANDed like [`input_filters`](#input_filters)): narrows the **daily-data sales** in `lost_sales_pct`'s denominator, `lost_sales / (sales + lost_sales)`, and nothing else (applied to `scoped_daily` in `build_pipeline_frames` before the weekly rollup). Empty = no narrowing.

Use `sales_filter` when the lost-sales table covers a **narrower population than `daily_data`**, typically a model built to exclude e-commerce (tbretail's `model_id=top_down_excluding_ecom`, directly or via `report_dfu`): the numerator has no ecom lost sales while `daily_data` sales still carry ecom revenue, so the denominator is inflated and `lost_sales_pct` reads **low**. Excluding the same stores puts both halves on one population:

```python
"lost_sales_source": {
    ...
    "sales_filter": ["store_id NOT IN (9001, 9002)"],   # the ecom fulfilment "stores"
},
```

`input_filters.daily_data` would also fix the ratio but removes ecom from **every** daily-data metric, which legitimately include it; `metrics.population_filters` can't reach it (it applies to `lost_base`, already at product-week grain with store sales summed in, and takes dimension/value specs, not SQL).

Under `report_end = "latest_day"` lost sales reach only the last complete Saturday ([Latest-day report end](#latest-day-report-end-report_end--latest_day)). The sales half of the denominator is real daily-data sales only (GIT-only days carry none); blocked days leave it only when `lost_sales_pct` is in [`blocked_scope.metrics`](#blocked_scope), and the weekly numerator never changes.

### `instock`

Where `in_stock_rate` (and the in-stock side of `weighted_instock_rate`) comes from: a `method` plus one sub-section per method. Both sub-sections are in every config (all keys present); only the one `method` names is used.

```python
"instock": {
    "method": "daily",                  # "daily" | "weekly_source" | "lost_sales_source"
    "daily": {
        "count_start": "earliest",      # "first_daily_row" (generic default) | "scope_start" | "earliest"
        "require_daily_data": True,
        "history_start": "2024-01-21",  # None = window start
        "usable_only": True,
        "input_filters": ["store_id NOT IN (829, 639, 917)"],
    },
    "weekly_source": {
        "path_segments": ["reporting", "future_visibility", "reporting_inv_fc_dfu", "report_dfu"],  # required for "weekly_source"
        "week_col": "TY_week_start_date",
        "product_col": None,
        "store_col": None,
        "in_stock_col": "TY_total_days_instock",
        "total_days_col": "TY_total_day",
        "product_agg_level_col": "product_agg_level",
        "fallback_sources": [],         # extra column-sets of the same table (see below)
    },
},
```

| `method` | `in_stock_rate` is built from |
| --- | --- |
| `"daily"` | `noob/daily-data` over the scope pairs (store-level scope grain required) |
| `"weekly_source"` | a separate weekly table (`weekly_source`) |
| `"lost_sales_source"` | `lost_sales_source`'s `in_stock_col` / `total_days_col`, on the same rows as lost sales (generic default; a null `total_days` falls back to the fiscal week's day count) |

`materialize()` raises on: an unknown `method`; empty `weekly_source.path_segments` under `"weekly_source"`; `"daily"` with `scope.grain = "product"`; `"daily"` or `"weekly_source"` with `lost_sales_ensemble.enabled` (the ensemble blends in-stock columns they do not produce); `daily.count_start` outside its three values (checked whatever the method), `scope_start` / `earliest` without `scope.columns.start`, or `first_daily_row` without `require_daily_data`; `daily.history_start` after the window start; `report_end = "latest_day"` or [`goods_in_transit.store_instock`](#goods_in_transit) unless the method is `"daily"`. The method is `settings["INSTOCK_METHOD"]`; env `KPI_INSTOCK_METHOD` overrides it.

**`method = "daily"`** (`pipeline.build_instock_daily`; the Metric Details text is generated from these settings), per scope pair:

1. Every scoped pair counts, so every metric shares one scope. `daily.input_filters` (Spark SQL on `product_id` / `store_id` only) narrow the pair universe, so a store group can leave in-stock without touching other metrics.
2. `get_instock_daily_raw` reads daily-data **without** `input_filters.daily_data` (usually `usable = 1`, which would hide the unusable days this method subtracts), from `history_start` to `REPORT_END_DATE` (filtered on the raw date column before `to_date` for Delta pruning), restricted to those pairs, blocked days removed when `in_stock_rate` is in [`blocked_scope.metrics`](#blocked_scope), cached. It is not family-rolled (daily-data is already rolled to the family main upstream). The run raises unless daily-data's latest date for those pairs reaches `REPORT_END_DATE` (later days would count as out of stock).
3. Count start: `first_daily_row` (first daily row from `history_start`), `scope_start` (the scope table's start) or `earliest` of the two, clipped to the window start. `require_daily_data` drops pairs without any daily row.
4. Store-days = every day from the count start to `REPORT_END_DATE` (a day with no daily row is out of stock) minus blocked days (when `in_stock_rate` is in `blocked_scope.metrics`) minus, with `usable_only`, days with `usable != 1`.
5. In-stock day = `inventory > 0` on a usable day; with `goods_in_transit.store_instock` also a day with store GIT (`destination_type = 0`, `quantity > 0`, family main, shifted by `date_shift_days`); united (OH OR GIT), never summed; blocked and unusable days drop out too.

The output has the shape of the weekly `inst_data` (`stocked_pairs` / `available_days` per pair-week plus product dims), so `compute_kpis`, population filters and comparable pairs work unchanged; day counts come from fiscal week bounds. Under `latest_day` the week containing day K is two rows per pair, carrying `last_day_index` ([Latest-day report end](#latest-day-report-end-report_end--latest_day)).

**`method = "weekly_source"`** reads in-stock rate and total-days from a separate table (e.g. in-stock calculated by another pipeline than the lost-sales model), read and scope-restricted independently of lost sales (`read_instock_weekly`; `_aggregate_lost_sales_pairweek` then aggregates only `lost_sales`). It is semi-joined to scope at its own grain, so every in-scope pair-week it has counts whether or not lost sales has a row; the two meet at the final per-period join. A pair-week with null/zero `total_days` is dropped, never padded to a full week. Blocked days cannot be removed (no per-day grain). `product_col` / `product_agg_level_col` follow the [`lost_sales_source`](#lost_sales_source) rule.

- **`store_col: None`**: for a source with no per-store dimension (e.g. `report_dfu`, product × week); the scope semi-join drops `store_id` so rows are never fanned out per store. Independent of `lost_sales_source`'s store grain. tbretail's `weekly_source` (kept configured for a switch back) is the example above: `report_dfu` with `TY_` columns, `product_col`/`store_col` `None`, `product_agg_level_col` through `path_segments.product_planning_level`.
- `sim_instock_days`/`sim_total_days` are the future-visibility *simulation's* projections, blended into `TY_total_days_instock`/`TY_total_day` only for weeks on or after the simulation's run week; the raw `sim_` columns would report simulated numbers for history, so `in_stock_col` / `total_days_col` point at the `TY_` columns.
- **`fallback_sources`**: extra column-sets of the SAME table, appended after the primary to fill weeks it lacks; each needs its own `week_col`/`in_stock_col`/`total_days_col` (`product_col`/`store_col`/`product_agg_level_col` inherited unless overridden). A fallback never overrides a `(product[, store], week)` the primary or an earlier fallback covered (`read_instock_source`'s `left_anti` + `unionByName`); safe because the ratio is computed identically. `report_dfu`'s `TY_` window only reaches back a trailing build horizon (tens of weeks); `LY_`/`LLY_` carry the same formula for the week exactly 52/104 weeks before each `TY_` week:

```python
"fallback_sources": [
    {"week_col": "LY_week_start_date", "in_stock_col": "LY_total_days_instock", "total_days_col": "LY_total_day"},
    {"week_col": "LLY_week_start_date", "in_stock_col": "LLY_total_days_instock", "total_days_col": "LLY_total_day"},
],
```

**`method = "lost_sales_source"`** needs nothing else: the columns mapped in [`lost_sales_source`](#lost_sales_source) are used.

### `inventory_warehouse`

DC/warehouse daily inventory table backing `dc_mean_stock`, `total_mean_stock`, `WOS_DC` and `WOS_TOTAL`. No column mapping (columns are canonical: `product_id`, `warehouse_id`, `date`, `inventory`):

```python
"path_segments": {
    ...
    "inventory_warehouse": ["operation", "inventory_warehouse"],
},
"input_filters": {
    ...
    "inventory_warehouse": [],
},
```

Read via `read_inventory_warehouse_source`; its window-filtered, family-rolled result is cached per run (`pipeline._get_inventory_warehouse_parent_rolled`). Built into `dc_daily` (`pipeline.build_dc_daily`), restricted by left-semi join to the SAME in-scope `(product_id, Year, Week)` population as every other frame, so under `scope.time = "weekly"` a product's DC inventory is kept only for the weeks it is in scope. DC data has no store dimension; filter unwanted warehouse rows via `input_filters.inventory_warehouse`.

**Item-family rollup before restriction (unconditional, NOT gated by `dc_instock.enabled`).** `scope_core` (and the scope table's rows) is in **parent** id space (its producer maps `product_id -> coalesce(parent_id, product_id)` via `item_family`'s `is_main=false` rows), but `inventory_warehouse`'s `product_id` is raw. `build_dc_daily` therefore rolls it to parent id (`pipeline._roll_to_item_family_parent`, re-aggregating `F.sum("inventory")` at `(product_id, warehouse_id, date)`) **before** restricting to `scope_core`; otherwise DC inventory on a superseded/child `product_id` would be dropped. So `path_segments.item_family` must point at a real table **whenever `path_segments.inventory_warehouse` is configured**, and `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` shift wherever a family had inventory split across old and current item codes.

### `dc_instock`

**DC product scope.** The store scope leads: every DC metric (`dc_mean_stock`, `WOS_DC`, the DC part of `WOS_TOTAL` / `total_mean_stock`, `dc_in_stock_rate`) reads `inventory_warehouse` for the **store** scope's products (one scope table snapshot, every week). With `scope.dc_solution_id` set (tbretail 22), only the DC (network) scope's product × warehouse pairs among them count (`scope.build_dc_scope` reads that solution's `operation/scope` pairs, family main, earliest start, active, into `ctx.dc_scope_pairs`, which also give DC blocked days their start dates). With `None`, every warehouse counts.

**Gated** (`dc_instock.enabled`, default `False`), but `item_family` is **not** (see [`inventory_warehouse`](#inventory_warehouse)). Backs `dc_in_stock_rate`. Unlike `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL`, which use only days `inventory_warehouse` has a row for, it **expands** rows into a continuous daily grid per `(product_id, warehouse_id)` and counts gaps as stockouts.

```python
"path_segments": {
    ...
    "item_family": ["operation", "item_family"],
},
"input_filters": {
    ...
    "item_family": [],
},
"item_family_source": {
    "product_col": "product_id",
    "parent_col": "parent_id",
    "is_main_col": "is_main",
},
"dc_instock": {
    "enabled": False,
    "stock_threshold": 0,              # a day counts as "stocked" when inventory > stock_threshold
},
```

- **Goods in transit**: [`goods_in_transit.dc_instock`](#goods_in_transit) `True` makes a grid day with GIT to the DC also stocked (union), shifted by `goods_in_transit.date_shift_days` (tbretail `-1`).
- **DC blocked scope** ([`blocked_scope`](#blocked_scope)'s `dc_solution_id`): DC blocked days leave the stocked and available days only when `dc_in_stock_rate` is in `blocked_scope.metrics`.
- **`item_family_source`** ([`item_family`](#item_family)) maps the parent/child table's columns; used by `build_dc_daily` and `build_dc_inst` via `pipeline._roll_to_item_family_parent`, and read whenever `inventory_warehouse` is configured.
- **Window start.** Each pair's grid runs from **its own first `inventory_warehouse` row** to `REPORT_END_DATE`, not from a scope `start_date` or a flat window: the same "first day with any history" signal `daily_data_expanded` uses for the **store-level** in-stock denominator (`customer-analysis-tbretail`'s `05_future_visibility_data_prep.py`), so both series share one definition.
- **Trade-off:** a pair ranged at a DC but never stocked inside the window has no row and is **absent**, not 0%. A pair that *stops* being stocked is still covered: its grid continues to `REPORT_END_DATE` and every later day is a stockout.

**Build** (`pipeline.build_dc_inst`): from rolled-up `inventory_warehouse` (`_get_inventory_warehouse_parent_rolled`, shared with `build_dc_daily`, window-filtered) take `first_stocked_date = MIN(date)` per pair; `F.explode(F.sequence(first_stocked_date, REPORT_END_DATE))` gives the pair's days, joined to the fiscal calendar; restrict to the SAME in-scope `(product_id, Year, Week)` population as `build_dc_daily` before the inventory join; left-join inventory with `F.coalesce(inventory, 0)` (a grid day with no row is a stockout); flag DC blocked days (`ctx.dc_blocked_days`, with `scope.dc_solution_id`) as `is_blocked` and left-join DC GIT days (with `goods_in_transit.dc_instock`); aggregate to `(product_id, warehouse_id, Year, Week)`:

- `dc_stocked_days = COUNT(inventory > stock_threshold OR goods in transit)`, `dc_available_days = COUNT(*)`, both over unblocked days only when `dc_in_stock_rate` is in `blocked_scope.metrics` (rows with no available day are dropped), plus `dc_unblocked_days` (unblocked days either way; the comparable-pairs universe reads it).

`dc_in_stock_rate = F.greatest(0.0, Σ(dc_stocked_days) ÷ Σ(dc_available_days))` at the period grain (no sales-weighted rollup). **Disabled path:** with `enabled=False` (default) `build_dc_inst` returns an empty, correctly-shaped frame, so `dc_in_stock_rate` is always a literal-`null` column in `kpi_long`.

### `item_family`

Parent/child product roll-up: a superseded/child product (`is_main = false`) rolls onto its parent (`coalesce(parent_id, product_id)`, `pipeline._roll_to_item_family_parent`).

```python
"path_segments": {..., "item_family": ["operation", "item_family"]},
"item_family_source": {            # column names of the item_family table
    "product_col": "product_id",
    "parent_col": "parent_id",
    "is_main_col": "is_main",
},
"item_family_rollup": {            # roll to the family main, per source
    "daily_data": True,
    "lost_sales": False,
    "inventory_warehouse": True,
},
```

`item_family_source` renames columns to `product_id` / `parent_id` / `is_main` at read time (`read_item_family_source`, `ctx.item_family_raw`). `path_segments.item_family` must point at a real table whenever any rollup toggle is `True` or [`goods_in_transit.roll_to_family_main`](#goods_in_transit) is `True`; it is read whenever `inventory_warehouse` is configured.

- `daily_data` (default `True`): without it `build_scoped_daily`'s join to the parent-rolled `scope_core` silently drops daily rows still carrying a child product_id. Can shift historical numbers for products with a supersede history.
- `lost_sales` (default `False`): `report_dfu` already substitutes upstream, so a second roll-up is likely a no-op; opt-in safety net.
- `inventory_warehouse` (default `True`): see [`inventory_warehouse`](#inventory_warehouse).
- The scope has its own toggle, `scope.roll_to_family_main`.
- Goods in transit has its own toggle, `goods_in_transit.roll_to_family_main`.

### `blocked_scope`

UI-blocked days, applied per metric. Default off: on exactly when `ui_parameters_path` is set.

```python
"blocked_scope": {
    "ui_parameters_path": "ui-data/parameter_config/<timestamp>_<id>",  # under the datastore root; None = off
    "rule": "after_scope_start",   # or "all"
    "solution_id": 21,             # store blocks of these solution(s) only (int or list), whatever scope reads
    "dc_solution_id": None,        # DC blocks of these solution(s) (tbretail 22); None = no DC blocks
    "kinds": ["product", "product_destination", "destination"],  # store block folders read
    "dc_kinds": ["product", "product_destination"],  # DC block folders read
    "metrics": "all",              # "all" (every metric of METRICS_ALL) or a list of metric names
},
```

**Block solutions**: the snapshot is filtered to `solution_id` / `dc_solution_id` independently of `scope`'s solutions; adding e.g. allocation (51) to `scope.solution_id` widens scope but does not pull in its blocks unless 51 is also listed here. DC blocks need `scope.dc_solution_id`. tbretail: 21 and 22. Always set `ui_parameters_path` explicitly (the newest snapshot folder may hold no blocks for the solution); a missing listed `blocked_scope/<kind>` folder fails the run (tbretail's `dc_blocked_scope` has no `destination` folder, so `dc_kinds` omits it).

**Store blocks** come from `{ui_parameters_path}/blocked_scope/{product,product_destination,destination}` (parquet; `destination_id` is the store), matched to the scope pairs (requires `scope.columns.start` and `store`). With `rule = "after_scope_start"` a block applies only when `block.start_date >= scope_start` (same day: applies); an earlier block is ignored because the pair was set up again after it; `"all"` applies every matched block. A block covers `start_date` to `end_date` (null = open-ended), clipped to the window. Block `product_id`s are not rolled to the family main.

**Blocked-day representation.** Blocked days are built as cached, disjoint per-pair date intervals `(product_id, store_id, first_day, last_day)` (`ctx.blocked_days`, `scope.build_blocked_days` via `scope._applied_block_intervals`, shared with DC blocks); overlapping or adjacent blocks of a pair are merged. Frames flag or drop a day with a range join on (pair, `first_day <= date <= last_day`) rather than a per-pair-day table. The printed "blocked pair-days in window" count is the number of covered pair-days.

**DC blocks** (`dc_solution_id`, int not bool, e.g. 22; `None` = none) work the same from `{ui_parameters_path}/dc_blocked_scope/{product,product_destination}` (`destination_id` = warehouse), by `rule`. A DC pair's `scope_start` comes from that solution's rows of the scope table (same `run_date`, family roll-up, earliest start and `active_only` as the store scope, location = warehouse); DC pairs outside it get no blocks. Built once per run as `(product_id, warehouse_id, first_day, last_day)` intervals, `ctx.dc_blocked_days` (`scope.build_dc_blocked_days`).

**`metrics` — which metrics drop blocked days.** Blocked days stay in the frames, flagged `is_blocked` (on `scoped_daily` and `dc_daily`, from one range join in `pipeline.build_scoped_daily` / `build_dc_daily`). A metric reads **only unblocked rows when it is named in `metrics`**, and **blocked and unblocked rows alike when it is not**. `"all"` (generic default) means every name of `METRICS_ALL`; an unknown name raises (the allowed names are listed); `settings["BLOCKED_SCOPE"]["metrics"]` holds the resolved list in `METRICS_ALL` order. tbretail names the inventory, WOS, in-stock and DC metrics and leaves sales units, sales revenue, AUR, AUC, the distinct counts and `lost_sales_pct` out: a blocked pair can still sell its existing stock, and the report never removes blocked days from history for those metrics. Per-metric gating is tabulated in [Inventory metrics: blocked days and goods in transit](#inventory-metrics-blocked-days-and-goods-in-transit).

`metrics.compute_kpis` aggregates once per frame and family with conditional aggregation (`F.when(~is_blocked, x)` in sums, averages and distinct counts, plus per-day `<metric>_day` / `<metric>_has` columns so a day with only blocked rows is not a day of a gated average). When every reported metric (`metrics.metric_cols`) is named, blocked rows leave the sales rows too, so a period × slice with only blocked days gets no output row. The comparable-pairs universe stays the real, unblocked rows whatever the gate says; `scope_diff` and the scope debug are unaffected. Weekly sources with no per-day grain (`lost_sales_source` such as `report_dfu`, `instock.method = "weekly_source"`) cannot be filtered and are left as they are.

### `goods_in_transit`

**Gated** (default off: `date_shift_days = None`). One section for every use of goods in transit (GIT): the daily in-stock day, the DC in-stock day and the inventory metrics. Needs `path_segments.goods_in_transit` (`operation/goods_in_transit` snapshots; `destination_type` 0 = store, 1 = warehouse).

```python
"goods_in_transit": {
    "date_shift_days": -1,        # None = GIT off everywhere; an int (not bool): snapshot dated D+1 = end of day D
    "roll_to_family_main": True,  # roll every GIT read to the family main
    "store_instock": True,        # daily in-stock day = on-hand > 0 or store GIT > 0 (needs instock.method "daily")
    "dc_instock": True,           # DC in-stock grid day also counts DC GIT
    "inventory_metrics": ["total_inventory", "mean_stock", "WOS", "WOS_DC", "inventory_turnover_rate"],  # [] = off
},
```

All five keys are required.

- **`date_shift_days`**: a snapshot dated D+1 describes the end of day D, so tbretail uses `-1` (the last report day needs the next day's snapshot; without it that day has no GIT). `None` turns GIT off everywhere; `store_instock` / `dc_instock` must then be `False` and `inventory_metrics` empty.
- **`roll_to_family_main`** (default `True`): rolls every GIT read to the family main (`pipeline._goods_in_transit_quantity`); `False` only for a source already rolled upstream.
- **`store_instock`**: a store-day of the [daily in-stock](#instock) also counts as in stock when store GIT > 0 (`destination_type = 0`, `quantity > 0`), united with on-hand days, never summed.
- **`dc_instock`**: a DC grid day of [`dc_instock`](#dc_instock) also counts as stocked when GIT to the DC (`destination_type = 1`, `quantity > 0`, `destination_id` = `warehouse_id`); a union. Independent of `dc_instock.enabled`.
- **`inventory_metrics`**: any subset of `INVENTORY_GIT_METRICS_ALL` (top of `config.py`), each on its own: `total_inventory`, `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `dc_mean_stock`, `total_mean_stock`, `WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`, `inventory_turnover_rate`. An unnamed metric keeps on-hand only. Store metrics count store GIT, `dc_mean_stock` / `WOS_DC` DC GIT, `total_mean_stock` / `WOS_TOTAL` both; turnover counts it in its mean stock (sales units unchanged). Report metrics are chosen separately in `metrics.metric_cols` from `METRICS_ALL`. Needs `use_fiscal_calendar = True` when not empty.

`materialize()` raises on: a non-int (or bool) `date_shift_days`; `date_shift_days = None` with `store_instock` / `dc_instock` `True` or non-empty `inventory_metrics`; `store_instock` without `instock.method = "daily"`; an unknown `inventory_metrics` name; non-empty `inventory_metrics` without `use_fiscal_calendar = True`. The keys are `settings["GOODS_IN_TRANSIT"]`.

A named metric uses **on-hand + GIT units** every day (retail = units × `price_without_tax`, cost = units × `cogs`, rounded to 2 decimals like `inventory_retail` / `inventory_cost`); an unnamed one uses on-hand only on days with a real daily-data (or `inventory_warehouse`) row (`metrics._day_stock` / `_day_avg`). A day with GIT but no daily row counts as a zero-on-hand day in a named average (`mean_stock`, `WOS`'s average daily inventory).

**Store side** (`pipeline.build_scoped_daily`, only when a metric in `context.STORE_GIT_METRICS` is named; otherwise no GIT read, `has_daily_row = True`, `git_quantity = 0`): daily data (window-filtered, `input_filters.daily_data` applied, scoped pairs) → store GIT per `(product_id, store_id, date)` (`destination_type = 0`, `quantity > 0`, shifted, **summed**, family main, window, scoped pairs) → full outer join to the daily rows (summed per pair-day first; a GIT-only day gets `sales_quantity` / `sales_revenue` / `inventory` = 0 and `has_daily_row = False`) → blocked-day flag (`ctx.blocked_days`, so a metric in [`blocked_scope.metrics`](#blocked_scope) drops a blocked day's daily row and GIT alike) → scope product-week semi-join → calendar and product attributes. GIT-only days whose daily row was **removed by `input_filters.daily_data`** (tbretail: `usable = 1`) are dropped too (`inputs.get_daily_data_excluded_days` reads those days once per run, family-rolled, and every scope variant left-joins it), so an unusable day never returns as a zero-sales inventory day.

**DC side** (`pipeline.build_dc_daily`, only when `dc_mean_stock`, `total_mean_stock`, `WOS_DC` or `WOS_TOTAL` is named): DC GIT (`destination_type = 1`, `quantity > 0`, same shift, summed per `(product_id, warehouse_id, date)`, family main, window) is full-outer-joined to the rolled `inventory_warehouse` rows (a GIT-only day has inventory 0 and `has_inventory_row = False`), `Year`/`Week` attached, rows restricted to `scope_core`'s product-weeks; DC blocked days then flag both kinds of rows.

In `metrics.compute_kpis`, sales (`total_sales_*`, `AUR`, `AUC`, distinct counts) and `weighted_instock_rate`'s sales weights read real rows only; WOS, mean stock, total mean stock, turnover and DC metrics each build their own frame (on-hand + GIT only when named) and are joined on the period keys; `population_filters` apply per group (GIT rows carry product dimensions). **Never changed by `inventory_metrics`:** sales, the daily in-stock (own `store_instock`), lost sales (real rows only in the denominator), the DC in-stock rate (own `dc_instock`) and the comparable-pairs universe (real rows only). A named metric's Metric Details text says it counts store / DC goods in transit.

### `output`

See [Output saves](#output-saves) for mode behaviour, merge keys, workflows, and caveats.

```python
"output": {
    "save_outputs": True,
    "path_segments": ["analysis", "kpi_reports", "outputs"],
    "run_date": None,
    "save_mode": "incremental",
    "allow_overwrite_existing": False,
    "recompute_comparisons_from_history": True,
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

See [HTML report](#html-report).

## Environment variable overrides

| Variable                          | Effect                                                       |
| --------------------------------- | ------------------------------------------------------------ |
| `KPI_BUCKET`                      | Datastore mount (default `/mnt/invent-{customer}-datastore`) |
| `KPI_CUSTOMER`                    | Customer slug                                                |
| `KPI_AS_OF_DATE`, `KPI_RUN_MIN_DATE` | Override `as_of_date`, `run_min_date`                     |
| `KPI_REPORT_END`                  | Overrides `reporting_window.report_end` (`as_of` / `complete_month` / `latest_day`) |
| `KPI_HALF_PERIODS`                | `true`/`false` — overrides `fiscal_calendar.half_periods`    |
| `KPI_USE_HYBRID_SCOPE`            | `true`/`false` — overrides `scope.use_hybrid_scope`          |
| `KPI_RUN_SCOPE_DIFF`              | `true`/`false` — enable scope-vs-score scope diff          |
| `KPI_COMPARABLE_PAIRS`            | `true`/`false` — enable comparable (like-for-like) pairs     |
| `KPI_COMPARISONS`                 | Comma-separated subset of `yoy,ytd` — selects which comparisons to compute |
| `KPI_RECOMPUTE_COMPARISONS`       | `true`/`false` — recompute comparisons from merged history (default `true` under incremental) |
| `KPI_LOST_SALES_ENSEMBLE`         | `true`/`false` — blend fast (120d) + slow (365d) lost-sales models by speed cluster |
| `KPI_LOST_SALES_SLOW_PATH`        | Comma-separated path segments for the 365-day model          |
| `KPI_SPEED_CLUSTER_PATH`          | Comma-separated path segments for the speed-cluster attributes table |
| `KPI_SPEED_CLUSTER_FORMAT`        | `long` (default) or `wide` — speed-cluster table shape       |
| `KPI_SPEED_CLUSTER_ATTRIBUTE`     | `attribute_name` selecting the speed cluster (default `sales_speed`, format=`long`) |
| `KPI_SPEED_CLUSTER_VALUE_COL`     | Column holding the numeric cluster (default `product_speed_cluster`, format=`wide`) |
| `KPI_FAST_MOVER_CLUSTERS`         | Comma-separated cluster ints taking the fast model (default `1,2,3`) |
| `KPI_LOST_SALES_WEEK_COL`, `KPI_LOST_SALES_PRODUCT_COL`, `KPI_LOST_SALES_STORE_COL`, `KPI_LOST_SALES_COL`, `KPI_LOST_SALES_IN_STOCK_COL`, `KPI_LOST_SALES_TOTAL_DAYS_COL` | Override `lost_sales_source.week_col` / `product_col` / `store_col` / `lost_sales_col` / `in_stock_col` / `total_days_col` (defaults `week_start_date`, `product_id`, `store_id`, `lost_sales`, `in_stock`, `details.total_days`) |
| `KPI_INSTOCK_METHOD`              | `daily` / `weekly_source` / `lost_sales_source` — overrides `instock.method` |
| `KPI_INSTOCK_SOURCE_PATH`         | Comma-separated path segments for `instock.weekly_source.path_segments` |
| `KPI_INSTOCK_WEEK_COL`, `KPI_INSTOCK_PRODUCT_COL`, `KPI_INSTOCK_STORE_COL`, `KPI_INSTOCK_IN_STOCK_COL`, `KPI_INSTOCK_TOTAL_DAYS_COL` | Override `instock.weekly_source.week_col` / `product_col` / `store_col` / `in_stock_col` / `total_days_col` (defaults `week_start_date`, `product_id`, `store_id`, `in_stock`, `total_days`) |
| `KPI_USE_FISCAL_CALENDAR`         | `true`/`false`                                               |
| `KPI_SCOPE_MIN_PERCENTILE`        | e.g. `20` or `0.2`                                           |
| `KPI_SCOPE_MIN_WEEKS_FOR_FILTER`  | Integer                                                      |
| `KPI_SLICE_DIMENSIONS`            | Comma-separated column names                                 |
| `KPI_SAVE_OUTPUTS`, `KPI_ALLOW_OVERWRITE_EXISTING` | `true`/`false`                              |
| `KPI_OUTPUT_SAVE_MODE`            | `initial`, `incremental`, or `full_refresh`                  |
| `KPI_OUTPUT_PATH`                 | Comma-separated path segments                                |
| `KPI_OUTPUT_RUN_DATE`             | `output.run_date` partition (default: `as_of_date`)          |
| `KPI_HTML_*`                      | HTML report overrides, listed under [HTML report](#html-report) |
| `KPI_RUN_MODE`                    | `full` or `html_only` — skip pipeline and render HTML from saved outputs |

## Metrics

Default metrics (configurable in `CONFIG["metrics"]`):

- Sales / inventory: `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `total_inventory`
- Coverage: `distinct_product_count`, `distinct_store_count`, `distinct_pair_count`
- Stock/service: `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate`, `in_stock_rate`, `weighted_instock_rate`, `lost_sales_pct`
- DC/warehouse + combined inventory (needs `path_segments.inventory_warehouse`, see [`inventory_warehouse`](#inventory_warehouse)): `dc_mean_stock`, `total_mean_stock`, `WOS_DC`, `WOS_TOTAL`
- DC in-stock (gated by `dc_instock.enabled`, needs `path_segments.item_family`, see [`dc_instock`](#dc_instock)): `dc_in_stock_rate`
- The inventory metrics can each count goods in transit on top of on-hand (gated, `goods_in_transit.inventory_metrics`, see [`goods_in_transit`](#goods_in_transit))

Every metric uses all scoped stores; there is no global store-exclusion key. The per-metric exceptions are `instock.daily.input_filters` (stores out of in-stock under `instock.method = "daily"`), `lost_sales_source.sales_filter` (out of `lost_sales_pct`) and `metrics.population_filters` (a metric's product population). tbretail uses exactly these: **NON-COMP** (`IS_COMP == "no"`) is removed from in-stock only (`population_filters.in_stock_rate`), **ECOM** stores 829 / 639 / 917 from in-stock and lost sales only (`instock.daily.input_filters`, `lost_sales_source.sales_filter` and the ECOM-excluding model behind `report_dfu`). Every other metric keeps both on every root tab (Overall, NVROUT, LFL), period type, comparison, comparable table and the HTML (all share one `compute_kpis` call); the LFL (`comp`) root is `IS_COMP == "yes"`, so NON-COMP is absent there. To drop a store from everything use `input_filters.daily_data` (plus `instock.daily.input_filters` under `"daily"`, whose read skips `input_filters.daily_data`) or exclude it upstream; for **`lost_sales_pct` only** use [`lost_sales_source.sales_filter`](#lost_sales_source).

**One population per source.** Every metric read from `noob/daily-data` (sales, `total_inventory`, `mean_stock`, `WOS`, turnover, weighted-instock's weights) uses the **same** rows under every `scope.grain`, except that [`goods_in_transit`](#goods_in_transit) adds GIT-only days for the metrics it gates and [`blocked_scope`](#blocked_scope) reads or drops blocked days per `blocked_scope.metrics` (see [Inventory metrics](#inventory-metrics-blocked-days-and-goods-in-transit)); `population_filters` / `sales_filter` are the only other deviations. Lost sales and in-stock come from their own source restricted at that source's grain; DC inventory comes from `inventory_warehouse`, family-rolled to parent `product_id` ([`dc_instock`](#dc_instock)).

**Lost Sales %** = `100 × sum(lost_sales) / sum(floor(weekly_sales + lost_sales))`; the denominator includes imputed lost demand. `weekly_sales` comes from `daily_data` and `lost_sales` from `lost_sales_source`, so a narrower-population source makes the ratio read low (see [`lost_sales_source.sales_filter`](#lost_sales_source)).

**In-Stock Rate** = `sum(in_stock_days) / sum(available_days)` from the [`instock`](#instock) method's source: daily-data store-days (`daily`), a separate weekly table (`weekly_source`) or the top-down lost-sales output (`lost_sales_source`).

**Weighted In-Stock Rate** = sales-weighted average of weekly in-stock rates (each fiscal week weighted by its sales volume when rolling up to the period). Reported as pp-change in comparisons.

**WOS** = per-product per-fiscal-week WOS after summing daily inventory/sales across all scoped stores at product×date (`avg_daily_inventory / weekly_sales`), rolled up to the period with a sales-weighted average (not computed at product×store×week grain). Average daily inventory is weighted by `week_days / 7` (`week_days` = calendar days of that fiscal week in the view, 7 for a whole week), so `WOS = Σ(avg_daily_inventory × week_days/7) ÷ Σ(weekly_sales)` differs from the plain form only for the week `latest_day` cuts mid-week ([Latest-day report end](#latest-day-report-end-report_end--latest_day)); the same weighting applies to `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`. Inventory is on-hand, or on-hand + GIT for metrics in [`goods_in_transit.inventory_metrics`](#goods_in_transit) (sales, in-stock and lost sales never include GIT); a WOS metric drops blocked days from inventory and sales when named in [`blocked_scope.metrics`](#blocked_scope).

**WOS (DC)** and **WOS (Total)** use the same grain and rollup on the SAME `daily_data_week` frame (each metric with its own gate columns) left-joined with a DC-inventory frame (weeks with no DC record fill 0, not dropped): `WOS_DC = avg_daily_dc_inventory / weekly_sales`; `WOS_TOTAL = (avg_daily_total_inventory + avg_daily_dc_inventory) / weekly_sales`. DC inventory is restricted to the same in-scope product-weeks as every other metric ([`inventory_warehouse`](#inventory_warehouse)).

**DC In-Stock Rate** = `F.greatest(0.0, sum(dc_stocked_days) / sum(dc_available_days))` at DC/warehouse level (gated by `dc_instock.enabled`, [`dc_instock`](#dc_instock)). Unlike other DC metrics **the denominator is an expanded grid, not a row count**: each pair's daily rows run from its first `inventory_warehouse` row to `REPORT_END_DATE` with gaps 0-filled as stockouts, whereas `dc_mean_stock`/`WOS_DC` average existing rows. A pair that **stops** being stocked keeps adding stockout days here while the others go quiet for it; a pair **never stocked** in the window is absent from all. With `enabled=False` (default) it is a literal `null` column.

**Inventory Turnover Rate** = Sales Units ÷ Mean Stock for the same period grain (mean stock includes GIT when named in `goods_in_transit.inventory_metrics`, sales units never; both drop blocked days when named in `blocked_scope.metrics`). The HTML report labels it per tab: **Annual**, **YTD**, **Quarterly**, **Monthly**, or **Weekly** Inventory Turnover Rate.

### Inventory metrics: blocked days and goods in transit

Two independent per-metric gates act on the metrics read from `noob/daily-data` / `inventory_warehouse`:

| Metric | Frame | Drops blocked days when in [`blocked_scope.metrics`](#blocked_scope) | Adds goods in transit when in [`goods_in_transit.inventory_metrics`](#goods_in_transit) |
| --- | --- | --- | --- |
| `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `distinct_*_count` | `scoped_daily` real rows | yes, each its own gate (`AUR` / `AUC`: numerator and sales units both) | never |
| `total_inventory`, `mean_stock` (+ retail / cost), `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate` | `scoped_daily` | yes (inventory and the sales it divides by; a blocked day drops its GIT too) | store GIT |
| `dc_mean_stock`, `WOS_DC` | `dc_daily` | yes (DC blocked days; `WOS_DC`'s store sales denominator uses store blocked days, same gate) | DC GIT |
| `total_mean_stock`, `WOS_TOTAL` | `scoped_daily` + `dc_daily` | yes (store and DC blocked days) | store and DC GIT |
| `in_stock_rate`, `weighted_instock_rate` | daily in-stock frame (`pipeline.build_instock_daily`) | frame built without blocked store-days, on-hand days and GIT days when `in_stock_rate` is named; `weighted_instock_rate`'s sales weights when it is named | `goods_in_transit.store_instock` (the frame's in-stock days) |
| `dc_in_stock_rate` | DC grid (`pipeline.build_dc_inst`) | leaves DC blocked days out of stocked and available days when named | `goods_in_transit.dc_instock` |
| `lost_sales_pct` | `lost_base` | the daily-data sales half of the denominator (`daily_for_lost` in `build_pipeline_frames`), when named; the weekly numerator never changes | never |

A metric not named in `blocked_scope.metrics` reads blocked and unblocked rows alike. A day counts in a per-day average (`mean_stock`, `dc_mean_stock`, `total_mean_stock`, WOS average daily inventory, turnover mean stock) only when it has a row the metric reads (without GIT a real row, with GIT any row), so a gated metric skips a day whose rows are all blocked. One conditional-aggregation pass per frame in `metrics.compute_kpis` (`_reads`, `_read_only`, `_day_stock`, `_day_avg`).

### Population filters (restrict ONE metric's own population)

`metrics.population_filters` narrows one metric's product population without touching scope, roots or other metrics (e.g. "in-stock rate should never count NON-COMP, even in the `overall` root"). It applies **on top of** the root/cut in effect, so it is a no-op inside a root that already restricts to the same value (filtering `IS_COMP` inside `comp`).

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

Shape: `{metric_col: {dim_col: value_filter_spec}}`; `dim_col` is any `dimension_sources`/`slices` column already joined onto products (`IS_COMP`, `IS_NVROUT`, `brand`), and `value_filter_spec` has the same list/dict shape as [`slices.value_filters`](#value-filters-restrict-cut-values--drop-the-null-bucket).

**Metrics computed in one shared aggregation pass can only be filtered together** (`kpi_pipeline/filters.py`'s `METRIC_FILTER_GROUPS`):

| Group | Metric cols |
| ----- | ----------- |
| `sales` | `total_sales_quantity`, `total_sales_revenue`, `total_inventory`, `AUR`, `AUC`, `distinct_product_count`, `distinct_store_count`, `distinct_pair_count` |
| `wos` | `WOS`, `wos_revenue`, `wos_cost` |
| `wos_dc_total` | `WOS_DC`, `WOS_TOTAL` |
| `mean_stock` | `mean_stock`, `mean_stock_retail`, `mean_stock_cost` |
| `dc_inventory` | `dc_mean_stock`, `total_mean_stock` |
| `turnover` | `inventory_turnover_rate` |
| `instock` | `in_stock_rate` |
| `weighted_instock` | `weighted_instock_rate` |
| `dc_instock` | `dc_in_stock_rate` |
| `lost_sales` | `lost_sales_pct` |

An entry on one column applies to the whole group; *conflicting* specs on two columns of a group fail loudly at run time. `materialize()` fails loudly on an unknown `metric_col` or a malformed `value_filter_spec`. A metric with no entry is unaffected.

## tbretail setup

Notes behind `tbretail_config.py` (its comments point here).

**One-time CSV exports** (Databricks cell; CSVs under `/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/KPI-NEW/data/`):

1. NON-COMP (NFG list) product IDs (`dimension_sources["ngf_comp_split"]`, file `non_comp_ids_20260817.csv`): join an NFG Excel list (`itemcode` column, `Grand Total` row dropped) against products and write the `product_id` values.

   ```python
   pdf_nfg = pd.read_excel(NFG_EXCEL_PATH)
   col_ic = [c for c in pdf_nfg.columns if str(c).strip().lower() == "itemcode"]
   item_col = col_ic[0] if col_ic else pdf_nfg.columns[2]
   nfg_codes = [
       c for c in pdf_nfg[item_col].dropna().astype(str).str.strip().unique()
       if c and c.lower() != "grand total"
   ]
   ```

2. NGF item product IDs (`ngf_product_ids.csv`): same join on an NGF item list (`pdf_ngf["itemcode"]`). NGF items stay in daily-data / scope and Overall and are only flagged (`IS_COMP = 'no'`), so comp vs non-comp can be sliced in one report. `fillna: {"IS_COMP": "yes"}` sets every non-NGF product to `'yes'` (otherwise NULL, since the CSV only covers NGF items). The configured `ngf_comp_split.path` currently points at `non_comp_ids_20260817.csv` (the step-1 file), not `ngf_product_ids.csv`; unclear whether that is deliberate reuse or the step-2 CSV was never generated, so verify before treating either as correct.

**tbretail scope is the open scope table (platform `operation/scope`) only.** Former manual additions (JAB, NGF products, `nvrout_scope_backfill`) and the NON-COMP removal no longer exist: the additions put products into scope at every store with daily data, outside the scope table and its rules (no roll-up, active filter, scope start or blocks), which pulled NVROUT in-stock 5-20 points below the in-stock script that matched the client. NVROUT / COMP / NON-COMP are labels from `dimension_sources`.

**NON-COMP in-stock rule.** kpi-skill-toolkit's Overall in-stock excludes NON-COMP products (`inst_products = nvr_ids ∪ comp_ids_primary`). This pipeline's `overall` root has no restriction of its own, so NON-COMP products (then in scope via the since-removed "NGF products" addition) counted toward Overall's in-stock rate, the confirmed cause of the 2026-09-03/04 Overall-instock-only mismatch (COMP / NVROUT matched, since NON-COMP is in neither root). `metrics.population_filters.in_stock_rate = {"IS_COMP": {"exclude": ["no"]}}` aligns Overall without touching scope, roots or other metrics.

**Blocked-scope snapshot.** `blocked_scope.ui_parameters_path` comes from the Airflow variable `ui_parameters_path`. The newest folder when it was set (`2026-09-30-204511_...`) only had solution 51 blocks; the configured folder is `2026-09-30-065549_...`. Always pin the folder.

**Fiscal calendar.** The fiscal year runs Feb-Jan, so fiscal month / quarter numbers don't match the real calendar (fiscal month 07 observed spanning real 8/2-8/29): `fiscal_calendar.column_map.month_name_col` is read verbatim for the Monthly tab label. `run_min_date` 2025-02-08 resolves to Sunday 2025-02-02, the start of fiscal 2025.

**Lost sales source.** `path_segments.lost_sales` is `report_dfu` (future_visibility's pre-blended fast / slow output), used instead of `lost_sales_ensemble` (OFF, the fallback if `report_dfu` stops being usable). `report_dfu` has no `store_id`, so `store_col = None` ([`lost_sales_source`](#lost_sales_source)); its model is `top_down_excluding_ecom`, hence `sales_filter` excludes the same three ECOM stores (829 / 639 / 917, from customer-analysis-tbretail's `store_replenishment/future_visibility/lost_sales_product_120dayslookback.py:66` and the identical filter in `_365dayslookback.py:67`; keep the two in sync).

**Metrics not reported.** `wos_revenue`, `weighted_instock_rate` and `dc_in_stock_rate` are left out of `metrics.metric_cols` (and `dc_instock` is OFF, so `dc_in_stock_rate` would be null); they stay in `blocked_scope.metrics` and `goods_in_transit.inventory_metrics`, so enabling one needs no other edit.

**Dimension sources.** `IS_NVROUT` comes from `operation/extended_product`, which must have one row per `product_id` (an arbitrary row is kept otherwise); absent products get NULL, which `fillna` turns into `'no'`. If a product can have several program values, pre-aggregate to one NVROUT flag per product and point `path` at that instead of `path_segments`.

## Programmatic use

```python
from kpi_pipeline import KPIRunner
from kpi_pipeline.io import save_outputs, load_saved_outputs

# Pre-flight scope debug (distinct product/store counts overall + per slice)
runner = KPIRunner(spark, settings)
runner.prepare_scopes()
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

**Inline results.** `KPIRunner.run` displays results as each step finishes: after `build_kpis`, the overall kpi_long rows (root and dimension `"overall"`) of the latest period of each `period_type` (`runner.latest_overall_kpis()`); after `build_comparisons`, the overall YoY / YTD displays; after `build_comparable_pairs`, each enabled comparable kind's overall display.

## Performance notes

Patterns to preserve when changing internals:

- **Products table**: read once, cached and broadcast, reused by all scope variants.
- **Daily data and lost sales**: cached once per run via `get_daily_data_raw` and `read_lost_sales_weekly` (the notebook previews read the sources directly). `get_daily_data_raw` caches only the report window's rows (`input_filters.daily_data` applied, rolled to the family main) and only the columns its readers use (`product_id`, `store_id`, `date`, `sales_revenue`, `sales_quantity`, `inventory`, plus the week column on the civil calendar). The daily in-stock method reads `noob/daily-data` separately (`get_instock_daily_raw`: no input filters, from `instock.daily.history_start`); the two reads differ in rows and id space, so they are not shared.
- **Score scope join**: fiscal-calendar equi-join, not a date-range join on week bounds (avoids a nested-loop scan of the daily frame).
- **Score scope inventory**: last available daily snapshot in the fiscal week (`max_by(inventory, date)` in `build_weekly_scope`), not Saturday-only.
- **HTML weekly columns**: sorted by `week_start_date` from `fiscal_week`, not lexicographic `Year_Week`.
- **KPI sort**: final sort in pandas after `toPandas()`, not Spark `orderBy` (removes a shuffle stage from every aggregation).

If runs are slow, check that the scope table path is correct (scope table not empty) and `run_min_date` is Sunday-aligned (or null for full YTD).

## Known limitations

- **Weekly tab with sparse weeks**: `weekly_display_weeks` shows the N most recent weeks **present in `kpi_long`**, not necessarily consecutive fiscal weeks (after a narrow `run_min_date` or partial backfill).
- **Quarter/Half/Monthly trend tabs never show an in-progress period** and there is no toggle ([Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)); read `kpi_long`'s Weekly rows if you need it.
- **`report_end = "latest_day"`** needs `instock.method = "daily"`; a fiscal year starting before `EFFECTIVE_REPORT_START_DATE` is not shown in YTD; YTD lost sales is whole weeks to the last Saturday, so it covers fewer days than other YTD metrics.

## Troubleshooting

| Symptom                     | Likely cause                                                                |
| --------------------------- | --------------------------------------------------------------------------- |
| `lost_sales_pct` reads lower than expected | Lost-sales source covers a narrower population than `daily_data` (classically ecom-excluded), inflating the denominator. Use [`lost_sales_source.sales_filter`](#lost_sales_source) |
| `initial save blocked`      | Output tables already exist; use `incremental` or `full_refresh` (see [Output saves](#output-saves)) |
| Overlapping periods skipped / saved Delta stale vs notebook | Expected with `incremental` + `allow_overwrite_existing=False`; set `True` or use `full_refresh` |
| Empty cut dimension       | Column missing from `master-data/products` or derived SQL failed validation. If it lives on another table, add a [dimension source](#dimension-sources--roots-population-tabs-from-other-tables) (a root, not a cut) |
| Dimension source errors on read | An **enabled** source fails loudly on bad path / missing column / bad expression; fix it or set `enabled: False` |
| A root you expected is missing, or you want only one root value | Set `root_values` on that entry (`{"yes": "nvrout"}` makes exactly one root); omit it to auto-discover one root per distinct value. `NULL` never gets its own root. |
| Ensemble run fails loudly on read | Wrong `slow_path_segments` / `speed_cluster_path_segments`, or the attributes table lacks the `attribute_name` value. Fix it or set `enabled: False`. For a **wide** cluster table (no `attribute_name`/`attribute_value`) set `speed_cluster_format: "wide"` and `speed_cluster_value_col` |
| Monthly tab empty or stops earlier than Quarterly/Annual | Fiscal weeks in the window have a null `Fiscal_Month` in `one_time_uploads/fiscal_cal` while `Fiscal_Quarter`/`Fiscal_Year` are complete. `kpi_pipeline/fiscal.py` raises listing the weeks; fix the upload or narrow the window to covered months. |
| `time="weekly"`: a pair's earliest weeks are missing though it is active | Pairs tied to the source's earliest week are backfilled by default (`scope.backfill_leading_gap`, [`scope`](#scope)); a later first-seen week is left as-is (a real new store/product). Set `False` for existing history saved without the option. |
| `time="weekly"` raises `ValueError` about `columns.date` on load | `use_fiscal_calendar=True` requires `scope.columns.date`; native `columns.year`/`columns.week` is valid only under `use_fiscal_calendar=False`. |
| A comparable `quarter` link looks wrong / never appears though 2+ years have data | That quarter must be **fully elapsed** inside the window in those years ([Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half)); the latest year's in-progress quarter, or an earliest year cut by a `run_min_date` not on a quarter boundary, is excluded |
| Latest Quarter/Monthly row missing, or a stale partial-period row in saved Delta | The period has not fully elapsed ([Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)). A partial row saved by an earlier run stays until `allow_overwrite_existing=True` or `full_refresh` |
| `ValueError: reporting_window.report_end='latest_day' requires instock.method='daily'` | `latest_day` splits the daily in-stock frame's fiscal week, which weekly sources cannot do. Use `instock.method = "daily"` or `as_of` / `complete_month`. |
| Current fiscal year missing from Annual/YoY under `latest_day` | By design: Annual shows complete years only; the current year is in YTD. A year is left out of YTD when its first date precedes `EFFECTIVE_REPORT_START_DATE`: move `run_min_date` back to that year's start. The run prints `YTD years` and the years left out. |
| `ValueError: goods_in_transit...` on config load | Unknown `inventory_metrics` name; `date_shift_days` is `None` while `store_instock` / `dc_instock` / `inventory_metrics` ask for GIT, or not an int (bool rejected); `store_instock` without `instock.method = "daily"`; non-empty `inventory_metrics` with `use_fiscal_calendar` `False`. See [`goods_in_transit`](#goods_in_transit). |
| `ValueError: blocked_scope.metrics...` on config load | Not `"all"` or a list, or a name not in `METRICS_ALL` (the message lists them). See [`blocked_scope`](#blocked_scope). |

## HTML report

Cell 6 of `main.ipynb` generates a standalone, offline HTML file after the pipeline run (or after loading saved outputs in `html_only` mode). Enabled by default (`html_report.enabled`).

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
| **Executive header** | Client, reporting window, as-of date, scope mode, slice dimensions, generated timestamp; with `report_end = "latest_day"` also a **Period basis** card (YTD to the report end, complete periods elsewhere, lost sales through the last Saturday) |
| **Period tabs** | Annual / **YTD** / Quarter / Half / Monthly / Weekly (horizontal; Half only with `fiscal_calendar.half_periods`) |
| **Slice dimension tabs** | Overall + every slice column in `kpi_long` (inferred from data and config) |
| **Value tabs** | Vertical sidebar within each slice dimension, one panel per value (e.g. each brand) |
| **KPI tables** | Metrics as rows (colour-coded), periods as columns; inventory turnover is labelled **Annual** / **YTD** / **Quarterly** / **Half-Yearly** / **Monthly** / **Weekly** per tab |
| **Comparison** | YoY / YTD per value panel, on the Annual/YTD tabs only: one wide value+delta table (the KPI table's period columns plus one delta column per consecutive-year link; YoY exactly one) in place of the plain value table. Quarter/Half/Monthly/Weekly tabs show the plain value-trend table. |
| **Comparable (Like-for-Like)** | With `comparable_pairs.enabled=True`, a separated section under the comparison table per enabled kind (see [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half)). |
| **Metric Details tab** | Definition, store scope and formula for every active metric. In-Stock Rate is described from `instock.daily` when `instock.method = "daily"`; blocked-day exclusion is mentioned only for metrics named in `blocked_scope.metrics`, and a metric named in `goods_in_transit.inventory_metrics` states that it counts store / DC goods in transit; Lost Sales % states the last-Saturday basis under `latest_day`. |

Slice dimensions and values are **inferred from `kpi_long`**, so a different slice column (e.g. `category`) needs no code change.

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
    "half_display_halves": 4,            # Half tab: N most recent halves; null = all
    "yearly_display_years": 5,           # Annual tab: N most recent years; null = all
    "root_labels": {},                   # root id -> tab label, e.g. {"comp": "LFL", "nvrout": "NVROUT"}
    "dimension_labels": {},              # slice dimension name -> tab label (display only), e.g. {"brand": "Banner"}
}
```

`filename` may use only `{customer}` and `{report_end}` (validated in `materialize()`; `KPIRunner.build_html_report` formats the template `HTML_REPORT_FILENAME_TEMPLATE` with the final `REPORT_END_DATE`); `output_path_segments` is the datastore **folder** (`HTML_REPORT_OUTPUT_DIR`; the filename is appended). `root_labels` renames root tabs (unlisted roots fall back to `Overall` / the root id; tbretail: `comp` -> `LFL`, `nvrout` -> `NVROUT`). `dimension_labels` renames a slice dimension wherever shown (dimension tabs, header card; tbretail: `brand` -> `Banner`), display only (`kpi_long` and saved outputs keep the raw key); `materialize()` raises unless it is a `str -> str` dict. Table cells are center-aligned and every tab label has its all-lowercase words capitalized (`_tab_label`: `annual` -> `Annual`, `jab` -> `Jab`; `YTD`, `SMW` kept) for every client.

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
| `KPI_HTML_HALF_HALVES` | Recent halves in Half tab (default 4; empty = all) |
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

Only the keys you provide are overridden; other metrics keep their defaults from `kpi_pipeline/html_report.py`.

### Programmatic use

```python
html_path = runner.build_html_report(local_dir=".")
```

Or call the renderer directly:

```python
from kpi_pipeline.html_report import render_kpi_html, DEFAULT_METRIC_DEFINITIONS
render_kpi_html(ctx, "/dbfs/mnt/.../report.html", report_title="My KPI Report")
```

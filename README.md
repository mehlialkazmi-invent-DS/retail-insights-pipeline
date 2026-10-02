<p align="center">
  <img src="docs/images/readme-banner.png" alt="Retail KPI analytics — sales, inventory, in-stock rate, and weeks of supply" width="100%" />
</p>

# Retail Insights Pipeline

PySpark toolkit for weekly, monthly, quarterly, annual, and YTD retail KPIs with configurable scope (defined-only or hybrid with score backfill), optional manual scope adjustments, comparable (like-for-like) pair analysis, and incremental Delta output saves.

Designed to run on **Databricks** against the customer Delta datastore (`/mnt/invent-{customer}-datastore`).

## What it produces


| Output                                  | Description                                                                                                                                            |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kpi_long`                              | One tidy table: `period_type` (annual / **ytd** / quarter / half / monthly / weekly; `half` only when `fiscal_calendar.half_periods` is on), `period`, `root`, `dimension`, `dimension_value`, plus all configured metrics. `root` is `"overall"` plus one per configured [dimension_source root](#roots-and-cuts-report-structure); `dimension`/`dimension_value` is the cut within that root. Filter this to reproduce any root/cut/period panel. |
| `comparison_yoy / ytd`                  | Prior vs current period with formatted display columns, per root × cut. YoY is the last two annual periods; YTD compares the **same** elapsed-window **across years**, chained across every consecutive year pair present (see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)). With `report_end = "latest_day"` the annual periods are complete fiscal years only and YTD is the same fiscal day of every year (see [Latest-day report end](#latest-day-report-end-report_end--latest_day)). There is no separate QoQ/MoM/WoW comparison table — see the Quarter/Half/Monthly/Weekly `kpi_long` period_type rows for recent-period value trends. Both are recomputed from the full merged kpi_long history on incremental saves. |
| `scope_diff`                            | Side-by-side annual KPIs for **defined-only** vs **score-only** scope (optional sanity check). Only computed when `scope.run_scope_diff=True`. Compares scope **before** manual adjustments — intentional diagnostic of defined vs score coverage. Its years are the Annual tab's: with `report_end = "latest_day"` complete fiscal years only. |
| `comparable_kpi_long` / `comparable_comparison_{ytd,yoy,quarter}` | Like-for-like metrics over only the pairs present in every qualifying year (one shared universe per kind across its own consecutive-year links), per root × cut. Gated on `comparable_pairs.enabled=True`; which kinds via `comparable_pairs.kinds` (default `["ytd"]`). See [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half). |
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
    ├── comparable.py   # Gated like-for-like YTD comparison (all-years pair restriction)
    ├── io.py           # Incremental Delta saves + save plan preview
    ├── html_report.py  # Standalone HTML renderer (offline, tabbed report)
    └── context.py      # Shared runtime state (KPIContext)
```

## Quick start (Databricks)

**`config.py` is a generic reference template, not any specific customer's deployed config.** Every optional feature (`scope_adjustments`, `dimension_sources`, `lost_sales_ensemble`, `instock` methods `daily` / `weekly_source`, `comparable_pairs`, `dc_instock`, `blocked_scope`, `goods_in_transit`) ships disabled with an illustrative placeholder example — copy this file per client, then replace `customer`, every `path_segments` entry, `defined_scope`'s column names, and any business rules (scope adjustments, dimension sources) with that client's own values. A real, fully-wired-up deployed config for one customer (tbretail) lives in this repo as `./tbretail_config.py` (copies one directory up, `../tbretail_config.py` / `../tbretail_config_2.py`, are older deployed versions and must be replaced by it before a run) — diff against it to see what a genuinely customized config looks like in practice (it currently reads `lost_sales_source` directly from a customer-specific reporting table instead of running `lost_sales_ensemble`, builds in-stock with `instock.method = "daily"`, and has real `scope_adjustments`/`dimension_sources` business rules wired in; its one-time setup notes are in [tbretail setup](#tbretail-setup)). Every config keeps **all** sections and keys, switched off when unused: the merged sections `instock`, `goods_in_transit` and `blocked_scope.metrics` replace the former per-feature keys (old key -> new key table in the CHANGELOG).

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
  - `instock.method` — where in-stock comes from (`daily` / `weekly_source` / `lost_sales_source`); `blocked_scope.metrics` — which metrics drop UI-blocked days; `goods_in_transit` — where goods in transit counts (all gated, see [`instock`](#instock), [`blocked_scope`](#blocked_scope), [`goods_in_transit`](#goods_in_transit))
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

`scope_source.mode = "operation_scope"` swaps the scope table for the platform scope (see [`scope_source`](#scope_source)); the grains behave the same.

Downstream always consumes `ctx.scope_keys` — `[product_id, Year, Week]` for `"product"`, `[product_id, store_id, Year, Week]` for `"product_store"`/`"product_store_week"`.

### Defined scope only (default: `use_hybrid_scope = False`)

Final scope = `defined_scope_keys` as built at the configured grain. For `product`/`product_store` that means every scope pair (or product) **× every week in the report window** — the scope table's own weeks are ignored entirely (`date_col`/`year_col`/`week_col` are not read). For `product_store_week`, only the scope table's own (product, store, week) rows survive, window-filtered. Score scope is **not** computed unless `scope.run_scope_diff=True`.

### Hybrid scope (`use_hybrid_scope = True`)

Hybrid = defined scope (at the configured grain) **+ score backfill on the window weeks the defined scope does not cover**:

1. **Covered weeks** — fiscal weeks in the window present in `defined_scope_keys`.
2. **Missing weeks** — fiscal weeks in the window with no rows in `defined_scope_keys`. These are backfilled from **score scope** (`scope_origin=score`): computed once over the **full** window, each `(product_id, store_id)` keeps the weeks whose weekly sales and **last available in-week inventory** clear a per-pair percentile threshold (**all stores included** — scope membership is store-agnostic), restricted to the missing weeks.
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

**CSV location** — `"location"` controls where a CSV physically lives (applies to scope adjustments *and* [dimension sources](#dimension-sources--roots-population-tabs-from-other-tables)):

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

So with `root_values: {"is_nvrout": {"yes": "nvrout"}}` and `slices.dimensions: ["brand"]`, the report has: `overall` root × `overall` cut (grand total), `overall` × `brand` (brand breakdown across everything), `nvrout` × `overall` (NVROUT total), `nvrout` × `brand` (brand breakdown within NVROUT only) — mirroring kpi-skill-toolkit's `overall_annual_segment` / `nvr_all_annual_brand` style outputs. `kpi_long`, the HTML report (root becomes an outer tab when there's more than one), and comparisons (`root` column added to `comparison_yoy`/`comparison_ytd`/every `comparable_comparison_{kind}`) are all root × cut aware.

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
| Narrow ONE metric's own population, inside every root/cut | `metrics.population_filters` | Restricts that **metric only** (see [Metrics](#metrics)) |

`scope_adjustments` decide *which rows exist at all*; roots decide *which population a report view covers*; cuts decide *how a population is broken down*. A typical setup uses all three: a scope addition, a root for a segment flag, and a cut for brand.

## Comparable pairs (like-for-like: ytd / yoy / quarter / half)

**Gated, opt-in** (default off). When enabled, metrics are recomputed over **only the `(product_id, store_id)` pairs present in EVERY qualifying year**, then compared. This isolates like-for-like movement from mix shifts caused by newly listed or closed pairs. Four independent kinds are available via `comparable_pairs.kinds`:

| Kind | Population | Chained links |
|---|---|---|
| `ytd` | Pairs present in every year of the run window, on each year's elapsed (fully-closed-months) window — with `report_end = "latest_day"`, on the **same fiscal day** of every year (days 1..K), over the years whose days 1..K are all in the window (`ctx.ytd_years`) | Every consecutive year pair |
| `yoy` | Pairs present in every year of the run window, on the **full window year** (not the YTD-elapsed subset) — with `report_end = "latest_day"`, **complete fiscal years** only | Every consecutive year pair (not just the latest two, unlike the regular non-comparable YoY) |
| `quarter` | Computed **independently per quarter number** — for quarter Q, only years where Q falls **entirely inside the report window** are considered (see below), and a pair must be present in Q of every one of those years to count | Every consecutive year pair *within that quarter's own year-set* |
| `half` | Same as `quarter`, **per half number** (H1 = fiscal quarters 1-2, H2 = 3-4): only years where that half falls entirely inside the report window count, and a pair must be present in that half of every one of those years. Needs `fiscal_calendar.half_periods = True`. | Every consecutive year pair *within that half's own year-set* |

There is no comparable QoQ/MoM/WoW — those don't exist as comparison kinds at all (see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)).

**`yoy`'s window-boundary years can be partial — this is intentional.** If `run_min_date`/`as_of_date` don't land on Jan 1 / Dec 31, the earliest/latest window years are partial, exactly like the existing non-comparable Annual/YoY tab. `yoy` mirrors that accepted behaviour rather than correcting for it. (Not under `report_end = "latest_day"`: the Annual frames only hold complete fiscal years there, so `yoy` only compares complete years, and `ytd` only the same-fiscal-day windows — see [Latest-day report end](#latest-day-report-end-report_end--latest_day).)

**`quarter` guards against comparing a partial quarter to a full one.** `REPORT_END_DATE` is a week boundary (last completed Saturday), never quarter-aligned, so whichever quarter is currently "in progress" for the latest window year is virtually always partial. A year only counts toward a given quarter number's population if that quarter's real calendar weeks (from the fiscal calendar, not just whichever weeks have data) fall **entirely** within `[EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]` — the same "fully elapsed" principle used by `fiscal.complete_fiscal_periods` generally, and specifically by `ytd`'s own elapsed-window check below (which uses this same helper, but at **month** grain, not quarter grain — see [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)). **The underlying completeness check had a live bug until this session:** it previously read week bounds from a frame already clipped to the report window, so its "is this period over" test was trivially true for any period present in the window at all. `quarter`'s check here (`comparable.py`'s `_complete_period_years`, shared with `half`) was already built correctly (unclipped) and served as the template for that fix. The `half` kind uses the same guard at half grain.

**The pair universe is pair-level under every `defined_scope.grain`, including `"product"`.** `scoped_daily` carries `store_id` straight from `noob/daily-data` whatever the scope grain (a product-grain scope restricts by product but leaves every store's rows intact), so like-for-like always means the *same product-store pairs* in every year — it never weakens to a product-only match just because the report's own scope is store-agnostic. (That's a separate question from `comparable_pairs.grain` below, which controls this population's own grain deliberately, by config, not as a side effect of `defined_scope.grain`.)

**`comparable_pairs.grain` — what counts as "the same pair," independent of `defined_scope.grain`:**

| `grain` | Store-side population |
|---|---|
| `"product_store"` (default) | Distinct `(product_id, store_id)` pairs present in every qualifying year — existing behaviour, unchanged. |
| `"product"` | Distinct `product_id` values present in every qualifying year; every store of a qualifying product is then kept. Store-estate churn is **not** isolated — a product that opened/closed at a store between compared years still moves the metrics. Use only when a pair-level intersection leaves too small a population to be meaningful. |

Only the store-side population (`scoped_daily`/`inst_data`/`lost_base`/`scope_pairs`/`scope_pair_weeks`) is affected — `dc_daily`/`dc_inst` always keep their own `(product_id, warehouse_id)` universe regardless of `grain` (DC/warehouse has no store dimension to drop).

```python
"comparable_pairs": {
    "enabled": True,     # default False
    "kinds": ["ytd", "yoy", "quarter", "half"],   # default ["ytd"] -- any subset of COMPARABLE_KINDS_ALL
    "grain": "product_store",   # default; "product" = same-pair population is product_id alone
    "pair_days": "unblocked",   # or "all": blocked days also make a pair present
},
```

**`pair_days`** (`"unblocked"` default | `"all"`): which days make a pair "present" in a year when the same-pair universe is built. `"unblocked"` counts only real daily-data rows outside blocked days (and DC rows outside DC blocked days); `"all"` counts blocked days too. Goods-in-transit-only days never count. Either way each metric then applies its own `blocked_scope.metrics` gate on the comparable pairs, exactly as on the other tabs. tbretail: `"unblocked"`.

Or set `KPI_COMPARABLE_PAIRS=true`. Each kind's save/recompute is gated independently by `comparable_pairs.kinds` — **not** by the regular `comparisons.enabled` selection, which is a separate, unrelated setting (previously comparable pairs required `"ytd"` in `comparisons.enabled`; that coupling is gone).

**How it works — one fixed universe per kind, shared by every link within it**

For `ytd`/`yoy`, the pair universe is computed **once**, as the intersection across **every** qualifying year — not per link. Concretely, with years 2024/2025/2026 all present, only pairs present in **2024 AND 2025 AND 2026** count. A pair present in 2025 and 2026 but *not* 2024 is excluded entirely, from every link — it does not count for the 2025-vs-2026 link either. For `quarter` (and `half`), this same rule applies **per quarter / half number independently**: Q1's universe only considers years where Q1 is complete, Q2's universe only considers years where Q2 is complete, and a pair common across years for Q1 says nothing about whether it's also common for Q2 (likewise H1 vs H2).

That single per-kind population is then reused for every consecutive-year link within it, so the **same year carries the same metric value in every link it participates in** — 2025's value as the "current" value in the 2024-vs-2025 link and as the "prior" value in the 2025-vs-2026 link are computed over the identical all-years-restricted population.

The universe is built from **real** daily-data / `inventory_warehouse` rows only: a goods-in-transit-only day ([`goods_in_transit`](#goods_in_transit)) or a blocked day ([`blocked_scope`](#blocked_scope), whatever `blocked_scope.metrics` says about the metrics) never makes a pair present in a year (their rows are still restricted to the common pairs afterwards).

All metric frames (sales/inventory, in-stock, lost sales) are restricted to this one all-years pair set before metrics are computed, for **Overall and every slice**. Because each slice dimension is a product attribute, the single overall intersection grouped by slice equals a per-slice intersection.

The restriction key is picked **per frame**, from the columns that frame actually has:

| Frame | Restricted on |
|---|---|
| `scoped_daily` | `comparable_pairs.grain`'s own key columns — `(product_id, store_id)` under `"product_store"` (default), `product_id` alone under `"product"`. Independent of `defined_scope.grain` — `scoped_daily` is store-level whatever the scope grain, so `comparable_pairs.grain` is the only thing that decides this. |
| `inst_data`, `lost_base`, `scope_pairs`, `scope_pair_weeks` | `(product_id, store_id)` when they carry a store dimension **and** `comparable_pairs.grain="product_store"`; otherwise the pair universe's **distinct products** |
| `dc_daily`, `dc_inst` | `(product_id, warehouse_id)` — not affected by `comparable_pairs.grain` (no store dimension to drop) — but **one independent same-pairs universe each**: `dc_daily`'s from `dc_daily`, `dc_inst`'s from `dc_inst` (see [`dc_instock`](#dc_instock)) |

In-stock and lost-sales frames carry `store_id` only when `lost_sales_source.store_col` is set — the scope join filters their rows but never attaches a store dimension the source lacked, whatever the scope grain. When that source is store-less, those frames are matched on the pair universe's distinct products — collapsed to distinct products first, so the join can't fan their rows out one-per-store — and their values stay product-level totals; only the product universe becomes like-for-like.

DC/warehouse inventory has no store dimension at all, so `dc_daily`/`dc_inst` can never share the store-side keys. Each gets its **own** `(product_id, warehouse_id)` all-years intersection, computed from itself — three same-pairs universes in total, each on its own terms rather than one forced onto the others.

`dc_daily` and `dc_inst` are both item-family-rolled to parent `product_id` (see [`dc_instock`](#dc_instock)'s note on `inventory_warehouse`) and both derive from `inventory_warehouse`, so neither grain nor id space nor source is a reason to keep their universes separate. `dc_inst` deliberately still does **not** reuse `dc_daily`'s universe, though: `dc_inst` 0-fills every day from a pair's first stocked day to the end of the report window, so a pair that stopped being stocked partway through still has rows — all of them stockouts — in the later years where `dc_daily` has none. Its per-year universe is a superset of `dc_daily`'s, and reusing `dc_daily`'s intersection would delete exactly the sustained stockouts `dc_in_stock_rate` exists to surface.

**Outputs**
- `comparable_kpi_long` — one shared table across every enabled kind, tagged with `comparison_type` (`"ytd"`/`"yoy"`/`"quarter"`/`"half"`), `comparable_pair_count` (that kind's shared universe size, same across every link within it), `link_prior_year`/`link_current_year` (which link a row belongs to — kept so incremental save's merge key doesn't collide across links, since the same year can appear in up to two links), and, for `quarter` / `half` rows only, a plain `quarter_number` / `half_number` column (not part of the row key — `period_type`/`period` already disambiguate every quarter or half/year combination, e.g. `2025-Q1`, `2025-H1`).
- `comparable_comparison_ytd` / `comparable_comparison_yoy` / `comparable_comparison_quarter` / `comparable_comparison_half` — one comparison table per enabled kind (same schema as the regular `comparison_ytd`/`comparison_yoy` tables; the quarter table additionally keys on `quarter_number`, the half table on `half_number`, so the save keys of different numbers never collide).
- HTML report — a **"Comparable (Like-for-Like)"** section, visually separated from the regular comparison table above it by a divider, on each kind's own tab: one consolidated wide value+delta table for `ytd` (YTD tab) and `yoy` (Annual tab); **one narrow block per quarter number** on the Quarter tab and **per half number** on the Half tab (mixing all 4 quarters into one table would need a value column per quarter-year and a delta column per quarter-link, and stops being readable). Each block has its own heading (`Q1 · Like-for-like`, `H1 · Like-for-like`) and is separated from the next by a rule and extra spacing.
- Notebook — a "Comparable pairs (like-for-like)" cell.

**Important**
- A comparable comparison for a given kind is produced only when it has at least 2 qualifying years (`quarter` / `half`: at least 2 years where that quarter / half number is fully elapsed). When the run window is narrow (single-week refresh), a kind will be skipped for that run — but any previously saved `comparable_kpi_long` history is preserved and each kind's comparison is recomputed from it on the next full-window run.
- `comparable_kpi_long` is merged incrementally across runs (same as `kpi_long`), and each enabled kind's `comparable_comparison_{kind}` is recomputed from the merged history — so a single-week refresh can still produce a comparable comparison relative to prior saved history. The merge key includes `link_prior_year`/`link_current_year` precisely so that two different links' rows for the same year never collide.
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

Inputs in config (`reporting_window`):

- `as_of_date` — end anchor
- `run_min_date` — optional start narrow (aligned to Sunday)
- `report_end` — `"as_of"` (default), `"complete_month"` or `"latest_day"` (see below; env `KPI_REPORT_END`)

Resolved automatically:

- **Start**: Sunday of Jan 1 (YTD) or Sunday of `run_min_date`
- **End**: **Last completed Saturday** on or before `as_of_date` (excludes the in-progress fiscal week) — with `report_end = "complete_month"`, cut back further, see below; with `report_end = "latest_day"`, `as_of_date` itself, see [Latest-day report end](#latest-day-report-end-report_end--latest_day)
- **Scope weeks**: any fiscal week whose Sun–Sat range **overlaps** the effective window is a window week; scope pairs are applied across all of them (and, under hybrid, covered vs missing weeks are split within this set)

**Input date ranges printed at read time**: to make it obvious what a run actually consumed (and catch stale/incomplete source data early), the two main time-series inputs always print the date span present in the source, independent of the usual verbose/quiet read logging:
- `daily_data` — min/max of its date column, printed once (the read is cached per run).
- `lost_sales` — min/max of `week_start_date`, printed once per source read (twice when `lost_sales_ensemble.enabled=True`: once for the fast/120-day source, once for the slow/365-day source).

These reflect what's actually in the source table (before the report-window filter is applied), so a max date short of the expected `REPORT_END_DATE` is a sign the source data is behind.

### Complete-month report cutoff (`report_end`)

`report_end = "as_of"` (default, generic `config.py`) keeps `REPORT_END_DATE` at the last completed Saturday. `report_end = "complete_month"` (`tbretail_config.py`) cuts `REPORT_END_DATE` back to the **last day of the most recent fully elapsed month** on or before that Saturday, so no metric or view shows a part-month: monthly, quarter, half, YTD (the complete months of every year), annual (the current year is year-to-date through that month), weekly, the regular comparisons and every comparable (LFL) kind.

The cut happens at run time, not in `materialize()`, because month ends live in the `fiscal_cal` upload: `fiscal.apply_report_end_mode` runs at the start of `KPIRunner.build_dimensions` (and of `run_html_only`) and overwrites `settings["REPORT_END_DATE"]`. Everything downstream already reads that setting, so the fiscal windows, the daily in-stock (`instock.method = "daily"`) read and store-day count, the blocked-days window and the HTML header all use the cut date. The cut is idempotent (running `build_dimensions` twice changes nothing).

- **With a fiscal calendar** (`use_fiscal_calendar=True`) the month is a fiscal month, read from the **unclipped** upload (`fiscal._fiscal_period_bounds`). The upload must extend **past** the as-of Saturday, otherwise the run raises (an upload ending earlier would make its own last date look like a month end), and `run_min_date` must reach the start of the cut month, otherwise it raises: `no month ends between ...` when none ends in the window, or `the cut month ... starts before the report start` when the cut month begins before `EFFECTIVE_REPORT_START_DATE` (the month's `period_start`). On the civil path the cut month starts on the 1st and is checked the same way. A fiscal month ends on a Saturday because fiscal months are whole weeks.
- **Without one** (`use_fiscal_calendar=False`) the month is a calendar month: the cut is the last day of the previous calendar month (or `REPORT_END_DATE` itself when that is a month end). That is usually not a Saturday, so the week containing the cut is clipped; `kpi_long._drop_partial_trailing_week` drops that partial trailing column from the Weekly tab. On this path months are bucketed by each week's start date (`Fiscal_Month` = month of `week_start_date`), so the result is a calendar-month **cut**, not exact calendar-month totals.

`OUTPUT_RUN_DATE` (the saved `run_date=` partition) still follows `as_of_date`.

**HTML filename.** `html_report.filename` is a template with the placeholders `{customer}` and `{report_end}` only (validated in `materialize()`; anything else raises). It stays a template in `HTML_REPORT_FILENAME_TEMPLATE`; `KPIRunner.build_html_report` formats it with the final (cut) `REPORT_END_DATE`. `HTML_REPORT_OUTPUT_DIR` is the optional datastore folder (`html_report.output_path_segments`); the file is written to `{HTML_REPORT_OUTPUT_DIR}/{filename}`.

**Incremental saves.** The weekly-refresh pattern of narrowing `run_min_date` no longer works with `complete_month`, because the window must reach the start of the cut month. With `save_mode = "incremental"` and `allow_overwrite_existing = False`, switching a client from `as_of` to `complete_month` keeps the `as_of` versions of the current annual row and of weekly rows after the cut in history; use `full_refresh` for that switch (tbretail uses `full_refresh`).

### Latest-day report end (`report_end = "latest_day"`)

`report_end = "latest_day"` (`tbretail_config.py`; generic `config.py` keeps `"as_of"`) makes **YTD run to the latest day** while every other view shows **complete periods only**. `REPORT_END_DATE` is `as_of_date` itself (not the last Saturday, not a month end): `materialize()` resolves it (`_resolve_report_window`), so everything that reads it — scopes, blocked days, the daily in-stock read, the daily and DC reads, the HTML filename — sees the same date, and `fiscal.apply_report_end_mode` leaves it unchanged. Set `as_of_date` to a day `noob/daily-data` has reached (the daily in-stock check still raises when daily-data's latest date is before `REPORT_END_DATE`); the `fiscal_cal` upload must extend past it. `KPI_REPORT_END=latest_day` works as for the other modes. The mode requires `instock.method = "daily"` (`materialize()` raises otherwise): the YTD cut splits a fiscal week of the daily in-stock frame, which a weekly in-stock source cannot do.

| View | What it shows |
|---|---|
| **YTD** | Days 1..**K** of every fiscal year, the **same fiscal day** for every year, where K = day of the fiscal year (1-based: days since that year's first date, plus 1) of `REPORT_END_DATE`. Fiscal calendar: each year's first date comes from the **unclipped** `fiscal_cal` upload; civil calendar: the day of the calendar year. Only years whose days 1..K lie inside the report window are shown (`ctx.ytd_years`; a year whose first date precedes `EFFECTIVE_REPORT_START_DATE` is left out rather than shown short), so `run_min_date` must reach the start of the first fiscal year you want in YTD. |
| **Annual** | **Complete** fiscal years only (real start `>= EFFECTIVE_REPORT_START_DATE` and real end `<= REPORT_END_DATE`): the current fiscal year appears in YTD, never here. YoY therefore compares the latest two complete years. |
| **Quarter / Half / Monthly** | Complete periods only, as before ([Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)). |
| **Weekly** | Whole weeks only: the trailing partial week is dropped (and does not take one of the `weekly_display_weeks` slots). |
| **Lost Sales %** | Lost sales only has data through the **last Saturday on or before `REPORT_END_DATE`**, so in every view only weeks ending on or before it count (`pipeline.build_pipeline_frames` filters `lost_base` on `week_end_date`), and **YTD uses weeks 1..(fiscal week of that Saturday) for every year** (whole weeks, the same for all years; no week at all when that Saturday falls in the previous fiscal year). The missing days after the last Saturday never dilute `lost_sales_pct` or misalign YoY YTD; numerator and denominator both come from `lost_base`. |

Example: fiscal years starting Sunday 2025-02-02 and 2026-02-01, `REPORT_END_DATE` 2026-08-02 (a Sunday) → K = 183. YTD 2026 is 2026-02-01..2026-08-02 and YTD 2025 is its days 1..183, 2025-02-02..2025-08-03. Lost sales ends 2026-08-01 (fiscal week 26), so YTD lost sales is weeks 1..26 of both years.

**How the YTD cut is built** (`fiscal.build_latest_day_windows`, run once per run from `build_fiscal_and_products`): `ctx.ytd_through_day` (K), `ctx.ytd_years`, `ctx.ytd_lost_sales_last_week` and `ctx.day_calendar` — the window's `(date, Year, Week)` plus `day_index` (day of the fiscal year) and `last_day_index`. Daily frames (`scoped_daily`, `dc_daily`) carry `day_index`, and YTD keeps `day_index <= K` of the YTD years (`kpi_long._ytd_latest_day_frames`). The pair-week in-stock frames cannot be cut by a date, so `build_instock_daily` and `build_dc_inst` **split the fiscal week that contains day K** (week 27 in the example, whose day 183 is its first day) into the days `<= K` and the days after, as two rows per pair with their own `stocked_pairs` / `available_days` (stocked, blocked and unusable days are counted per part, so each part is exact), and give every row a `last_day_index` (the fiscal-year day index of the last day the row covers). YTD keeps `last_day_index <= K`; every other view sums both parts, so nothing else changes and no other week is exploded. The `ytd` rows of `lost_base` keep `Week <= ctx.ytd_lost_sales_last_week`.

**WOS and the part week.** YTD ends on day K, so the fiscal week containing K enters the WOS family (`WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`) with only its days up to K. `scoped_daily` carries `week_days` (calendar days of the row's fiscal week in the window, `pipeline._with_week_days`; 7 outside this mode); the YTD frames replace it with the days up to K (`fiscal.week_day_counts`, `kpi_long._ytd_latest_day_frames`), and `metrics.compute_kpis` weights each product-week's average daily inventory by `week_days / 7`. Without that, the part week would add a full week of average inventory to the numerator and only its days of sales to the denominator (about +3% on a 26-week YTD whose last week has one day, much more early in the year). Whole weeks have `week_days = 7`, so every other view is unchanged.

**Incremental saves.** A YTD window moves with every `as_of_date`, so under `incremental` an existing `ytd` row of `kpi_long` / `comparable_kpi_long` is **always replaced** by the new run's (`io.merge_table_incremental`, whatever `allow_overwrite_existing` says); every other period type is a complete period and merges as usual. The comparison tables are recomputed from the merged history as before. After switching an existing deployment from `as_of` / `complete_month` to `latest_day`, use `full_refresh` once: rows of the old modes (e.g. the partial current-year Annual row) would otherwise stay in the merged history.

### Time grain completeness

After the calendar is built, `fiscal.require_complete_time_grain` checks that **every date** between `EFFECTIVE_REPORT_START_DATE` and `REPORT_END_DATE` is in it and raises `time grain (...) is missing N date(s) between ... : [first 20 dates]` otherwise. On the civil path (`use_fiscal_calendar=False`) the calendar is only the dates present in `noob/daily-data` (after `input_filters.daily_data`), so a gap there means missing source data; a missing date would otherwise drop out of every daily metric and shift week bounds. The same check covers a `fiscal_cal` upload with holes.

### Fiscal calendar vs native time grain

Controlled by `fiscal_calendar.use_fiscal_calendar`:

- **`True` (default)** — Year/Week/Quarter/Month come from the uploaded `one_time_uploads/fiscal_cal` table.
- **`False`** — the time grain is derived from `noob/daily-data` (`fiscal_calendar.daily_time_columns`, which has only `date`/`week` keys), read through the same config-filtered `daily_data` read every other consumer uses (so `input_filters.daily_data` applies here too). **Year is the CALENDAR year of `date`** (`F.year(date)`), and **Week is the native fiscal week column** (`daily_time_columns.week`). Quarter/Month are also derived from `date`.

  Year is deliberately never read from a raw source `year` column — it can carry the **ISO week-year**, which labels late-December weeks as the *following* year (e.g. a week starting Dec 29, 2025 shown as `year=2026`). Using it directly would mislabel December as `Q4 2026` instead of `Q4 2025`. Deriving Year from `date` avoids this.

  **Caveat**: a native fiscal week that straddles Jan 1 is split into two partial weeks in the Weekly view — one dated in the old calendar year, one in the new — instead of being a single week. Quarter, Month, and annual rollups are unaffected and remain correct.

### Half periods (H1 / H2)

`fiscal_calendar.half_periods` (default `False`; tbretail `True`; env `KPI_HALF_PERIODS`) adds a `half` period type next to `quarter`. `Fiscal_Half` is derived on the fiscal week frame from `Fiscal_Quarter` (quarters 1-2 -> H1, 3-4 -> H2) on both the `fiscal_cal` upload and the civil-calendar path, and rides along on every metric frame like `Fiscal_Quarter`. With it on:

- `kpi_long` gets `period_type = "half"` rows labelled `2025-H1`; only **complete** halves appear (same unclipped-calendar rule as quarters, see [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs)).
- The HTML report gets a **Half** tab (value trend, last `html_report.half_display_halves` halves, default 4).
- `comparable_pairs.kinds` may include `"half"` (rejected when `half_periods` is off): the same half of the prior year, with a pair universe built independently per half number.

There is no *regular* half comparison table, for the same reason there is none for quarters (see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)); half-over-half-of-last-year is the comparable `half` kind.

### Complete periods only (Quarter, Half & Monthly trend tabs)

**`REPORT_END_DATE` itself is unchanged by this guard** — still the last completed Saturday on or before `as_of_date`, or the complete-month cut when `report_end = "complete_month"` (see [Complete-month report cutoff](#complete-month-report-cutoff-report_end); that cut lands on a month boundary, never on a quarter or half boundary, so this guard still applies). This is a purely additive guard on top of it, not a redefinition of the reporting window.

Before this, the Quarter and Monthly `kpi_long` `period_type` rows (and, since `fiscal_calendar.half_periods`, the Half rows — same rule at half grain; the value-trend tabs — not YTD, which already had its own separate mechanism, see below) showed whatever real data existed up to `REPORT_END_DATE`, including an **in-progress trailing quarter/month** — e.g. a fiscal quarter only 1 week old plotted right next to full 13-week ones. Now a `(Year, Fiscal_Quarter)` / `(Year, Fiscal_Half)` / `(Year, Fiscal_Month)` only appears in `kpi_long` once it has **fully elapsed on both edges**: not truncated at the trailing (most recent) edge, and not truncated at the window's own start either — the latter matters when `run_min_date` doesn't land on a period boundary. This is computed once per run (`fiscal.complete_fiscal_periods`, cached per `Fiscal_Quarter`/`Fiscal_Half`/`Fiscal_Month` on `ctx.complete_fiscal_periods`) and semi-joined onto every metric frame before the Quarter/Half/Monthly rollup (`kpi_long._drop_incomplete_periods`).

With `report_end = "latest_day"` the same completeness set is also computed at **Year** and **Week** grain (`fiscal.LATEST_DAY_COMPLETE_PERIOD_COLUMNS`) and applied to the Annual and Weekly tabs, since `REPORT_END_DATE` is then any day (see [Latest-day report end](#latest-day-report-end-report_end--latest_day)); the YTD tab uses its own same-fiscal-day window there instead of the closed-month set below.

The Weekly tab needs no equivalent change while `REPORT_END_DATE` is a Saturday — the trailing week is always whole. The one exception is `report_end = "complete_month"` without a fiscal calendar, where the clipped partial week is dropped explicitly (see [Complete-month report cutoff](#complete-month-report-cutoff-report_end)). The `Fiscal_Half` completeness set is only computed when `half_periods` is on.

**A real pre-existing bug this surfaced and fixed — plus a follow-up grain fix.** The YTD tab's own "which periods count" mechanism (`fiscal._compute_available_fiscal_months` — sums only the fiscal periods fully closed for the latest year, applied to every year, so YTD stays apples-to-apples — see [Selecting which comparisons to run](#selecting-which-comparisons-to-run)) has been in production for a while. It previously read week bounds from `ctx.fiscal_week`, which is itself already clipped to `[EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]` — so its own completeness check was **trivially true for any period present in the window at all**. In practice, this meant the YTD tab's "fully elapsed" guard never actually excluded an in-progress period. It's fixed now — the check delegates to the same new, correctly-unclipped `complete_fiscal_periods` this section describes. (The comparable-pairs `quarter` kind's own equivalent check, `comparable.py`'s `_complete_period_years`, was already built correctly and served as the template for this fix — see [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half).)

**Separately, the elapsed-window grain itself was also switched from quarter to month** (`available_fiscal_quarters` → `available_fiscal_months`; `kpi_long._with_ytd_filter` now filters on `Fiscal_Month` membership instead of `Fiscal_Quarter`): quarter-grain understated YTD whenever the "current" quarter was in progress but had one or more of its own months already closed — which is virtually always true, since `REPORT_END_DATE` essentially never lands on a quarter boundary. Month-grain picks up those already-complete months immediately instead of waiting for the whole quarter to close, while keeping the identical apples-to-apples mechanism (the same set of period numbers is still summed for every year).

**Live impact — this changes what the already-deployed report displays, not just future runs.** Once this ships: a currently-visible partial trailing quarter/month row disappears from the Quarter/Monthly tabs (for a **freshly computed** run — see the incremental-save caveat below), and the YTD tab's own quarter selection may also shift, since it is now actually checking completeness for the first time rather than trivially passing. Whether a specific run's *output* changes depends on whether `reporting_window.as_of_date` happens to land exactly on a fiscal period boundary for that fiscal calendar — check `config.py`'s (or `tbretail_config.py`'s) `reporting_window.as_of_date` against the actual `fiscal_cal` upload if you need to know for a specific run; there's no way to determine that from config alone since fiscal period boundaries live in the uploaded calendar table, not in code.

**Incremental-save caveat.** This exclusion is applied when `kpi_long` is *computed*, not as a display-only trim — an incomplete period never enters `ctx.kpi_long` (or the Delta save) for that run. But under `save_mode="incremental"` with the default `allow_overwrite_existing=False`, a partial-period row saved by a **previous** run (before this fix) stays in the merged history: the new run simply doesn't reproduce that merge key, so there's nothing to append over it and nothing removes it. A fresh HTML report built directly from this run's in-memory `ctx.kpi_long` won't show the stale partial row, but the *saved* Delta table will still carry it until you re-run with `allow_overwrite_existing=True` or `full_refresh`.

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
| `kpi_long` | All metrics × periods × slices (annual / ytd / quarter / half / monthly / weekly) | `period_type`, `period`, `root`, `dimension`, `dimension_value` |
| `comparison_yoy` | YoY comparison rows | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `comparison_ytd` | YTD comparison rows (one row set per consecutive-year pair, elapsed-window sums) | same as YoY |
| `scope_diff` | Defined vs score annual diff (when `run_scope_diff=True`) | `Year`, `metric` |
| `comparable_kpi_long` | Comparable (like-for-like) per-link metrics for every enabled kind + `comparable_pair_count` + `comparison_type` (when `comparable_pairs.enabled=True`) | `comparison_type`, `period_type`, `period`, `root`, `dimension`, `dimension_value`, `link_prior_year`, `link_current_year` |
| `comparable_comparison_ytd` / `_yoy` / `_quarter` / `_half` | Comparable comparison rows for that kind (present only when `comparable_pairs.enabled=True` and the kind is in `comparable_pairs.kinds`) | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` (same shape as YoY/YTD; `_quarter` additionally keys on `quarter_number`, `_half` on `half_number` — usually recomputed from merged `comparable_kpi_long` and overwritten wholesale, see below) |

There is no `comparison_qoq`/`comparison_mom`/`comparison_wow` table — see [Selecting which comparisons to run](#selecting-which-comparisons-to-run).

Merge keys are defined in `kpi_pipeline/io.py` (`TABLE_ROW_KEYS`). Incremental mode uses these keys to decide append vs skip vs overwrite. `comparison_*` tables are recomputed from the merged `kpi_long` and overwritten wholesale; `comparable_kpi_long` is merged incrementally like `kpi_long` and each enabled kind's `comparable_comparison_{kind}` is then recomputed from the merged `comparable_kpi_long` (see [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half)).

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
- **Comparisons recomputed from merged history (incremental):** after merging `kpi_long`, the toolkit re-reads the merged partition and recomputes YoY/YTD from the **full saved history**, then overwrites the comparison tables in that partition. A single-week refresh can therefore still produce a YoY vs last year. The same applies to every comparable (like-for-like) table: `comparable_kpi_long` is merged incrementally and each enabled kind's `comparable_comparison_{kind}` is recomputed from it. `ctx.comparison_*` / `ctx.comparable_comparison_{ytd,yoy,quarter}` and the notebook/HTML displays reflect the merged history after save. Disable with `output.recompute_comparisons_from_history=False` (or `KPI_RECOMPUTE_COMPARISONS=false`) to keep comparisons scoped to the current run.
- **Notebook vs saved Delta on overlapping period values:** under `allow_overwrite_existing=False`, overlapping `kpi_long` keys keep the prior saved values (e.g. a stale partial-year annual total is not replaced by a narrower re-run). Enable overwrite or use `full_refresh` to replace them.
- **`report_end = "latest_day"`:** `ytd` rows of `kpi_long` / `comparable_kpi_long` are always replaced on an incremental merge (their window moves with every `as_of_date`), regardless of `allow_overwrite_existing`; the save plan counts them as `overwrite`. Switching an existing deployment to `latest_day` needs one `full_refresh` — see [Latest-day report end](#latest-day-report-end-report_end--latest_day).
- **Empty outputs skipped:** If a table is empty for this run (e.g. comparisons on a very narrow window), the write for that table is skipped and prior Delta data is left unchanged — even under `full_refresh`.

### Selecting which comparisons to run

`comparisons.enabled` chooses which period-over-period comparisons are **computed, printed, saved, and rendered**. Pick any subset of `"yoy"`, `"ytd"` — these are the only two comparison kinds:

```python
"comparisons": {
    "enabled": ["yoy"],          # only year-over-year; ytd is skipped entirely
},
```

- **`yoy`** — full calendar/fiscal year vs the prior full year (last two annual periods; with `report_end = "latest_day"` the latest two **complete** fiscal years).
- **`ytd`** — each year's **elapsed window** (only the fiscal months fully closed as of `as_of_date` for the latest year — see below; with `report_end = "latest_day"` the **same fiscal day** of every year instead, days 1..K, see [Latest-day report end](#latest-day-report-end-report_end--latest_day)) vs the prior year's same window, chained across consecutive years (e.g. `2026 YTD` vs `2025 YTD`, and `2025 YTD` vs `2024 YTD`). Use this instead of `yoy` once the current year is only partially reported — `yoy` would otherwise compare a partial current year against a full prior year, which reads as a much bigger drop/gain than what actually happened.
- **There is no separate QoQ/MoM/WoW comparison table.** The Quarter/Monthly/Weekly period tabs in the HTML report (and the corresponding `period_type` rows in `kpi_long`, always produced in full) show recent-quarter/month/week **value trends** — see the display-trim settings under [HTML report](#html-report) — without a delta/comparison table. If you need a quarter-over-quarter or month-over-month percentage change, compute it from consecutive `kpi_long` rows directly.
- Only the selected `comparisons.enabled` kinds produce `comparison_{kind}` Delta tables and HTML comparison columns. Unselected kinds are never computed or written. Comparable pairs is gated **independently** via `comparable_pairs.enabled` + `comparable_pairs.kinds` — not tied to this selection at all (see [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half)).
- `kpi_long` (the raw per-period metrics) is **always** produced in full, including a `"ytd"` `period_type` — this setting only gates the *comparison* tables, not the underlying period data.
- **YTD's elapsed window** (all modes except `latest_day`, which uses days 1..K instead — see [Latest-day report end](#latest-day-report-end-report_end--latest_day)): computed once per run from the **latest year present** — a fiscal **month** counts as "closed" only if every one of its weeks ends on or before `REPORT_END_DATE` (month-grain, not quarter-grain — see below). That same set of month numbers (e.g. just months 01–06) is then summed for **every** year, so the comparison stays apples-to-apples even mid-year. A single-month or single-year window degrades gracefully: YTD still sums whatever months exist, and the comparison itself is simply absent if only one year is present. **This "closed" check was genuinely broken until this session** — see [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs): it read week bounds from a frame already clipped to the report window, so the check trivially passed for any period present at all and never actually excluded an in-progress period. Now fixed to check against the real, unclipped fiscal calendar. **Also switched from quarter-grain to month-grain in a follow-up fix** — quarter-grain understated YTD whenever the "current" quarter was in progress but had one or more of its own months already closed (virtually always, since `REPORT_END_DATE` essentially never lands on a quarter boundary); month-grain picks those up immediately instead of waiting for the whole quarter to close.
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
    "backfill_leading_gap": True,   # product_store_week only
}
```

`grain` selects the scope universe (see [Scope modes](#scope-modes) above):

- `"product"` — distinct `product_id`; `store_col` is not required.
- `"product_store"` (default) — distinct `(product_id, store_id)`; **`store_col` is required**.
- `"product_store_week"` — the scope table's own `(product_id, store_id, week)` rows; **`store_col` is required**, and `date_col` (or `year_col`+`week_col`) is required to resolve weeks.

`date_col`/`year_col`/`week_col` are read **only for the `product_store_week` grain** (to resolve the scope table's own weeks) — for `product_store_week` this applies both to defined scope and, under hybrid, to finding covered vs missing weeks. For `"product"`/`"product_store"` they are ignored entirely.

**Not the same knob as `comparable_pairs.grain`.** This `grain` decides scope *membership* (which product/pair/week rows are in the report at all). [`comparable_pairs.grain`](#comparable-pairs-like-for-like-ytd--yoy--quarter--half) is a separate, independent setting that only matters when comparable pairs is enabled — it decides what "the same pair across years" means for that feature's own like-for-like population, regardless of which `defined_scope.grain` the rest of the report uses.

DATE path (preferred, when your scope table has a date column):

```python
"date_col": "week_start_date",
"year_col": None,
"week_col": None,
```

NATIVE path (set `date_col: None` and both `year_col`/`week_col`) — only valid when `fiscal_calendar.use_fiscal_calendar=False`. It takes `Year`/`Week` verbatim from your source, bypassing `fiscal_cal`/`fiscal_week` entirely, so `Year` must already be a genuine calendar year — see the ⚠️ warning under [Manual scope adjustments](#manual-scope-adjustments) for why an ISO week-year column here would silently mislabel late-December weeks. **If `use_fiscal_calendar=True`, `date_col` is required** — `materialize()`/`build_defined_scope` raises loudly rather than trusting a scope source whose own week numbering might not match `fiscal_cal`'s (the same date-vs-native gating `daily_data`/`lost_sales_source` already apply, generalized here since `defined_scope` is the only source where a date column is genuinely optional in config).

**`backfill_leading_gap`** (`product_store_week` only, default `True`): if the scope source's own earliest available week (across every pair) starts later than the report window's own start, that gap is a data-availability limit of the source itself — only the pairs tied to that earliest week are assumed in scope back to the window's own start, same "min date in window" principle [`dc_instock`](#dc_instock)'s per-pair grid uses, never a hardcoded floor date. A pair whose own first-seen week is later still (later than the source's earliest week, not merely later than the window start) is left untouched — that later start is a real signal (a new store, a new product), not a leading-gap artifact. Only the leading gap is filled; later starts, mid-window gaps, and end dates are honoured exactly as the source records them. `product`/`product_store` grains are unaffected — they already apply every scoped pair to the whole window. Set `False` for a deployment with **existing** `product_store_week` history saved before this option existed — turning it on for such a deployment would mix two scope definitions in one incrementally-merged table; a fresh `product_store_week` adoption is unaffected either way.

`materialize()` fails loudly if `grain` is invalid, if `store_col` is missing for `product_store`/`product_store_week`, or if `product_store_week` is missing both `date_col` and `year_col`/`week_col`. `build_defined_scope` additionally fails loudly if `product_store_week` + `use_fiscal_calendar=True` + `date_col=None`.

**Item-family rollup**: `defined_scope` also participates in `item_family_rollup` (see [`item_family`](#item_family) and [`inventory_warehouse`](#inventory_warehouse)'s note on the mechanism) via a 4th key, `item_family_rollup.defined_scope` (default `False`, applies to every grain). OFF by default because a scope source may already be rolled to parent `product_id` upstream (tbretail's is) — available as an opt-in safety net for a client whose isn't.

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
    "sales_filter": [],                   # narrows ONLY lost_sales_pct's sales denominator (see below)
}
```

**Behaviour.** Downstream code always sees canonical column names (`week_start_date`, `product_id`, `store_id`, `lost_sales`, `in_stock`, `total_days`) regardless of the mapping. `week_col`/`product_col`/`store_col` are renamed to their canonical join-key names at read time in `kpi_pipeline/inputs.py`; `lost_sales_col`/`in_stock_col`/`total_days_col` are read under their configured names and aliased to the canonical output names during aggregation in `kpi_pipeline/pipeline.py` (`_aggregate_lost_sales_pairweek`). Defaults reproduce tbretail's current schema exactly, so enabling this block with all defaults produces no behaviour change for existing customers.

**`product_col` / `product_agg_level_col` — configure exactly one, never both.** If the source has a native `product_id`-level column, set `product_col` to it. If it doesn't (keyed by planning/DFU level instead, e.g. `reporting_inv_fc_dfu/report_dfu`), set `product_col: None` and set `product_agg_level_col` to that column's name instead — the pipeline left-joins it to `path_segments.product_planning_level` (renaming `planning_level_id` to this column) to backfill `product_id`, mirroring kpi-skill-toolkit's own fallback. **Precedence:** if `product_col` is set AND actually present on the source, it always wins — `product_agg_level_col` is only ever consulted when `product_col` is `None`/absent-from-source, even if both happen to be configured at once, so leaving both set is misleading rather than additive. Setting `product_col: None` with no `product_agg_level_col` configured fails loudly at read time.

**`store_col: None`** (optional): for a source with no per-store dimension. The scope join **collapses to the source's own grain** — `store_id` is dropped from the join keys and the (deduplicated) scope is semi-joined on `(product_id, Year, Week)` — so one product-week keeps exactly one row. This matters because `lost_sales` is an absolute count, not a ratio: fanning that single value out to one row per scoped store would inflate any later sum across stores by the store-count factor. The resulting `lost_base`/`inst_data` frames carry no `store_id`, and `lost_sales_pct` is computed against the product-week sales total (summed across scoped stores) so numerator and denominator describe the same population.

Residual limitation, which no join can fix: the source's value covers the product's **entire** store footprint, which may be wider than a `product_store` scope that includes only some of its stores. A store-less source has no per-store detail to restrict. Prefer a genuinely per-store source when one exists — set `store_col` and you get a true per-store match end to end.

**`sales_filter`** (optional, list of Spark SQL expressions): narrows the **daily-data sales** that form the other half of `lost_sales_pct`'s denominator — `lost_sales / (sales + lost_sales)` — and nothing else. `total_sales_quantity`, `mean_stock`, `WOS`, `in_stock_rate` and every other metric keep the full population.

Set it when the lost-sales table covers a **narrower population than `daily_data`**, which otherwise puts the two halves of that ratio on different populations. The common case is a lost-sales model built to exclude e-commerce (e.g. tbretail's `model_id=top_down_excluding_ecom`, reached either directly or via `report_dfu`): the numerator carries no ecom lost sales, while `daily_data`'s sales still carry ecom revenue. The denominator is inflated relative to the numerator, so `lost_sales_pct` reads **low** — and the gap grows with the ecom share. Excluding the same stores here puts both halves back on one population:

```python
"lost_sales_source": {
    ...
    "sales_filter": ["store_id NOT IN (9001, 9002)"],   # the ecom fulfilment "stores"
},
```

Any column present on `daily_data` can be used, and expressions are ANDed (same convention as [`input_filters`](#input_filters)). Applied to `scoped_daily` in `build_pipeline_frames` before the weekly sales rollup, so it narrows the rows *inside* the scope rather than changing the scope itself. Empty (default) = no narrowing, byte-identical to previous behaviour.

**Lost sales through the last Saturday (`report_end = "latest_day"`).** The weekly lost-sales table only has data through the last complete Saturday, while `REPORT_END_DATE` can be any day. With `latest_day`, `lost_base` keeps only the weeks whose `week_end_date` is on or before the last Saturday on or before `REPORT_END_DATE` in **every** view, and YTD uses weeks 1..(fiscal week of that Saturday) for every year, so missing lost sales for the days after it never dilute `lost_sales_pct` — see [Latest-day report end](#latest-day-report-end-report_end--latest_day). The sales half of the denominator is real daily-data sales only (goods-in-transit-only days of [`goods_in_transit`](#goods_in_transit) carry none). With [`blocked_scope`](#blocked_scope), the blocked days leave that sales half only when `lost_sales_pct` is in `blocked_scope.metrics`; the weekly lost-sales numerator has no per-day grain and never changes.

**Why not the other two knobs.** `input_filters.daily_data` would fix the ratio but applies to **every** daily-data metric, so ecom would also vanish from `total_sales_quantity`/`mean_stock`/`WOS` — usually not what you want, since those legitimately include ecom. `metrics.population_filters` can't reach it at all: it is applied to `lost_base`, which is already at product-week grain with store sales summed in, so the ecom contribution is baked in before that filter ever runs (and it takes dimension/value specs, not SQL).

### `instock`

Where `in_stock_rate` (and the in-stock side of `weighted_instock_rate`) comes from. One section with a `method` and one sub-section per method; both sub-sections are in every config (all keys present, indexed directly), and only the one `method` names is used.

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
| `"daily"` | `noob/daily-data` over the scope pairs (store-level scope grain required): see below |
| `"weekly_source"` | a separate weekly table (`weekly_source`): see below |
| `"lost_sales_source"` | `lost_sales_source`'s own `in_stock_col` / `total_days_col`, on the same rows as lost sales (the generic default; a null `total_days` falls back to the fiscal week's day count) |

`materialize()` raises on: an unknown `method` (the allowed names are listed); `weekly_source.path_segments` empty under `"weekly_source"`; `"daily"` with `defined_scope.grain = "product"`; `"daily"` or `"weekly_source"` together with `lost_sales_ensemble.enabled` (the ensemble blends the in-stock columns the other methods do not produce, and uses the chosen model's `total_days` to decide whether a pair-week exists); `daily.count_start` outside its three values, or `scope_start` / `earliest` without `scope_source.mode = "operation_scope"`, or `first_daily_row` without `require_daily_data`; `daily.history_start` after the report window start; `reporting_window.report_end = "latest_day"` unless the method is `"daily"` (the YTD cut splits a fiscal week of the daily in-stock frame, which a weekly source cannot do); and [`goods_in_transit.store_instock`](#goods_in_transit) unless the method is `"daily"`. `daily.count_start` is validated whatever the method. The method is also `settings["INSTOCK_METHOD"]`; env `KPI_INSTOCK_METHOD` overrides it.

**`method = "daily"`** builds `in_stock_rate` from `noob/daily-data` instead of a weekly source. The report's Metric Details text for In-Stock Rate is generated from these settings.

`daily.count_start` `scope_start` / `earliest` need `scope_source.mode = "operation_scope"`; `first_daily_row` (the generic default) does not and requires `require_daily_data`.

Method (`pipeline.build_instock_daily`), per scope pair (needs a store-level scope grain):

1. Every scoped pair counts — the operation-scope pairs and the pairs `scope_adjustments` added alike (the client reference script leaves the added pairs out of in-stock; this pipeline deliberately does not, so every metric shares one scope). `daily.input_filters` (Spark SQL on `product_id` / `store_id` only) narrow the pair universe — this is how a store group leaves in-stock without touching other metrics.
2. Daily-data is read **without** `input_filters.daily_data` (that list usually contains `usable = 1`, which would hide the unusable days this method has to subtract), filtered on the raw date column from `history_start` to `REPORT_END_DATE` before `to_date` (so Delta file pruning applies), restricted to those pairs, blocked days removed when `in_stock_rate` is in [`blocked_scope.metrics`](#blocked_scope), then cached. It is not item-family-rolled here: `noob/daily-data` is already rolled to the family main upstream, the same id space as the rolled scope pairs. The run raises unless daily-data's latest date for those pairs reaches `REPORT_END_DATE`: days after it would count as out of stock.
3. Count start: `first_daily_row` (first daily row from `history_start`), `scope_start` (operation-scope start), or `earliest` of the two — clipped to the window start. With `require_daily_data` pairs without any daily row are dropped. Pairs added by `scope_adjustments` have no `scope_start` and count from their first daily row, whatever `count_start` is.
4. Store-days = every day from the count start to `REPORT_END_DATE` (a day without a daily row is out of stock) minus blocked days (when `in_stock_rate` is in `blocked_scope.metrics`) minus, with `usable_only`, days with `usable != 1`.
5. In-stock day = `inventory > 0` on a usable day; with `goods_in_transit.store_instock` also a day with store goods in transit (`operation/goods_in_transit`, `destination_type = 0`, `quantity > 0`, rolled to the family main). A snapshot dated D+1 describes the end of day D, so dates shift by `goods_in_transit.date_shift_days`. The days are united (OH OR GIT), never summed; blocked (when removed) and unusable days drop out of them too.

The output has the same shape as the weekly `inst_data` (`stocked_pairs` / `available_days` per pair-week plus product dims), so `metrics.compute_kpis`, population filters and comparable pairs work unchanged. Day counts per pair-week come from the fiscal week bounds, not from exploding every pair-day.

With `reporting_window.report_end = "latest_day"` the fiscal week that contains day K (the YTD cut) is counted as two rows per pair — its days up to K and the days after — and every row carries `last_day_index`, so YTD takes exactly days 1..K; see [Latest-day report end](#latest-day-report-end-report_end--latest_day).

**`method = "weekly_source"`** reads in-stock rate and total-days from a separate table (e.g. because your in-stock rate is calculated from a different data pipeline than your lost-sales model), read and scope-restricted independently of lost sales.

**Behaviour.** With `method = "lost_sales_source"`, `in_stock`/`total_days` are aggregated from `lost_sales_source`'s table, on the same rows as lost sales. With `"weekly_source"` (or `"daily"`), in-stock becomes fully independent of lost sales: `_aggregate_lost_sales_pairweek` stops aggregating `in_stock`/`total_days` from the lost-sales table (only `lost_sales` is aggregated from it), and `read_instock_weekly` reads and aggregates the `weekly_source` table on its own. `build_pipeline_frames` then semi-joins it to scope at its own grain — exactly like lost sales — so every in-scope pair-week the in-stock table has counts, whether or not lost sales has a row for it. The two only meet at the final per-period aggregate join, alongside the daily-data and DC metric families. A pair-week with a null/zero `total_days` in `weekly_source` is dropped, never padded to a full week. A blocked day cannot be removed from a weekly source (no per-day grain).

**`product_col` / `product_agg_level_col`**: same "configure exactly one, `product_col` wins if present" rule as `lost_sales_source` above.

**`store_col: None`** — for a source with no per-store dimension (e.g. `reporting_inv_fc_dfu/report_dfu`, aggregated to product × week only). The scope semi-join drops `store_id` from its keys, so the source is restricted to scope at product × week and never fanned out across stores. Its store grain is independent of `lost_sales_source`'s — either can be store-less without affecting the other. tbretail's `weekly_source` (kept configured for a switch back) is exactly the example above: `report_dfu` with its `TY_` columns, `product_col` and `store_col` `None`, and `product_agg_level_col` mapping through `path_segments.product_planning_level`.

`sim_instock_days`/`sim_total_days` on this table are the future-visibility *simulation's* projected values, not observed actuals — they're only blended into `TY_total_days_instock`/`TY_total_day` for weeks on or after the simulation's own run week (i.e. current/future weeks with no real history yet). Reading the raw `sim_` columns directly would report simulated numbers even for historical periods, so `in_stock_col` / `total_days_col` point at the `TY_` columns.

**`fallback_sources`** (optional): a list of additional column-sets, read from the SAME `path_segments` table, appended after the primary column-set to fill in weeks it doesn't have. Each entry needs its own `week_col`/`in_stock_col`/`total_days_col`; `product_col`/`store_col`/`product_agg_level_col` are inherited from the parent `weekly_source` block unless overridden. A fallback never overrides a `(product[, store], week)` the primary — or an earlier fallback — already covered; it only fills genuinely missing weeks (`read_instock_source`'s `left_anti` + `unionByName`). This is safe because `in_stock`/`total_days` is a ratio and both column-sets compute it the same way for any real week they both happen to cover.

`report_dfu`'s own `TY_` window only reaches back `path_segments.reporting_inv_fc_dfu`'s own trailing build horizon (tens of weeks, not years) — `LY_`/`LLY_` carry the identical formula for the calendar week exactly 52/104 weeks before each row's own `TY_` week, so they backfill older history `TY_` alone can't reach:

```python
"fallback_sources": [
    {"week_col": "LY_week_start_date", "in_stock_col": "LY_total_days_instock", "total_days_col": "LY_total_day"},
    {"week_col": "LLY_week_start_date", "in_stock_col": "LLY_total_days_instock", "total_days_col": "LLY_total_day"},
],
```

**`method = "lost_sales_source"`** needs nothing else: the in-stock columns are the ones mapped in [`lost_sales_source`](#lost_sales_source).

### `inventory_warehouse`

DC/warehouse daily inventory table, backing `dc_mean_stock`, `total_mean_stock`, `WOS_DC`, and `WOS_TOTAL`. Simpler than `lost_sales_source`/`instock.weekly_source` — no column mapping, since the source table's columns are already canonical (`product_id`, `warehouse_id`, `date`, `inventory`):

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

Read via `read_inventory_warehouse_source` (mirrors `read_daily_data_source`) and cached per run as `ctx.inventory_warehouse_raw`. Built into `dc_daily` (`pipeline.build_dc_daily`) restricted by left-semi join to the SAME in-scope `(product_id, Year, Week)` population as every other frame for that scope/root — not an independently-scoped universe, and week-aware: under `product_store_week` grain, a product's DC inventory is only kept for the weeks it's actually in scope, not every week it happens to appear in the report window. DC data has no store dimension, so there is no store-exclusion config for it; filter unwanted warehouse rows via `input_filters.inventory_warehouse` instead.

**Item-family rollup before restriction (unconditional, NOT gated by `dc_instock.enabled`).** `scope_core` (and therefore `defined_scope`) is already in **parent** id space — its producer maps `product_id -> coalesce(parent_id, product_id)` via `item_family`'s `is_main=false` rows before ever writing the scope table. `inventory_warehouse`'s own `product_id`, by contrast, is raw. `pipeline.build_dc_daily` now rolls it to parent id (`pipeline._roll_to_item_family_parent`, re-aggregating `F.sum("inventory")` at `(product_id, warehouse_id, date)` after the mapping) **before** restricting to `scope_core` — this fixed a real bug where DC inventory sitting on a superseded/child `product_id` was silently dropped by the old raw-id semi-join. Practically: `path_segments.item_family` must point at a real table **any time `path_segments.inventory_warehouse` is configured**, not just when `dc_instock.enabled=True` — and `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` will shift wherever a product family had inventory split across old and current item codes. See [`dc_instock`](#dc_instock) below for `item_family_source`'s column mapping.

### `dc_instock`

**DC product scope.** The store scope leads. Every DC metric (`dc_mean_stock`, `WOS_DC`, the DC part of `WOS_TOTAL` / `total_mean_stock`, `dc_in_stock_rate`) reads `inventory_warehouse` for the **store** scope's products (one `operation/scope` snapshot, applied to every week). With `scope_source.dc_solution_id` set (tbretail 22), only the DC (network) scope's product × warehouse pairs among them count: `scope.build_dc_scope` reads that solution's `operation/scope` pairs (rolled to the main, earliest start, active) into `ctx.dc_scope_pairs`, which also give the DC blocked days their start dates. With `None`, every warehouse counts.

**Gated** (`dc_instock.enabled`, default `False`) — but `item_family` (below) is **not** gated; see the `inventory_warehouse` note above. Backs `dc_in_stock_rate`. Unlike `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL`, which report only on days `inventory_warehouse` actually has a row for, this metric **expands** those rows into a continuous daily grid per `(product_id, warehouse_id)` pair and counts the gaps as stockouts.

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

**Goods in transit**: [`goods_in_transit.dc_instock`](#goods_in_transit) (`True` = a grid day with goods in transit to the DC — `destination_type = 1`, `quantity > 0`, `destination_id` = `warehouse_id`, rolled to the family main — also counts as stocked; a union, never a sum) with `goods_in_transit.date_shift_days` (tbretail `-1`: a snapshot dated D+1 describes the end of day D).

**DC blocked scope**: see [`blocked_scope`](#blocked_scope)'s `dc_solution_id`: DC blocked days leave `dc_in_stock_rate`'s stocked and available days only when `dc_in_stock_rate` is in `blocked_scope.metrics`; the other DC metrics follow their own entries there.

**`item_family_source`** (see [`item_family`](#item_family)) maps a parent/child product rollup table's columns to canonical `product_id`/`parent_id`/`is_main` (read via `read_item_family_source`, cached as `ctx.item_family_raw`). Rows with `is_main = false` are superseded/child items; `parent_id` is the current/main product_id they roll up onto. Used by **both** `build_dc_daily` (see the `inventory_warehouse` note above) and `build_dc_inst` below, via the shared `pipeline._roll_to_item_family_parent` helper — read unconditionally whenever `inventory_warehouse` is configured, not just when `dc_instock.enabled=True`.

**Where each pair's window starts.** Every pair's grid runs from **its own first `inventory_warehouse` row** to `REPORT_END_DATE`, not from a scope table's `start_date` and not from a flat window shared by every pair. `inventory_warehouse` carries a row whenever a pair holds stock, which makes that first row the same "first day this pair has any history" signal `daily_data_expanded` uses to bound the **store-level** in-stock denominator (`customer-analysis-tbretail`'s `05_future_visibility_data_prep.py`) — so both in-stock series are measured on one definition rather than two.

**Trade-off:** a pair that is ranged at a DC but was never once stocked inside the report window has no `inventory_warehouse` row anywhere, so it is **absent** from the metric rather than reading 0%. A pair that *stops* being stocked partway through is still covered — its grid continues to `REPORT_END_DATE`, and every later day counts as a stockout.

**How `dc_in_stock_rate` is built** (`pipeline.build_dc_inst`), in order:

1. Take rolled-up `inventory_warehouse` (`pipeline._get_inventory_warehouse_parent_rolled`, shared with `build_dc_daily` — no second Delta scan, and already filtered to the report window) and reduce it to one `first_stocked_date = MIN(date)` per `(product_id, warehouse_id)`.
2. `F.explode(F.sequence(first_stocked_date, REPORT_END_DATE))` to get that pair's own daily row space, then join the fiscal calendar for `Year`/`Week`. Because the inventory frame is window-filtered upstream, `first_stocked_date` can never precede `EFFECTIVE_REPORT_START_DATE` and needs no further clamping.
3. Restrict to the SAME in-scope `(product_id, Year, Week)` population every other frame for that scope/root is restricted to — the identical left-semi restriction `build_dc_daily` applies against `scope_core`. Done before the inventory join, so that join only runs over in-scope rows.
4. Left-join the same rolled-up `inventory_warehouse` back onto the grid, `F.coalesce(inventory, 0)` — a grid day with no matching inventory row is a genuine stockout day, not a missing one to drop.
5. With `scope_source.dc_solution_id`, flag the DC blocked days (`ctx.dc_blocked_days`) on the grid (`is_blocked`); with `goods_in_transit.dc_instock`, left-join the DC goods-in-transit days.
6. Aggregate to `(product_id, warehouse_id, Year, Week)`: `dc_stocked_days = COUNT(inventory > stock_threshold OR goods in transit)`, `dc_available_days = COUNT(*)`, both over the unblocked days only when `dc_in_stock_rate` is in `blocked_scope.metrics` (rows left with no available day are dropped), plus `dc_unblocked_days`, the unblocked days either way (the comparable-pairs universe reads it). Join `ctx.product_dims`/`ctx.fiscal_week` exactly as `dc_daily` does.

`dc_in_stock_rate = F.greatest(0.0, Σ(dc_stocked_days) ÷ Σ(dc_available_days))` — computed directly at the period grain, mirroring `in_stock_rate`'s own block (no sales-weighted rollup, unlike `weighted_instock_rate`/`WOS`).

**Disabled path.** When `dc_instock.enabled=False` (default), `build_dc_inst` skips the expansion entirely and returns a correctly-shaped but empty frame, so `dc_in_stock_rate` is always present in `kpi_long` as a literal-`null` column — the output shape stays constant whether or not the feature is turned on.

### `item_family`

Parent/child product roll-up: a superseded/child product (`is_main = false`) rolls up onto its current parent (`coalesce(parent_id, product_id)`, `pipeline._roll_to_item_family_parent`).

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
    "defined_scope": False,
},
```

`item_family_source` renames the table's columns to canonical `product_id` / `parent_id` / `is_main` at read time (`read_item_family_source`, cached as `ctx.item_family_raw`). `path_segments.item_family` must point at a real table whenever any rollup toggle is `True` or [`goods_in_transit.roll_to_family_main`](#goods_in_transit) is `True`; it is read unconditionally whenever `inventory_warehouse` is configured.

- `daily_data` (default `True`): `build_scoped_daily`'s join to the already-parent-rolled `scope_core` / products otherwise silently drops any daily-data row still carrying a child product_id. Turning it on can shift historical numbers for a product with a supersede history.
- `lost_sales` (default `False`): `report_dfu` already does its own supersede substitution upstream, so a second roll-up would likely be a no-op; an opt-in safety net.
- `inventory_warehouse` (default `True`): see the note under [`inventory_warehouse`](#inventory_warehouse).
- `defined_scope` (default `False`, every grain): a scope source may already be rolled to parent `product_id` upstream (tbretail's is); an opt-in safety net for a client whose is not.
- Goods in transit has its own toggle, `goods_in_transit.roll_to_family_main`.

### `scope_source`

Chooses the table that defines the scope universe. Default `"defined_scope"` keeps the behaviour above. `"operation_scope"` builds the universe from the platform scope table (`path_segments.scope`, `operation/scope`):

```python
"scope_source": {
    "mode": "operation_scope",   # "defined_scope" | "operation_scope"
    "solution_id": 21,           # int or list of ints (e.g. [21, 24]); also the blocked_scope solution(s)
    "dc_solution_id": None,      # DC (network) scope, int or list (tbretail 22): DC metrics' warehouse pairs among store-scope products; DC blocks
    "run_date": None,            # Sunday "YYYY-MM-DD"; None = latest Sunday on or before today
    "roll_to_family_main": True,
    "instock_main_eligible_only": False,  # True: in-stock only where the main itself is eligible
    "instock_exclude_unsuperseded_sizes": False,  # True: in-stock leaves out sizes not in a supersession
    "active_only": True,
},
```

**`instock_main_eligible_only`** (tbretail `True`, generic `False`): in-stock (and weighted in-stock, which uses the same frame) counts only the stores where the main item itself is eligible — a store where only a superseded (sub) item is eligible was intentionally not assorted the new item, so it leaves in-stock (client rule). Those stores stay in every other metric (sales, revenue, inventory, WOS, turnover, lost sales), so all volume and inventory is captured. The pair's start is still the earliest start of the main and sub rows there. Built from `ctx.operation_scope_pairs.main_eligible` (`scope._scope_start_pairs`) and applied in `pipeline.build_instock_daily`; scope additions are not operation-scope pairs and are kept. Requires `mode = "operation_scope"`, `roll_to_family_main = True` and `instock.method = "daily"`.

**`instock_exclude_unsuperseded_sizes`** (tbretail `True`, generic `False`): in-stock leaves out the sizes "not created in the supersession" — a product in no `item_family` row whose class color (`products.option_code`) has at least one size in `item_family` (as main or sub). The client treats these like NGF: out of in-stock, still in sales, revenue, inventory, WOS, turnover and lost sales (`pipeline._unsuperseded_sizes`, applied in `build_instock_daily`). Requires `instock.method = "daily"`.

Rows of `solution_id` for that `run_date` that are still open (`end_date` null or `>= run_date`) are reduced to `(product_id, store_id)` pairs; with `roll_to_family_main`, every row is rolled to its family main (`coalesce(parent_id, product_id)`; a product without a main keeps its own id), so a store where only a sub-item is in scope gets the main, and each pair keeps its **earliest** `start_date` as `scope_start` (kept on `ctx.operation_scope_pairs`; `scope._scope_start_pairs`). `active_only` keeps products with `is_active = true`. The scope is built once and every metric uses it. `scope_adjustments`, `input_filters.daily_data` and the population filters apply on top as before; `input_filters.defined_scope` does **not** (the `defined_scope` table is not read in this mode, so tbretail's `week_start_date < '2026-08-02'` entry is unused). Needs `defined_scope.grain` of `product` or `product_store` (`product_store_week` is rejected; the in-stock and blocked days need `product_store`).

`run_date` defaults to the latest Sunday on or before **today** (`scope._scope_run_date`), not the report's as-of date, so a backdated run or a `KPI_AS_OF_DATE` override still reads today's scope: set `run_date` for a reproducible backfill.

**Scope additions are not operation-scope pairs.** A `scope_adjustments` addition skips `roll_to_family_main` / `active_only`, has no `scope_start` and receives no blocks; a product-only addition (`store_col = None`) becomes every store with a daily-data row in the window at the `product_store` grain. That widens every metric: additions are part of the one scope all metrics use, in-stock included — `instock.method = "daily"` counts an addition from its first daily row, with no family roll-up, active or blocked-scope filter (tbretail's JAB, NGF products and `nvrout_scope_backfill` additions count everywhere). Block product_ids are **not** rolled: only blocks on the main's own `product_id` apply, as in the client reference script.

### `blocked_scope`

UI-blocked days, applied per metric. Default off: it is on exactly when `ui_parameters_path` is set.

```python
"blocked_scope": {
    "ui_parameters_path": "ui-data/parameter_config/<timestamp>_<id>",  # under the datastore root; None = off
    "rule": "after_scope_start",   # or "all"
    "kinds": ["product", "product_destination", "destination"],  # store block folders read
    "dc_kinds": ["product", "product_destination"],  # DC block folders read
    "metrics": "all",              # "all" (every metric of METRICS_ALL) or a list of metric names
},
```

Reads the UI snapshot `{ui_parameters_path}/blocked_scope/{product,product_destination,destination}` (parquet; `destination_id` is the store) for `scope_source.solution_id` and matches the blocks to the operation-scope pairs (so it requires `scope_source.mode = "operation_scope"`). With `rule = "after_scope_start"` a block applies to a pair only when `block.start_date >= scope_start` (same day: the block applies); an earlier block is ignored because the pair was set up again after it. `"all"` applies every matched block. An applied block covers the pair's days from its `start_date` to its `end_date` (null = open-ended), clipped to the report window. The result is one cached `(product_id, store_id, date)` frame (`ctx.blocked_days`, built in `scope.build_blocked_days`). Block `product_id`s are not rolled to the family main: only blocks on the main's own `product_id` apply (`scope._applied_block_days`, shared with the DC blocks). Always set `ui_parameters_path` explicitly (the newest snapshot folder may hold no blocks for the solution); a missing `blocked_scope/<kind>` folder fails the run. Pairs added by `scope_adjustments` are not operation-scope pairs and are never blocked.

`dc_solution_id` (None = no DC blocks, int not bool, e.g. 22) does the same for DC blocks: the days of `{ui_parameters_path}/dc_blocked_scope/{product,product_destination}` blocks of that solution (`destination_id` = warehouse), by `rule`. Each DC pair's `scope_start` comes from `operation/scope` of that solution (same `run_date`, family roll-up, earliest start and `active_only` as `scope_source`, location = warehouse); DC pairs outside that scope receive no blocks. Built once per run as `ctx.dc_blocked_days` (`scope.build_dc_blocked_days`). Requires `ui_parameters_path`; only the folders in `blocked_scope.kinds` / `dc_kinds` are read, and a missing listed folder fails the run (tbretail's `dc_blocked_scope` has no `destination` folder, so `dc_kinds` leaves it out).

**`metrics` — which metrics drop blocked days.** Blocked days stay in the frames, flagged (`is_blocked` on `scoped_daily` and `dc_daily`, from one left join of the blocked days in `pipeline.build_scoped_daily` / `build_dc_daily`). Each metric then reads **only the unblocked rows when it is named in `metrics`**, and **blocked and unblocked rows alike when it is not**. `metrics` is `"all"` (every name of `METRICS_ALL`) or a list of those names; an unknown name raises (the allowed names are listed), and `settings["BLOCKED_SCOPE"]["metrics"]` holds the resolved explicit list in `METRICS_ALL` order. `"all"` (the generic default) reproduces exactly what turning blocked scope on did before this key existed. tbretail names the inventory, WOS, in-stock and DC metrics and leaves sales units, sales revenue, AUR, AUC, the distinct counts and `lost_sales_pct` out: a blocked pair can still sell its existing stock, and the report never removes blocked days from history for those metrics.

| Metric(s) | What the gate does |
| --- | --- |
| `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `distinct_product_count`, `distinct_store_count`, `distinct_pair_count` | each metric its own gate on the real daily rows (`AUR` / `AUC`: numerator and sales units both) |
| `total_inventory`, `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate` | each metric its own gate, on its inventory and, for the WOS metrics and the turnover, on the sales it divides by; with [`goods_in_transit`](#goods_in_transit) a blocked day drops its GIT too |
| `total_mean_stock`, `WOS_TOTAL` | the metric's own gate, on both its store part (store blocked days) and its DC part (DC blocked days) |
| `dc_mean_stock`, `WOS_DC` | the metric's own gate on DC blocked days (`WOS_DC`'s store sales denominator: store blocked days, same gate) |
| `in_stock_rate` | the daily in-stock frame (`pipeline.build_instock_daily`) leaves out blocked store-days, on-hand days and GIT days only when named |
| `weighted_instock_rate` | uses that same in-stock frame (so `in_stock_rate`'s gate decides its in-stock side); its sales weights drop blocked days when named |
| `dc_in_stock_rate` | `pipeline.build_dc_inst` leaves DC blocked days out of the stocked and available days only when named |
| `lost_sales_pct` | the daily-data sales in its denominator (`build_pipeline_frames`' `daily_for_lost`) drop blocked days only when named; the weekly lost-sales numerator has no per-day grain and never changes |

`metrics.compute_kpis` keeps one aggregation pass per frame and family: conditional aggregation (`F.when(~is_blocked, x)` inside the sums, averages and distinct counts, and per-day `<metric>_day` / `<metric>_has` columns so a day with only blocked rows is not a day of a gated average). When no sales metric reads blocked rows (all named), they are left out of the sales rows too, so a period × slice with blocked days only gets no output row, as before. The comparable-pairs pair and year universe stays the real, unblocked rows whatever the gate says, and `scope_diff` and the scope debug keep their meaning. The Metric Details text of a metric states the blocked-days exclusion only when the metric is named. Weekly sources with no per-store/day grain — `lost_sales_source` such as `report_dfu`, or `instock.method = "weekly_source"` — cannot be filtered per day and are left as they are.

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

All five keys are required (indexed directly). Changing the old per-feature settings into this one section is listed in the CHANGELOG.

- **`date_shift_days`**: a snapshot dated D+1 describes the end of day D, so tbretail uses `-1` (for D = `REPORT_END_DATE` that needs the snapshot dated the next day: if it is not there yet, the last day has no goods in transit). `None` turns GIT off everywhere; `store_instock` / `dc_instock` must then be `False` and `inventory_metrics` empty.
- **`roll_to_family_main`** (default `True`): rolls every `operation/goods_in_transit` read to the family main before it is used — store in-stock, DC in-stock and the inventory metrics, all through `pipeline._goods_in_transit_quantity`. Set `False` only for a GIT source that is already rolled upstream.
- **`store_instock`**: a store-day of the [daily in-stock](#instock) also counts as in stock when store GIT > 0 (`destination_type = 0`, `quantity > 0`), united with the on-hand days, never summed. Needs `instock.method = "daily"`.
- **`dc_instock`**: a DC grid day of [`dc_instock`](#dc_instock) also counts as stocked when GIT to the DC (`destination_type = 1`, `quantity > 0`, `destination_id` = `warehouse_id`) — a union, never a sum. Independent of `dc_instock.enabled` (it applies once that is on).
- **`inventory_metrics`**: any subset of `INVENTORY_GIT_METRICS_ALL` (top of `config.py`), each on its own: `total_inventory`, `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `dc_mean_stock`, `total_mean_stock`, `WOS`, `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`, `inventory_turnover_rate`. A metric you do not name keeps on-hand only and its exact previous value. Store metrics count store GIT, `dc_mean_stock` / `WOS_DC` DC GIT, and `total_mean_stock` / `WOS_TOTAL` both their store and their DC GIT; `inventory_turnover_rate` counts it in its mean stock (its sales units are unchanged). The report metrics themselves are chosen in `metrics.metric_cols`, from `METRICS_ALL` (every metric the pipeline can report). Needs `fiscal_calendar.use_fiscal_calendar = True` when not empty.

`materialize()` raises on: a non-int (or bool) `date_shift_days`; `date_shift_days = None` with `store_instock` / `dc_instock` `True` or a non-empty `inventory_metrics`; `store_instock` without `instock.method = "daily"`; an unknown name in `inventory_metrics` (the allowed names are listed); and a non-empty `inventory_metrics` without `fiscal_calendar.use_fiscal_calendar = True`. The five keys are `settings["GOODS_IN_TRANSIT"]` (`inventory_metrics` in canonical order).

A named inventory metric uses **on-hand + GIT units** on every day; retail = units × `price_without_tax` and cost = units × `cogs`, rounded to 2 decimals like `inventory_retail` / `inventory_cost`. A metric not named uses on-hand only on the days with a real daily-data (or `inventory_warehouse`) row, exactly as before (`metrics._day_stock` / `_day_avg`: a GIT-only row has on-hand 0, so only the set of days differs). A day with GIT but no daily row counts as a day with zero on-hand in a named average (`mean_stock`, `WOS`'s average daily inventory).

**Store side** (`pipeline.build_scoped_daily`, only when a metric that reads store GIT — `context.STORE_GIT_METRICS` — is named; otherwise there is no GIT read at all and every row has `has_daily_row = True`, `git_quantity = 0`). The order is: daily data (window-filtered, `input_filters.daily_data` applied, restricted to the scoped pairs when the scope has a store dimension) → store GIT quantity → join both → blocked-day flag → scope product-week semi-join → calendar and product attributes. The scoped-pair restriction runs first only to keep the GIT join to scoped pairs; it commutes with the blocked-day flag, so the result is the same.

1. Store GIT quantity per `(product_id, store_id, date)`: `goods_in_transit` with `destination_type = 0` and `quantity > 0`, the snapshot date shifted by `date_shift_days`, **summed** and rolled to the **family main** (`_goods_in_transit_quantity` — the same helper behind the in-stock goods-in-transit days, which derive from it unchanged), limited to the report window and to the scoped pairs (at `product` scope grain the scope's product-weeks restrict it later instead).
2. Full outer join to the daily rows on `(product_id, store_id, date)` (the daily rows are summed to one row per pair-day first, so a quantity attaches to a pair-day once). A GIT-only day gets `sales_quantity` / `sales_revenue` / `inventory` = 0 and `has_daily_row = False`; `git_quantity` is 0 where there is none.
3. GIT-only days whose daily-data row was **removed by `input_filters.daily_data`** (tbretail: `usable = 1`) are dropped as well (`inputs.get_daily_data_excluded_days` reads the raw table once per run for the days the filter removes, with the same family roll-up, caches it on the context, and every scope variant left-joins it onto its GIT days): an unusable day must not come back as a zero-sales inventory day.
4. Blocked scope (`ctx.blocked_days`) then flags both kinds of rows, so a metric in [`blocked_scope.metrics`](#blocked_scope) drops a blocked day's daily row and its GIT alike.

**Computed separately, joined as before** (`metrics.compute_kpis`): sales (`total_sales_*`, `AUR`, `AUC`, distinct counts) and `weighted_instock_rate`'s sales weights read real rows only; WOS (units / revenue / cost), mean stock, total mean stock, turnover and the DC metrics each build their own frame, each metric reading on-hand + GIT only when it is named, and are joined on the period keys exactly as before. `metrics.population_filters` apply per group as before (GIT rows carry the product dimensions, so a filter such as in-stock's NON-COMP exclusion is unaffected).

**DC side** (`pipeline.build_dc_daily`, only when a metric that reads DC GIT — `dc_mean_stock`, `total_mean_stock`, `WOS_DC`, `WOS_TOTAL` — is named): DC GIT (`destination_type = 1`, `quantity > 0`, same shift, summed per `(product_id, warehouse_id, date)`, family main, report window) is full-outer-joined to the rolled `inventory_warehouse` rows; a GIT-only day has inventory 0 and `has_inventory_row = False`. `Year`/`Week` are then attached and the rows restricted to `scope_core`'s product-weeks exactly as before. DC blocked days (`scope_source.dc_solution_id`) then flag DC rows of both kinds, as blocked days do on the store side.

**Never changed by `inventory_metrics`:** sales, the daily in-stock (it has its own `store_instock` union of days), lost sales (its sales denominator uses real rows only) and the DC in-stock rate (its own `dc_instock`). The comparable-pairs pair universe is built from real rows only ([Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half)). The Metric Details text of a named metric states that it counts store / DC goods in transit.

### `output`

See [Output saves](#output-saves) for full mode behaviour, merge keys, workflows, and caveats.

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

See [HTML report](#html-report) section.

## Environment variable overrides


| Variable                          | Effect                                                       |
| --------------------------------- | ------------------------------------------------------------ |
| `KPI_BUCKET`                      | Datastore mount (default `/mnt/invent-{customer}-datastore`) |
| `KPI_CUSTOMER`                    | Customer slug                                                |
| `KPI_AS_OF_DATE`                  | Overrides `as_of_date`                                       |
| `KPI_RUN_MIN_DATE`                | Overrides `run_min_date`                                     |
| `KPI_REPORT_END`                  | Overrides `reporting_window.report_end` (`as_of` / `complete_month` / `latest_day`) |
| `KPI_HALF_PERIODS`                | `true`/`false` — overrides `fiscal_calendar.half_periods`    |
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
| `KPI_INSTOCK_METHOD`              | `daily` / `weekly_source` / `lost_sales_source` — overrides `instock.method` |
| `KPI_INSTOCK_SOURCE_PATH`         | Comma-separated path segments for `instock.weekly_source.path_segments` |
| `KPI_INSTOCK_WEEK_COL`            | Overrides `instock.weekly_source.week_col` (default `week_start_date`) |
| `KPI_INSTOCK_PRODUCT_COL`         | Overrides `instock.weekly_source.product_col` (default `product_id`) |
| `KPI_INSTOCK_STORE_COL`           | Overrides `instock.weekly_source.store_col` (default `store_id`) |
| `KPI_INSTOCK_IN_STOCK_COL`        | Overrides `instock.weekly_source.in_stock_col` (default `in_stock`) |
| `KPI_INSTOCK_TOTAL_DAYS_COL`      | Overrides `instock.weekly_source.total_days_col` (default `total_days`) |
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
| `KPI_HTML_HALF_HALVES`            | Recent halves in Half tab (default 4; empty = all)           |
| `KPI_HTML_YEARLY_YEARS`           | Recent years in Annual tab (default 5; empty = all)          |
| `KPI_RUN_MODE`                    | `full` or `html_only` — skip pipeline and render HTML from saved outputs |


## Metrics

Default metrics (configurable in `CONFIG["metrics"]`):

- Sales / inventory (all scoped stores): `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `total_inventory`
- Coverage (all scoped stores): `distinct_product_count`, `distinct_store_count`, `distinct_pair_count`
- Stock/service (all scoped stores): `mean_stock`, `mean_stock_retail`, `mean_stock_cost`, `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate`, `in_stock_rate`, `weighted_instock_rate`, `lost_sales_pct`
- DC/warehouse + combined inventory (requires `path_segments.inventory_warehouse`, see [`inventory_warehouse`](#inventory_warehouse) below): `dc_mean_stock`, `total_mean_stock`, `WOS_DC`, `WOS_TOTAL`
- Goods in transit on the inventory metrics (gated, `goods_in_transit.inventory_metrics`, see [`goods_in_transit`](#goods_in_transit)): `total_inventory`, `mean_stock` (+ retail / cost), `WOS` / `wos_revenue` / `wos_cost`, `inventory_turnover_rate`, `dc_mean_stock`, `WOS_DC`, `total_mean_stock` and `WOS_TOTAL` can each count goods in transit on top of on-hand
- DC in-stock (gated, `dc_instock.enabled`, requires `path_segments.item_family`, see [`dc_instock`](#dc_instock) below): `dc_in_stock_rate`

Every metric uses all scoped stores — there is no global store-exclusion config key; the exceptions are per metric and explicit: with `instock.method = "daily"`, `instock.daily.input_filters` leaves stores out of in-stock, `lost_sales_source.sales_filter` leaves them out of `lost_sales_pct`, and `metrics.population_filters` narrows a metric's product population. tbretail uses exactly these: **NON-COMP** (`IS_COMP == "no"`) is removed from in-stock only (`population_filters.in_stock_rate`), **ECOM** stores 829 / 639 / 917 from in-stock and lost sales only (`instock.daily.input_filters`, `lost_sales_source.sales_filter` and the ECOM-excluding model behind `report_dfu`); every other metric keeps NON-COMP and ECOM, on every root tab (Overall, NVROUT, LFL), every period type, the comparisons, the comparable (LFL) tables and the HTML — all of them are built from the same `compute_kpis` call, so there is no path around these rules. The LFL (`comp`) root is `IS_COMP == "yes"` by definition, so NON-COMP is absent from every metric on that tab. If a store should never contribute to anything (e.g. an e-com fulfillment "store"), filter it out via `input_filters.daily_data` (and `instock.daily.input_filters` with `instock.method = "daily"`, whose read skips `input_filters.daily_data`), or rely on your `lost_sales_source`/`instock.weekly_source` tables already excluding it upstream. To exclude it from **`lost_sales_pct` only** — because the lost-sales table itself excludes it and the ratio would otherwise mix populations — use [`lost_sales_source.sales_filter`](#lost_sales_source) instead (see [Config reference](#config-reference)).

**One population per source.** Every metric read from `noob/daily-data` — sales, `total_inventory`, `mean_stock`, `WOS`, turnover, and weighted-instock's sales weights — is computed from the **same** rows, under every `defined_scope.grain` (with [`goods_in_transit`](#goods_in_transit) on, the metrics it gates also read the goods-in-transit-only days of those same pairs; everything else still reads the real rows only; with [`blocked_scope`](#blocked_scope) on, each metric reads or drops the blocked days of those same rows per `blocked_scope.metrics`, see [Inventory metrics](#inventory-metrics-blocked-days-and-goods-in-transit)). Apart from `blocked_scope.metrics` (a metric not in the list also reads the blocked days) and the explicit exceptions below, two metrics in the same output row never describe different populations. Lost sales and in-stock come from their own source (`lost_sales_source` / `instock.weekly_source`, or the daily in-stock frame) and are restricted separately, at that source's own grain; DC inventory likewise comes from `inventory_warehouse`, item-family-rolled onto parent `product_id` (same id space as `scope_core` — see [`dc_instock`](#dc_instock)) before matching. `dc_in_stock_rate` reads that same frame but expands it into a per-pair daily grid rather than consuming its rows directly — see [`dc_instock`](#dc_instock). There are exactly two deliberate exceptions: per-metric `population_filters`, and `lost_sales_source.sales_filter` — which narrows the sales half of `lost_sales_pct`'s denominator so it matches a lost-sales source covering a narrower population than `daily_data` (see [`lost_sales_source`](#lost_sales_source)). Both apply only where you explicitly configure them.

**Lost Sales %** = `100 × sum(lost_sales) / sum(floor(weekly_sales + lost_sales))` — denominator includes imputed lost demand. `weekly_sales` comes from `daily_data` while `lost_sales` comes from `lost_sales_source`, so if that source covers a narrower population (an ecom-excluding model, say) the two halves describe different populations and the ratio reads low — see [`lost_sales_source.sales_filter`](#lost_sales_source).

**In-Stock Rate** = `sum(in_stock_days) / sum(available_days)`, from the [`instock`](#instock) method's source: the daily-data store-days (`daily`), a separate weekly table (`weekly_source`) or the top-down lost-sales output (`lost_sales_source`).

**Weighted In-Stock Rate** = sales-weighted average of weekly in-stock rates: each fiscal week's in-stock rate is weighted by that week's sales volume when rolling up to the reporting period. Weeks with higher sales carry more weight. Reported as pp-change in comparisons.

**WOS** = per-product per-fiscal-week WOS after summing daily inventory/sales across all scoped stores at product×date (`avg_daily_inventory / weekly_sales`), then rolled up to the reporting period using a sales-weighted average. Not computed at product×store×week grain. Each product-week's average daily inventory is weighted by `week_days / 7`, where `week_days` is the calendar days of that fiscal week in the view: 1 for a whole week, so `WOS = Σ(avg_daily_inventory × week_days/7) ÷ Σ(weekly_sales)` is unchanged everywhere except the one week `report_end = "latest_day"` cuts mid-week (YTD's last week, which stops at day K; see [Latest-day report end](#latest-day-report-end-report_end--latest_day)), which counts as the fraction of a week of inventory its days cover against the sales of the same days. The same weighting applies to `wos_revenue`, `wos_cost`, `WOS_DC` and `WOS_TOTAL`. Inventory is on-hand, or on-hand + goods in transit for the metrics named in [`goods_in_transit.inventory_metrics`](#goods_in_transit) (each name switches its own metric; sales, in-stock and lost sales never include it), and each WOS metric drops blocked days from its inventory and its sales when it is named in [`blocked_scope.metrics`](#blocked_scope).

**WOS (DC)** and **WOS (Total)** follow the identical product×fiscal-week grain and sales-weighted rollup as WOS — they use the SAME `daily_data_week` frame (each metric with its own gate columns), left-joined with a DC-inventory frame built at the same grain (weeks with no DC record fill to 0, not dropped). `WOS_DC = avg_daily_dc_inventory / weekly_sales`; `WOS_TOTAL = (avg_daily_total_inventory + avg_daily_dc_inventory) / weekly_sales`. DC inventory is restricted to the same in-scope product-week population as every other metric in the report (see [`inventory_warehouse`](#inventory_warehouse)).

**DC In-Stock Rate** = `F.greatest(0.0, sum(dc_stocked_days) / sum(dc_available_days))`, at DC/warehouse level (gated by `dc_instock.enabled`, see [`dc_instock`](#dc_instock)). Unlike every other DC metric above, **the denominator is an expanded grid, not a row count**: each `(product_id, warehouse_id)` pair's daily row space runs from its own first `inventory_warehouse` row through `REPORT_END_DATE`, with every gap 0-filled and counted as a stockout day — where `dc_mean_stock`/`WOS_DC` simply average whatever rows exist. Practically: a pair that **stops** being stocked mid-window keeps contributing stockout days to `dc_in_stock_rate` for the rest of the window, while `dc_mean_stock`/`WOS_DC` go quiet for it. A pair that was **never once stocked** inside the window is absent from all of them alike, since `inventory_warehouse` has no row to anchor it — see [`dc_instock`](#dc_instock) for that trade-off. When `dc_instock.enabled=False` (default), this metric is a literal `null` column instead of a rate — see [`dc_instock`](#dc_instock)'s "Disabled path".

**Inventory Turnover Rate** = Sales Units ÷ Mean Stock for the same period grain (the mean stock includes goods in transit when `inventory_turnover_rate` is in `goods_in_transit.inventory_metrics`; the sales units never do; both drop blocked days when it is in `blocked_scope.metrics`). The HTML report labels it per-tab: **Annual**, **YTD**, **Quarterly**, **Monthly**, or **Weekly** Inventory Turnover Rate.

### Inventory metrics: blocked days and goods in transit

Two independent per-metric gates act on the metrics read from `noob/daily-data` / `inventory_warehouse`:

| Metric | Frame | Drops blocked days when in [`blocked_scope.metrics`](#blocked_scope) | Adds goods in transit when in [`goods_in_transit.inventory_metrics`](#goods_in_transit) |
| --- | --- | --- | --- |
| `total_sales_quantity`, `total_sales_revenue`, `AUR`, `AUC`, `distinct_*_count` | `scoped_daily` real rows | yes, each its own gate | never |
| `total_inventory`, `mean_stock` (+ retail / cost), `WOS`, `wos_revenue`, `wos_cost`, `inventory_turnover_rate` | `scoped_daily` | yes (inventory and the sales it divides by) | store GIT |
| `dc_mean_stock`, `WOS_DC` | `dc_daily` | yes (DC blocked days) | DC GIT |
| `total_mean_stock`, `WOS_TOTAL` | `scoped_daily` + `dc_daily` | yes (store and DC blocked days) | store and DC GIT |
| `in_stock_rate`, `weighted_instock_rate` | daily in-stock frame | frame built without blocked days when `in_stock_rate` is named; `weighted_instock_rate`'s sales weights when it is named | `goods_in_transit.store_instock` (the frame's in-stock days) |
| `dc_in_stock_rate` | DC grid | when named | `goods_in_transit.dc_instock` |
| `lost_sales_pct` | `lost_base` | the sales half of the denominator, when named | never |

A metric not named in `blocked_scope.metrics` reads blocked and unblocked rows alike. A day counts in a per-day average (`mean_stock`, `dc_mean_stock`, `total_mean_stock`, the WOS average daily inventory, the turnover's mean stock) only when it has a row the metric reads: without GIT a real (daily-data / `inventory_warehouse`) row, with GIT any row; so a gated metric skips a day whose rows are all blocked. All of this is one conditional-aggregation pass per frame in `metrics.compute_kpis` (`_reads`, `_read_only`, `_day_stock`, `_day_avg`).

### Population filters (restrict ONE metric's own population)

`metrics.population_filters` narrows a single metric's product population without touching scope, roots, or any other metric — e.g. "in-stock rate should never count NON-COMP products, even inside the `overall` root" or "WOS without NVROUT." Applied **on top of** whatever root/cut is already in effect (so it's a no-op inside a root that already restricts to the same value — e.g. filtering `IS_COMP` inside the `comp` root changes nothing there, only inside `overall`).

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

Shape: `{metric_col: {dim_col: value_filter_spec}}`. `dim_col` is any `dimension_sources`/`slices` column already joined onto products (e.g. `IS_COMP`, `IS_NVROUT`, `brand`). `value_filter_spec` is the exact same list/dict shape as [`slices.value_filters`](#value-filters-restrict-cut-values--drop-the-null-bucket).

**Constraint — metrics computed in one shared aggregation pass can only be filtered together**, not independently of each other (see `kpi_pipeline/filters.py`'s `METRIC_FILTER_GROUPS`):

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

Setting an entry on any one column in a group applies it to the whole group. Setting *conflicting* specs on two columns in the same group fails loudly at run time rather than silently picking one. A metric with no entry behaves exactly as before — this is fully additive and opt-in.

Validated at `materialize()` time: an unknown `metric_col` key, or a malformed `value_filter_spec`, fails loudly (same validation `slices.value_filters` already gets).

## tbretail setup

Notes behind `tbretail_config.py` (its comments point here).

**One-time CSV exports** (run as a Databricks cell; the CSVs sit under `/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/KPI-NEW/data/`):

1. JAB product IDs (`jab_product_ids.csv`, `scope_adjustments.additions[0]`): join the JAB Excel against products and write the `product_id` values.

   ```python
   from pyspark.sql import functions as F
   jab_codes = load_jab_skuloc_itemcodes(NFG_EXCEL_PATH_JAB)  # from the kpi_metrics notebook
   jab_ids = (
       spark.read.format("delta").load(PATH_PRODUCTS)
       .filter(F.col("product_code").isin(jab_codes))
       .select("product_id").distinct()
   )
   jab_ids.toPandas().to_csv(".../KPI-NEW/data/jab_product_ids.csv", index=False)
   ```

2. NON-COMP (NFG list) product IDs (`scope_adjustments.removals[0]`, OFF): the same pattern from the NFG Excel (`itemcode` column, `Grand Total` row dropped), written to `non_comp_product_ids.csv`.

   ```python
   pdf_nfg = pd.read_excel(NFG_EXCEL_PATH)
   col_ic = [c for c in pdf_nfg.columns if str(c).strip().lower() == "itemcode"]
   item_col = col_ic[0] if col_ic else pdf_nfg.columns[2]
   nfg_codes = [
       c for c in pdf_nfg[item_col].dropna().astype(str).str.strip().unique()
       if c and c.lower() != "grand total"
   ]
   ```

3. NGF item product IDs (`dimension_sources["ngf_comp_split"]`): the same join on an NGF item list (`pdf_ngf["itemcode"]`), written to `ngf_product_ids.csv`. Unlike step 2 this does **not** remove items from scope: NGF items stay in daily-data / scope and in the Overall numbers and are only flagged (`IS_COMP = 'no'`), so comp vs non-comp can be sliced in one report. The entry's `fillna: {"IS_COMP": "yes"}` imputes every non-NGF product to `'yes'`; without it they would come back NULL, since the CSV only covers NGF items.

   The configured `ngf_comp_split.path` currently points at `non_comp_ids_20260817.csv` (the step-2 NON-COMP file), not at `ngf_product_ids.csv` — unclear whether that is deliberate reuse or the step-3 CSV was never generated; verify before treating either as correct.

**The "NGF products" addition is UNCONFIRMED.** `scope_adjustments.additions[1]` points at the same CSV as the NON-COMP removal (`removals[0]`, OFF), whose own description says NFG / NON-COMP products "are excluded from KPI scope" — but as an *addition* with `store_col = None` it does the opposite: it unions every `(product, store, week)` selling those products into scope, regardless of `removals[0]`. It is not part of the checklist above. It looks like an accidental copy of `removals[0]` with the label swapped, but that is **not** confirmed with a human: do not disable or "fix" it without checking, since it currently decides which products count in the live KPI numbers.

**NON-COMP in-stock rule.** kpi-skill-toolkit's Overall in-stock deliberately excludes NON-COMP products (its `inst_products = nvr_ids ∪ comp_ids_primary`). This pipeline's `overall` root applies no restriction of its own, so NON-COMP products (added through the "NGF products" addition, removal off) were counting toward Overall's in-stock rate — the confirmed cause of the 2026-09-03/04 Overall-instock-only mismatch (COMP / NVROUT matched, since NON-COMP is part of neither root). `metrics.population_filters.in_stock_rate = {"IS_COMP": {"exclude": ["no"]}}` brings Overall's in-stock population in line without touching scope, roots or any other metric.

**Blocked-scope snapshot.** `blocked_scope.ui_parameters_path` comes from the Airflow variable `ui_parameters_path`. The newest folder when it was set (`2026-09-30-204511_...`) only had solution 51 blocks; the configured folder is `2026-09-30-065549_...`. Always pin the folder explicitly.

**Fiscal calendar.** tbretail's fiscal year runs Feb-Jan, so fiscal month / quarter numbers do not match the real calendar (fiscal month 07 has been observed spanning real 8/2-8/29): `fiscal_calendar.column_map.month_name_col` is read verbatim for the Monthly tab label. `run_min_date` 2025-02-08 resolves to Sunday 2025-02-02, the start of fiscal 2025.

**Lost sales source.** `path_segments.lost_sales` is `report_dfu` (future_visibility's own pre-blended fast / slow output), which replaces `lost_sales_ensemble` (OFF, kept as the fallback if `report_dfu` stops being usable). `report_dfu` has no `store_id`, so `store_col = None` (see [`lost_sales_source`](#lost_sales_source)); its model is `top_down_excluding_ecom`, hence `sales_filter` excludes the same three ECOM stores (829 / 639 / 917, copied from customer-analysis-tbretail's `store_replenishment/future_visibility/lost_sales_product_120dayslookback.py:66` and the identical filter in `_365dayslookback.py:67`; keep the two in sync).

**Metrics not reported.** `wos_revenue`, `weighted_instock_rate` and `dc_in_stock_rate` are left out of `metrics.metric_cols` (and `dc_instock` is OFF, so `dc_in_stock_rate` would be null); they stay in `blocked_scope.metrics` and `goods_in_transit.inventory_metrics` so switching one on needs no other edit.

**Dimension sources.** `IS_NVROUT` comes from `operation/extended_product`, which must have one row per `product_id` (the toolkit keeps an arbitrary row per product); products absent from it get NULL, which `fillna` turns into `'no'`. If a product can have several program values, pre-aggregate the table to one NVROUT flag per product and point `path` at it instead of `path_segments`.

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

If you see slow runs, check: (1) scope table path is correct so defined scope is not empty, (2) `run_min_date` is aligned to a Sunday (or left null for full YTD).

## Known limitations

- **`defined_scope.grain = "product"`**: the scope universe is `distinct(product_id)` and applies to every store selling the in-scope products across the window; instock/lost-sales pairs come from each source's own weekly data.
- **Incremental skip vs notebook output**: Saved Delta can retain old values for overlapping period keys while the notebook shows fresh `kpi_long` — set `allow_overwrite_existing=True` to replace.
- **YTD with a single year**: degrades gracefully to "no comparison" rather than erroring — the `"ytd"` period_type rows in `kpi_long` still show whatever data is present.
- **Weekly tab with sparse weeks**: the Weekly period tab's display trim (`weekly_display_weeks`) shows the N most recent weeks **present in `kpi_long`**, not necessarily consecutive fiscal weeks when weekly coverage is sparse (e.g. after a narrow `run_min_date` or partial backfill). There is no WoW comparison table to be affected by this — see [Selecting which comparisons to run](#selecting-which-comparisons-to-run).
- **Quarter/Half/Monthly trend tabs never show an in-progress period**: a `(Year, Fiscal_Quarter)`/`(Year, Fiscal_Half)`/`(Year, Fiscal_Month)` only appears once fully elapsed on both window edges — see [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs). There is no config toggle to show the partial trailing period anyway; if you need it, read `kpi_long`'s Weekly rows directly instead.
- **`goods_in_transit.inventory_metrics`**: needs `fiscal_calendar.use_fiscal_calendar = True`; the last day of the window has goods in transit only once the next day's snapshot exists (`date_shift_days = -1`).
- **`blocked_scope`**: weekly sources (`lost_sales_source`, `instock.method = "weekly_source"`) cannot drop blocked days; the lost-sales numerator never changes, only the sales half of `lost_sales_pct`'s denominator can (when named in `blocked_scope.metrics`).
- **`report_end = "latest_day"`**: needs `instock.method = "daily"`; a fiscal year whose first date is before `EFFECTIVE_REPORT_START_DATE` is not shown in YTD; YTD lost sales is whole weeks up to the last Saturday, so it covers fewer days than the other YTD metrics.
- **`comparable_pairs.grain = "product"`**: store-estate churn is not isolated — a product that gained/lost a store between compared years still moves the like-for-like metrics, since every store of a qualifying product is kept regardless. Use only when a pair-level intersection leaves too small a population.

## Troubleshooting


| Symptom                     | Likely cause                                                                |
| --------------------------- | --------------------------------------------------------------------------- |
| `lost_sales_pct` reads lower than expected | Numerator and denominator are on different populations — the lost-sales source excludes something `daily_data` still carries (classically e-commerce), inflating the denominator. Narrow the sales half with [`lost_sales_source.sales_filter`](#lost_sales_source) |
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
| DC-based metrics (`WOS_DC`, `WOS_TOTAL`, `dc_mean_stock`, `total_mean_stock`) looked too high for some weeks under `defined_scope.grain = "product_store_week"` | Fixed — `build_dc_daily` previously restricted DC/warehouse data to scope by `product_id` alone, dropping the week dimension. Under `product_store_week` grain, scope membership genuinely varies by week, so a product's DC inventory was being kept for weeks it had already fallen out of scope. Now restricted to `(product_id, Year, Week)`, matching `scope_core` exactly. Does not affect `"product"`/`"product_store"` grain (scope is uniform across every week there already, so results are unchanged). Re-run to get corrected values if you're on `product_store_week` grain. |
| `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` shifted after upgrading, with no config change | Expected, one-time — `build_dc_daily` now item-family-rolls `inventory_warehouse` to parent `product_id` before restricting to `scope_core` (previously raw `product_id`, silently dropping DC inventory sitting on a superseded/child item code — see [`inventory_warehouse`](#inventory_warehouse)). Any product family with DC inventory split across old and current item codes will show a step change. Also means `path_segments.item_family` must now point at a real table any time `path_segments.inventory_warehouse` does. |
| A `(product_id, warehouse_id)` pair you expect is missing from `dc_in_stock_rate` entirely | Expected — each pair's grid is anchored to its own first `inventory_warehouse` row, so a pair ranged at a DC but never once stocked inside the report window has nothing to anchor to and is absent rather than reading 0%. See [`dc_instock`](#dc_instock). |
| Under `product_store_week` grain, a pair's earliest weeks are missing even though it's clearly still active | If that pair's own first-seen week matches the scope source's own earliest available week, this is fixed — backfilled by default (`defined_scope.backfill_leading_gap`, default `True`) — see [`defined_scope`](#defined_scope). A pair whose first-seen week is genuinely later than the source's own earliest week is left as-is by design (a real new store/product, not a data gap). Set `False` if you have existing `product_store_week` history saved before this option existed. |
| `defined_scope.grain='product_store_week'` raises `ValueError` about `date_col` on load | `use_fiscal_calendar=True` requires `defined_scope.date_col` — the NATIVE `year_col`/`week_col` path is only valid under `use_fiscal_calendar=False`, since it can't be reconciled against `fiscal_cal`/`fiscal_week`'s own numbering. Set `date_col` to a real date column on your scope source. |
| A comparable-pairs `quarter` link looks wrong / a quarter number never produces a link even though 2+ years clearly have data for it | Check whether that quarter is **fully elapsed** within the report window for those years (see [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half)) — `REPORT_END_DATE` is a week boundary, so the "current" quarter for the latest window year is excluded unless it has actually fully closed by then; a `run_min_date` that doesn't land on a quarter boundary can exclude the earliest year's occurrence the same way. |
| `dc_in_stock_rate` keeps falling for a product long after it stopped being ordered | Expected — a pair's grid runs to `REPORT_END_DATE` once it has started, so every day after its last stocked day counts as a stockout. `dc_mean_stock`/`WOS_DC` go quiet for the same pair instead. See [`dc_instock`](#dc_instock). |
| Comparable pairs population is smaller than expected, or `comparable_pair_count` looks too low | Check `comparable_pairs.grain` — under the default `"product_store"`, a product that opened/closed at even one store between compared years drops that store's pair from the whole population. Set `grain: "product"` to compare at product level instead (store-estate churn then isn't isolated) — see [Comparable pairs](#comparable-pairs-like-for-like-ytd--yoy--quarter--half). |
| The Quarter/Monthly trend tab's most recent row disappeared after upgrading, with no config change | Expected, one-time — that period hadn't actually fully elapsed yet; it's now correctly excluded instead of showing a partial quarter/month next to full ones. See [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs). Re-run with `allow_overwrite_existing=True` (or `full_refresh`) if a stale partial-period row is still showing from a Delta partition saved before this fix. |
| `ValueError: reporting_window.report_end='latest_day' requires instock.method='daily'` | `latest_day` splits the daily in-stock frame's fiscal week for YTD, which weekly in-stock sources cannot do — set `instock.method = "daily"` or use `as_of` / `complete_month`. |
| The current fiscal year is missing from the Annual tab / YoY after switching to `latest_day` | By design: the Annual tab shows complete fiscal years only; the current year appears in YTD. A year is also left out of YTD when its first date precedes `EFFECTIVE_REPORT_START_DATE` (its days 1..K would be partial): move `run_min_date` back to the start of that fiscal year. The run prints `YTD years` and the years left out. |
| Annual / Monthly / Weekly rows from before `latest_day` are still in the saved `kpi_long` | Incremental merge keeps old keys (only `ytd` rows are always replaced). Run one `save_mode="full_refresh"` after switching modes. |
| `lost_sales_pct` stops a few days before the other metrics under `latest_day` | By design: lost sales only reaches the last Saturday on or before `REPORT_END_DATE`; every view uses whole weeks up to it and YTD uses weeks 1..(that Saturday's fiscal week). |
| YTD `WOS` (and `wos_revenue`, `wos_cost`, `WOS_DC`, `WOS_TOTAL`) under `latest_day` differs from the previous mode's YTD | By design: YTD ends mid-week, and that week's average daily inventory is weighted by elapsed days / 7 (`week_days`), so a part week is not counted as a full week of inventory against a part week of sales. Whole weeks, and every other view, are unchanged. See [Latest-day report end](#latest-day-report-end-report_end--latest_day). |
| `ValueError: goods_in_transit...` on config load | `inventory_metrics` has an unknown name, `date_shift_days` is `None` while `store_instock` / `dc_instock` / `inventory_metrics` ask for goods in transit, `date_shift_days` is not an int (bool is rejected), `store_instock` is set without `instock.method = "daily"`, or `fiscal_calendar.use_fiscal_calendar` is `False` with a non-empty `inventory_metrics`. See [`goods_in_transit`](#goods_in_transit). |
| `ValueError: blocked_scope.metrics...` on config load | The value is not `"all"` or a list, or a name is not in `METRICS_ALL` (the message lists the allowed names). See [`blocked_scope`](#blocked_scope). |
| A metric still counts blocked days | It is not named in `blocked_scope.metrics` (a metric not named reads blocked and unblocked rows alike); add it to the list. |
| Total inventory / WOS / mean stock rose after adding a metric to `goods_in_transit.inventory_metrics` | Expected: that metric now adds goods in transit to on-hand (a day with only goods in transit adds a day of zero on-hand to its averages). Drop the name from `inventory_metrics` to return it to on-hand only; metrics not named never change. |
| The last day has no goods in transit with `date_shift_days = -1` | The snapshot dated `REPORT_END_DATE + 1` (the end-of-day state of `REPORT_END_DATE`) is not in `goods_in_transit` yet; later runs pick it up. |
| YTD's elapsed-period selection (`ctx.available_fiscal_months`) changed after upgrading, with no config change | Expected, one-time — two stacked fixes: the "is this period fully elapsed" check was previously trivially true for any period present in the window (a real bug, now fixed), and the grain itself moved from quarter to month (a quarter in progress can still have already-closed months). See [Complete periods only](#complete-periods-only-quarter-half--monthly-trend-tabs). |


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
| **Executive header** | Client, reporting window, as-of date, scope mode, slice dimensions, generated timestamp; with `report_end = "latest_day"` also a **Period basis** card (YTD to the report end, complete periods elsewhere, lost sales through the last Saturday) |
| **Period tabs** | Annual / **YTD** / Quarter / Half / Monthly / Weekly (horizontal; Half only with `fiscal_calendar.half_periods`) |
| **Slice dimension tabs** | Overall + every slice column in `kpi_long` (inferred automatically from data and config) |
| **Value tabs** | Vertical sidebar within each slice dimension — one panel per value (e.g. each brand) |
| **KPI tables** | Metrics as rows (colour-coded), periods as columns; inventory turnover is labelled **Annual** / **YTD** / **Quarterly** / **Half-Yearly** / **Monthly** / **Weekly** per tab |
| **Comparison** | YoY / YTD per value panel, shown only on the Annual/YTD tabs — one consolidated wide value+delta table (the same period value columns as the KPI table above, e.g. 2024/2025/2026, plus one delta column per consecutive-year link, YoY always exactly one link) in place of the plain value table. Quarter/Half/Monthly/Weekly tabs show the plain value-trend table only, no *regular* comparison table. |
| **Comparable (Like-for-Like)** | When `comparable_pairs.enabled=True`, a visually separated section beneath the comparison table, per enabled `comparable_pairs.kinds` entry: one consolidated wide value+delta table on the Annual tab (`yoy`) and YTD tab (`ytd`); **one narrow block per quarter number** on the Quarter tab (`quarter` — mixing all 4 quarters into one table would be unreadable) and **per half number** on the Half tab (`half`); each block has a `Q1 · Like-for-like` / `H1 · Like-for-like` heading and a rule between blocks |
| **Metric Details tab** | Definition, store scope, and formula for every active metric; In-Stock Rate is described from the `instock.daily` settings when `instock.method = "daily"` (it mentions blocked days only when `blocked_scope` is on and `in_stock_rate` is in `blocked_scope.metrics`; the sales / inventory metrics note blocked days only when named there); a metric named in `goods_in_transit.inventory_metrics` states that its store / DC part counts goods in transit; Lost Sales % states the last-Saturday basis under `latest_day` |

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
    "half_display_halves": 4,            # Half tab: N most recent halves; null = all
    "yearly_display_years": 5,           # Annual tab: N most recent years; null = all
    "root_labels": {},                   # root id -> tab label, e.g. {"comp": "LFL", "nvrout": "NVROUT"}
    "dimension_labels": {},              # slice dimension name -> tab label (display only), e.g. {"brand": "Banner"}
}
```

`filename` may use only `{customer}` and `{report_end}`; `output_path_segments` is the datastore **folder** (the filename is appended). `root_labels` renames the root tabs (roots not listed fall back to `Overall` / the root id; tbretail: `comp` -> `LFL`, `nvrout` -> `NVROUT`). `dimension_labels` renames a slice dimension wherever its name is shown in the report (the dimension tabs and the header's slice-dimensions card; tbretail: `brand` -> `Banner`), still capitalized by `_tab_label`; it is display only, so `kpi_long` and the saved outputs keep the raw dimension key, and `materialize()` raises unless it is a dict of `str -> str`. All table cells are center-aligned, and every tab label has its all-lowercase words capitalized (`_tab_label`: `annual` -> `Annual`, value tab `jab` -> `Jab`; words with capitals such as `YTD` or `SMW` are kept) — for every client.

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

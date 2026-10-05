# Configuration reference

Every config section and key of `config.py` (the generic reference template) and `tbretail_config.py` (tbretail's deployed config). Both files keep **all** sections and keys, switched off when unused. `materialize()` validates the config, applies the `KPI_*` environment overrides ([Environment variable overrides](#environment-variable-overrides)) and resolves paths and the reporting window. Values shown are `config.py`'s defaults unless a block says "tbretail".

`config.py` is a generic reference template, not any customer's deployed config: every optional feature ships disabled with a placeholder, so copy it per client and replace `customer`, every `path_segments` entry, `scope.columns` and the business rules (dimension sources, population filters).

## Section index

| Section | What it controls | Where it is explained |
| ------- | ---------------- | --------------------- |
| `customer` | Datastore bucket `/mnt/invent-{customer}-datastore` | [`customer`](#customer) |
| `run` | Run mode `full` / `html_only` | [`run`](#run) |
| `reporting_window` | `as_of_date`, `run_min_date`, `report_end` | [`reporting_window`](#reporting_window), [LOGIC_FLOW.md](LOGIC_FLOW.md#reporting-window-and-report-end) |
| `fiscal_calendar` | Fiscal vs native time grain, half periods | [`fiscal_calendar`](#fiscal_calendar) |
| `path_segments` | Folders of each source table under the bucket | [`path_segments`](#path_segments) |
| `input_filters` | Spark SQL filters applied when reading each source | [`input_filters`](#input_filters) |
| `item_family_source`, `item_family_rollup` | Parent / child product map | [`item_family`](#item_family) |
| `sales_basis` | Which sales the sales metrics read: net (daily-data) or gross (transactional sales) | [`sales_basis`](#sales_basis) |
| `scope`, `score_scope` | Which product x store pairs count | [`scope`](#scope), [`score_scope`](#score_scope) |
| `blocked_scope` | UI-blocked days, per metric | [`blocked_scope`](#blocked_scope) |
| `instock`, `dc_instock`, `inventory_warehouse` | Where in-stock rates and DC inventory come from | [`instock`](#instock), [`dc_instock`](#dc_instock), [`inventory_warehouse`](#inventory_warehouse) |
| `goods_in_transit` | Goods in transit added to on-hand | [`goods_in_transit`](#goods_in_transit) |
| `lost_sales_source`, `lost_sales_ensemble` | Lost-sales column mapping and fast / slow blend | [`lost_sales_source`](#lost_sales_source), [`lost_sales_ensemble`](#lost_sales_ensemble) |
| `slices`, `dimension_sources` | Cuts and root tabs | [`slices`](#slices), [`dimension_sources`](#dimension_sources) |
| `metrics` | Reported metrics, labels, population filters | [`metrics`](#metrics) |
| `comparisons`, `comparable_pairs` | Regular and like-for-like comparisons | [`comparisons`](#selecting-which-comparisons-to-run), [`comparable_pairs`](#comparable_pairs) |
| `output` | Delta saves | [`output`](#output), [OUTPUTS.md](OUTPUTS.md) |
| `html_report` | HTML report | [`html_report`](#html_report), [HTML_REPORT.md](HTML_REPORT.md) |

## `customer`

```python
"customer": "your_customer",  # datastore bucket: /mnt/invent-{customer}-datastore
```

`KPI_BUCKET` overrides the datastore mount, `KPI_CUSTOMER` the customer slug.

## `reporting_window`

```python
"reporting_window": {
    "as_of_date": "2026-09-01",  # last day of data to report; update before each run
    "run_min_date": None,        # None = YTD from Jan 1; "2024-01-01" for multi-year
    "report_end": "as_of",       # "as_of" | "complete_month" | "latest_day" ("latest_day" needs instock.method "daily")
},
```

`run_min_date` is aligned to the Sunday on or before it. `report_end` selects how the window ends; the three modes, the resolved start and end, and the time-grain checks are explained in [LOGIC_FLOW.md](LOGIC_FLOW.md#reporting-window-and-report-end) (see [Complete-month report cutoff](LOGIC_FLOW.md#complete-month-report-cutoff-report_end) and [Latest-day report end](LOGIC_FLOW.md#latest-day-report-end-report_end--latest_day)). Env: `KPI_AS_OF_DATE`, `KPI_RUN_MIN_DATE`, `KPI_REPORT_END`.

## `fiscal_calendar`

```python
"fiscal_calendar": {
    "use_fiscal_calendar": True,  # True = periods from the fiscal_cal upload; False = from daily-data dates
    "half_periods": False,        # True adds the Half tab and the "half" comparable kind
    "column_map": {               # fiscal_cal upload columns; set month_name_col when the fiscal year is offset
        "quarter_col": "Quarter",
        "month_col": "Month",
        "month_name_col": "month_name",
    },
    "daily_time_columns": {       # raw daily-data columns; Year always comes from date, week is civil path only
        "date": "date",
        "week": "week",
    },
},
```

Behaviour of `use_fiscal_calendar` and `half_periods` is explained in [Fiscal calendar vs native time grain](LOGIC_FLOW.md#fiscal-calendar-vs-native-time-grain) and [Half periods](LOGIC_FLOW.md#half-periods-h1--h2). Env: `KPI_USE_FISCAL_CALENDAR`, `KPI_HALF_PERIODS`.

## `path_segments`

Folders under the datastore bucket, one list of segments per source:

| Key | Default | Source |
| --- | ------- | ------ |
| `fiscal` | `one_time_uploads/fiscal_cal` | Fiscal calendar upload |
| `daily_data` | `noob/daily-data` | Store x product daily sales and inventory |
| `inventory_warehouse` | `operation/inventory_warehouse` | DC daily inventory |
| `item_family` | `operation/item_family` | Parent / child product map |
| `scope` | `analysis/instock_rate/instock_rate_scope` | The scope table |
| `goods_in_transit` | `operation/goods_in_transit` | `destination_type` 0 = store, 1 = warehouse |
| `products` | `master-data/products` | Product attributes: slice dimensions, active flag |
| `lost_sales` | `noob/lost-sales` | Add a `model_id=...` segment if partitioned by model |
| `product_planning_level` | `operation/product_planning_level` | `product_agg_level` to `product_id` map |
| `transactional_sales` | `operation/transactional_sales` | Read only with `sales_basis = "gross"`: one row per transaction line, `sales_type` regular / promo / clearence / return |

## `input_filters`

The notebook reads the **scope table**, **lost sales**, and **daily data** separately before the pipeline run so you can inspect them. Config filters are applied to both previews and the pipeline.


```python
"input_filters": {
    "scope": [  # also applied to the DC scope read
        # "store_id NOT IN (829, 639, 917)",
    ],
    "lost_sales": [],
    "daily_data": [],
    "transactional_sales": ["sales_type != 'return'"],  # read only with sales_basis "gross"
}
```

Each entry is a Spark SQL expression passed to `.filter()`. You can also filter ad hoc in the notebook preview cell (e.g. `.filter("brand = 'NIKE'")`).

`transactional_sales` is read only under [`sales_basis = "gross"`](#sales_basis); its filters run on the raw columns (after the window filter, before the five needed columns are kept), so any column of the table can be used. The default `sales_type != 'return'` is what makes it gross: return rows are stored positive, so a list without it counts them as sales. Add to the list to narrow further (e.g. `"store_id NOT IN (829, 639, 917)"`, `"sales_type != 'clearence'"`).

Preview cells re-read the same tables with the same config filters. The pipeline caches daily data and lost-sales weekly aggregates within each run.

**Ensemble note:** when `lost_sales_ensemble.enabled=True`, `input_filters.lost_sales` applies to **both** the fast and slow lost-sales sources (same schema). Cell 2 also previews the slow source and the speed-cluster table; see [`lost_sales_ensemble`](#lost_sales_ensemble).

## `scope`

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

**Not the same knob as `comparable_pairs.grain`.** `scope.grain` decides scope *membership*; [`comparable_pairs.grain`](LOGIC_FLOW.md#comparable-pairs-like-for-like-ytd--yoy--quarter--half) decides what "the same pair across years" means for that feature only.

`materialize()` fails loudly when: `time` / `grain` is invalid; `columns.product` is unset, or `columns.store` for `product_store`; `weekly` has neither `columns.date` nor `year` + `week`, or has `start` / `end`; `daily` has `date` / `year` / `week`; `columns.solution` is set without `solution_id`; `run_date` is not a Sunday; and for a rule whose column is missing: `dc_solution_id` needs `columns.solution`, `start` and `store`; `blocked_scope.ui_parameters_path` needs `columns.start` and `store`; `instock.daily.count_start` other than `first_daily_row` needs `columns.start`; `instock_main_eligible_only` needs `columns.start`, `roll_to_family_main` and `instock.method = "daily"`; `instock_exclude_unsuperseded_sizes` needs `instock.method = "daily"`. A scope with no keys inside the report window (daily or weekly) raises `ValueError` in `scope.build_scope`.

When `run_scope_diff=False` (default), score-scope computation is skipped unless `use_hybrid_scope=True` (hybrid backfill needs it). The notebook scope-diff cell and `scope_diff` Delta output are omitted.

- **`instock_main_eligible_only`** (tbretail `True`, generic `False`): in-stock (and weighted in-stock) counts only stores where the main item itself is eligible; a store where only a superseded (sub) item is eligible was intentionally not assorted the new item (client rule). Those stores stay in every other metric. The pair's start is still the earliest of the main and sub rows. Built from `ctx.scope_pairs.main_eligible`, applied in `pipeline.build_instock_daily`.
- **`instock_exclude_unsuperseded_sizes`** (tbretail `True`, generic `False`): in-stock leaves out sizes "not created in the supersession": a product in no `item_family` row whose class color (`products.option_code`) has at least one size in `item_family` (main or sub). Treated like NGF: out of in-stock, still in every other metric (`pipeline._unsuperseded_sizes`).

Block product_ids are **not** rolled: only blocks on the main's own `product_id` apply, as in the client reference script.

## `score_scope`

Used when `scope.use_hybrid_scope=True` or `scope.run_scope_diff=True`. Applies to the **missing** (uncovered) weeks under hybrid scope.

```python
"score_scope": {
    "min_percentile": 0.2,
    "min_weeks_for_filter": 2,
}
```

## `lost_sales_ensemble`

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

## `lost_sales_source`

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

Under `report_end = "latest_day"` lost sales reach only the last complete Saturday ([Latest-day report end](LOGIC_FLOW.md#latest-day-report-end-report_end--latest_day)). The sales half of the denominator is real daily-data sales only (GIT-only days carry none); blocked days leave it only when `lost_sales_pct` is in [`blocked_scope.metrics`](#blocked_scope), and the weekly numerator never changes.

## `instock`

Where `in_stock_rate` (and the in-stock side of `weighted_instock_rate`) comes from: a `method` plus one sub-section per method. Both sub-sections are in every config (all keys present); only the one `method` names is used.

```python
"instock": {
    "method": "daily",                  # "daily" | "weekly_source" | "lost_sales_source"
    "daily": {
        "count_start": "earliest",      # "first_daily_row" (generic default) | "scope_start" | "earliest"
        "require_daily_data": True,
        "history_start": "2024-01-21",  # None = window start
        "usable_only": True,
        "sales_counts_as_stocked": False,  # a day with sales > 0 but inventory <= 0 counts as stocked
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
5. In-stock day = a usable day with `inventory > 0`, or (with `sales_counts_as_stocked`) a day with `sales_quantity > 0` even if `inventory <= 0`; with `goods_in_transit.store_instock` also a day with store GIT (`destination_type = 0`, `quantity > 0`, family main, shifted by `date_shift_days`); united (OH OR sales OR GIT), never summed; blocked and unusable days drop out too.

The output has the shape of the weekly `inst_data` (`stocked_pairs` / `available_days` per pair-week plus product dims), so `compute_kpis`, population filters and comparable pairs work unchanged; day counts come from fiscal week bounds. Under `latest_day` the week containing day K is two rows per pair, carrying `last_day_index` ([Latest-day report end](LOGIC_FLOW.md#latest-day-report-end-report_end--latest_day)).

**`method = "weekly_source"`** reads in-stock rate and total-days from a separate table (e.g. in-stock calculated by another pipeline than the lost-sales model), read and scope-restricted independently of lost sales (`read_instock_weekly`; `_aggregate_lost_sales_pairweek` then aggregates only `lost_sales`). It is semi-joined to scope at its own grain, so every in-scope pair-week it has counts whether or not lost sales has a row; the two meet at the final per-period join. A pair-week with null/zero `total_days` is dropped, never padded to a full week. With `in_stock_rate` in [`blocked_scope.metrics`](#blocked_scope), a pair-week whose every window day is blocked is dropped (see `blocked_scope`). `product_col` / `product_agg_level_col` follow the [`lost_sales_source`](#lost_sales_source) rule.

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

## `inventory_warehouse`

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

**Item-family rollup before restriction (unconditional, NOT gated by `dc_instock.enabled`).** `scope_core` (and the scope table's rows) is in **parent** id space (its producer maps `product_id -> coalesce(parent_id, product_id)` via `item_family`'s `is_main=false` rows), but `inventory_warehouse`'s `product_id` is raw. `build_dc_daily` therefore rolls it to parent id (`inputs.roll_to_item_family_parent`, re-aggregating `F.sum("inventory")` at `(product_id, warehouse_id, date)`) **before** restricting to `scope_core`; otherwise DC inventory on a superseded/child `product_id` would be dropped. So `path_segments.item_family` must point at a real table **whenever `path_segments.inventory_warehouse` is configured**, and `dc_mean_stock`/`WOS_DC`/`WOS_TOTAL` shift wherever a family had inventory split across old and current item codes.

## `dc_instock`

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
- **`item_family_source`** ([`item_family`](#item_family)) maps the parent/child table's columns; used by `build_dc_daily` and `build_dc_inst` via `inputs.roll_to_item_family_parent`, and read whenever `inventory_warehouse` is configured.
- **Window start.** Each pair's grid runs from **its own first `inventory_warehouse` row** to `REPORT_END_DATE`, not from a scope `start_date` or a flat window: the same "first day with any history" signal `daily_data_expanded` uses for the **store-level** in-stock denominator (`customer-analysis-tbretail`'s `05_future_visibility_data_prep.py`), so both series share one definition.
- **Trade-off:** a pair ranged at a DC but never stocked inside the window has no row and is **absent**, not 0%. A pair that *stops* being stocked is still covered: its grid continues to `REPORT_END_DATE` and every later day is a stockout.

**Build** (`pipeline.build_dc_inst`): from rolled-up `inventory_warehouse` (`_get_inventory_warehouse_parent_rolled`, shared with `build_dc_daily`, window-filtered) take `first_stocked_date = MIN(date)` per pair; `F.explode(F.sequence(first_stocked_date, REPORT_END_DATE))` gives the pair's days, joined to the fiscal calendar; restrict to the SAME in-scope `(product_id, Year, Week)` population as `build_dc_daily` before the inventory join; left-join inventory with `F.coalesce(inventory, 0)` (a grid day with no row is a stockout); flag DC blocked days (`ctx.dc_blocked_days`, with `scope.dc_solution_id`) as `is_blocked` and left-join DC GIT days (with `goods_in_transit.dc_instock`); aggregate to `(product_id, warehouse_id, Year, Week)`:

- `dc_stocked_days = COUNT(inventory > stock_threshold OR goods in transit)`, `dc_available_days = COUNT(*)`, both over unblocked days only when `dc_in_stock_rate` is in `blocked_scope.metrics` (rows with no available day are dropped), plus `dc_unblocked_days` (unblocked days either way; the comparable-pairs universe reads it).

`dc_in_stock_rate = F.greatest(0.0, Σ(dc_stocked_days) ÷ Σ(dc_available_days))` at the period grain (no sales-weighted rollup). **Disabled path:** with `enabled=False` (default) `build_dc_inst` returns an empty, correctly-shaped frame, so `dc_in_stock_rate` is always a literal-`null` column in `kpi_long`.

## `item_family`

Parent/child product roll-up: a superseded/child product (`is_main = false`) rolls onto its parent (`coalesce(parent_id, product_id)`, `inputs.roll_to_item_family_parent`).

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
    "transactional_sales": True,
},
```

`item_family_source` renames columns to `product_id` / `parent_id` / `is_main` at read time (`read_item_family_source`, `ctx.item_family_raw`). `path_segments.item_family` must point at a real table whenever any rollup toggle is `True` or [`goods_in_transit.roll_to_family_main`](#goods_in_transit) is `True`; it is read whenever `inventory_warehouse` is configured.

- `daily_data` (default `True`): without it `build_scoped_daily`'s join to the parent-rolled `scope_core` silently drops daily rows still carrying a child product_id. Can shift historical numbers for products with a supersede history.
- `transactional_sales` (default `True`; env `KPI_ITEM_FAMILY_ROLLUP_TRANSACTIONAL_SALES` in `config.py`): rolls the transactional sales to the family main before they are summed; used only under [`sales_basis = "gross"`](#sales_basis). Gross is joined onto daily-data, so under `"gross"` it must equal `daily_data`: `materialize()` raises otherwise. When the key is absent it follows `daily_data`.
- `lost_sales` (default `False`): `report_dfu` already substitutes upstream, so a second roll-up is likely a no-op; opt-in safety net.
- `inventory_warehouse` (default `True`): see [`inventory_warehouse`](#inventory_warehouse).
- The scope has its own toggle, `scope.roll_to_family_main`.
- Goods in transit has its own toggle, `goods_in_transit.roll_to_family_main`.

## `sales_basis`

Which sales every sales metric reads. Top-level, validated by `materialize()` (anything but `"net"` or `"gross"` raises `ValueError`); env `KPI_SALES_BASIS` overrides it.

```python
"path_segments": {..., "transactional_sales": ["operation", "transactional_sales"]},  # read only for "gross"
"sales_basis": "net",   # "net" (default) | "gross"
```

| Value | `sales_revenue` / `sales_quantity` |
| --- | --- |
| `"net"` | `noob/daily-data` as it is: net of returns, and product-store-days with net quantity <= 0 or net revenue < 0 are already dropped upstream. `transactional_sales` is never read and nothing is joined. |
| `"gross"` | **All** the rows of `operation/transactional_sales` that pass `input_filters.transactional_sales` (default `sales_type != 'return'`: return rows are stored positive, types are `regular` / `promo` / `clearence` / `return`), rolled to the family main and summed per product x store x date. Every transactional day counts, whether or not daily-data has a row for it. |

**Mechanism** (`inputs.get_daily_data_raw`, inside its existing per-run cache): after `input_filters.daily_data`, the window filter and the family roll-up, `transactional_sales` is read once (window on the raw `date` column for file pruning, then `input_filters.transactional_sales`, default `sales_type != 'return'`, then only `product_id`, `store_id`, `date`, `sales_revenue`, `sales_quantity`), rolled to the family main when `item_family_rollup.transactional_sales` is on (before the sum, because daily-data is in the family-main id space), summed per product x store x date and **full-outer-joined** on `(product_id, store_id, date)`. `sales_revenue` / `sales_quantity` are then **replaced** by `coalesce(gross, 0)`. A transactional day with no daily-data row (none exists, or `input_filters.daily_data` removed it, e.g. `usable = 1`) becomes a row of its own with inventory 0 and `gross_only = True`; `build_scoped_daily` turns the flag into `has_daily_row = False`, `has_sales_row = True` and `has_stock_row = False` (unless goods in transit exist that day). The sales metrics (sales units / revenue, `AUR`, `AUC`, WOS and turnover sales, weighted in-stock weights, the lost-sales denominator, distinct counts, comparable-pair presence) read `has_sales_row`; every inventory metric still reads only the daily-data days (and goods-in-transit days), so a sales-only day never adds a zero-stock day. Scope, the active-product filter and the family roll-up still apply to the added days.

**What gross does and does not do.**

- It applies no daily-data filter (`usable = 1`), no ECOM removal and no blocked-day removal to the added days: the only reductions are the transactional filter (non-return), the family roll-up, scope and the active filter. Blocked scope still gates the inventory metrics named in `blocked_scope.metrics`, never the sales metrics. `lost_sales_source.sales_filter` (ECOM) still narrows the sales half of the `lost_sales_pct` denominator.
- A daily-data row with no non-return sales gets 0. With the daily-data roll-up on, a child and its parent on the same date are summed into one row first, so a day's gross attaches once.
- Units come from the same table as revenue, so `AUR` (revenue / units), `AUC` (`sales_cost` / units), WOS (stock / weekly sales units; `wos_revenue` / `wos_cost` revenue and cost) and `inventory_turnover_rate` stay consistent. Replacing only revenue would put gross revenue over net units.
- On the civil calendar a sales-only day takes its native `week` from the daily-data rows of the same date (`_with_gross_sales`); the civil time grain is built from the real daily-data rows only, so a window date with no daily-data row at all still raises "time grain is missing date(s)", as under `net`.
- The daily in-stock frame (`get_instock_daily_raw`) reads inventory and `usable` only and does not depend on the basis. The notebook's Cell 2 preview reads `noob/daily-data` directly, so it shows net sales whatever the basis.
- The basis changes what a saved `kpi_long` row means. An incremental save must not mix bases: use `output.save_mode = "full_refresh"` when you change it (the run prints a note under `incremental`).

**Cost.** `"net"` is byte-identical to a build without the setting. `"gross"` adds one windowed, column-pruned read of `transactional_sales`, one aggregation and one full-outer join, all inside the cached daily-data frame built once per run (no extra cache, no extra pass per KPI table). Sales-only days add rows to `scoped_daily`, which is collapsed to product level like the rest. With the daily-data roll-up on there is one more aggregation on the daily side.

## `blocked_scope`

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

**Blocked-day representation.** Blocked days are built as cached, disjoint per-pair date intervals `(product_id, store_id, first_day, last_day)` (`ctx.blocked_days`, `scope.build_blocked_days` via `scope._applied_block_intervals`, shared with DC blocks); overlapping or adjacent blocks of a pair are merged. Frames flag or drop a day with a range join on (pair, `first_day <= date <= last_day`) rather than a per-pair-day table. The printed "blocked pair-days in window" count is the number of covered pair-days. Under `instock.method` `weekly_source` / `lost_sales_source` with `in_stock_rate` named and an in-stock source without a store column, `scope.build_blocked_product_days` also builds `ctx.blocked_product_days`, the cached `(product_id, first_day, last_day)` intervals of the `"product"` kind only, matched to each product's earliest `scope_start` under `rule` (`product_destination` / `destination` name a store that source does not have, and the run prints this); every store-level frame (sales, WOS, turnover, inventory, the daily in-stock frame, a weekly in-stock source with a store column) still applies every kind in `blocked_scope.kinds`, `product_destination` and `destination` included.

**DC blocks** (`dc_solution_id`, int not bool, e.g. 22; `None` = none) work the same from `{ui_parameters_path}/dc_blocked_scope/{product,product_destination}` (`destination_id` = warehouse), by `rule`. A DC pair's `scope_start` comes from that solution's rows of the scope table (same `run_date`, family roll-up, earliest start and `active_only` as the store scope, location = warehouse); DC pairs outside it get no blocks. Built once per run as `(product_id, warehouse_id, first_day, last_day)` intervals, `ctx.dc_blocked_days` (`scope.build_dc_blocked_days`).

**`metrics` — which metrics drop blocked days.** Blocked days stay in the frames, flagged `is_blocked` (on `scoped_daily` and `dc_daily`, from one range join in `pipeline.build_scoped_daily` / `build_dc_daily`). A metric reads **only unblocked rows when it is named in `metrics`**, and **blocked and unblocked rows alike when it is not**. `"all"` (generic default) means every name of `METRICS_ALL`; an unknown name raises (the allowed names are listed); `settings["BLOCKED_SCOPE"]["metrics"]` holds the resolved list in `METRICS_ALL` order. tbretail names the inventory, WOS, in-stock and DC metrics and leaves sales units, sales revenue, AUR, AUC, the distinct counts and `lost_sales_pct` out: a blocked pair can still sell its existing stock, and the report never removes blocked days from history for those metrics. Per-metric gating is tabulated in [Inventory metrics: blocked days and goods in transit](METRICS.md#inventory-metrics-blocked-days-and-goods-in-transit).

`metrics.compute_kpis` aggregates once per frame and family with conditional aggregation (`F.when(~is_blocked, x)` in sums, averages and distinct counts, plus per-day `<metric>_day` / `<metric>_has` columns so a day with only blocked rows is not a day of a gated average). When every reported metric (`metrics.metric_cols`) is named, blocked rows leave the sales rows too, so a period × slice with only blocked days gets no output row. The comparable-pairs universe stays the real, unblocked rows whatever the gate says; `scope_diff` and the scope debug are unaffected. **In-stock metrics are blocked together:** listing either `in_stock_rate` or `weighted_instock_rate` adds the other in `materialize()` (printed note), as both read one in-stock frame. **In-stock method:** `daily` removes blocked days per day. `weekly_source` / `lost_sales_source` only have in-stock days per week, so a pair-week (product-week for a source without a store column) is dropped from the in-stock frame only when every day of the week inside the report window is blocked (`pipeline._drop_fully_blocked_weeks`); a partly blocked week stays. Lost sales and `lost_base` keep every week, and the sales sources (`lost_sales_pct`'s numerator) are never filtered.

## `goods_in_transit`

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

## `output`

See [Output saves](OUTPUTS.md#output-saves) for mode behaviour, merge keys, workflows, and caveats.

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

## `run`

```python
"run": {
    "mode": "full",   # "full" computes from the source tables; "html_only" renders saved outputs
}
```

`KPI_RUN_MODE` overrides the mode. See [HTML report — Run mode](HTML_REPORT.md#run-mode-html-only-from-saved-data).

## `html_report`

See [HTML report](HTML_REPORT.md#html-report).

## `slices`

```python
"slices": {
    "dimensions": ["brand"],
    "derived_dimensions": {"example_derived": "CASE WHEN brand = 'A' THEN 'Group A' ELSE 'Other' END"},
    "value_filters": {},  # per-dimension include / exclude of values
},
```

`dimensions` are breakdown (cut) columns from the products table; `derived_dimensions` maps a new cut name to a Spark SQL expression over the products table. `value_filters` restricts which values of a cut appear (see [Value filters](LOGIC_FLOW.md#value-filters-restrict-cut-values--drop-the-null-bucket)). Cuts are applied within every root ([Roots and cuts](LOGIC_FLOW.md#roots-and-cuts-report-structure)). **Every cut column (`dimensions` and `derived_dimensions`) must be a string column**: a number, date or boolean cut raises a `ValueError`; cast it in `derived_dimensions`, e.g. `CAST(col AS STRING)`. Env: `KPI_SLICE_DIMENSIONS`.

## `dimension_sources`

A list of external tables that add dimensions and root tabs; each enabled column becomes a root. Keys per entry (from `config.py`):

```python
{
    "enabled": False,
    "label": "example_dimension_source",
    "source": "delta",                       # "delta" | "csv"
    "path_segments": ["operation", "some_attribute_table"],
    "join_key": "product_id",
    "columns": [],                           # source columns taken as they are
    "derived": {"IS_EXAMPLE_FLAG": "CASE WHEN some_column = 'X' THEN 'yes' ELSE 'no' END"},
    "fillna": {"IS_EXAMPLE_FLAG": "no"},     # products absent from the source get NULL, not the ELSE branch
    "root_values": {"IS_EXAMPLE_FLAG": {"yes": "example_root"}},  # root "example_root" = flag "yes"
},
```

CSV sources use `path`, `location` (`datastore` | `workspace`) and `csv_options` instead of `path_segments`. The full behaviour (one row per `join_key`, left join, `fillna`, fail-loud, CSV location) is in [Dimension sources](LOGIC_FLOW.md#dimension-sources--roots-population-tabs-from-other-tables).

## `metrics`

```python
"metrics": {
    "metric_cols": [...],         # metrics reported, from METRICS_ALL
    "scope_diff_metrics": [...],  # metrics in the scope diff (scope.run_scope_diff), from metric_cols
    "labels": {...},              # display name per metric
    "pp_change_metrics": ["in_stock_rate", "weighted_instock_rate", "dc_in_stock_rate", "lost_sales_pct"],
    "population_filters": {},     # {metric: {dim_col: value filter}} narrows one metric
},
```

`pp_change_metrics` are rate metrics whose change is shown in percentage points, not percent. The metric list is in [METRICS.md](METRICS.md); `population_filters` is explained in [Population filters](METRICS.md#population-filters-restrict-one-metrics-own-population).

## Selecting which comparisons to run

Config section `comparisons`.

`comparisons.enabled` chooses which comparisons are **computed, printed, saved, and rendered**: any subset of `"yoy"`, `"ytd"` (the only two kinds).

```python
"comparisons": {
    "enabled": ["yoy"],          # only year-over-year; ytd is skipped entirely
},
```

- **`yoy`** — full year vs the prior full year (last two annual periods; under `latest_day` the latest two **complete** fiscal years).
- **`ytd`** — each year's **elapsed window** vs the prior year's same window, chained across consecutive years (`2026 YTD` vs `2025 YTD`, `2025 YTD` vs `2024 YTD`). Under `latest_day` the window is the **same fiscal day** of every year (days 1..K, see [Latest-day report end](LOGIC_FLOW.md#latest-day-report-end-report_end--latest_day)); otherwise it is the fiscal **months** fully closed for the latest year (every week ends on or before `REPORT_END_DATE`, checked against the unclipped calendar, see [Complete periods only](LOGIC_FLOW.md#complete-periods-only-quarter-half--monthly-trend-tabs)), the same month set summed for **every** year. Use it instead of `yoy` while the current year is partial. A single-month or single-year window still sums what exists; the comparison is absent with one year.
- **No QoQ/MoM/WoW comparison table.** The Quarter/Monthly/Weekly tabs (and `kpi_long` `period_type` rows, always produced in full) show value trends; compute quarter-over-quarter or month-over-month changes from consecutive `kpi_long` rows.
- Only selected kinds produce `comparison_{kind}` tables and HTML columns. `kpi_long` is **always** produced in full, including `"ytd"` rows. Comparable pairs is gated independently via `comparable_pairs`.
- **HTML**: `ytd` with several year-pairs renders as stacked mini tables, one per pair; `yoy` as a single table.
- **From saved history:** with `save_mode="incremental"` and `recompute_comparisons_from_history=True` comparisons are rebuilt from the **full merged `kpi_long`**, so `["yoy"]` on a one-week run compares the current partial year with last year's saved annual total (needs saved history at an earlier `run_date`).
- Invalid or empty selections fail loudly at `materialize()`. Env: `KPI_COMPARISONS="yoy,ytd"`.

## `comparable_pairs`

```python
"comparable_pairs": {
    "enabled": False,
    "kinds": ["ytd"],            # any of COMPARABLE_KINDS_ALL
    "grain": "product_store",    # or "product"
    "pair_days": "unblocked",    # pairs present on "unblocked" days in every year, or on "all" (incl. blocked)
},
```

Needs `run_min_date` spanning 2+ years. Population, kinds, `grain`, `pair_days` and outputs are explained in [Comparable pairs](LOGIC_FLOW.md#comparable-pairs-like-for-like-ytd--yoy--quarter--half). Env: `KPI_COMPARABLE_PAIRS`.

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
| `KPI_HTML_*`                      | HTML report overrides, listed under [HTML report](HTML_REPORT.md#html-report) |
| `KPI_RUN_MODE`                    | `full` or `html_only` — skip pipeline and render HTML from saved outputs |
| `KPI_SALES_BASIS`                 | `net` or `gross` — overrides `sales_basis` (any other value raises) |

<p align="center">
  <img src="docs/images/readme-banner.png" alt="Retail KPI analytics — sales, inventory, in-stock rate, and weeks of supply" width="100%" />
</p>

# Retail Insights Pipeline

PySpark toolkit for weekly, monthly, quarterly, annual, and YTD retail KPIs with configurable scope, comparable (like-for-like) pair analysis, and incremental Delta output saves. It runs on **Databricks** against the customer Delta datastore (`/mnt/invent-{customer}-datastore`).

This page is the setup-and-run guide. Everything else (config reference, logic, metrics, outputs) is in [`docs/`](#documentation).

## What it produces

One tidy table, `kpi_long`, with every configured metric (sales, inventory, WOS, in-stock rate, lost sales %, ...) by period type (annual / ytd / quarter / half / monthly / weekly), report root and breakdown cut; YoY and YTD comparison tables (`comparison_yoy`, `comparison_ytd`); optionally the scope-vs-score check (`scope_diff`) and the like-for-like tables (`comparable_kpi_long`, `comparable_comparison_*`); and a standalone offline tabbed HTML report. Tables are saved as Delta, partitioned by `run_date`. See [docs/OUTPUTS.md](docs/OUTPUTS.md) for every table and its keys.

## Prerequisites

- Databricks cluster with PySpark and access to `/mnt/invent-{customer}-datastore` (or override with `KPI_BUCKET`).
- `algo_helpers` available on the cluster (`from algo_helpers import fundamentals as fund`).
- Delta tables referenced in the config must exist for the chosen date window.

## Files to upload

Upload these to one Databricks workspace folder:

- `main.ipynb`
- `config.py`
- the entire `kpi_pipeline/` folder

`config.py` is a generic reference template, not any customer's deployed config. Every optional feature ships disabled with a placeholder, so copy it per client and replace `customer`, every `path_segments` entry, `scope.columns` and the business rules (dimension sources, population filters). tbretail's deployed config is `tbretail_config.py`; the copies one directory up (`../tbretail_config.py`, `../tbretail_config_2.py`) are older versions and must be replaced by it before a run. The notebook loads its config with `%run ./config` in Cell 1; change that line to use another file. See [docs/TBRETAIL.md](docs/TBRETAIL.md) for the tbretail setup notes.

## Config keys to set first

Edit `CONFIG` in `config.py`. At minimum:

- `reporting_window.as_of_date` — anchor date; `reporting_window.run_min_date` — optional narrow start (Sunday-aligned).
- `scope` — `time`, `grain` and `columns` (the column mapping) for your scope Delta table; `path_segments.scope` — its path segments under the datastore bucket; `scope.use_hybrid_scope` — `False` (default) for the scope table alone, `True` for hybrid (covered weeks + score backfill on missing weeks).
- `input_filters` — optional Spark SQL filters on the scope table, lost sales, daily data.
- `slices.dimensions` — product-master columns to cut by (e.g. `brand`), applied within every root.
- `dimension_sources` — optional: each column becomes a root (e.g. NVROUT from `extended_product`).
- `instock.method`, `blocked_scope.metrics`, `goods_in_transit` — all gated.
- `sales_basis` — `"net"` (default, `noob/daily-data` sales, net of returns) or `"gross"` (non-return rows of `operation/transactional_sales`, joined onto daily-data). One Sales Revenue is shown either way; changing it changes what saved `kpi_long` means, so use `output.save_mode = "full_refresh"`. See [docs/CONFIG.md](docs/CONFIG.md#sales_basis).
- `output.save_outputs` and `output.save_mode` — `initial`, `incremental`, or `full_refresh`.
- `run.mode` — `full` (default) or `html_only`.

Every key is explained in [docs/CONFIG.md](docs/CONFIG.md). Any key can also be overridden at run time with a `KPI_*` environment variable (listed there).

## Run the notebook

Open `main.ipynb` and run the cells top to bottom (Run All works). Each cell:

1. **Cell 1 — config summary.** Loads the config (`%run ./config`), calls `materialize()`, creates `KPIRunner` and prints the resolved settings (paths, date window, sales basis, slices).
2. **Cell 2 — input previews.** Reads the scope table, lost sales and daily data separately with the same `input_filters` the pipeline uses, prints each source's date range and displays a sample. Add ad-hoc notebook filters here if needed. It shows `noob/daily-data` as stored, so under `sales_basis = "gross"` its sales columns are still the net ones. Skipped when `run.mode = html_only`.
3. **Scope debug.** `runner.prepare_scopes()` builds the dimensions and scope once (Cell 3 reuses them), then `runner.scope_debug_summary()` shows distinct product, store and pair counts overall and per slice value at each removal stage. Read-only sanity check before the heavy computation.
4. **Cell 3 — run.** `runner.run(fund_paste=fund.paste, save=True)` computes everything.
   - It prints the plan (`PLAN: N KPI tables ...`) once the scopes are built, then one progress line per KPI table, which is one period type with all its roots and cuts: `[kpi_long 1/6 | run 1/11] annual · 3 roots x 3 cuts — took | elapsed | ~left`.
   - Results are shown as each step finishes: `kpi_long`'s overall rows for the latest period of each period type, then the overall YoY / YTD, then each comparable kind's overall comparison.
   - With `output.save_outputs: True`, each table is **saved as soon as its step builds it** (`kpi_long`, then the comparison tables, then the comparable tables, then `scope_diff`), so an interrupted run keeps every table it finished. `save_mode = "initial"` is checked against every table before anything is computed.
5. **Cell 4 — what was written.** Prints `ctx.save_plan`: rows appended / overwritten / skipped per table. If it shows skipped rows and you meant to replace those periods, set `output.allow_overwrite_existing = True`, re-run Cell 1 and re-save with Cell 5.
6. **Cell 5 — RESAVE.** Set `RESAVE = True` to re-save every built table at once: after a `save=False` run, after finishing a step an interrupted Cell 3 did not reach (e.g. `runner.build_comparable_pairs()`), or after changing `allow_overwrite_existing`. Empty tables are skipped, never written empty.
7. **Sample cells.** Scope summary by `scope_origin`, a `kpi_long` sample, the YoY / YTD comparisons (overall and by slice), the comparable-pairs cell (only with `comparable_pairs.enabled`) and the scope diff (only with `scope.run_scope_diff`).
8. **Cell 6 — HTML report.** `runner.build_html_report(local_dir=".")` writes the standalone offline HTML file (enabled by default, `html_report.enabled`) and shows a link to it.

### HTML only mode

Set `run.mode: "html_only"` (or `KPI_RUN_MODE=html_only`) to skip the pipeline and render the report from previously saved Delta outputs. Cell 3 then loads `kpi_long` and the comparison tables from `{PATH_OUTPUT_ROOT}/{table}/run_date={OUTPUT_RUN_DATE}/` instead of computing; Cell 2, the Scope debug cell and the save cells do not apply. Set `output.run_date` to load a different snapshot. See [docs/HTML_REPORT.md](docs/HTML_REPORT.md).

## Where outputs go

- **Delta tables** under `PATH_OUTPUT_ROOT`, partitioned by `run_date` (default `reporting_window.as_of_date`):

  ```
  {bucket}/{output.path_segments}/{table_name}/run_date={as_of_date}/
  ```

  Default example: `.../analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`. Nothing is written unless `output.save_outputs` is on.
- **HTML report**: written to the notebook's working directory (`local_dir="."`), named by `html_report.filename` (default `kpi_report_{customer}_{report_end}.html`); also copied to the datastore when `html_report.output_path_segments` is set.

Save modes (`initial`, `incremental`, `full_refresh`), merge keys and typical workflows (first backfill, weekly refresh, re-running a saved week) are in [docs/OUTPUTS.md](docs/OUTPUTS.md).

## Documentation

| Doc | Contents |
| --- | -------- |
| [docs/CONFIG.md](docs/CONFIG.md) | Every config section and key: purpose, values, defaults, interactions, examples; environment variable overrides |
| [docs/LOGIC_FLOW.md](docs/LOGIC_FLOW.md) | How source tables become KPI numbers: stages, data sources, scope, metrics, comparisons, comparable pairs, outputs, the tbretail flow; detailed scope, root, comparable and reporting-window rules |
| [docs/METRICS.md](docs/METRICS.md) | Metric definitions, blocked-day and goods-in-transit gates per metric, population filters |
| [docs/OUTPUTS.md](docs/OUTPUTS.md) | Output tables, save modes, merge keys, run metadata, typical workflows, caveats |
| [docs/HTML_REPORT.md](docs/HTML_REPORT.md) | HTML run mode, report contents, `html_report` keys, overriding metric definitions |
| [docs/MODULES.md](docs/MODULES.md) | What each file and `kpi_pipeline/*.py` module does; programmatic use |
| [docs/TBRETAIL.md](docs/TBRETAIL.md) | Notes behind `tbretail_config.py` |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Performance notes, known limitations, symptom / cause table |
| [docs/ORBIT_METRICS_COMPARISON.md](docs/ORBIT_METRICS_COMPARISON.md) | This pipeline's metric methodology compared with Orbit's `kpi-review` |

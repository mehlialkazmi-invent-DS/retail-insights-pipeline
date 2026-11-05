# Outputs

What the pipeline produces, where it saves it, and how the save modes behave. Configuration keys are in [CONFIG.md](CONFIG.md#output); the HTML report is described in [HTML_REPORT.md](HTML_REPORT.md).

## What it produces

| Output                                  | Description                                                                                                                                            |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `kpi_long`                              | One tidy table: `period_type` (annual / **ytd** / quarter / half / monthly / weekly; `half` only with `fiscal_calendar.half_periods`), `period`, `root`, `dimension`, `dimension_value`, plus all configured metrics. `root` is `"overall"` plus one per [dimension_source root](LOGIC_FLOW.md#roots-and-cuts-report-structure); `dimension`/`dimension_value` is the cut within that root. Filter it to reproduce any root/cut/period panel. |
| `comparison_yoy / ytd`                  | Prior vs current period with formatted display columns, per root × cut. YoY = last two annual periods; YTD = the **same** elapsed window **across years**, chained across every consecutive year pair (see [Selecting which comparisons to run](CONFIG.md#selecting-which-comparisons-to-run)). With `report_end = "latest_day"` annual periods are complete fiscal years only and YTD is the same fiscal day of every year ([Latest-day report end](LOGIC_FLOW.md#latest-day-report-end-report_end--latest_day)). No QoQ/MoM/WoW table; use the Quarter/Half/Monthly/Weekly `kpi_long` rows for trends. Recomputed from the full merged kpi_long on incremental saves. |
| `scope_diff`                            | Side-by-side annual KPIs for **scope-table-only** vs **score-only** scope (sanity check, only with `scope.run_scope_diff=True`), computed on the scope-table and score scopes; its columns are `Year, metric, scope, score, abs_diff, pct_diff`. Its years are the Annual tab's. |
| `comparable_kpi_long` / `comparable_comparison_{ytd,yoy,quarter}` | Like-for-like metrics over only the pairs present in every qualifying year, per root × cut. Gated on `comparable_pairs.enabled=True`; kinds via `comparable_pairs.kinds` (default `["ytd"]`). See [Comparable pairs](LOGIC_FLOW.md#comparable-pairs-like-for-like-ytd--yoy--quarter--half). |
| **HTML report**                         | Standalone offline tabbed HTML (outer root tab when more than one root), Metric Details, client/period info panel ([HTML report](HTML_REPORT.md#html-report)). |

Delta tables are saved under `PATH_OUTPUT_ROOT`, partitioned by `run_date` per table:

```
{bucket}/{output.path_segments}/{table_name}/run_date={as_of_date}/
```

Default example: `.../analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`

## Output saves

Results persist to Delta under one root:

```
{bucket}/{output.path_segments}/{table_name}/run_date={run_date}/
```

Default: `/mnt/invent-{customer}-datastore/analysis/kpi_reports/outputs/kpi_long/run_date=2026-06-15/`. `run_date` defaults to `reporting_window.as_of_date`; override with `output.run_date` / `KPI_OUTPUT_RUN_DATE` to load a specific snapshot (`html_only` uses the same partition). `output.save_outputs` defaults to `False`; when it is on, the notebook's `run(save=True)` writes each table as soon as its step builds it.

**`kpi_long` is saved in full, never trimmed.** The HTML display limits (`weekly_display_weeks` etc., see [HTML report](HTML_REPORT.md#html-report)) only narrow a separate in-memory `ctx.kpi_long_display`; `ctx.kpi_long` and the save always hold every computed period.

### Tables written

| Table | Contents | Merge keys (incremental mode) |
| ----- | -------- | ----------------------------- |
| `kpi_long` | All metrics × periods × slices (annual / ytd / quarter / half / monthly / weekly) | `period_type`, `period`, `root`, `dimension`, `dimension_value` |
| `comparison_yoy` | YoY comparison rows | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` |
| `comparison_ytd` | YTD comparison rows (one row set per consecutive-year pair, elapsed-window sums) | same as YoY |
| `scope_diff` | Scope vs score annual diff (when `run_scope_diff=True`) | `Year`, `metric` |
| `comparable_kpi_long` | Comparable (like-for-like) per-link metrics for every enabled kind + `comparable_pair_count` + `comparison_type` (when `comparable_pairs.enabled=True`) | `comparison_type`, `period_type`, `period`, `root`, `dimension`, `dimension_value`, `link_prior_year`, `link_current_year` |
| `comparable_comparison_ytd` / `_yoy` / `_quarter` / `_half` | Comparable comparison rows for that kind (only when `comparable_pairs.enabled=True` and the kind is in `comparable_pairs.kinds`) | `comparison_type`, `root`, `dimension`, `dimension_value`, `metric_key`, `current_period` (same shape as YoY/YTD; `_quarter` additionally keys on `quarter_number`, `_half` on `half_number`); usually recomputed from merged `comparable_kpi_long` and overwritten wholesale |


Merge keys are defined in `kpi_pipeline/io.py` (`TABLE_ROW_KEYS`). `comparison_*` tables are recomputed from the merged `kpi_long` and overwritten wholesale; `comparable_kpi_long` is merged like `kpi_long` and each enabled kind's `comparable_comparison_{kind}` is recomputed from it ([Comparable pairs](LOGIC_FLOW.md#comparable-pairs-like-for-like-ytd--yoy--quarter--half)). There is no `comparison_qoq`/`_mom`/`_wow` table.

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

1. **Cell 3** — `runner.run(fund_paste=fund.paste, save=True)` computes KPIs and, with `save_outputs: True`, writes each table **as soon as its step builds it**: `kpi_long` after the KPI step, the comparison tables after the comparisons, the comparable tables after the comparable step, `scope_diff` last. A slow or interrupted later step never loses a finished table. `save_mode='initial'` is checked against every table **before** anything is computed. Cell 3 binds `ctx = runner.ctx` first, so the later cells still see what an interrupted run built. Results and progress print inline as steps finish (see [Programmatic use](MODULES.md#programmatic-use)).
2. **Cell 4** — prints what Cell 3 wrote (`ctx.save_plan`: append / overwrite / skip counts per table).
3. **Cell 5** — `save_outputs(ctx, fund.paste)` re-saves every built table at once, only when `RESAVE = True`: after a `save=False` run, after finishing a step an interrupted Cell 3 did not reach (e.g. `runner.build_comparable_pairs()`), or after changing `allow_overwrite_existing`. Empty tables are skipped, never written empty.

If Cell 4 shows skipped rows and you meant to replace those periods, set `allow_overwrite_existing=True`, re-run Cell 1 and re-save with Cell 5.

**Progress.** Once the scopes are built, Cell 3 prints the plan, `PLAN: N KPI tables (kpi_long … + comparable … + scope_diff …)`. A KPI table is one period type with every root × cut computed in one aggregation (one Spark `toPandas`, the run's main cost); comparable plans ytd / yoy 1 build, quarter 4, half 2, each build covering every root × cut. Each stage prints a header, and each table a line such as `[kpi_long 1/6 | run 1/11] annual · 3 roots x 3 cuts — took | elapsed | ~left`. A comparable build that is skipped (fewer than 2 qualifying years, no common pair) is dropped from the plan with a printed line. The time left is the average per table so far, so it is rough; the first kpi_long table also pays for building the cached frames.

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
- **`sales_basis` must not change under incremental:** `kpi_long` and every comparison table carry no basis column, so rows saved under `"net"` and rows saved under `"gross"` would merge into one table with mixed meaning. When you change `sales_basis`, use `output.save_mode = "full_refresh"` (the run prints a note under `incremental`).
- **Notebook vs saved Delta:** under `allow_overwrite_existing=False`, overlapping `kpi_long` keys keep prior saved values (e.g. a stale partial-year annual total survives a narrower re-run). Enable overwrite or use `full_refresh`.
- **`report_end = "latest_day"`:** `ytd` rows are always replaced on incremental merge and counted as `overwrite` in the plan ([Latest-day report end](LOGIC_FLOW.md#latest-day-report-end-report_end--latest_day)).
- **Empty outputs are skipped:** an empty table (e.g. comparisons on a very narrow window) is not written and prior Delta data stays, even under `full_refresh`.

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

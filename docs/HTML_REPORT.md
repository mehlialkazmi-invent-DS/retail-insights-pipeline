# HTML report

Cell 6 of `main.ipynb` generates a standalone, offline HTML file after the pipeline run (or after loading saved outputs in `html_only` mode). Enabled by default (`html_report.enabled`).

## Run mode: HTML only from saved data

Set `run.mode: "html_only"` in `config.py` to skip the full pipeline and render the HTML report from previously saved Delta outputs:

```python
"run": {"mode": "html_only"},
"output": {"path_segments": ["analysis", "kpi_reports", "outputs"]},
```

Requires `kpi_long` and comparison tables at `{PATH_OUTPUT_ROOT}/{table}/run_date={OUTPUT_RUN_DATE}/`. Cell 3 loads them via `runner.run()`; Cell 6 renders HTML. Set `output.run_date` to load a different snapshot.

Environment override: `KPI_RUN_MODE=html_only`

## What the report contains

| Section | Description |
| ------- | ----------- |
| **Executive header** | Client, reporting window, as-of date, scope mode, slice dimensions, generated timestamp; with `report_end = "latest_day"` also a **Period basis** card (YTD to the report end, complete periods elsewhere, lost sales through the last Saturday) |
| **Period tabs** | Annual / **YTD** / Quarter / Half / Monthly / Weekly (horizontal; Half only with `fiscal_calendar.half_periods`); every tab label renders upper case (ANNUAL, YTD, ..., METRIC DETAILS) |
| **Slice dimension tabs** | Overall + every slice column in `kpi_long` (inferred from data and config) |
| **Value tabs** | Vertical sidebar within each slice dimension, one panel per value (e.g. each brand) |
| **KPI tables** | Metrics as rows (colour-coded), periods as columns; inventory turnover is labelled **Annual** / **YTD** / **Quarterly** / **Half-Yearly** / **Monthly** / **Weekly** per tab |
| **Comparison** | YoY / YTD per value panel, on the Annual/YTD tabs only: one wide value+delta table (the KPI table's period columns plus one delta column per consecutive-year link; YoY exactly one) in place of the plain value table. Quarter/Half/Monthly/Weekly tabs show the plain value-trend table. The change of a WOS metric is computed from the displayed whole-week (floored) values, so 23 vs 23 shows `+0.0%`. |
| **Comparable (Like-for-Like)** | With `comparable_pairs.enabled=True`, a separated section under the comparison table per enabled kind (see [Comparable pairs](LOGIC_FLOW.md#comparable-pairs-like-for-like-ytd--yoy--quarter--half)). |
| **Metric Details tab** | Definition, store scope and formula for every active metric, centered and in business wording. A **Methodology tab** before it (`html_report._methodology_html`) explains in business wording how the report is built: cards for sales basis, what is included, period, views, blocked days, stock in transit, in-stock rules, lost sales and like-for-like, then a per-metric table of what it is based on, its filters and conditions, blocked days removed (Yes / No) and stock in transit included (Yes / No). Both are generated from the run's settings, so they need no config and follow every change; each line is a fixed sentence switched on by its setting. Under `sales_basis = "gross"` Sales Revenue and Sales Units say they are gross (`transactional_sales` rows passing `input_filters.transactional_sales`, on every transactional day) and the metrics that divide by or weight with sales (AUR, AUC, WOS, turnover, weighted in-stock, lost sales %) add "Sales are gross (before returns)"; In-Stock Rate is described from `instock.daily` when `instock.method = "daily"`; blocked-day exclusion is mentioned only for metrics named in `blocked_scope.metrics`, and a metric named in `goods_in_transit.inventory_metrics` states that it counts store / DC goods in transit; Lost Sales % states the last-Saturday basis under `latest_day`. |

Slice dimensions and values are **inferred from `kpi_long`**, so a different slice column (e.g. `category`) needs no code change.

## Config keys

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

`filename` may use only `{customer}` and `{report_end}` (validated in `materialize()`; `KPIRunner.build_html_report` formats the template `HTML_REPORT_FILENAME_TEMPLATE` with the final `REPORT_END_DATE`); `output_path_segments` is the datastore **folder** (`HTML_REPORT_OUTPUT_DIR`; the filename is appended). `root_labels` renames root tabs (unlisted roots fall back to `Overall` / the root id; tbretail: `comp` -> `LFL`, `nvrout` -> `NVROUT`). `dimension_labels` renames a slice dimension wherever shown (dimension tabs, header card; tbretail: `brand` -> `Banner`), display only (`kpi_long` and saved outputs keep the raw key); `materialize()` raises unless it is a `str -> str` dict. Table cells are center-aligned and every tab label (root, period, dimension, value and Metric Details tabs) is upper-cased (`html_report._tab_label`: `annual` -> `ANNUAL`, `jab` -> `JAB`, `Metric Details` -> `METRIC DETAILS`) for every client; display only, the header's slice-dimensions card and other text keep their case.

## Environment variable overrides

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

## Overriding metric definitions

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

## Programmatic use

```python
html_path = runner.build_html_report(local_dir=".")
```

Or call the renderer directly:

```python
from kpi_pipeline.html_report import render_kpi_html, DEFAULT_METRIC_DEFINITIONS
render_kpi_html(ctx, "/dbfs/mnt/.../report.html", report_title="My KPI Report")
```

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
| **Banner x Division tab** | With `html_report.banner_division_tab.enabled`, one extra tab in every root (see [Banner x Division tab](#banner-x-division-tab)) |
| **Value tabs** | Vertical sidebar within each slice dimension, one panel per value (e.g. each brand) |
| **KPI tables** | Metrics as rows (colour-coded), periods as columns; inventory turnover is labelled **Annual** / **YTD** / **Quarterly** / **Half-Yearly** / **Monthly** / **Weekly** per tab |
| **Comparison** | YoY / YTD per value panel, on the Annual/YTD tabs only: one wide value+delta table (the KPI table's period columns plus one delta column per consecutive-year link; YoY exactly one) in place of the plain value table. Quarter/Half/Monthly/Weekly tabs show the plain value-trend table. The change of a WOS metric is computed from the displayed whole-week (floored) values, so 23 vs 23 shows `+0.0%`. |
| **Comparable (Like-for-Like)** | With `comparable_pairs.enabled=True`, a separated section under the comparison table per enabled kind (see [Comparable pairs](LOGIC_FLOW.md#comparable-pairs-like-for-like-ytd--yoy--quarter--half)). |
| **Metric Details tab** | Definition, store scope and formula for every active metric, centered and in business wording. A **Methodology tab** before it (`html_report._methodology_html`) explains in business wording how the report is built: cards for sales basis, what is included, period, views, blocked days, stock in transit, in-stock rules, lost sales and like-for-like, and a "Reading the trends" card (the report is an observational study, not a controlled experiment: no control group, so changes over time are associations; like-for-like, shown only with `comparable_pairs.enabled`, removes the mix effect of items entering or leaving but not season, promotions, assortment, pricing or the market; a daily scope applies the current scope list to every day), then a per-metric table of what it is based on, its filters and conditions, blocked days removed (Yes / No) and stock in transit included (Yes / No). Both are generated from the run's settings, so they need no config and follow every change; each line is a fixed sentence switched on by its setting. Under `sales_basis = "gross"` Sales Revenue and Sales Units say they are gross (`transactional_sales` rows passing `input_filters.transactional_sales`, on every transactional day) and the metrics that divide by or weight with sales (AUR, AUC, WOS, turnover, weighted in-stock, lost sales %) add "Sales are gross (before returns)"; In-Stock Rate is described from `instock.daily` when `instock.method = "daily"`; blocked-day exclusion is mentioned only for metrics named in `blocked_scope.metrics`, and a metric named in `goods_in_transit.inventory_metrics` states that it counts store / DC goods in transit; Lost Sales % states the last-Saturday basis under `latest_day`. The Methodology tab ends with a **data section** (`html_report._coverage_html`, built by `inputs.collect_data_coverage` on the first report of a run): per source its table, the first and last date of each measure it carries (daily data: rows kept by the filters, sales, inventory; lost sales weeks and weeks with lost sales above 0; DC inventory; goods in transit by destination, dated by the day each snapshot describes (`date_shift_days`); transactional sales under gross; blocked days; a weekly scope's weeks) and the `input_filters` in force, printed as configured. A daily scope has no dates (one scope applied back over the window), so it shows the pair start dates. Dates are clipped to the report window (`EFFECTIVE_REPORT_START_DATE` to `REPORT_END_DATE`), so a source that starts late or ends early inside it stands out; history before it, loads after it and forecast weeks are left out; pair start dates are not clipped. One aggregation per source, run when the report of a full run is built. `html_only` leaves the data section out: reading the sources then would date them by the day the HTML is built, not by the saved run. |

Slice dimensions and values are **inferred from `kpi_long`**, so a different slice column (e.g. `category`) needs no code change.

## Config keys

```python
"html_report": {
    "enabled": True,
    "filename": "kpi_report_{customer}_{report_end}.html",
    "report_title": "KPI Report",        # None / empty = "<CUSTOMER> KPI Report"
    "output_path_segments": None,        # None = local only; or path segments to also save to datastore
    "metric_definitions": {},            # override DEFAULT_METRIC_DEFINITIONS entries
    "weekly_display_weeks": 5,           # Weekly tab: N most recent weeks; null = all
    "monthly_display_months": 5,         # Monthly tab: N most recent months; null = all
    "quarterly_display_quarters": 5,     # Quarter tab: N most recent quarters; null = all
    "half_display_halves": 4,            # Half tab: N most recent halves; null = all
    "yearly_display_years": None,        # Annual tab: N most recent years; None = all
    "root_labels": {},                   # root id -> tab label, e.g. {"comp": "LFL", "nvrout": "NVROUT"}
    "dimension_labels": {},              # slice dimension name -> tab label (display only), e.g. {"brand": "Banner"}
    "banner_division_tab": {             # Banner x Division tab (see below); disabled in the template
        "enabled": False,
        "label": "Banner x Division",    # tab label
        "banner_dimension": "example_derived",             # slice dimension whose values are the banners (the total column)
        "banner_division_dimension": "example_banner_division",  # derived dimension: <banner><separator><division>
        "separator": " | ",              # the one slices.derived_dimensions[banner_division_dimension] joins with
        "banner_order": [],              # banners first, in this order; the rest sorted
        "division_order": [],            # divisions first, in this order; the rest in sorted code order
        "division_labels": {},           # division value -> column title; unlisted show the raw value
        "quarters": 4, "halves": 4, "years": 2,  # recent complete periods shown; independent of *_display_*
    },
}
```

`filename` may use only `{customer}` and `{report_end}` (validated in `materialize()`; `KPIRunner.build_html_report` formats the template `HTML_REPORT_FILENAME_TEMPLATE` with the final `REPORT_END_DATE`); `output_path_segments` is the datastore **folder** (`HTML_REPORT_OUTPUT_DIR`; the filename is appended). `root_labels` renames root tabs (unlisted roots fall back to `Overall` / the root id; tbretail: `comp` -> `LFL`, `nvrout` -> `NVROUT`). `dimension_labels` renames a slice dimension wherever shown (dimension tabs, header card; tbretail: `brand` -> `Banner`), display only (`kpi_long` and saved outputs keep the raw key); `materialize()` raises unless it is a `str -> str` dict. Table cells are center-aligned and every tab label (root, period, dimension, value and Metric Details tabs) is upper-cased (`html_report._tab_label`: `annual` -> `ANNUAL`, `jab` -> `JAB`, `Metric Details` -> `METRIC DETAILS`) for every client; display only, the header's slice-dimensions card and other text keep their case.

## Banner x Division tab

`html_report.banner_division_tab` adds one tab to every root (Overall / NVROUT / LFL), a peer of the period tabs. It has two rows of CSS-only radio sub-tabs (no JavaScript): one per **banner**, in `banner_order` (tbretail: KNG, TMW, JAB, MSP; the first checked; each shows its division count), and one per period kind: **Quarters** (checked by default) / **Halves** / **YTD** / **Years**, in that order; a kind without periods gets no sub-tab, and the kind choice carries across banners. The controls stay pinned at the top while scrolling. Each panel shows two wide tables for the chosen banner and kind, the root's values and then its like-for-like values (below). In each: metrics are rows; the **Banner total** block is sticky on the left (`position: sticky; left: 0`, with the metric names) and scrolls with nothing else, then one block of period columns per division scrolls right (tbretail 2026-11-07, Overall and LFL: KNG 20 divisions, TMW 7, JAB 6, MSP 7; NVROUT has no KNG and 2 divisions per other banner; the products table lists more codes, e.g. 29 for KNG, but only divisions with rows in `kpi_long` appear). Showing only one kind's columns keeps the sticky pane about 500-650px wide so several divisions stay visible on a laptop screen. Numbers are right-aligned with tabular figures, Δ columns are shaded and coloured by sign.
- **Like-for-like** (below the values): the comparable-pairs section of the period tabs, per banner total and division. For each shown period with a like-for-like link (the same period a year earlier), three columns: the prior period's value, the current period's value (both over the products and stores present in both periods; the prior one greyed) and the change, from `comparable_comparison_yoy` / `_ytd` / `_quarter` / `_half` of the root. A division without comparable pairs shows `—`; the section note counts the divisions that have them (tbretail 2026-11-07: KNG 17 of 20, every TMW, JAB and MSP division). A kind missing from `comparable_pairs.kinds` (tbretail: `quarter`, `half`) shows a note to add it and rerun; no rerun is needed for kinds already saved, `run.mode = "html_only"` renders them.

- **Periods**: the most recent complete `quarters` (4), `halves` (4) and `years` (2; YTD shows the same number of years), independent of the `*_display_*` keys. The tab reads the untrimmed `kpi_long`, which holds complete periods only, the same rule as the Quarter / Half / Annual tabs. Years also get a `Δ <year>` column per consecutive-year link shown, the YoY change exactly as the Annual tab prints it (`comparison_yoy`); quarters and halves show values only, as at banner level. YTD gets the same `Δ` columns from `comparison_ytd` when `comparisons.enabled` includes `ytd` (tbretail: not enabled, so values only).
- **Hide / show periods**: in each sub-tab a "Show periods" chip row of CSS-only checkboxes (no JavaScript), one per period of that kind; unticking a period hides its columns in every block and banner of that sub-tab, its like-for-like columns included (a year's `Δ` column goes with the year).
- **Data**: config only. `banner_division_dimension` is a `slices.derived_dimensions` entry that joins the banner and the division with `separator`, e.g. tbretail `CONCAT(brand, ' | ', product_group_3_code)`; the banner total is the `banner_dimension` row of the same banner (tbretail `brand`: KNG, TMW, JAB or MSP). The brand prefix of the division code is kept (`JAB_DIV_10`, `MSP_DIV_10`, `KNG_DIV_10`). The derived dimension is hidden from the dimension tabs and the "Split by" line while the tab is enabled. It is one more cut computed for every period type, so it adds compute for each; disable the tab to drop the cost (and remove the dimension).
- **Order and names**: divisions sort by code, `division_order` lists codes to put first; `division_labels` maps the division value (the part after the separator, e.g. `JAB_DIV_10`) to a column title, unlisted codes show the raw code (tbretail leaves the map empty).
- **Validation** (`materialize()`): when enabled, both dimensions must be in `slices.dimensions` / `slices.derived_dimensions`, the banner x division one must be derived and its SQL must contain `separator`, the counts must be positive integers, the orders lists of strings, `division_labels` a `str -> str` dict, or it raises. The renderer raises when the dimension has no `kpi_long` rows (rerun the pipeline after adding it).

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
| `KPI_HTML_YEARLY_YEARS` | Recent years in Annual tab (default all; empty = all) |
| `KPI_HTML_BANNER_DIVISION_ENABLED` | `true`/`false`: `banner_division_tab.enabled` |
| `KPI_HTML_BANNER_DIVISION_QUARTERS`, `KPI_HTML_BANNER_DIVISION_HALVES`, `KPI_HTML_BANNER_DIVISION_YEARS` | Periods shown on the Banner x Division tab (integers) |

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

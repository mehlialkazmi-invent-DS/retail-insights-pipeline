---
name: new-client-kpis-toolkit
description: >-
  Scaffold a brand-new copy of the generate-kpis-toolkit for a new retail
  client on Databricks.  Collects client parameters via a short intake,
  creates a new client folder, writes a correctly configured config.py, and
  points to the operate skill for day-2 work.
  Use when a user says: "set up KPI toolkit for a new client", "create a new
  client KPI folder", "onboard <client> to the KPI pipeline", or similar.
---

# Scaffold generate-kpis-toolkit for a New Client

Source toolkit: `generate-kpis-toolkit/`  
After scaffolding, the operate skill covers all day-2 work:  
`.cursor/skills/generate-kpis-toolkit/SKILL.md`

---

## Step 0 — Intake (ask the user; resolve from context if already stated)

Collect the following before generating any files.  Ask at most **2 questions at once**.

### Required

| Parameter | Question to ask | Example |
|-----------|----------------|---------|
| `customer` | What is the client slug (lowercase, no spaces)? | `newretail` |
| `as_of_date` | What is the anchor date for the first run? (`YYYY-MM-DD`) | `2026-06-15` |
| `excluded_store_ids` | Which store IDs are e-com / non-service stores to exclude from WOS/instock metrics? (comma-separated integers, or `[]`) | `[829, 639]` |
| `defined_scope_path_segments` | What are the Delta path segments for the instock scope table under `{bucket}`? (comma-separated) | `analysis, instock_rate, instock_rate_scope` |
| `defined_scope_cols` | Column names in the scope table for product, store, and date. Example: `product_id, store_id, week_start_date` | — |

### Optional (use safe defaults if not provided)

| Parameter | Default | Notes |
|-----------|---------|-------|
| `run_min_date` | `null` (full YTD) | Narrow start for the first run |
| `use_hybrid_scope` | `True` | `False` = defined scope only |
| `lost_sales_model_id` | `top_down_excluding_ecom` | Changes `path_segments.lost_sales` model suffix |
| `slice_dimensions` | `["brand"]` | Columns from the products master table |
| `save_outputs` | `False` | Set `True` when ready to persist |
| `html_enabled` | `True` | `False` to skip HTML report |
| `output_path` | `["analysis", "kpi_reports", "outputs"]` | Under the datastore bucket |
| `destination_folder` | `{customer}-kpis-toolkit/` | Where to create the new client folder |

---

## Step 1 — Resolve parameters

Infer anything that can be resolved from context (open files, git branch, recent messages).  
Set defaults for anything not specified.  Show the user a summary before writing any files:

```
Client slug:          {customer}
Destination folder:   {customer}-kpis-toolkit/
Datastore bucket:     /mnt/invent-{customer}-datastore
As-of date:           {as_of_date}
Run min date:         {run_min_date or "null (full YTD)"}
Scope mode:           {"hybrid" if use_hybrid_scope else "defined only"}
Defined scope path:   {bucket}/{defined_scope_path_segments}
Defined scope cols:   product={product_col}, store={store_col}, date={date_col}
Excluded store IDs:   {excluded_store_ids}
Slice dimensions:     {slice_dimensions}
Save outputs:         {save_outputs}
HTML report:          {html_enabled}
```

Ask for confirmation or corrections before proceeding.

---

## Step 2 — Create the client folder

```
{customer}-kpis-toolkit/
├── config.py        ← write from template below
├── main.ipynb       ← copy from generate-kpis-toolkit/main.ipynb
└── kpi_pipeline/    ← copy entire directory from generate-kpis-toolkit/kpi_pipeline/
```

**To copy files**: use the Shell tool:
```bash
cp -r generate-kpis-toolkit/kpi_pipeline \
      {customer}-kpis-toolkit/kpi_pipeline

cp generate-kpis-toolkit/main.ipynb \
   {customer}-kpis-toolkit/main.ipynb
```

Then write `config.py` from the template below.

---

## Step 3 — Write config.py

Fill in the template with the collected parameters.  Use the Write tool.

```python
# Edit CONFIG below, then in main.ipynb:  %run ./config  ->  settings = materialize(fund.paste)

import copy
import datetime
import os
from typing import Any, Callable, Dict, Optional

CONFIG: Dict[str, Any] = {
    "customer": "{customer}",
    "reporting_window": {
        "as_of_date": "{as_of_date}",
        "run_min_date": {run_min_date_repr},   # null/"" = full YTD from Jan 1
    },
    "service_metrics": {
        "excluded_store_ids": {excluded_store_ids},
    },
    "fiscal_calendar": {
        "use_fiscal_calendar": True,
        "daily_time_columns": {
            "date": "date",
            "year": "year",
            "week": "week",
        },
    },
    "score_scope": {
        "min_percentile": 0.2,
        "min_weeks_for_filter": 2,
    },
    "scope": {
        "use_hybrid_scope": {use_hybrid_scope},
    },
    "scope_adjustments": {
        "additions": [
            {
                "enabled": False,
                "label": "manual_add",
                "source": "delta",
                "path_segments": ["analysis", "kpi_toolkit", "manual_scope_additions"],
                "join_keys": ["product_id", "store_id"],
                "product_col": "product_id",
                "store_col": "store_id",
                "date_col": "week_start_date",
                "year_col": None,
                "week_col": None,
            }
        ],
        "removals": [
            {
                "enabled": False,
                "source": "delta",
                "path_segments": ["analysis", "kpi_toolkit", "manual_scope_removals"],
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": "store_id",
                "date_col": None,
                "year_col": None,
                "week_col": None,
            }
        ],
    },
    "path_segments": {
        "fiscal": ["one_time_uploads", "fiscal_cal"],
        "daily_data": ["noob", "daily-data"],
        "products": ["master-data", "products"],
        "lost_sales": ["noob", "lost-sales", "model_id={lost_sales_model_id}"],
        "defined_scope": {defined_scope_path_segments_list},
    },
    "defined_scope": {
        "product_col": "{product_col}",
        "store_col": {store_col_repr},
        "date_col": {date_col_repr},
        "year_col": {year_col_repr},
        "week_col": {week_col_repr},
    },
    "input_filters": {
        "defined_scope": [],
        "lost_sales": [],
        "daily_data": [],
    },
    "slices": {
        "dimensions": {slice_dimensions},
        "derived_dimensions": {},
    },
    "metrics": {
        "metric_cols": [
            "total_sales_quantity",
            "total_sales_revenue",
            "AUR",
            "total_inventory",
            "distinct_product_count",
            "distinct_store_count",
            "distinct_pair_count",
            "mean_stock",
            "mean_stock_retail",
            "mean_stock_cost",
            "WOS",
            "wos_revenue",
            "wos_cost",
            "inventory_turnover_rate",
            "in_stock_rate",
            "lost_sales_pct",
        ],
        "key_metrics": [
            "total_sales_quantity",
            "total_sales_revenue",
            "total_inventory",
            "distinct_product_count",
            "distinct_pair_count",
            "WOS",
            "in_stock_rate",
            "lost_sales_pct",
        ],
        "labels": {
            "total_sales_revenue": "Sales Revenue",
            "total_sales_quantity": "Sales Units",
            "AUR": "AUR",
            "in_stock_rate": "In-Stock Rate",
            "total_inventory": "Total Inventory",
            "mean_stock": "Daily stock avg (M units)",
            "mean_stock_retail": "Daily stock avg retail (M $)",
            "mean_stock_cost": "Daily stock avg cost (M $)",
            "WOS": "WOS (units)",
            "wos_revenue": "WOS revenue",
            "wos_cost": "WOS cost",
            "inventory_turnover_rate": "Inventory Turnover Rate",
            "lost_sales_pct": "Lost Sales %",
            "distinct_product_count": "Distinct products",
            "distinct_store_count": "Distinct stores",
            "distinct_pair_count": "Distinct pairs",
        },
        "pp_change_metrics": ["in_stock_rate", "lost_sales_pct"],
    },
    "output": {
        "save_outputs": {save_outputs},
        "path_segments": {output_path_segments_list},
        "save_mode": "incremental",
        "allow_overwrite_existing": False,
    },
    "html_report": {
        "enabled": {html_enabled},
        "filename": "kpi_report_{customer}_{report_end}.html",
        "report_title": None,
        "output_path_segments": None,
        "metric_definitions": {},
    },
}
```

After the CONFIG dict, copy **verbatim** the helper functions and `materialize()` from  
`generate-kpis-toolkit/config.py` (lines after the closing `}`).  
Do not re-implement them.

---

## Step 4 — Validate

Tell the user to:

1. Upload the new folder to Databricks (same three items: `config.py`, `main.ipynb`, `kpi_pipeline/`).
2. Run Cell 1 (`%run ./config; settings = materialize(fund.paste)`) to check paths and dates.
3. Run Cell 2 to preview inputs — verify the scope table, lost-sales, and daily data load correctly.
4. If Cell 2 passes, run Cell 3 (pipeline) and Cell 6 (HTML report).

**Common first-run issues:**

| Issue | Fix |
|-------|-----|
| `defined_scope` empty | Check `path_segments.defined_scope` and `defined_scope.*_col` mapping |
| Lost sales path not found | Check `lost_sales_model_id` — it must match the actual Delta partition |
| Fiscal calendar not found | Confirm `one_time_uploads/fiscal_cal` exists; update `path_segments.fiscal` |
| Score backfills all weeks | Defined scope table is empty for the window — check path and date range |

---

## Step 5 — Hand off to operate skill

After the scaffold is done, tell the user:

> The toolkit is ready. For day-2 configuration (adding slices, manual scope adjustments, output saves, metric customisation, troubleshooting), use the **generate-kpis-toolkit operate skill** at  
> `{customer}-kpis-toolkit/.cursor/skills/generate-kpis-toolkit/SKILL.md`  
> — or copy it from the source toolkit and the AI will follow it automatically.

Optionally copy the operate skill into the new folder:
```bash
cp -r generate-kpis-toolkit/.cursor \
      {customer}-kpis-toolkit/.cursor
```

---

## Parameter resolution cheat-sheet

| User says | Resolves to |
|-----------|------------|
| "no store grain" / "product-level scope" | `store_col: null`, `date_col` still needed |
| "use year/week columns, not date" | `date_col: null`, `year_col: "year"`, `week_col: "week"` |
| "native year/week" | same as above |
| "defined scope only" | `use_hybrid_scope: False` |
| "full YTD" | `run_min_date: null` or `""` |
| "last week only" | `run_min_date: <this week's Sunday>` |
| "365-day model" | `lost_sales_model_id: top_down_365d_excluding_ecom` (confirm exact partition name) |
| "save to Delta" | `save_outputs: True`, `save_mode: "initial"` for first run |
| "no HTML" | `html_enabled: False` |

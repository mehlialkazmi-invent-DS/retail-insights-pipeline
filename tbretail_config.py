# TBretail customer config for the retail-insights-pipeline toolkit.
# Usage (Databricks, same folder as main.ipynb): %run ./tbretail_config
#                                                 settings = materialize(fund.paste)
# See retail-insights-pipeline/README.md for full documentation.
#
# SETUP CHECKLIST (one-time, before first run):
#   1. Export JAB product IDs → CSV.
#      In Databricks, run a quick cell to join the JAB Excel against products and
#      write product_id values to the workspace path below, e.g.:
#
#        from pyspark.sql import functions as F
#        import pandas as pd
#        jab_codes = load_jab_skuloc_itemcodes(NFG_EXCEL_PATH_JAB)  # from kpi_metrics notebook
#        jab_ids = (
#            spark.read.format("delta").load(PATH_PRODUCTS)
#            .filter(F.col("product_code").isin(jab_codes))
#            .select("product_id").distinct()
#        )
#        jab_ids.toPandas().to_csv(
#            "/dbfs/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
#            "KPI-NEW/data/jab_product_ids.csv", index=False
#        )
#      Then flip scope_adjustments.additions[0].enabled = True.
#
#   2. Export NON-COMP (NFG list) product IDs → CSV.
#      In Databricks, run a quick cell to join the NFG Excel against products and
#      write product_id values to the workspace path below, e.g.:
#
#        import pandas as pd
#        from pyspark.sql import functions as F
#        pdf_nfg = pd.read_excel(NFG_EXCEL_PATH)
#        col_ic = [c for c in pdf_nfg.columns if str(c).strip().lower() == "itemcode"]
#        item_col = col_ic[0] if col_ic else pdf_nfg.columns[2]
#        nfg_codes = [
#            c for c in pdf_nfg[item_col].dropna().astype(str).str.strip().unique()
#            if c and c.lower() != "grand total"
#        ]
#        non_comp_ids = (
#            spark.read.format("delta").load(PATH_PRODUCTS)
#            .filter(F.col("product_code").isin(nfg_codes))
#            .select("product_id").distinct()
#        )
#        non_comp_ids.toPandas().to_csv(
#            "/dbfs/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
#            "KPI-NEW/data/non_comp_product_ids.csv", index=False
#        )
#      Then flip scope_adjustments.removals[0].enabled = True.
#
#   3. Export NGF item product IDs → CSV (for dimension_sources["ngf_comp_split"]).
#      Unlike step 2 above, this does NOT remove items from scope — NGF items stay in
#      daily_data/scope and in the Overall numbers; they're just flagged so you can
#      slice comp vs non-comp within the same report. Same join pattern as step 1/2:
#
#        import pandas as pd
#        from pyspark.sql import functions as F
#        pdf_ngf = pd.read_excel(NGF_EXCEL_PATH)  # your NGF item list
#        ngf_codes = [str(c).strip() for c in pdf_ngf["itemcode"].dropna().unique()]
#        ngf_ids = (
#            spark.read.format("delta").load(PATH_PRODUCTS)
#            .filter(F.col("product_code").isin(ngf_codes))
#            .select("product_id").distinct()
#        )
#        ngf_ids.toPandas().to_csv(
#            "/dbfs/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
#            "KPI-NEW/data/ngf_product_ids.csv", index=False
#        )
#      Then flip dimension_sources (the "ngf_comp_split" entry).enabled = True.
#      The entry's "fillna": {"is_comp": "yes"} imputes non-NGF products to 'yes' —
#      without it they'd come back NULL, since this CSV only covers NGF items.
#      NOTE: the actually-configured dimension_sources[1].path below currently points at
#      non_comp_ids_20260817.csv (the step-2 NON-COMP file), not ngf_product_ids.csv as
#      exported above — unclear whether that's deliberate reuse or the CSV in step 3 was
#      never actually generated/wired in. Verify before treating either as correct.
#
#   4. Update reporting_window.as_of_date before each run.
#
# WHAT THIS CONFIG PRODUCES (verified against the actual settings below):
#   Scope:       the platform operation/scope (scope_source.mode=operation_scope, solution 21, latest
#                Sunday run, main items only, active products only) at product_store grain
#                (use_hybrid_scope=False, no backfill); JAB additions (additions[0], on); an
#                UNDOCUMENTED second addition unioning NON-COMP / NGF products into scope
#                (additions[1], on — see CAUTION comment at that block, unconfirmed with a human);
#                the nvrout_scope_backfill addition (additions[2], on). Additions are not
#                operation-scope pairs (no family roll-up / active filter, no scope_start, no blocks) and
#                each product-only addition spans every store with a daily row, but they belong to the
#                one scope every metric uses, in-stock included: in-stock counts them from their
#                first daily row. NON-COMP removal itself is OFF (removals[0].enabled=False),
#                despite scope_adjustments' own comment saying NFG/NON-COMP products "are excluded
#                from KPI scope".
#   Lost sales:  read directly from future_visibility's report_dfu (its own pre-blended
#                fast/slow ensemble output) via lost_sales_source — lost_sales_ensemble
#                below is OFF; this repo no longer runs its own fast/slow blend.
#   Blocked:     UI blocked scope (blocked_scope, client rule: a block applies only when it starts on or
#                after the pair's scope start) removes the blocked days from every daily-data-derived
#                metric and from in-stock. lost_sales_source (report_dfu, weekly, no store) is not
#                filtered by it.
#   Instock:     built from noob/daily-data by instock_daily (OH or goods-in-transit, count start =
#                earlier of scope start and first daily row, ECOM stores excluded from in-stock only,
#                scope additions included, counted from their first daily row)
#                instead of report_dfu (instock_source is OFF; its config is kept for a switch back).
#                metrics.population_filters excludes NON-COMP (IS_COMP=='no') from in_stock_rate
#                specifically — even in the unrestricted "overall" root —
#                to match kpi-skill-toolkit's own Overall instock population (nvr_ids ∪
#                comp_ids_primary, NON-COMP excluded by design). NON-COMP still counts toward
#                every other Overall metric (sales, inventory, lost_sales_pct, etc.), same as
#                kpi-skill-toolkit; lost sales excludes ECOM only (via report_dfu and sales_filter).
#   Service metrics (WOS, turnover, mean_stock, instock, lost sales %): every metric uses all
#                scoped stores, except that ECOM stores 829 / 639 / 917 are left out of in-stock
#                only (instock_daily.input_filters). Add a store_id exclusion to
#                input_filters.daily_data to drop a store from every metric.
#   Dimensions:  is_nvrout (NVROUT vs COMP), brand, SMW (KNG vs SMW)
#   Window:      reporting_window.report_end="complete_month": every metric and view ends on the last
#                complete fiscal month; half_periods on (Half tab, H1 = Q1-Q2, H2 = Q3-Q4).
#   Comparisons: YoY / YTD (plus quarter / half period values on their own tabs)
#   Comparable:  like-for-like pairs present in every qualifying year, kinds ytd / yoy / quarter /
#                half, over an all-years pair universe
#   Report:      TBretail KPI Report HTML with all slices and comparable tables (root labels
#                comp -> LFL, nvrout -> NVROUT; wos_revenue / weighted_instock_rate /
#                dc_in_stock_rate removed; dc_instock off, with DC goods in transit and DC blocked
#                scope (solution 22) set for when it is on; dimension_labels brand -> Banner)

import copy
import datetime
import os
from typing import Any, Callable, Dict, Optional

# Period-over-period comparison kinds the pipeline can produce, in canonical order.
# Kept in sync with retail-insights-pipeline/config.py. QoQ/MoM/WoW comparison tables were
# dropped for simplicity — the Quarter/Monthly/Weekly period tabs already show recent-period
# value trends, covering "recent quarters/months/weeks" without a separate delta table.
COMPARISON_KINDS_ALL = ("yoy", "ytd")

# Comparable-pairs (like-for-like) kinds the pipeline can produce, in canonical order. Kept in
# sync with retail-insights-pipeline/config.py -- see its COMPARABLE_KINDS_ALL comment.
COMPARABLE_KINDS_ALL = ("ytd", "yoy", "quarter", "half")

CONFIG: Dict[str, Any] = {
    # =============================================================================
    # IDENTITY & RUN WINDOW
    # =============================================================================
    "customer": "tbretail",
    "run": {
        # full       = compute KPIs from source Delta tables (default)
        # html_only  = load saved outputs from output.path_segments and render HTML only
        "mode": "full",
    },
    "reporting_window": {
        # Update as_of_date before each run. REPORT_END_DATE resolves to the last
        # completed Saturday on or before this date.
        "as_of_date": "2026-08-02",
        "run_min_date": "2025-02-08",  # None = YTD from Jan 1; e.g. "2025-01-01" for multi-year
        # complete_month: the report ends at the last day of the most recent fully elapsed fiscal
        # month on or before the last completed Saturday; every metric and view stops there. The
        # fiscal_cal upload must extend past that Saturday and run_min_date must reach the start of
        # the cut month. See config.py's reporting_window.report_end.
        "report_end": "complete_month",
    },
    # =============================================================================
    # CALENDAR
    # =============================================================================
    "fiscal_calendar": {
        "use_fiscal_calendar": True,
        # Adds the Half tab (H1 = fiscal quarters 1-2, H2 = 3-4) and the "half" comparable kind.
        "half_periods": True,
        # tbretail's fiscal_cal upload columns (used since use_fiscal_calendar=True above). Each
        # is auto-detected/optional -- read when present, derived when absent. tbretail's fiscal
        # year runs Feb-Jan, so fiscal month/quarter numbers don't match the real calendar (fiscal
        # month 07 has been observed spanning real 8/2-8/29) -- month_name_col is read verbatim
        # for the Monthly tab label for exactly that reason, instead of deriving one from the
        # (fiscal, not calendar) month number.
        "column_map": {
            "quarter_col": "Quarter",
            "month_col": "Month",
            "month_name_col": "month_name",
        },
        # Column-name map for the RAW noob/daily-data table -- only consulted on the CIVIL path
        # (use_fiscal_calendar=False, not tbretail's setting above). No "year" key: Year always
        # comes from `date` (F.year(date)), never a raw source year column -- that column can
        # carry the ISO week-year (late-December weeks labelled as the next year). See fiscal.py.
        "daily_time_columns": {
            "date": "date",
            "week": "week",
        },
    },
    # =============================================================================
    # SCOPE & POPULATION
    # =============================================================================
    "score_scope": {
        # Only consulted for the MISSING weeks under hybrid scope (see "scope" below).
        "min_percentile": 0.2,
        "min_weeks_for_filter": 2,
    },
    "scope": {
        # Defined-scope grain (product / product_store / product_store_week) is set in
        # "defined_scope" below. Hybrid only adds a backfill on top of that:
        #   False (default) = final scope is the defined scope as-is.
        #   True (hybrid)   = also backfill (from score scope) the window weeks the defined
        #                     scope leaves uncovered. No-op for the week-agnostic grains
        #                     (product, product_store); meaningful for product_store_week.
        "use_hybrid_scope": False,
        "run_scope_diff": False,
    },
    "defined_scope": {
        # grain — how the scope table defines membership:
        #   "product"            -> distinct product_id (store- and week-agnostic: every store,
        #                           every window week, for each in-scope product).
        #   "product_store"      -> distinct (product_id, store_id); week-agnostic (default).
        #   "product_store_week" -> the scope table's own (product, store, week) rows are honoured
        #                           (strict); weeks come from date_col (or year_col/week_col).
        # Week-agnostic grains span the whole report window; product_store_week is the only grain
        # whose hybrid backfill fills weeks the scope table does not cover.
        # product_store: operation_scope mode (scope_source below) is pair-level, and the daily
        # in-stock / blocked scope need the store dimension.
        "grain": "product_store",
        "product_col": "product_id",
        "store_col": "store_id",        # required for product_store / product_store_week grains
        # date/year/week: read ONLY for product_store_week grain (to resolve the scope's weeks).
        #   DATE path:   date_col -> fiscal_cal -> Year/Week (preferred).
        #   NATIVE path: date_col=None, set year_col/week_col (year must be a true calendar year).
        "date_col": "week_start_date",
        "year_col": None,
        "week_col": None,
        # product_store_week only -- no-op at our current grain ("product_store"). See config.py.
        "backfill_leading_gap": True,
    },
    "scope_adjustments": {
        # ---------------------------------------------------------------------------
        # ADDITIONS: JAB products (from NFG_EXCEL_PATH_JAB)
        # ---------------------------------------------------------------------------
        # These are replenishment products whose product_codes start with "JAB".
        # They are included in scope regardless of the defined scope table.
        # See SETUP CHECKLIST at the top of this file for how to create the CSV.
        "additions": [
            {
                "enabled": True,  # flip to True after creating the CSV
                "label": "jab_products",
                "source": "csv",
                "path": (
                    "/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
                    "KPI-NEW/data/jab_product_ids.csv"
                ),
                "location": "workspace",
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,
                "date_col": None,
                "year_col": None,
                "week_col": None,
            },
            # CAUTION -- UNCONFIRMED, needs a decision before this repo is shared further:
            # this entry points at the SAME CSV as the "REMOVALS: NON-COMP products" block
            # below, which says NFG/NON-COMP products "are excluded from KPI scope" -- but
            # placing it here, as an ADDITION with store_col=None, does the opposite: it
            # unions every (product, store, week) selling these products into scope,
            # unconditionally, regardless of removals[0] below (currently disabled). It is
            # not mentioned in the SETUP CHECKLIST above, unlike every other adjustment in
            # this file. This looks like an accidental copy of removals[0] into additions
            # with the label swapped, but that has NOT been confirmed with a human -- do
            # not disable or "fix" this without checking, since it currently affects which
            # products count toward live production KPI numbers.
            {
                "enabled": True,  # flip to True after creating the CSV
                "label": "NGF products",
                "source": "csv",
                "path": (
                    "/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
                    "KPI-NEW/data/non_comp_ids_20260817.csv"
                ),
                "location": "workspace",
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,
                "date_col": None,
                "year_col": None,
                "week_col": None,
            }, 
            
            {
                "enabled": True,
                "label": "nvrout_scope_backfill",
                "source": "csv",          # or "delta" if you have a Delta table instead
                "path": "/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
                    "KPI-NEW/data/nvrout_product_ids.csv",
                "location": "workspace",  # "datastore" if it's a Delta table under the bucket instead
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,        # None = union this product in for ALL stores/weeks, unconditionally
                "date_col": None,
                "year_col": None,
                "week_col": None,
            }
        ],
        # ---------------------------------------------------------------------------
        # REMOVALS: NON-COMP products (from NFG_EXCEL_PATH / NFG Excel list)
        # ---------------------------------------------------------------------------
        # Products on the NFG (Non-Future-Growth) list are excluded from KPI scope.
        # After removal, is_nvrout='yes' = NVROUT segment; is_nvrout='no' = COMP segment.
        # See SETUP CHECKLIST at the top of this file for how to create the CSV.
        "removals": [
            {
                "enabled": False,  # flip to True after creating the CSV
                "source": "csv",
                "path": (
                    "/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
                    "KPI-NEW/data/non_comp_ids_20260817.csv"
                ),
                "location": "workspace",
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,
                "date_col": None,
                "year_col": None,
                "week_col": None,
            }
        ],
    },
    # =============================================================================
    # DATA SOURCES
    # =============================================================================
    "path_segments": {
        "fiscal": ["one_time_uploads", "fiscal_cal"],
        "daily_data": ["noob", "daily-data"],
        # DC/warehouse daily inventory (product_id, warehouse_id, date, inventory) -- backs
        # dc_mean_stock/total_mean_stock/WOS_DC/WOS_TOTAL. No column mapping needed (unlike
        # lost_sales_source/instock_source): source columns are already canonical.
        "inventory_warehouse": ["operation", "inventory_warehouse"],
        # Parent/child item-family map -- superseded/child products (is_main=false) roll up onto
        # parent_id. Used by the inventory_warehouse rollup; read unconditionally.
        "item_family": ["operation", "item_family"],
        # Platform scope table backing scope_source.mode="operation_scope".
        "scope": ["operation", "scope"],
        # Store goods-in-transit snapshots backing instock_daily.git_date_shift_days.
        "goods_in_transit": ["operation", "goods_in_transit"],
        "products": ["master-data", "products"],
        "lost_sales": ["reporting", "future_visibility", "reporting_inv_fc_dfu", "report_dfu"],
        "defined_scope": ["analysis", "instock_rate", "instock_rate_scope"],
        # product_agg_level -> product_id map for lost_sales_source/instock_source's
        # product_agg_level_col (see README).
        "product_planning_level": ["operation", "product_planning_level"],
    },
    "input_filters": {
        # Optional Spark SQL expressions applied when reading each source.
        # NOTE: when lost_sales_ensemble.enabled=True, this "lost_sales" filter list is
        # applied to BOTH the fast (path_segments.lost_sales) and slow
        # (lost_sales_ensemble.slow_path_segments) sources — same schema, same filters.
        # NOT applied while scope_source.mode="operation_scope" (read only in "defined_scope" mode).
        "defined_scope": ["week_start_date < '2026-08-02'"],
        "lost_sales": [],
        "daily_data": ["usable = 1"],
        "inventory_warehouse": [],
        "item_family": [],
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES SOURCE — column mapping for the raw lost-sales table
    # ---------------------------------------------------------------------------
    # path_segments.lost_sales points at report_dfu (future_visibility's own pre-blended
    # fast/slow ensemble output), not the raw noob/lost-sales model tables -- replaces
    # lost_sales_ensemble below (now off) so lost_sales comes from this ONE source instead
    # of retail-insights-pipeline running its own separate blend.
    #
    # in_stock_col/total_days_col below are UNUSED while instock_daily.enabled=True (or
    # instock_source.enabled=True) -- _aggregate_lost_sales_pairweek skips them in those modes.
    # Left populated anyway so this block is self-sufficient if in-stock moves back to this table.
    #
    # product_col="product_id": report_dfu has a genuine, native product_id column (confirmed
    # directly against the live table) -- no DFU/planning-level join needed here at all, so
    # product_agg_level_col stays None. If a future source ever DOES only carry a DFU/
    # planning-level key with a real one-to-many relationship to product_id, set
    # product_col=None and set product_agg_level_col instead -- configure exactly one of the
    # two, never both (product_col always wins when set and present on the source). See README
    # and _map_product_agg_level_to_product_id's docstring (kpi_pipeline/inputs.py), which fails
    # loudly if neither is usable, or if product_agg_level_col is configured but not present.
    #
    # CAUTION -- store_col=None: lost_sales is an absolute count; report_dfu has no store_id,
    # so this pair-week's value gets broadcast across every scoped store of the product and
    # OVER-COUNTS if later summed across stores (same risk documented on instock_source below,
    # but that one is a ratio -- safe there, NOT safe here).
    "lost_sales_source": {
        "week_col": "TY_week_start_date",
        "product_col": None,
        "store_col": None,  # see CAUTION above
        "lost_sales_col": "lost_sales",
        "in_stock_col": "TY_total_days_instock",  # actual, not sim_instock_days
        "total_days_col": "TY_total_day",         # actual, not sim_total_days
        "product_agg_level_col": 'product_agg_level',  # not needed -- report_dfu has a native product_id column
        # Spark SQL expressions narrowing the DAILY-DATA sales that form the OTHER half of
        # lost_sales_pct's denominator (lost_sales / (sales + lost_sales)). Nothing else changes.
        #
        # report_dfu's lost_sales comes from model_id=top_down_excluding_ecom, so the numerator
        # carries no ecom while daily_data's sales still do -- inflating the denominator and
        # reading lost_sales_pct LOW. This excludes the same three ecom stores the model itself
        # drops, copied from the script that produces it (customer-analysis-tbretail's
        # store_replenishment/future_visibility/lost_sales_product_120dayslookback.py:66, and the
        # identical filter in _365dayslookback.py:67). Keep the two in sync if that list changes.
        "sales_filter": ["store_id NOT IN (829, 639, 917)"],
    },
    # ---------------------------------------------------------------------------
    # INSTOCK SOURCE — optional override to read in-stock days from a DIFFERENT table
    # ---------------------------------------------------------------------------
    # OFF for tbretail (instock_daily below replaces it); kept configured for a switch back. When on,
    # it reads report_dfu directly (same table as lost_sales_source), but
    # unlike that block, uses fallback_sources so in-stock reaches back further than
    # TY_ alone would -- LY_/LLY_ backfill weeks that have rolled off TY_'s own trailing
    # window (see README's fallback_sources docs). lost_sales itself stays on
    # lost_sales_source above (its own TY_ column is trusted across the full date range,
    # no LY_/LLY_ backfill needed there -- see that block's own comments). Mutually
    # exclusive with lost_sales_ensemble below (off, no conflict).
    #
    # product_col="product_id": report_dfu has a genuine, native product_id column (confirmed
    # directly against the live table) -- no DFU/planning-level join needed, product_agg_level_col
    # stays None. Same fact as lost_sales_source above (same table); same "configure exactly
    # one of product_col / product_agg_level_col" rule applies here too.
    "instock_source": {
        "enabled": False,  # replaced by instock_daily below; kept configured for an easy switch back
        "path_segments": ["reporting", "future_visibility", "reporting_inv_fc_dfu", "report_dfu"],
        "week_col": "TY_week_start_date",
        "product_col": None,
        "store_col": None,
        "in_stock_col": "TY_total_days_instock",  # actual, not sim_instock_days
        "total_days_col": "TY_total_day",         # actual, not sim_total_days
        "product_agg_level_col": 'product_agg_level',  # not needed -- report_dfu has a native product_id column
        "fallback_sources": [
            {"week_col": "LY_week_start_date", "in_stock_col": "LY_total_days_instock", "total_days_col": "LY_total_day"},
            {"week_col": "LLY_week_start_date", "in_stock_col": "LLY_total_days_instock", "total_days_col": "LLY_total_day"},
        ],
    },
    # ---------------------------------------------------------------------------
    # ITEM FAMILY SOURCE — parent/child product rollup for every DC frame
    # ---------------------------------------------------------------------------
    # Maps a superseded/child product_id (is_main=false) to its current parent_id (see README's
    # "dc_instock" section). Renamed to canonical product_id/parent_id/is_main at read time.
    "item_family_source": {
        "product_col": "product_id",
        "parent_col": "parent_id",
        "is_main_col": "is_main",
    },
    # ---------------------------------------------------------------------------
    # ITEM FAMILY ROLLUP — per-source toggle for the child->parent product_id rollup
    # ---------------------------------------------------------------------------
    # inventory_warehouse defaults ON (preserves the previous always-on behaviour).
    # daily_data defaults ON too -- build_scoped_daily's own join to already-parent-rolled
    # scope_core/ctx.products_attr otherwise silently DROPS any daily-data row still carrying a
    # child/superseded product_id (a pre-existing bug; this toggle fixes it by default). Turning
    # it on can shift historical numbers for any product with a supersede history.
    # lost_sales defaults OFF -- report_dfu already does its own supersede substitution upstream,
    # so a second rollup here would likely be a no-op; kept available as an opt-in safety net.
    # defined_scope defaults OFF too -- our scope source is already rolled to parent product_id
    # upstream. Requires path_segments.item_family whenever any of the four is True.
    "item_family_rollup": {
        "daily_data": True,
        "lost_sales": False,
        "inventory_warehouse": True,
        "defined_scope": False,
    },
    # ---------------------------------------------------------------------------
    # DC INSTOCK — DC in-stock rate from an expanded inventory_warehouse grid
    # ---------------------------------------------------------------------------
    # OFF for tbretail (no DC in-stock metric in the report). When on, each pair's grid starts at its
    # own first stocked day, so a pair ranged at a DC but never once stocked is absent rather than
    # reading 0% (see README's "dc_instock").
    #   git_date_shift_days:       None = stocked means inventory > stock_threshold only. An integer
    #                              also counts days with goods in transit to the DC
    #                              (goods_in_transit destination_type 1, quantity > 0, rolled to the
    #                              family main); a snapshot dated D+1 describes the end of day D -> -1.
    #   blocked_scope_solution_id: None = no DC blocks. An integer (the DC solution, 22 for tbretail)
    #                              removes {blocked_scope.ui_parameters_path}/dc_blocked_scope days of
    #                              that solution by blocked_scope.rule, against each DC pair's
    #                              scope_start from operation/scope of that solution (same run_date /
    #                              roll-up / active filter as scope_source). DC pairs outside it get no
    #                              blocks. Requires blocked_scope.ui_parameters_path.
    # Both are set for tbretail and take effect once "enabled" is turned on.
    "dc_instock": {
        "enabled": False,
        "stock_threshold": 0,  # a day counts as "stocked" when inventory > stock_threshold
        "git_date_shift_days": -1,
        "blocked_scope_solution_id": 22,
    },
    # ---------------------------------------------------------------------------
    # SCOPE SOURCE — which table defines the scope universe
    # ---------------------------------------------------------------------------
    #   "defined_scope"   -> the table at path_segments.defined_scope, read per "defined_scope" above.
    #   "operation_scope" -> the platform scope table (path_segments.scope): solution_id, one
    #                        run_date (None = latest Sunday on or before today), rows open on that
    #                        date (end_date null or >= run_date). Needs defined_scope.grain
    #                        product or product_store. solution_id also selects the solution of the
    #                        blocked_scope snapshot.
    #                        roll_to_family_main: every row is rolled to its family main (no main:
    #                        own product_id), so a store with only a sub-item in scope gets the main.
    #                        Every pair keeps its EARLIEST start_date (scope_start), used by
    #                        blocked_scope and instock_daily.count_start. Block product_ids are not
    #                        rolled: only blocks on the main's own product_id apply.
    #                        active_only: products with is_active = true only.
    # scope_adjustments apply on top of either mode, as before, and input_filters.daily_data and
    # metrics.population_filters still apply. input_filters.defined_scope is read only in
    # "defined_scope" mode: operation_scope mode ignores it.
    # CAUTION: scope_adjustments additions are not operation-scope pairs: they skip roll_to_family_main /
    # active_only, have no scope_start and receive no blocks. A product-only addition (store_col None)
    # becomes every store with a daily-data row in the window. Every metric, in-stock included, uses
    # this one scope with its additions: in-stock counts an addition from its first daily row, with no
    # family roll-up, active or blocked-scope filter.
    "scope_source": {
        "mode": "operation_scope",
        "solution_id": 21,
        "run_date": None,  # "YYYY-MM-DD" Sunday; None = latest Sunday on or before today
        "roll_to_family_main": True,
        "active_only": True,
    },
    # ---------------------------------------------------------------------------
    # BLOCKED SCOPE — UI blocks removed from every daily-data-derived metric
    # ---------------------------------------------------------------------------
    # ON for tbretail (OFF in the generic config: ui_parameters_path None). Reads the UI parameter snapshot
    # {ui_parameters_path}/blocked_scope/{product,product_destination,destination} (parquet;
    # destination_id = store) for scope_source.solution_id.
    # Requires scope_source.mode="operation_scope" (blocks are matched to its pairs).
    #   rule "after_scope_start": a block applies to a pair only when block.start_date >=
    #                             the pair's scope_start (same day: applies); an earlier block is
    #                             ignored (the pair was set up again after it).
    #   rule "all":               every matched block applies.
    # An applied block removes the pair's days from start_date to end_date (null = open-ended),
    # clipped to the report window, from scoped sales/inventory metrics AND daily in-stock.
    # Weekly sources with no per-store/day grain (lost_sales_source, e.g. report_dfu) cannot be
    # filtered per day and are left as they are. Always set ui_parameters_path explicitly: the
    # newest snapshot folder can hold no blocks for this solution.
    "blocked_scope": {
        # Airflow variable ui_parameters_path (the newest folder, 2026-09-30-204511_..., has only
        # solution 51 blocks).
        "ui_parameters_path": "ui-data/parameter_config/2026-09-30-065549_23d44fd8-8e05-475b-835d-8812ffb50b21",
        "rule": "after_scope_start",
    },
    # ---------------------------------------------------------------------------
    # INSTOCK DAILY — in-stock rate built from daily-data (replaces the weekly in-stock read)
    # ---------------------------------------------------------------------------
    # ON for tbretail (OFF in the generic config): in_stock_rate comes from noob/daily-data over the scope pairs
    # (store-level scope grain required) instead of lost_sales_source / instock_source (turn
    # instock_source off). Output keeps the weekly in-stock shape, so metrics and population_filters
    # work unchanged. See README's "instock_daily" section.
    #   git_date_shift_days:  None = in-stock day is on-hand > 0 only. An integer adds store goods-in-transit
    #                         > 0 days (union of days); a snapshot dated D+1 describes the end of day D,
    #                         so the date shifts by -1.
    #   count_start:          where a pair's store-days start: "first_daily_row", "scope_start"
    #                         (operation scope start date) or "earliest" of the two; clipped to the
    #                         window start. Days from there to the window end without a daily-data
    #                         row count as out of stock.
    #   require_daily_data:   drop pairs with no daily-data row at all (never stocked or sold).
    #   history_start:        first day searched for a pair's first daily row (None = window start;
    #                         set earlier so pairs stocked before the window count from its start).
    #   usable_only:          days with usable != 1 leave both store-days and in-stock days.
    #   input_filters:        Spark SQL on product_id / store_id only, applied to the pair universe
    #                         (e.g. drop e-commerce stores from in-stock without touching other metrics).
    # Every scoped pair counts, scope_adjustments additions included: an added pair has no scope_start,
    # so it counts from its first daily row, and it gets no blocks.
    "instock_daily": {
        "enabled": True,
        "git_date_shift_days": -1,
        "count_start": "earliest",
        "require_daily_data": True,
        # report_dfu's earliest week; pairs stocked before the window count from its start.
        "history_start": "2024-01-21",
        "usable_only": True,
        # ECOM stores leave in-stock only (still counted in sales / inventory metrics).
        "input_filters": ["store_id NOT IN (829, 639, 917)"],
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES ENSEMBLE — blend two lost-sales models by product sales speed
    # ---------------------------------------------------------------------------
    # OFF for TBretail: lost_sales_source above now reads report_dfu's already-blended
    # output directly, so this repo no longer needs to run its own fast/slow blend. Left
    # here, disabled, as the fallback path if report_dfu ever stops being usable as a source
    # (e.g. scope/grain concerns -- see lost_sales_source's CAUTION comment above).
    "lost_sales_ensemble": {
        "enabled": False,
        "slow_path_segments": ["noob", "lost-sales", "model_id=top_down_excluding_ecom_365days"],
        # Product sales-speed source. TBretail's speed_cluster_path_segments below is the
        # platform's long-format attributes table (one row per product_id x attribute_name) —
        # speed_cluster_format="long" is the matching shape.
        "speed_cluster_path_segments": ["noob", "product-cluster-attributes-snapshot"],
        "speed_cluster_format": "long",
        "speed_cluster_attribute_name": "sales_speed",
        "speed_cluster_value_col": "product_speed_cluster",  # unused under format="long"
        "fast_mover_clusters": [1, 2, 3],
    },
    # =============================================================================
    # CUTS & DIMENSIONS
    # =============================================================================
    # ---------------------------------------------------------------------------
    # SLICES — dimensions sourced from master-data/products
    # ---------------------------------------------------------------------------
    # brand:     raw column on products table
    # SMW: derived — KNG vs SMW split (same logic as v4 notebook)
    "slices": {
        "dimensions": ["brand"],
        "derived_dimensions": {
            "SMW": "CASE WHEN brand = 'KNG' THEN 'KNG' ELSE 'SMW' END",
        },
        # Restrict which values of a slice dimension appear in the breakdown (that
        # dimension only; Overall and other slices are unaffected). Two shapes:
        #   LIST (include-only): omit -> all incl NULL | [] -> all non-null | ["A","B"] -> only those
        #   DICT (include/exclude): {"include": ["A"]} keep only A | {"exclude": ["A"]} keep the
        #       rest incl NULL | add "keep_null": True/False to force the NULL bucket.
        "value_filters": {},
    },
    # ---------------------------------------------------------------------------
    # DIMENSION SOURCES — NVROUT flag from operation/extended_product
    # ---------------------------------------------------------------------------
    # is_nvrout = 'yes' → NVROUT products
    # is_nvrout = 'no'  → COMP products (once NON-COMP removed via scope_adjustments)
    # is_nvrout = NULL  → products not present in extended_product (check coverage)
    #
    # IMPORTANT: extended_product must have one row per product_id for this flag to
    # be accurate. If a product can have multiple program values across rows, pre-
    # aggregate the table to a single NVROUT membership flag per product_id and
    # point "path" at that table instead of path_segments. The toolkit dedups
    # extended_product by product_id before joining, keeping an arbitrary row.
    "dimension_sources": [
        {
            "enabled": True,
            "label": "extended_product",
            "source": "delta",
            "path_segments": ["operation", "extended_product"],
            "join_key": "product_id",
            "columns": [],
            "derived": {
                # Products absent from extended_product get NULL, not 'no'.
                # If you need a clean yes/no split with no NULLs, make the source
                # cover the full product universe or replace this with a pre-agg table.
                "IS_NVROUT": "CASE WHEN program LIKE '%NVROUT%' THEN 'yes' ELSE 'no' END",
            },
            "fillna": {"IS_NVROUT": "no"},
            # Root "nvrout" = IS_NVROUT=='yes' only ('no'/NULL aren't their own root).
            "root_values": {"IS_NVROUT": {"yes": "nvrout"}},
        },
        # ---------------------------------------------------------------------------
        # NGF list -> COMP vs NON-COMP split (does NOT remove anything from scope;
        # NGF items stay in daily_data/scope and in the Overall numbers).
        #
        # derived SQL runs against the SOURCE table's own rows, before the left join.
        # Since this CSV only lists NGF product_ids, "derived" only ever fires for NGF
        # rows -> is_comp = 'no'. Every other product has no row in this CSV at all, so
        # after the left join it would get NULL, not 'yes' — fillna (below) imputes that
        # NULL to 'yes' instead, giving a clean two-value is_comp split with no NULLs,
        # despite the source only covering one side of the split.
        # ---------------------------------------------------------------------------
        {
            "enabled": True,  # flip to True after creating the CSV
            "label": "ngf_comp_split",
            "source": "csv",
            "path": (
                "/Workspace/Users/mehlial.kazmi@invent.ai/scripts/tickets and tasks/"
                "KPI-NEW/data/non_comp_ids_20260817.csv"
            ),
            "location": "workspace",
            "csv_options": {"header": True, "inferSchema": True},
            "join_key": "product_id",
            "columns": [],
            "derived": {
                "IS_COMP": "'no'",
            },
            "fillna": {"IS_COMP": "yes"},
            # Root "comp" = IS_COMP=='yes' only (NON-COMP isn't its own root).
            "root_values": {"IS_COMP": {"yes": "comp"}},
        },
    ],
    # =============================================================================
    # COMPARISONS
    # =============================================================================
    "comparisons": {
        # Which period-over-period comparisons to compute, print, save, and render.
        # Choose any subset of "yoy", "ytd". kpi_long (the raw per-period metrics, including
        # quarter/monthly/weekly value trends) is always produced in full regardless of this
        # setting — there is no separate QoQ/MoM/WoW comparison table.
        "enabled": ["yoy", "ytd"],
    },
    "comparable_pairs": {
        # Like-for-like: recomputes metrics over only the pairs present in EVERY qualifying year —
        # the same concept as v4's _pairs_same_calendar_years / sameytd. Requires run_min_date to
        # span at least 2 years (e.g. "2024-01-01" for a comparable 2024-vs-2025 link).
        "enabled": True,
        # ytd (existing) + yoy (full window year) + quarter / half (independent per quarter / half
        # number, only years where that quarter / half is fully elapsed count). See config.py's
        # COMPARABLE_KINDS_ALL.
        "kinds": ["ytd", "yoy", "quarter", "half"],
        # "product_store" (default) or "product" -- see config.py's comparable_pairs.grain comment.
        "grain": "product_store",
    },
    # =============================================================================
    # METRICS
    # =============================================================================
    "metrics": {
        "metric_cols": [
            "total_sales_quantity",
            "total_sales_revenue",
            "AUR",
            "AUC",
            "total_inventory",
            "distinct_product_count",
            "distinct_store_count",
            "distinct_pair_count",
            "mean_stock",
            "mean_stock_retail",
            "mean_stock_cost",
            "dc_mean_stock",
            "total_mean_stock",
            "WOS",
            "wos_cost",
            "WOS_DC",
            "WOS_TOTAL",
            "inventory_turnover_rate",
            "in_stock_rate",
            "lost_sales_pct",
        ],
        "scope_diff_metrics": [
            "total_sales_quantity",
            "total_sales_revenue",
            "total_inventory",
            "distinct_product_count",
            "distinct_pair_count",
            "WOS",
            "WOS_DC",
            "WOS_TOTAL",
            "in_stock_rate",
            "lost_sales_pct",
        ],
        "labels": {
            "total_sales_revenue": "Sales Revenue",
            "total_sales_quantity": "Sales Units",
            "AUR": "AUR",
            "AUC": "AUC",
            "total_inventory": "Total Inventory",
            "mean_stock": "Daily stock avg (M units)",
            "mean_stock_retail": "Daily stock avg retail (M $)",
            "mean_stock_cost": "Daily stock avg cost (M $)",
            "dc_mean_stock": "Daily DC stock avg (units)",
            "total_mean_stock": "Daily total stock avg (units)",
            "WOS": "WOS (units)",
            "wos_cost": "WOS cost",
            "WOS_DC": "WOS (DC)",
            "WOS_TOTAL": "WOS (Total)",
            "inventory_turnover_rate": "Inventory Turnover Rate",
            "in_stock_rate": "In-Stock Rate",
            "lost_sales_pct": "Lost Sales %",
            "distinct_product_count": "Distinct products",
            "distinct_store_count": "Distinct stores",
            "distinct_pair_count": "Distinct pairs",
        },
        "pp_change_metrics": ["in_stock_rate", "lost_sales_pct"],
        # ---------------------------------------------------------------------------
        # POPULATION FILTERS — narrow ONE metric's own population, on top of root/cut
        # ---------------------------------------------------------------------------
        # kpi-skill-toolkit's Overall in-stock deliberately excludes NON-COMP products
        # (inst_products = nvr_ids ∪ comp_ids_primary, "# should not have non comp" —
        # kpi_metrics_script_main notebooks). This pipeline's "overall" root applies no
        # restriction of its own, so NON-COMP products (added to scope via the
        # "NGF products" scope_adjustments addition above, NON-COMP removal disabled)
        # were counting toward Overall's in-stock rate — the confirmed cause of the
        # 2026-09-03/04 Overall-instock-only mismatch (COMP/NVROUT matched throughout,
        # since NON-COMP was never part of either of those roots to begin with).
        # IS_COMP=='no' -> NON-COMP (dimension_sources "ngf_comp_split" below);
        # excluding it here brings Overall's instock population in line with
        # kpi-skill-toolkit's, without touching scope, roots, or any other metric.
        "population_filters": {
            "in_stock_rate": {"IS_COMP": {"exclude": ["no"]}},
        },
    },
    # =============================================================================
    # OUTPUT & REPORTING
    # =============================================================================
    "output": {
        "save_outputs": True,
        "path_segments": ["analysis", "tbretail_kpis", "outputs"],
        "run_date": '2026-09-14',
        "save_mode": "full_refresh",
        "allow_overwrite_existing": True,
        "recompute_comparisons_from_history": True,
    },
    "html_report": {
        "enabled": True,
        "filename": "kpi_report_{customer}_{report_end}.html",
        "report_title": "TBretail KPI Report",
        "output_path_segments": None,
        "metric_definitions": {},
        # Show all years to cover the 2024 / 2025 / 2026 multi-year view.
        # Set to 3 or 4 to cap if the report gets too wide.
        "weekly_display_weeks": 5,
        "monthly_display_months": 5,
        "quarterly_display_quarters": 5,
        "half_display_halves": 4,
        "yearly_display_years": None,
        # root id -> tab label; roots not listed fall back to "Overall" / the root id.
        "root_labels": {"comp": "LFL", "nvrout": "NVROUT"},
        # slice dimension name -> tab label, display only (saved outputs keep the dimension name),
        # e.g. {"brand": "Banner"}; dimensions not listed show their name title-cased.
        "dimension_labels": {"brand": "Banner"},
    },
}


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in ("1", "true", "yes")


def _validate_value_filters(value_filters: Dict[str, Any]) -> None:
    """Fail loudly on a malformed value_filters entry.

    Each entry is either a LIST (include-only) or a DICT with any of the keys
    ``include`` / ``exclude`` / ``keep_null``. Anything else is a config error.
    """
    allowed_keys = {"include", "exclude", "keep_null"}
    for dim, spec in value_filters.items():
        if isinstance(spec, (list, tuple)):
            continue
        if isinstance(spec, dict):
            unknown = set(spec) - allowed_keys
            if unknown:
                raise ValueError(
                    f"value_filters[{dim!r}] has unknown key(s) {sorted(unknown)}; "
                    f"allowed keys: {sorted(allowed_keys)}"
                )
            if "keep_null" in spec and not isinstance(spec["keep_null"], bool):
                raise ValueError(f"value_filters[{dim!r}]['keep_null'] must be a boolean.")
            for k in ("include", "exclude"):
                if k in spec and spec[k] is not None and not isinstance(spec[k], (list, tuple)):
                    raise ValueError(f"value_filters[{dim!r}][{k!r}] must be a list.")
            continue
        raise ValueError(
            f"value_filters[{dim!r}] must be a list or a dict with include/exclude/keep_null; "
            f"got {type(spec).__name__}."
        )


def _validate_population_filters(population_filters: Dict[str, Any], metric_cols: list) -> None:
    """Fail loudly on a malformed metrics.population_filters entry.

    Each key must be a real metric_col; each value must be a dict of {dim_col: value_filter_spec},
    where value_filter_spec has the same shape _validate_value_filters already accepts (a list or
    a dict with include/exclude/keep_null).
    """
    known = set(metric_cols)
    for metric_col, dim_spec in population_filters.items():
        if metric_col not in known:
            raise ValueError(
                f"metrics.population_filters has an entry for {metric_col!r}, which is not in "
                f"metrics.metric_cols {sorted(known)}."
            )
        if not isinstance(dim_spec, dict):
            raise ValueError(
                f"metrics.population_filters[{metric_col!r}] must be a dict of "
                f"{{dim_col: value_filter_spec}}; got {type(dim_spec).__name__}."
            )
        _validate_value_filters(dim_spec)


def _validate_scope_diff_metrics(scope_diff_metrics: list, metric_cols: list) -> None:
    """Fail loudly if metrics.scope_diff_metrics names anything outside metrics.metric_cols.

    Unlike population_filters, nothing else validates this list -- build_scope_diff indexes
    a DataFrame with it directly (kpi_pipeline/comparisons.py), so a stale/typo'd name here
    would otherwise surface as a raw KeyError deep inside a scope.run_scope_diff=True run.
    """
    known = set(metric_cols)
    unknown = [m for m in scope_diff_metrics if m not in known]
    if unknown:
        raise ValueError(
            f"metrics.scope_diff_metrics has entr{'y' if len(unknown) == 1 else 'ies'} "
            f"{unknown} not in metrics.metric_cols {sorted(known)}."
        )


def _validate_scope_adjustments(scope_adjustments_cfg: Dict[str, Any]) -> None:
    """Fail loudly on an enabled scope_adjustments entry missing required fields.

    Every other field on an addition/removal entry (product_col, store_col, date_col, ...)
    is read via .get() with a default; join_keys and a source location are not -- they're
    indexed directly in kpi_pipeline/scope.py, so a copy-pasted entry that forgot one of them
    would otherwise KeyError deep inside scope building instead of failing at config load.
    """
    for section in ("additions", "removals"):
        for entry in scope_adjustments_cfg.get(section, []) or []:
            if not entry.get("enabled"):
                continue
            label = entry.get("label", f"scope_adjustments.{section}[unlabeled]")
            join_keys = entry.get("join_keys")
            if not join_keys or not isinstance(join_keys, (list, tuple)):
                raise ValueError(
                    f"scope_adjustments.{section} entry {label!r} is enabled but has no "
                    f"non-empty join_keys list."
                )
            if not entry.get("path") and not entry.get("path_segments"):
                raise ValueError(
                    f"scope_adjustments.{section} entry {label!r} is enabled but has neither "
                    f"'path' nor 'path_segments' set."
                )


def _parse_percentile(raw: str) -> float:
    value = float(raw)
    return value / 100.0 if value > 1 else value


def _apply_env_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Apply optional KPI_* environment variables over CONFIG (see README)."""
    out = copy.deepcopy(cfg)

    if "KPI_CUSTOMER" in os.environ:
        out["customer"] = os.environ["KPI_CUSTOMER"]

    rn = out.setdefault("run", {})
    if "KPI_RUN_MODE" in os.environ:
        rn["mode"] = os.environ["KPI_RUN_MODE"].strip().lower()

    rw = out.setdefault("reporting_window", {})
    if "KPI_AS_OF_DATE" in os.environ:
        rw["as_of_date"] = os.environ["KPI_AS_OF_DATE"]
    if "KPI_RUN_MIN_DATE" in os.environ:
        rw["run_min_date"] = os.environ["KPI_RUN_MIN_DATE"].strip() or None
    if "KPI_REPORT_END" in os.environ:
        rw["report_end"] = os.environ["KPI_REPORT_END"].strip().lower()

    fc = out.setdefault("fiscal_calendar", {})
    if "KPI_USE_FISCAL_CALENDAR" in os.environ:
        fc["use_fiscal_calendar"] = _parse_bool(os.environ["KPI_USE_FISCAL_CALENDAR"])
    if "KPI_HALF_PERIODS" in os.environ:
        fc["half_periods"] = _parse_bool(os.environ["KPI_HALF_PERIODS"])

    ss = out.setdefault("score_scope", {})
    if "KPI_SCOPE_MIN_PERCENTILE" in os.environ:
        ss["min_percentile"] = _parse_percentile(os.environ["KPI_SCOPE_MIN_PERCENTILE"])
    if "KPI_SCOPE_MIN_WEEKS_FOR_FILTER" in os.environ:
        ss["min_weeks_for_filter"] = int(os.environ["KPI_SCOPE_MIN_WEEKS_FOR_FILTER"])

    sc = out.setdefault("scope", {})
    if "KPI_USE_HYBRID_SCOPE" in os.environ:
        sc["use_hybrid_scope"] = _parse_bool(os.environ["KPI_USE_HYBRID_SCOPE"])
    if "KPI_RUN_SCOPE_DIFF" in os.environ:
        sc["run_scope_diff"] = _parse_bool(os.environ["KPI_RUN_SCOPE_DIFF"])

    cp = out.setdefault("comparable_pairs", {})
    if "KPI_COMPARABLE_PAIRS" in os.environ:
        cp["enabled"] = _parse_bool(os.environ["KPI_COMPARABLE_PAIRS"])

    cmp_cfg = out.setdefault("comparisons", {})
    if "KPI_COMPARISONS" in os.environ:
        cmp_cfg["enabled"] = [
            c.strip().lower() for c in os.environ["KPI_COMPARISONS"].split(",") if c.strip()
        ]

    lse = out.setdefault("lost_sales_ensemble", {})
    if "KPI_LOST_SALES_ENSEMBLE" in os.environ:
        lse["enabled"] = _parse_bool(os.environ["KPI_LOST_SALES_ENSEMBLE"])
    if "KPI_LOST_SALES_SLOW_PATH" in os.environ:
        lse["slow_path_segments"] = [
            s.strip() for s in os.environ["KPI_LOST_SALES_SLOW_PATH"].split(",") if s.strip()
        ]
    if "KPI_SPEED_CLUSTER_PATH" in os.environ:
        lse["speed_cluster_path_segments"] = [
            s.strip() for s in os.environ["KPI_SPEED_CLUSTER_PATH"].split(",") if s.strip()
        ]
    if "KPI_SPEED_CLUSTER_FORMAT" in os.environ:
        lse["speed_cluster_format"] = os.environ["KPI_SPEED_CLUSTER_FORMAT"].strip().lower()
    if "KPI_SPEED_CLUSTER_ATTRIBUTE" in os.environ:
        lse["speed_cluster_attribute_name"] = os.environ["KPI_SPEED_CLUSTER_ATTRIBUTE"].strip()
    if "KPI_SPEED_CLUSTER_VALUE_COL" in os.environ:
        lse["speed_cluster_value_col"] = os.environ["KPI_SPEED_CLUSTER_VALUE_COL"].strip()
    if "KPI_FAST_MOVER_CLUSTERS" in os.environ:
        lse["fast_mover_clusters"] = [
            int(c.strip()) for c in os.environ["KPI_FAST_MOVER_CLUSTERS"].split(",") if c.strip()
        ]

    lss = out.setdefault("lost_sales_source", {})
    if "KPI_LOST_SALES_WEEK_COL" in os.environ:
        lss["week_col"] = os.environ["KPI_LOST_SALES_WEEK_COL"].strip()
    if "KPI_LOST_SALES_PRODUCT_COL" in os.environ:
        lss["product_col"] = os.environ["KPI_LOST_SALES_PRODUCT_COL"].strip()
    if "KPI_LOST_SALES_STORE_COL" in os.environ:
        lss["store_col"] = os.environ["KPI_LOST_SALES_STORE_COL"].strip()
    if "KPI_LOST_SALES_COL" in os.environ:
        lss["lost_sales_col"] = os.environ["KPI_LOST_SALES_COL"].strip()
    if "KPI_LOST_SALES_IN_STOCK_COL" in os.environ:
        lss["in_stock_col"] = os.environ["KPI_LOST_SALES_IN_STOCK_COL"].strip()
    if "KPI_LOST_SALES_TOTAL_DAYS_COL" in os.environ:
        lss["total_days_col"] = os.environ["KPI_LOST_SALES_TOTAL_DAYS_COL"].strip()

    ins = out.setdefault("instock_source", {})
    if "KPI_INSTOCK_SOURCE_ENABLED" in os.environ:
        ins["enabled"] = _parse_bool(os.environ["KPI_INSTOCK_SOURCE_ENABLED"])
    if "KPI_INSTOCK_SOURCE_PATH" in os.environ:
        ins["path_segments"] = [
            s.strip() for s in os.environ["KPI_INSTOCK_SOURCE_PATH"].split(",") if s.strip()
        ]
    if "KPI_INSTOCK_WEEK_COL" in os.environ:
        ins["week_col"] = os.environ["KPI_INSTOCK_WEEK_COL"].strip()
    if "KPI_INSTOCK_PRODUCT_COL" in os.environ:
        ins["product_col"] = os.environ["KPI_INSTOCK_PRODUCT_COL"].strip()
    if "KPI_INSTOCK_STORE_COL" in os.environ:
        ins["store_col"] = os.environ["KPI_INSTOCK_STORE_COL"].strip()
    if "KPI_INSTOCK_IN_STOCK_COL" in os.environ:
        ins["in_stock_col"] = os.environ["KPI_INSTOCK_IN_STOCK_COL"].strip()
    if "KPI_INSTOCK_TOTAL_DAYS_COL" in os.environ:
        ins["total_days_col"] = os.environ["KPI_INSTOCK_TOTAL_DAYS_COL"].strip()

    op = out.setdefault("output", {})
    if "KPI_SAVE_OUTPUTS" in os.environ:
        op["save_outputs"] = _parse_bool(os.environ["KPI_SAVE_OUTPUTS"])
    if "KPI_OUTPUT_PATH" in os.environ:
        op["path_segments"] = [s.strip() for s in os.environ["KPI_OUTPUT_PATH"].split(",") if s.strip()]
    if "KPI_OUTPUT_RUN_DATE" in os.environ:
        op["run_date"] = os.environ["KPI_OUTPUT_RUN_DATE"].strip() or None
    if "KPI_OUTPUT_SAVE_MODE" in os.environ:
        op["save_mode"] = os.environ["KPI_OUTPUT_SAVE_MODE"].strip().lower()
    if "KPI_ALLOW_OVERWRITE_EXISTING" in os.environ:
        op["allow_overwrite_existing"] = _parse_bool(os.environ["KPI_ALLOW_OVERWRITE_EXISTING"])
    if "KPI_RECOMPUTE_COMPARISONS" in os.environ:
        op["recompute_comparisons_from_history"] = _parse_bool(os.environ["KPI_RECOMPUTE_COMPARISONS"])

    sl = out.setdefault("slices", {})
    if "KPI_SLICE_DIMENSIONS" in os.environ:
        sl["dimensions"] = [c.strip() for c in os.environ["KPI_SLICE_DIMENSIONS"].split(",") if c.strip()]

    hr = out.setdefault("html_report", {})
    if "KPI_HTML_ENABLED" in os.environ:
        hr["enabled"] = _parse_bool(os.environ["KPI_HTML_ENABLED"])
    if "KPI_HTML_FILENAME" in os.environ:
        hr["filename"] = os.environ["KPI_HTML_FILENAME"].strip()
    if "KPI_HTML_TITLE" in os.environ:
        hr["report_title"] = os.environ["KPI_HTML_TITLE"].strip()
    if "KPI_HTML_OUTPUT_PATH" in os.environ:
        hr["output_path_segments"] = [
            s.strip() for s in os.environ["KPI_HTML_OUTPUT_PATH"].split(",") if s.strip()
        ]
    if "KPI_HTML_WEEKLY_WEEKS" in os.environ:
        raw = os.environ["KPI_HTML_WEEKLY_WEEKS"].strip()
        hr["weekly_display_weeks"] = int(raw) if raw else None
    if "KPI_HTML_MONTHLY_MONTHS" in os.environ:
        raw = os.environ["KPI_HTML_MONTHLY_MONTHS"].strip()
        hr["monthly_display_months"] = int(raw) if raw else None
    if "KPI_HTML_QUARTERLY_QUARTERS" in os.environ:
        raw = os.environ["KPI_HTML_QUARTERLY_QUARTERS"].strip()
        hr["quarterly_display_quarters"] = int(raw) if raw else None
    if "KPI_HTML_HALF_HALVES" in os.environ:
        raw = os.environ["KPI_HTML_HALF_HALVES"].strip()
        hr["half_display_halves"] = int(raw) if raw else None
    if "KPI_HTML_YEARLY_YEARS" in os.environ:
        raw = os.environ["KPI_HTML_YEARLY_YEARS"].strip()
        hr["yearly_display_years"] = int(raw) if raw else None

    return out


def _sunday_of_week(d: datetime.date) -> datetime.date:
    """Sunday that starts the Sun–Sat week containing d (Python weekday: Sun=6)."""
    return d if d.weekday() == 6 else d - datetime.timedelta(days=d.weekday() + 1)


def _saturday_of_week(d: datetime.date) -> datetime.date:
    return _sunday_of_week(d) + datetime.timedelta(days=6)


def _last_completed_saturday(as_of: datetime.date) -> datetime.date:
    """Saturday ending the last full Sun–Sat week on or before as_of."""
    week_end = _saturday_of_week(as_of)
    if as_of >= week_end:
        return week_end
    return week_end - datetime.timedelta(days=7)


def _resolve_report_window(
    as_of: datetime.date,
    run_min: Optional[str],
) -> Dict[str, Any]:
    report_start = _sunday_of_week(datetime.date(as_of.year, 1, 1))
    report_end = _last_completed_saturday(as_of)
    run_week_start = _sunday_of_week(as_of)
    run_week_end = _saturday_of_week(as_of)

    run_min_resolved = _sunday_of_week(datetime.date.fromisoformat(run_min)) if run_min else None
    if run_min_resolved is not None:
        if run_min_resolved > report_end:
            raise ValueError(
                f"run_min_date Sunday ({run_min_resolved}) must be <= REPORT_END_DATE ({report_end})."
            )
        effective_start = run_min_resolved
    else:
        effective_start = report_start

    return {
        "AS_OF_DATE": as_of,
        "RUN_WEEK_START_DATE": run_week_start,
        "RUN_WEEK_END_DATE": run_week_end,
        "REPORT_START_DATE": report_start,
        "REPORT_END_DATE": report_end,
        "RUN_MIN_DATE": run_min_resolved,
        "EFFECTIVE_REPORT_START_DATE": effective_start,
    }


def materialize(fund_paste: Callable[..., str], cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Resolve CONFIG into flat settings dict for KPIRunner (paths, dates, metrics, slices)."""
    cfg = _apply_env_overrides(cfg or CONFIG)

    customer = cfg["customer"]
    rw = cfg["reporting_window"]
    run_min_raw = rw.get("run_min_date")
    run_min = run_min_raw.strip() if isinstance(run_min_raw, str) and run_min_raw.strip() else None

    bucket = os.environ.get("KPI_BUCKET", f"/mnt/invent-{customer}-datastore")
    path_segments = cfg["path_segments"]
    paths = {
        "PATH_FISCAL": fund_paste(bucket, *path_segments["fiscal"]),
        "PATH_DAILY_DATA": fund_paste(bucket, *path_segments["daily_data"]),
        "PATH_INVENTORY_WAREHOUSE": fund_paste(bucket, *path_segments["inventory_warehouse"]),
        "PATH_ITEM_FAMILY": fund_paste(bucket, *path_segments["item_family"]),
        "PATH_PRODUCTS": fund_paste(bucket, *path_segments["products"]),
        "PATH_LOST_SALES": fund_paste(bucket, *path_segments["lost_sales"]),
        "PATH_DEFINED_SCOPE": fund_paste(bucket, *path_segments["defined_scope"]),
        "PATH_LOST_SALES_SLOW": fund_paste(bucket, *cfg["lost_sales_ensemble"]["slow_path_segments"]),
        "PATH_SPEED_CLUSTER": fund_paste(bucket, *cfg["lost_sales_ensemble"]["speed_cluster_path_segments"]),
        "PATH_PRODUCT_PLANNING_LEVEL": fund_paste(bucket, *path_segments["product_planning_level"]),
        "PATH_SCOPE": fund_paste(bucket, *path_segments["scope"]),
        "PATH_GOODS_IN_TRANSIT": fund_paste(bucket, *path_segments["goods_in_transit"]),
    }

    lost_sales_source_cfg = cfg.get("lost_sales_source", {}) or {}
    lost_sales_column_map = {
        "week_col": lost_sales_source_cfg.get("week_col", "week_start_date"),
        "product_col": lost_sales_source_cfg.get("product_col", "product_id"),
        "store_col": lost_sales_source_cfg.get("store_col", "store_id"),
        "lost_sales_col": lost_sales_source_cfg.get("lost_sales_col", "lost_sales"),
        "in_stock_col": lost_sales_source_cfg.get("in_stock_col", "in_stock"),
        "total_days_col": lost_sales_source_cfg.get("total_days_col", "details.total_days"),
        "product_agg_level_col": lost_sales_source_cfg.get("product_agg_level_col"),
    }
    lost_sales_sales_filter = list(lost_sales_source_cfg.get("sales_filter") or [])

    instock_source_cfg = cfg.get("instock_source", {}) or {}
    instock_source_enabled = bool(instock_source_cfg.get("enabled", False))
    if instock_source_enabled and not instock_source_cfg.get("path_segments"):
        raise ValueError("instock_source.path_segments is required when instock_source.enabled=True")
    paths["PATH_INSTOCK_SOURCE"] = (
        fund_paste(bucket, *instock_source_cfg["path_segments"]) if instock_source_enabled else None
    )
    instock_source_column_map = {
        "week_col": instock_source_cfg.get("week_col", "week_start_date"),
        "product_col": instock_source_cfg.get("product_col", "product_id"),
        "store_col": instock_source_cfg.get("store_col", "store_id"),
        "in_stock_col": instock_source_cfg.get("in_stock_col", "in_stock"),
        "total_days_col": instock_source_cfg.get("total_days_col", "total_days"),
        "product_agg_level_col": instock_source_cfg.get("product_agg_level_col"),
        "fallback_sources": [
            {
                "week_col": fb["week_col"],
                "in_stock_col": fb["in_stock_col"],
                "total_days_col": fb["total_days_col"],
                "product_col": fb.get("product_col", instock_source_cfg.get("product_col", "product_id")),
                "store_col": fb.get("store_col", instock_source_cfg.get("store_col", "store_id")),
                "product_agg_level_col": fb.get("product_agg_level_col", instock_source_cfg.get("product_agg_level_col")),
            }
            for fb in instock_source_cfg.get("fallback_sources", []) or []
        ],
    }

    item_family_source_cfg = cfg.get("item_family_source", {}) or {}
    item_family_column_map = {
        "product_col": item_family_source_cfg.get("product_col", "product_id"),
        "parent_col": item_family_source_cfg.get("parent_col", "parent_id"),
        "is_main_col": item_family_source_cfg.get("is_main_col", "is_main"),
    }

    item_family_rollup_cfg = cfg.get("item_family_rollup", {}) or {}
    item_family_rollup = {
        "daily_data": bool(item_family_rollup_cfg.get("daily_data", True)),
        "lost_sales": bool(item_family_rollup_cfg.get("lost_sales", False)),
        "inventory_warehouse": bool(item_family_rollup_cfg.get("inventory_warehouse", True)),
        "defined_scope": bool(item_family_rollup_cfg.get("defined_scope", False)),
    }

    dc_instock_cfg = cfg.get("dc_instock", {}) or {}
    dc_instock_enabled = bool(dc_instock_cfg.get("enabled", False))
    dc_instock_stock_threshold = dc_instock_cfg.get("stock_threshold", 0)

    window = _resolve_report_window(
        datetime.date.fromisoformat(rw["as_of_date"]),
        run_min,
    )
    report_end_mode = rw["report_end"]
    if report_end_mode not in ("as_of", "complete_month"):
        raise ValueError(f"reporting_window.report_end must be 'as_of' or 'complete_month'; got {report_end_mode!r}")

    defined_scope = {**cfg["defined_scope"], "path": paths["PATH_DEFINED_SCOPE"]}

    grain = defined_scope.get("grain", "product_store")
    valid_grains = {"product", "product_store", "product_store_week"}
    if grain not in valid_grains:
        raise ValueError(f"defined_scope.grain must be one of {sorted(valid_grains)}; got {grain!r}")
    if grain in ("product_store", "product_store_week") and not defined_scope.get("store_col"):
        raise ValueError(f"defined_scope.grain={grain!r} requires defined_scope.store_col")
    if grain == "product_store_week" and not (
        defined_scope.get("date_col") or (defined_scope.get("year_col") and defined_scope.get("week_col"))
    ):
        raise ValueError(
            "defined_scope.grain='product_store_week' requires date_col OR both year_col and week_col"
        )
    defined_scope["grain"] = grain

    lse = cfg["lost_sales_ensemble"]
    if lse.get("enabled") and instock_source_enabled:
        raise ValueError(
            "lost_sales_ensemble.enabled and instock_source.enabled cannot both be True: "
            "the fast/slow blend selects in_stock/total_days from the chosen model and uses its "
            "total_days to decide row existence, neither of which is computed when "
            "instock_source is on. "
            "Use at most one of the two."
        )
    if lse.get("enabled"):
        fast_clusters = lse.get("fast_mover_clusters")
        if not isinstance(fast_clusters, (list, tuple)) or not fast_clusters:
            raise ValueError("lost_sales_ensemble.fast_mover_clusters must be a non-empty list when enabled")
        if not all(isinstance(c, int) for c in fast_clusters):
            raise ValueError("lost_sales_ensemble.fast_mover_clusters must be integers (e.g. [1, 2, 3])")
        speed_cluster_format = str(lse.get("speed_cluster_format", "long")).strip().lower()
        if speed_cluster_format not in ("long", "wide"):
            raise ValueError(
                f"lost_sales_ensemble.speed_cluster_format={speed_cluster_format!r} must be 'long' or 'wide'"
            )
        lse["speed_cluster_format"] = speed_cluster_format
        if speed_cluster_format == "long" and not lse.get("speed_cluster_attribute_name"):
            raise ValueError(
                "lost_sales_ensemble.speed_cluster_attribute_name is required when "
                "speed_cluster_format='long' (default) and enabled=True"
            )
        if speed_cluster_format == "wide" and not lse.get("speed_cluster_value_col"):
            raise ValueError(
                "lost_sales_ensemble.speed_cluster_value_col is required when "
                "speed_cluster_format='wide' and enabled=True"
            )

    scope_source_cfg = cfg["scope_source"]
    scope_source_run_date = scope_source_cfg["run_date"]
    scope_source = {
        "mode": scope_source_cfg["mode"],
        "solution_id": scope_source_cfg["solution_id"],
        "run_date": (
            datetime.date.fromisoformat(scope_source_run_date) if scope_source_run_date else None
        ),
        "roll_to_family_main": bool(scope_source_cfg["roll_to_family_main"]),
        "active_only": bool(scope_source_cfg["active_only"]),
    }
    if scope_source["mode"] not in ("defined_scope", "operation_scope"):
        raise ValueError(
            f"scope_source.mode must be 'defined_scope' or 'operation_scope'; got {scope_source['mode']!r}"
        )
    if type(scope_source["solution_id"]) is not int:
        raise ValueError("scope_source.solution_id must be an integer")
    if scope_source["run_date"] is not None and scope_source["run_date"].weekday() != 6:
        raise ValueError(f"scope_source.run_date must be a Sunday; got {scope_source['run_date']}")
    operation_scope_mode = scope_source["mode"] == "operation_scope"
    if operation_scope_mode and grain == "product_store_week":
        raise ValueError("scope_source.mode='operation_scope' does not support defined_scope.grain='product_store_week'")

    blocked_scope_cfg = cfg["blocked_scope"]
    ui_parameters_path = blocked_scope_cfg["ui_parameters_path"]
    blocked_scope = {
        "rule": blocked_scope_cfg["rule"],
        "path": (
            fund_paste(bucket, *ui_parameters_path.strip("/").split("/"), "blocked_scope")
            if ui_parameters_path is not None
            else None
        ),
        # DC blocks of the same snapshot, read only when dc_instock.blocked_scope_solution_id is set.
        "dc_path": (
            fund_paste(bucket, *ui_parameters_path.strip("/").split("/"), "dc_blocked_scope")
            if ui_parameters_path is not None
            else None
        ),
    }
    if blocked_scope["rule"] not in ("after_scope_start", "all"):
        raise ValueError(f"blocked_scope.rule must be 'after_scope_start' or 'all'; got {blocked_scope['rule']!r}")
    if blocked_scope["path"] is not None and not operation_scope_mode:
        raise ValueError("blocked_scope.ui_parameters_path requires scope_source.mode='operation_scope'")

    dc_instock_git_date_shift_days = dc_instock_cfg["git_date_shift_days"]
    if dc_instock_git_date_shift_days is not None and type(dc_instock_git_date_shift_days) is not int:
        raise ValueError("dc_instock.git_date_shift_days must be an integer or None")
    dc_instock_blocked_scope_solution_id = dc_instock_cfg["blocked_scope_solution_id"]
    if dc_instock_blocked_scope_solution_id is not None:
        if type(dc_instock_blocked_scope_solution_id) is not int:
            raise ValueError("dc_instock.blocked_scope_solution_id must be an integer or None")
        if blocked_scope["path"] is None:
            raise ValueError("dc_instock.blocked_scope_solution_id requires blocked_scope.ui_parameters_path")

    instock_daily_cfg = cfg["instock_daily"]
    history_start_raw = instock_daily_cfg["history_start"]
    instock_daily = {
        "enabled": bool(instock_daily_cfg["enabled"]),
        "git_date_shift_days": instock_daily_cfg["git_date_shift_days"],
        "count_start": instock_daily_cfg["count_start"],
        "require_daily_data": bool(instock_daily_cfg["require_daily_data"]),
        "history_start": datetime.date.fromisoformat(history_start_raw) if history_start_raw else None,
        "usable_only": bool(instock_daily_cfg["usable_only"]),
        "input_filters": list(instock_daily_cfg["input_filters"]),
    }
    if instock_daily["count_start"] not in ("first_daily_row", "scope_start", "earliest"):
        raise ValueError(
            "instock_daily.count_start must be 'first_daily_row', 'scope_start' or 'earliest'; "
            f"got {instock_daily['count_start']!r}"
        )
    if instock_daily["git_date_shift_days"] is not None and type(instock_daily["git_date_shift_days"]) is not int:
        raise ValueError("instock_daily.git_date_shift_days must be an integer or None")
    if instock_daily["history_start"] is not None and instock_daily["history_start"] > window["EFFECTIVE_REPORT_START_DATE"]:
        raise ValueError(
            f"instock_daily.history_start {instock_daily['history_start']} is after the report window "
            f"start {window['EFFECTIVE_REPORT_START_DATE']}"
        )
    if instock_daily["enabled"]:
        if grain == "product":
            raise ValueError("instock_daily.enabled=True requires a store-level defined_scope.grain (product_store)")
        if instock_source_enabled:
            raise ValueError(
                "instock_daily.enabled and instock_source.enabled cannot both be True: "
                "instock_daily replaces the weekly in-stock read. Use at most one of the two."
            )
        if cfg["lost_sales_ensemble"].get("enabled"):
            raise ValueError("instock_daily.enabled and lost_sales_ensemble.enabled cannot both be True")
        if instock_daily["count_start"] != "first_daily_row" and not operation_scope_mode:
            raise ValueError(
                f"instock_daily.count_start={instock_daily['count_start']!r} needs the operation scope start "
                "date: set scope_source.mode='operation_scope' or count_start='first_daily_row'."
            )
        if instock_daily["count_start"] == "first_daily_row" and not instock_daily["require_daily_data"]:
            raise ValueError("instock_daily.count_start='first_daily_row' requires require_daily_data=True")

    dimension_sources = []
    for src in cfg.get("dimension_sources", []) or []:
        resolved = dict(src)
        if not resolved.get("path") and resolved.get("path_segments"):
            resolved["path"] = fund_paste(bucket, *resolved["path_segments"])
        dimension_sources.append(resolved)

    # Root specs: EVERY column an enabled dimension_source contributes (columns + derived) is
    # root-defining -- never a cut (see README's "Dimension sources -> roots" section: "every one
    # of its columns becomes a root ... not a flat cut", and the mutual-exclusivity design
    # constraint). root_values supplies an explicit value->root-name mapping for a given dim_col;
    # a dim_col with no entry in root_values (or a source with root_values omitted entirely)
    # auto-discovers one root per distinct value instead (fiscal._resolve_root_definitions, since
    # that needs real data). Resolved here (not left for fiscal.py to re-derive) so root_values
    # stays the single place a root-defining column is declared, and so a root_values key that
    # doesn't match any of the source's own columns/derived fails loudly now instead of silently
    # never applying (that dim_col would otherwise fall through and become an ordinary cut).
    root_specs = []
    for src in dimension_sources:
        if not src.get("enabled"):
            continue
        contributed = list(
            dict.fromkeys(list(src.get("columns") or []) + list((src.get("derived") or {}).keys()))
        )
        root_values_cfg = dict(src.get("root_values") or {})
        unknown_root_value_cols = set(root_values_cfg) - set(contributed)
        if unknown_root_value_cols:
            raise ValueError(
                f"dimension_source {src.get('label', 'dimension_source')!r} root_values has "
                f"column(s) {sorted(unknown_root_value_cols)} not among its own columns/derived "
                f"{contributed}."
            )
        for dim_col in contributed:
            root_specs.append({"dim_col": dim_col, "root_values": root_values_cfg.get(dim_col) or {}})

    # Per-dimension value filters, from slices only. Applied to a slice's own breakdown
    # (see kpi_pipeline/kpi_long._filter_frames_for_dimension).
    slice_value_filters = dict(cfg["slices"].get("value_filters", {}) or {})
    _validate_value_filters(slice_value_filters)

    # Selected comparison kinds — validated and normalised to canonical order.
    comparisons_cfg = cfg.get("comparisons", {}) or {}
    requested_kinds = comparisons_cfg.get("enabled")
    if requested_kinds is None:
        requested_kinds = list(COMPARISON_KINDS_ALL)
    requested_set = {str(k).strip().lower() for k in requested_kinds}
    invalid_kinds = sorted(requested_set - set(COMPARISON_KINDS_ALL))
    if invalid_kinds:
        raise ValueError(
            f"comparisons.enabled has invalid kinds {invalid_kinds}; "
            f"allowed: {list(COMPARISON_KINDS_ALL)}"
        )
    comparison_kinds = [k for k in COMPARISON_KINDS_ALL if k in requested_set]
    if not comparison_kinds:
        raise ValueError(
            "comparisons.enabled resolved to an empty list; "
            f"choose at least one of {list(COMPARISON_KINDS_ALL)}"
        )

    # Selected comparable-pairs kinds — validated and normalised to canonical order. Only
    # meaningful when comparable_pairs.enabled=True; resolves to an empty list when disabled.
    comparable_pairs_cfg = cfg.get("comparable_pairs", {}) or {}
    comparable_pairs_enabled = bool(comparable_pairs_cfg.get("enabled", False))
    requested_comparable_kinds = comparable_pairs_cfg.get("kinds")
    if requested_comparable_kinds is None:
        requested_comparable_kinds = ["ytd"]
    comparable_requested_set = {str(k).strip().lower() for k in requested_comparable_kinds}
    invalid_comparable_kinds = sorted(comparable_requested_set - set(COMPARABLE_KINDS_ALL))
    if invalid_comparable_kinds:
        raise ValueError(
            f"comparable_pairs.kinds has invalid kinds {invalid_comparable_kinds}; "
            f"allowed: {list(COMPARABLE_KINDS_ALL)}"
        )
    comparable_kinds = (
        [k for k in COMPARABLE_KINDS_ALL if k in comparable_requested_set] if comparable_pairs_enabled else []
    )
    if comparable_pairs_enabled and not comparable_kinds:
        raise ValueError(
            "comparable_pairs.kinds resolved to an empty list while comparable_pairs.enabled=True; "
            f"choose at least one of {list(COMPARABLE_KINDS_ALL)}"
        )
    comparable_pairs_grain = comparable_pairs_cfg.get("grain", "product_store")
    if comparable_pairs_grain not in ("product_store", "product"):
        raise ValueError(
            f"comparable_pairs.grain must be one of ['product', 'product_store']; got {comparable_pairs_grain!r}"
        )

    half_periods = bool(cfg["fiscal_calendar"]["half_periods"])
    if "half" in comparable_kinds and not half_periods:
        raise ValueError("comparable_pairs.kinds 'half' needs fiscal_calendar.half_periods=True")

    output_cfg = cfg["output"]
    output_root = fund_paste(bucket, *output_cfg["path_segments"])
    run_date_raw = output_cfg.get("run_date")
    output_run_date = (
        run_date_raw.strip()
        if isinstance(run_date_raw, str) and run_date_raw.strip()
        else str(window["AS_OF_DATE"])
    )
    save_mode = output_cfg.get("save_mode", "incremental").lower()
    if save_mode not in {"initial", "incremental", "full_refresh"}:
        raise ValueError("output.save_mode must be one of: initial, incremental, full_refresh")

    metrics = cfg["metrics"]
    population_filters = dict(metrics.get("population_filters", {}) or {})
    _validate_population_filters(population_filters, metrics["metric_cols"])
    _validate_scope_diff_metrics(metrics["scope_diff_metrics"], metrics["metric_cols"])

    _validate_scope_adjustments(cfg.get("scope_adjustments", {}) or {})

    score_scope = cfg["score_scope"]
    min_pct = score_scope["min_percentile"]
    if min_pct > 1:
        min_pct = min_pct / 100.0

    html_cfg = cfg["html_report"]
    # The filename stays a template ({customer}, {report_end}): the report end is only final once
    # fiscal.apply_report_end_mode has cut it, so KPIRunner formats the filename when it writes.
    html_filename_template = html_cfg["filename"]
    try:
        html_filename_template.format(customer=customer, report_end="")
    except (KeyError, IndexError) as exc:
        raise ValueError(
            f"html_report.filename {html_filename_template!r} may only use the placeholders "
            "{customer} and {report_end}"
        ) from exc
    dimension_labels = html_cfg["dimension_labels"]
    if not isinstance(dimension_labels, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in dimension_labels.items()
    ):
        raise ValueError("html_report.dimension_labels must be a dict of slice dimension name -> tab label (str -> str)")
    html_output_segs = html_cfg["output_path_segments"]
    html_output_dir = fund_paste(bucket, *html_output_segs) if html_output_segs else None

    run_mode = cfg.get("run", {}).get("mode", "full").lower()
    if run_mode not in {"full", "html_only"}:
        raise ValueError("run.mode must be one of: full, html_only")

    return {
        "CONFIG": cfg,
        "CUSTOMER": customer,
        "RUN_MODE": run_mode,
        "BUCKET": bucket,
        **window,
        "REPORT_END_MODE": report_end_mode,
        "USE_FISCAL_CALENDAR": cfg["fiscal_calendar"]["use_fiscal_calendar"],
        "HALF_PERIODS": half_periods,
        "FISCAL_QUARTER_COL": cfg["fiscal_calendar"].get("column_map", {}).get("quarter_col"),
        "FISCAL_MONTH_COL": cfg["fiscal_calendar"].get("column_map", {}).get("month_col"),
        "FISCAL_MONTH_NAME_COL": cfg["fiscal_calendar"].get("column_map", {}).get("month_name_col"),
        "DAILY_TIME_COLUMNS": cfg["fiscal_calendar"]["daily_time_columns"],
        "SCOPE_MIN_PERCENTILE": min_pct,
        "SCOPE_MIN_WEEKS_FOR_FILTER": score_scope["min_weeks_for_filter"],
        "USE_HYBRID_SCOPE": cfg["scope"]["use_hybrid_scope"],
        "RUN_SCOPE_DIFF": cfg["scope"].get("run_scope_diff", False),
        "COMPARABLE_PAIRS_ENABLED": comparable_pairs_enabled,
        "COMPARABLE_KINDS": comparable_kinds,
        "COMPARABLE_PAIRS_GRAIN": comparable_pairs_grain,
        "COMPARISON_KINDS": comparison_kinds,
        "SCOPE_ADJUSTMENTS": cfg.get("scope_adjustments", {}),
        # .get() throughout, matching what the validators above actually require: they only
        # demand fast_mover_clusters when enabled=True, and speed_cluster_attribute_name when
        # format="long". Indexing these directly raised KeyError on configs the validators had
        # deliberately just accepted -- e.g. any format="wide" config omitting the unused
        # attribute-name key.
        "LOST_SALES_ENSEMBLE_ENABLED": lse.get("enabled", False),
        "FAST_MOVER_CLUSTERS": list(lse.get("fast_mover_clusters") or [1, 2, 3]),
        "SPEED_CLUSTER_FORMAT": lse.get("speed_cluster_format", "long"),
        "SPEED_CLUSTER_ATTRIBUTE_NAME": lse.get("speed_cluster_attribute_name"),
        "SPEED_CLUSTER_VALUE_COL": lse.get("speed_cluster_value_col", "product_speed_cluster"),
        "LOST_SALES_COLUMN_MAP": lost_sales_column_map,
        "LOST_SALES_SALES_FILTER": lost_sales_sales_filter,
        "ITEM_FAMILY_COLUMN_MAP": item_family_column_map,
        "ITEM_FAMILY_ROLLUP": item_family_rollup,
        "SCOPE_SOURCE": scope_source,
        "BLOCKED_SCOPE": blocked_scope,
        "INSTOCK_DAILY": instock_daily,
        "DC_INSTOCK_ENABLED": dc_instock_enabled,
        "DC_INSTOCK_STOCK_THRESHOLD": dc_instock_stock_threshold,
        "DC_INSTOCK_GIT_DATE_SHIFT_DAYS": dc_instock_git_date_shift_days,
        "DC_INSTOCK_BLOCKED_SCOPE_SOLUTION_ID": dc_instock_blocked_scope_solution_id,
        "INSTOCK_SOURCE_ENABLED": instock_source_enabled,
        "INSTOCK_SOURCE_COLUMN_MAP": instock_source_column_map,
        **paths,
        "DEFINED_SCOPE": defined_scope,
        "INPUT_FILTERS": cfg.get("input_filters", {}),
        "SLICE_DIMENSIONS": cfg["slices"]["dimensions"],
        "DERIVED_SLICE_DIMENSIONS": cfg["slices"]["derived_dimensions"],
        "DIMENSION_SOURCES": dimension_sources,
        "ROOT_SPECS": root_specs,
        "SLICE_VALUE_FILTERS": slice_value_filters,
        "METRIC_COLS": metrics["metric_cols"],
        "SCOPE_DIFF_METRICS": metrics["scope_diff_metrics"],
        "METRIC_LABELS": metrics["labels"],
        "PP_CHANGE_METRICS": frozenset(metrics["pp_change_metrics"]),
        "METRIC_POPULATION_FILTERS": population_filters,
        "SAVE_OUTPUTS": output_cfg["save_outputs"],
        "OUTPUT_SAVE_MODE": save_mode,
        "ALLOW_OVERWRITE_EXISTING": output_cfg.get("allow_overwrite_existing", False),
        "RECOMPUTE_COMPARISONS_FROM_HISTORY": output_cfg.get("recompute_comparisons_from_history", True),
        "PATH_OUTPUT_ROOT": output_root,
        "OUTPUT_RUN_DATE": output_run_date,
        # HTML report
        "HTML_REPORT_ENABLED": html_cfg.get("enabled", True),
        "HTML_REPORT_FILENAME_TEMPLATE": html_filename_template,
        "HTML_REPORT_TITLE": html_cfg.get("report_title") or None,
        "HTML_REPORT_METRIC_DEFS": html_cfg.get("metric_definitions") or {},
        "HTML_REPORT_OUTPUT_DIR": html_output_dir,
        "HTML_REPORT_WEEKLY_DISPLAY_WEEKS": html_cfg.get("weekly_display_weeks"),
        "HTML_REPORT_MONTHLY_DISPLAY_MONTHS": html_cfg.get("monthly_display_months"),
        "HTML_REPORT_QUARTERLY_DISPLAY_QUARTERS": html_cfg.get("quarterly_display_quarters"),
        "HTML_REPORT_HALF_DISPLAY_HALVES": html_cfg["half_display_halves"],
        "HTML_REPORT_YEARLY_DISPLAY_YEARS": html_cfg.get("yearly_display_years"),
        "HTML_REPORT_ROOT_LABELS": html_cfg["root_labels"],
        "HTML_REPORT_DIMENSION_LABELS": dimension_labels,
    }

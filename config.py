# Generic reference config for the retail-insights-pipeline toolkit -- a starting point for a
# NEW customer, not any specific customer's own deployed values. Copy this file, then replace
# every placeholder below with your own paths/business rules; see README.md for full docs on
# every CONFIG key.
# Usage (Databricks, same folder as main.ipynb): %run ./config
#                                                 settings = materialize(fund.paste)
#
# GETTING STARTED — minimum edits before your first run:
#   1. customer: your customer's slug (used to resolve the datastore bucket by default).
#   2. path_segments: point every entry at your own tables (defined_scope, daily_data,
#      products, lost_sales at minimum).
#   3. defined_scope: column names on YOUR scope table (product_col/store_col/date_col/etc).
#   4. reporting_window.as_of_date: today, or whatever date you want the report to run through.
#
# Excluding stores: there is no dedicated "exclude these stores" key. To keep a store out of every
# metric (e.g. an e-com fulfillment "store" with no real shelf inventory), add a store_id exclusion
# to input_filters.daily_data -- it applies everywhere daily_data is read (scope building included).
# To keep it out of in-stock only, use instock_daily.input_filters (when instock_daily is on). If
# your lost-sales/instock source tables already exclude such stores upstream (common -- see
# lost_sales_source/instock_source), nothing further is needed for those two metrics.
#
# OPTIONAL, gated features (all OFF/empty by default in this template — turn on as needed):
#   scope_adjustments   — manual scope additions/removals from a CSV/Delta source, e.g. a
#                         business-curated product list that should always be in (or out of)
#                         scope regardless of what the scope table itself says. See the
#                         disabled example below for the pattern (store_col=None + date_col/
#                         year_col/week_col=None = "every store, every week" for that source's
#                         product_ids).
#   dimension_sources   — join an external table onto products to add a new slice dimension
#                         that can also become its OWN root population tab (see README's
#                         "Dimension sources -> roots" section) — e.g. a channel or program
#                         flag from a table the products master doesn't itself carry.
#   lost_sales_ensemble — blend a fast/slow lost-sales model pair by product sales-speed
#                         cluster instead of reading a single lost-sales source as-is.
#   instock_source      — read in-stock rate from a SEPARATE table instead of lost_sales_
#                         source's own in_stock/total_days columns (e.g. when a different
#                         pipeline computes instock than the one computing lost sales).
#                         Supports fallback_sources to backfill weeks a rolling-window
#                         source's primary column-set doesn't reach (see README).
#   comparable_pairs    — like-for-like YTD over pairs present in EVERY year in the report
#                         window (one shared universe across every consecutive-year link, not
#                         a separate universe per link); requires 2+ years (run_min_date
#                         spanning that far back) to have anything to compute.
#   dc_instock          — DC in-stock rate (dc_in_stock_rate), gated. Expands inventory_warehouse
#                         into a daily product x warehouse grid running from each pair's own first
#                         stocked day to the report window's end, 0-filling the gaps as stockouts.
#                         Requires path_segments.item_family. See README's "dc_instock" section.
#   inventory_git       — goods in transit added to on-hand on the metrics you name (total_inventory,
#                         mean_stock, wos, inventory_turnover_rate, dc_mean_stock, wos_dc); the rest
#                         stay on-hand only. See README's "inventory_git" section.
#
#   NOTE: inventory_warehouse's product_id is item-family-rolled onto its parent
#   (path_segments.item_family) before being restricted to scope_core, matching defined_scope's
#   own already-parent-rolled id space -- required whenever inventory_warehouse is configured,
#   not just when dc_instock.enabled=True. Changes dc_mean_stock/WOS_DC/WOS_TOTAL for families
#   with inventory split across old and current item codes. See README's "dc_instock" section.

import copy
import datetime
import os
from typing import Any, Callable, Dict, Optional

# Period-over-period comparison kinds the pipeline can produce, in canonical order.
# QoQ/MoM/WoW comparison tables were dropped for simplicity — the Quarter/Monthly/Weekly period
# tabs already show recent-period value trends, covering "recent quarters/months/weeks" without
# a separate delta table.
COMPARISON_KINDS_ALL = ("yoy", "ytd")

# Comparable-pairs (like-for-like) kinds the pipeline can produce, in canonical order. Each is its
# own same-pairs-across-years population, computed independently:
#   ytd     — pairs present in every year of the window, compared on each year's elapsed window.
#   yoy     — pairs present in every year of the window, compared on the full year.
#   quarter — for each quarter number, pairs present in every year that HAS that quarter number,
#             compared within that quarter's own year-set (independent per quarter number).
#   half    — same as quarter, per half number (H1 = fiscal quarters 1-2, H2 = 3-4). Needs
#             fiscal_calendar.half_periods=True.
COMPARABLE_KINDS_ALL = ("ytd", "yoy", "quarter", "half")

# inventory_git.metrics gate names, in canonical order: the inventory metrics that can add goods in
# transit (GIT) to on-hand inventory. Store side: total_inventory, mean_stock (+ retail / cost), wos
# (WOS, wos_revenue, wos_cost and the store part of WOS_TOTAL), inventory_turnover_rate (the mean stock
# inside it). DC side: dc_mean_stock (and the DC part of total_mean_stock), wos_dc (WOS_DC and the DC
# part of WOS_TOTAL). total_mean_stock's store part follows mean_stock.
INVENTORY_GIT_METRICS_ALL = (
    "total_inventory", "mean_stock", "wos", "inventory_turnover_rate", "dc_mean_stock", "wos_dc",
)

CONFIG: Dict[str, Any] = {
    # =============================================================================
    # IDENTITY & RUN WINDOW
    # =============================================================================
    "customer": "your_customer",  # datastore bucket defaults to /mnt/invent-{customer}-datastore
    "run": {
        # full       = compute KPIs from source Delta tables (default)
        # html_only  = load saved outputs from output.path_segments and render HTML only
        "mode": "full",
    },
    "reporting_window": {
        # Update as_of_date before each run. REPORT_END_DATE resolves to the last
        # completed Saturday on or before this date (report_end "latest_day": the date itself).
        "as_of_date": "2026-09-01",
        "run_min_date": None,  # None = YTD from Jan 1; e.g. "2024-01-01" for multi-year
        # as_of          = the report ends at the last completed Saturday (REPORT_END_DATE above).
        # complete_month = the report ends at the last day of the most recent fully elapsed month on
        #                  or before that Saturday, and every metric and view (monthly, quarter,
        #                  half, YTD, annual, weekly, comparisons, comparable, daily in-stock, blocked
        #                  days) stops there. With a fiscal calendar the month is a fiscal month (the
        #                  upload must extend past that Saturday and run_min_date must reach the start
        #                  of the cut month). Without one it is a calendar month: the cut is usually
        #                  mid-week, the clipped trailing week is dropped from the Weekly tab, and
        #                  months are bucketed by each week's start date.
        # latest_day     = REPORT_END_DATE is as_of_date itself (set it to a day daily-data has reached;
        #                  the fiscal_cal upload must extend past it). YTD runs to that day, the same
        #                  fiscal day (K-th day of the fiscal year) for every year; the Annual, Quarter,
        #                  Half, Monthly and Weekly views show complete periods only (the current fiscal
        #                  year appears in YTD only). Lost sales only has data through the last
        #                  Saturday on or before as_of_date, so lost_sales_pct uses whole weeks up to
        #                  it in every view, and YTD uses weeks 1..(that Saturday's fiscal week) for
        #                  every year. Needs instock_daily.enabled=True (the week containing day K is
        #                  split for YTD). run_min_date must reach the start of the first fiscal year
        #                  you want in Annual / YTD.
        "report_end": "as_of",
    },
    # =============================================================================
    # CALENDAR
    # =============================================================================
    "fiscal_calendar": {
        "use_fiscal_calendar": True,
        # True adds the "half" period type (H1 = fiscal quarters 1-2, H2 = 3-4): a Half tab and the
        # "half" comparable_pairs kind. Only complete halves are shown, like quarters.
        "half_periods": False,
        # Your fiscal_cal upload's column names (used since use_fiscal_calendar=True above).
        # Each is auto-detected/optional -- read when present, derived when absent. If your
        # fiscal year is calendar-offset (e.g. doesn't start in January), set month_name_col so
        # the Monthly tab reads a real display label verbatim instead of deriving one from the
        # (fiscal, not calendar) month number -- a fiscal month number doesn't reliably map to
        # a real calendar month once the fiscal year itself is offset.
        "column_map": {
            "quarter_col": "Quarter",
            "month_col": "Month",
            "month_name_col": "month_name",
        },
        # Column-name map for the RAW daily-data table. "date" is always required -- read
        # unconditionally on both the fiscal and civil paths (kpi_pipeline/pipeline.py,
        # kpi_pipeline/scope.py). "week" is only consulted on the CIVIL path
        # (use_fiscal_calendar=False). No "year" key: Year always comes from `date`
        # (F.year(date)), never a raw source year column -- that column can carry the ISO
        # week-year (late-December weeks labelled as the next year). See fiscal.py.
        "daily_time_columns": {
            "date": "date",
            "week": "week",
        },
    },
    # =============================================================================
    # SCOPE & POPULATION
    # =============================================================================
    "score_scope": {
        # Used when scope.use_hybrid_scope=True (missing-week backfill under hybrid scope,
        # see "scope" below) OR scope.run_scope_diff=True (defined-vs-score diagnostic) --
        # either one alone triggers score-scope building (kpi_pipeline/scope.py's need_score).
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
        "grain": "product_store",
        "product_col": "product_id",
        "store_col": "store_id",        # required for product_store / product_store_week grains
        # date/year/week: read ONLY for product_store_week grain (to resolve the scope's weeks).
        #   DATE path:   date_col -> fiscal_cal -> Year/Week (preferred).
        #   NATIVE path: date_col=None, set year_col/week_col (year must be a true calendar year).
        "date_col": "week_start_date",
        "year_col": None,
        "week_col": None,
        # product_store_week only: if the scope source's own earliest available week (across
        # every pair) starts later than the report window's start, only the pairs tied to that
        # earliest week are assumed in scope back to the window's start -- a data-availability
        # limit of the source, not a per-pair signal. A pair whose own first-seen week is later
        # still (a new store/product) is left untouched (see kpi_pipeline/scope.py's
        # _defined_scope_weekly). True by default (matches product/product_store's own
        # always-whole-window behaviour). Set False for a deployment with EXISTING
        # product_store_week history saved before this option existed -- switching it on for such
        # a deployment mixes two scope definitions in one incrementally-merged table; a fresh
        # product_store_week adoption is unaffected either way.
        "backfill_leading_gap": True,
    },
    "scope_adjustments": {
        # ---------------------------------------------------------------------------
        # ADDITIONS: force specific products into scope regardless of the defined scope
        # table (e.g. a business-curated "always report on these" list). Disabled example
        # below shows the pattern -- store_col=None + date_col/year_col/week_col=None means
        # "every store, every week in the report window" for this source's product_ids.
        # ---------------------------------------------------------------------------
        "additions": [
            {
                "enabled": False,  # flip on once path points at a real table/CSV
                "label": "example_addition",
                "source": "csv",
                "path": "/Workspace/Shared/your_project/data/addition_product_ids.csv",
                "location": "workspace",
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,
                "date_col": None,
                "year_col": None,
                "week_col": None,
            },
        ],
        # ---------------------------------------------------------------------------
        # REMOVALS: exclude specific products from scope (e.g. a business-curated
        # "never report on these" list). Same shape/mechanics as additions above.
        # ---------------------------------------------------------------------------
        "removals": [
            {
                "enabled": False,  # flip on once path points at a real table/CSV
                "label": "example_removal",
                "source": "csv",
                "path": "/Workspace/Shared/your_project/data/removal_product_ids.csv",
                "location": "workspace",
                "csv_options": {"header": True, "inferSchema": True},
                "join_keys": ["product_id"],
                "product_col": "product_id",
                "store_col": None,
                "date_col": None,
                "year_col": None,
                "week_col": None,
            },
        ],
    },
    # =============================================================================
    # DATA SOURCES
    # =============================================================================
    "path_segments": {
        "fiscal": ["one_time_uploads", "fiscal_cal"],
        "daily_data": ["noob", "daily-data"],
        # DC/warehouse daily inventory -- backs dc_mean_stock/total_mean_stock/WOS_DC/WOS_TOTAL.
        # product_id is item-family-rolled onto its parent before restricting to scope_core
        # (already parent-rolled) -- required whenever this is configured, not gated by
        # dc_instock.enabled.
        "inventory_warehouse": ["operation", "inventory_warehouse"],
        # Parent/child item-family map -- superseded/child products (is_main=false) roll up onto
        # parent_id. Used by the inventory_warehouse rollup; read unconditionally.
        "item_family": ["operation", "item_family"],
        # Platform scope table backing scope_source.mode="operation_scope".
        "scope": ["operation", "scope"],
        # Goods-in-transit snapshots backing instock_daily.git_date_shift_days, dc_instock.git_date_shift_days
        # and inventory_git.git_date_shift_days (destination_type 0 = store, 1 = warehouse).
        "goods_in_transit": ["operation", "goods_in_transit"],
        "products": ["master-data", "products"],
        # Add a model_id=... path segment here if your lost-sales table is partitioned by model.
        "lost_sales": ["noob", "lost-sales"],
        "defined_scope": ["analysis", "instock_rate", "instock_rate_scope"],
        # product_agg_level -> product_id map for lost_sales_source/instock_source's
        # product_agg_level_col (see README) -- only needed if either source is keyed by a
        # planning/DFU level instead of product_id.
        "product_planning_level": ["operation", "product_planning_level"],
    },
    "input_filters": {
        # Optional Spark SQL expressions applied when reading each source.
        # NOTE: when lost_sales_ensemble.enabled=True, this "lost_sales" filter list is
        # applied to BOTH the fast (path_segments.lost_sales) and slow
        # (lost_sales_ensemble.slow_path_segments) sources — same schema, same filters.
        "defined_scope": [],
        "lost_sales": [],
        "daily_data": ["usable = 1"],
        "inventory_warehouse": [],
        "item_family": [],
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES SOURCE — column mapping for the raw lost-sales table
    # ---------------------------------------------------------------------------
    # Downstream code always sees canonical column names regardless of this mapping (see
    # README). Defaults below match a typical noob/lost-sales schema -- override only the
    # columns your own table names differently.
    # product_col / product_agg_level_col: configure exactly ONE, never both. If the source has
    # a native product_id-level column, set product_col to it (product_col always takes
    # precedence when present -- see README). If it doesn't (keyed by planning/DFU level
    # instead), set product_col to None and set product_agg_level_col -- the pipeline joins it
    # to path_segments.product_planning_level to derive product_id (see README).
    "lost_sales_source": {
        "week_col": "week_start_date",
        "product_col": "product_id",
        "store_col": "store_id",
        "lost_sales_col": "lost_sales",
        "in_stock_col": "in_stock",
        "total_days_col": "details.total_days",  # supports a dotted nested-struct path
        "product_agg_level_col": None,
        # Spark SQL expressions narrowing the DAILY-DATA sales that form the OTHER half of
        # lost_sales_pct's denominator (lost_sales / (sales + lost_sales)). Applies to nothing
        # else -- total_sales_quantity, mean_stock, WOS and friends keep the full population.
        #
        # Set this when the lost-sales table covers a NARROWER population than daily_data does.
        # A model built to exclude e-commerce, for example, gives a numerator with no ecom while
        # daily_data's sales still carry it, inflating the denominator and reading lost_sales_pct
        # LOW. Excluding the same stores here puts both halves on one population, e.g.:
        #   "sales_filter": ["store_id NOT IN (9001, 9002)"]
        # Any column present on daily_data can be used. Empty = no narrowing.
        "sales_filter": [],
    },
    # ---------------------------------------------------------------------------
    # INSTOCK SOURCE — optional override to read in-stock days from a DIFFERENT table
    # ---------------------------------------------------------------------------
    # OFF by default: in-stock/total-days come from lost_sales_source above. Turn this on
    # only when a DIFFERENT pipeline computes instock than the one computing lost sales.
    # Mutually exclusive with lost_sales_ensemble below.
    #
    # Real-world example (one deployment's actual config, kept here to illustrate
    # fallback_sources -- adapt the path/columns for your own source, don't copy verbatim):
    #   "enabled": True,
    #   "path_segments": ["reporting", "future_visibility", "some_report_table"],
    #   "week_col": "TY_week_start_date",
    #   "in_stock_col": "TY_total_days_instock",
    #   "total_days_col": "TY_total_day",
    #   "product_agg_level_col": "product_agg_level",
    #   "store_col": None,
    #   # fallback_sources: additional column-sets from the SAME table, appended in order to
    #   # fill weeks the primary column-set doesn't have (e.g. a rolling-window source whose
    #   # TY_ only reaches back so far, backfilled by LY_/LLY_ columns carrying the identical
    #   # formula for the calendar week 52/104 weeks earlier -- see README). Each fallback
    #   # entry inherits product_col/store_col/product_agg_level_col from above unless
    #   # overridden.
    #   "fallback_sources": [
    #       {"week_col": "LY_week_start_date", "in_stock_col": "LY_total_days_instock", "total_days_col": "LY_total_day"},
    #       {"week_col": "LLY_week_start_date", "in_stock_col": "LLY_total_days_instock", "total_days_col": "LLY_total_day"},
    #   ],
    # product_col / product_agg_level_col: same "configure exactly one" rule as
    # lost_sales_source above -- product_col (when set and present on the source) always wins.
    "instock_source": {
        "enabled": False,
        "path_segments": None,  # required when enabled=True, e.g. ["some", "instock", "table"]
        "week_col": "week_start_date",
        "product_col": "product_id",
        "store_col": "store_id",
        "in_stock_col": "in_stock",
        "total_days_col": "total_days",
        "product_agg_level_col": None,
        "fallback_sources": [],  # optional additional column-sets from the same table (see above)
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
    # inventory_warehouse defaults ON (preserves today's always-on behaviour).
    # daily_data defaults ON too -- build_scoped_daily's own join to already-parent-rolled
    # scope_core/ctx.products_attr otherwise silently DROPS any daily-data row still carrying a
    # child/superseded product_id (a pre-existing bug; this toggle fixes it by default). Turning
    # it on can shift historical numbers for any product with a supersede history.
    # lost_sales defaults OFF -- report_dfu already does its own supersede substitution upstream,
    # so a second rollup here would likely be a no-op; kept available as an opt-in safety net.
    # defined_scope defaults OFF for the same reason -- a client's scope source may already be
    # rolled to parent product_id upstream (as tbretail's is); this is an opt-in safety net for a
    # client whose scope source isn't. Requires path_segments.item_family whenever any of the
    # four is True.
    "item_family_rollup": {
        "daily_data": True,
        "lost_sales": False,
        "inventory_warehouse": True,
        "defined_scope": False,
        # Goods in transit (instock_daily / dc_instock / inventory_git) rolled to the family main.
        "goods_in_transit": True,
    },
    # ---------------------------------------------------------------------------
    # DC INSTOCK — gated: DC in-stock rate from an expanded inventory_warehouse grid
    # ---------------------------------------------------------------------------
    # OFF by default -- requires path_segments.item_family to point at a real table. Each pair's
    # grid starts at its own first stocked day, so a pair ranged at a DC but never once stocked is
    # absent rather than reading 0% (see README's "dc_instock" section). When disabled,
    # dc_in_stock_rate is emitted as a literal null column so the output shape stays constant.
    #   git_date_shift_days:       None = stocked means inventory > stock_threshold only. An integer
    #                              also counts days with goods in transit to the DC
    #                              (goods_in_transit destination_type 1, quantity > 0, rolled to the
    #                              family main); a snapshot dated D+1 describes the end of day D -> -1.
    "dc_instock": {
        "enabled": False,
        "stock_threshold": 0,  # a day counts as "stocked" when inventory > stock_threshold
        "git_date_shift_days": None,
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
        "mode": "defined_scope",
        "solution_id": 21,
        "run_date": None,  # "YYYY-MM-DD" Sunday; None = latest Sunday on or before today
        "roll_to_family_main": True,
        "active_only": True,
    },
    # ---------------------------------------------------------------------------
    # BLOCKED SCOPE — UI blocks removed from every daily-data-derived metric
    # ---------------------------------------------------------------------------
    # OFF by default (ui_parameters_path None); setting ui_parameters_path turns it on. Reads the UI
    # parameter snapshot {ui_parameters_path}/blocked_scope/{product,product_destination,destination}
    # (parquet; destination_id = store) for scope_source.solution_id.
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
    #   dc_solution_id:           None = no DC blocks. An integer (the DC solution, e.g. 22) removes
    #                             {ui_parameters_path}/dc_blocked_scope days of that solution, by "rule",
    #                             against each DC pair's scope_start from operation/scope of that solution
    #                             (same run_date / roll-up / active filter as scope_source), from every DC
    #                             metric: dc_mean_stock, WOS_DC, the DC part of WOS_TOTAL / total_mean_stock
    #                             and dc_in_stock_rate. DC pairs outside that scope get no blocks.
    "blocked_scope": {
        "ui_parameters_path": None,  # path under the datastore root; None = blocked scope off
        "rule": "after_scope_start",
        "dc_solution_id": None,
    },
    # ---------------------------------------------------------------------------
    # INSTOCK DAILY — in-stock rate built from daily-data (replaces the weekly in-stock read)
    # ---------------------------------------------------------------------------
    # OFF by default. When on, in_stock_rate comes from noob/daily-data over the scope pairs
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
        "enabled": False,
        "git_date_shift_days": -1,
        "count_start": "first_daily_row",  # "scope_start" / "earliest" need scope_source operation_scope
        "require_daily_data": True,
        "history_start": None,
        "usable_only": True,
        "input_filters": [],
    },
    # ---------------------------------------------------------------------------
    # INVENTORY GIT — gated goods in transit on the inventory metrics
    # ---------------------------------------------------------------------------
    # OFF by default (metrics = []): every inventory metric is on-hand only. A metric named in
    # "metrics" uses on-hand + goods in transit (GIT) units instead (retail = units x price_without_tax,
    # cost = units x cogs); every metric not named keeps on-hand only and its exact previous value.
    #   git_date_shift_days: None = off. An integer turns GIT on: a goods_in_transit snapshot dated D+1
    #                        describes the end of day D, so the date shifts by -1 (as instock_daily).
    #                        Required (an integer) when "metrics" is not empty.
    #   metrics:             any of "total_inventory", "mean_stock" (+ mean_stock_retail / _cost),
    #                        "wos" (WOS, wos_revenue, wos_cost and the store part of WOS_TOTAL),
    #                        "inventory_turnover_rate" (the mean stock inside it), "dc_mean_stock" (and
    #                        the DC part of total_mean_stock), "wos_dc" (WOS_DC and the DC part of
    #                        WOS_TOTAL). total_mean_stock's store part follows "mean_stock".
    # Store GIT = goods_in_transit destination_type 0, quantity > 0, summed per product x store x day
    # rolled to the family main, limited to scoped pairs; it is joined to the daily rows (a GIT-only
    # day gets zero sales / on-hand) BEFORE blocked scope removes days, so blocked days drop both. GIT-only
    # days on which input_filters.daily_data removed the pair's daily row (e.g. usable = 1) are dropped.
    # DC GIT = destination_type 1, per product x warehouse x day (DC blocked days are not applied to the
    # DC inventory metrics). Sales, in-stock and lost sales never change. Requires
    # fiscal_calendar.use_fiscal_calendar=True when "metrics" is not empty.
    "inventory_git": {
        "git_date_shift_days": None,
        "metrics": [],
    },
    # ---------------------------------------------------------------------------
    # LOST-SALES ENSEMBLE — blend two lost-sales models by product sales speed
    # ---------------------------------------------------------------------------
    # OFF by default: lost_sales_source above is read as a single source, as-is. Turn this on
    # to blend a fast-mover model (path_segments.lost_sales) with a slow-mover model (below) by
    # each product's own sales-speed cluster instead -- fast movers (cluster in
    # fast_mover_clusters) take the fast model; everyone else (other clusters AND products with
    # no/NULL cluster) takes the slow model. All three aggregate fields (lost_sales, in_stock,
    # total_days) for a given product/store/week always come from the SAME chosen model.
    "lost_sales_ensemble": {
        "enabled": False,
        "slow_path_segments": ["noob", "lost-sales"],  # e.g. a longer-lookback model variant
        # Product sales-speed source. "long" = a long-format attributes table (one row per
        # product_id x attribute_name); "wide" = the cluster is already its own column.
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
    "slices": {
        "dimensions": ["brand"],  # any column(s) on your products table
        "derived_dimensions": {
            # Example: a SQL CASE expression evaluated against the products table, producing
            # a new cut dimension not present as a raw column. Replace with your own, or
            # remove this entry if you don't need any derived dimensions.
            "example_derived": "CASE WHEN brand = 'A' THEN 'Group A' ELSE 'Other' END",
        },
        # Restrict which values of a slice dimension appear in the breakdown (that
        # dimension only; Overall and other slices are unaffected). Two shapes:
        #   LIST (include-only): omit -> all incl NULL | [] -> all non-null | ["A","B"] -> only those
        #   DICT (include/exclude): {"include": ["A"]} keep only A | {"exclude": ["A"]} keep the
        #       rest incl NULL | add "keep_null": True/False to force the NULL bucket.
        "value_filters": {},
    },
    # ---------------------------------------------------------------------------
    # DIMENSION SOURCES — optional external tables that become named ROOT populations
    # ---------------------------------------------------------------------------
    # Gated feature: disabled example below shows the pattern. Each enabled source is
    # left-joined onto products by join_key, contributes new slice dimension(s), and — via
    # root_values — can also become its own root population tab (see README's "Dimension
    # sources -> roots" section) instead of an ordinary flat cut.
    "dimension_sources": [
        {
            "enabled": False,
            "label": "example_dimension_source",
            "source": "delta",
            "path_segments": ["operation", "some_attribute_table"],
            "join_key": "product_id",
            "columns": [],
            "derived": {
                # Products absent from the source get NULL, not the ELSE branch -- fillna
                # (below) imputes that if you need a clean two-value split with no NULLs.
                "IS_EXAMPLE_FLAG": "CASE WHEN some_column = 'X' THEN 'yes' ELSE 'no' END",
            },
            "fillna": {"IS_EXAMPLE_FLAG": "no"},
            # Root "example_root" = IS_EXAMPLE_FLAG=='yes' only ('no'/NULL aren't their own root).
            "root_values": {"IS_EXAMPLE_FLAG": {"yes": "example_root"}},
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
        # OFF by default -- requires run_min_date to span at least 2 years (e.g. "2024-01-01"
        # to cover a 2024-vs-2025 link) to have anything to compute. Like-for-like: recomputes
        # metrics over only the (product_id, store_id) pairs present in EVERY year of the run
        # window -- one shared universe reused across every consecutive-year link, not a separate
        # universe per link.
        "enabled": False,
        # Which comparable kinds to compute -- see COMPARABLE_KINDS_ALL above. "quarter" builds
        # its own pair universe per quarter number (a pair must appear in that quarter of every
        # year that has it, independent of the other quarter numbers); "half" does the same per
        # half number.
        "kinds": ["ytd"],
        # grain — what a "same pair across every year" actually means. Independent of
        # defined_scope.grain above (scoped_daily carries store_id whatever the scope grain):
        #   "product_store" (default) -> the population is the distinct (product_id, store_id)
        #                     pairs present in every qualifying year. A product that opened or
        #                     closed in one store drops that store's rows from every year.
        #   "product"      -> the population is the distinct product_id values present in every
        #                     qualifying year; every store of a qualifying product is then kept.
        #                     Same-store movement is NOT isolated -- a product that gained or lost
        #                     stores between the compared years still shifts the metrics. Use it
        #                     when the store estate itself churns enough that a pair-level
        #                     intersection leaves too small a population to be meaningful.
        # Only the store-side population is affected; dc_daily/dc_inst keep their own
        # (product_id, warehouse_id) universe under both values (DC has no store dimension).
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
            "wos_revenue",
            "wos_cost",
            "WOS_DC",
            "WOS_TOTAL",
            "inventory_turnover_rate",
            "in_stock_rate",
            "weighted_instock_rate",
            "dc_in_stock_rate",
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
            "dc_in_stock_rate",
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
            "dc_mean_stock": "Daily DC stock avg (M units)",
            "total_mean_stock": "Daily total stock avg (M units)",
            "WOS": "WOS (units)",
            "wos_revenue": "WOS revenue",
            "wos_cost": "WOS cost",
            "WOS_DC": "WOS (DC)",
            "WOS_TOTAL": "WOS (Total)",
            "inventory_turnover_rate": "Inventory Turnover Rate",
            "in_stock_rate": "In-Stock Rate",
            "weighted_instock_rate": "Weighted In-Stock Rate",
            "dc_in_stock_rate": "DC In-Stock Rate",
            "lost_sales_pct": "Lost Sales %",
            "distinct_product_count": "Distinct products",
            "distinct_store_count": "Distinct stores",
            "distinct_pair_count": "Distinct pairs",
        },
        "pp_change_metrics": ["in_stock_rate", "weighted_instock_rate", "dc_in_stock_rate", "lost_sales_pct"],
        # ---------------------------------------------------------------------------
        # POPULATION FILTERS — restrict specific metrics to a narrower product population
        # ---------------------------------------------------------------------------
        # Optional. Excludes/includes products from ONE metric's calculation without touching
        # scope, roots, or any other metric -- e.g. "in-stock rate should never count NON-COMP
        # products, even in the Overall root" or "WOS without NVROUT". Applied ON TOP OF
        # whatever root/cut is already in effect (so it's a no-op inside a root that already
        # restricts to the same value, e.g. filtering IS_COMP inside the "comp" root).
        #
        # Shape: {metric_col: {dim_col: value_filter_spec}} -- dim_col is any dimension_sources
        # or slices column already joined onto products (e.g. "IS_COMP", "IS_NVROUT", "brand").
        # value_filter_spec is the SAME shape as slices.value_filters: a list (include-only) or
        # a dict with include/exclude/keep_null.
        #
        # CONSTRAINT: metric_cols computed in one shared aggregation pass can only be filtered
        # TOGETHER, not independently of each other -- see kpi_pipeline/filters.py's
        # METRIC_FILTER_GROUPS for the exact groupings:
        #   sales group:  total_sales_quantity, total_sales_revenue, total_inventory, AUR, AUC,
        #                 distinct_product_count, distinct_store_count, distinct_pair_count
        #   wos group:    WOS, wos_revenue, wos_cost
        #   wos_dc_total group: WOS_DC, WOS_TOTAL
        #   mean_stock group: mean_stock, mean_stock_retail, mean_stock_cost
        #   dc_inventory group: dc_mean_stock, total_mean_stock
        #   (inventory_turnover_rate, in_stock_rate, weighted_instock_rate, dc_in_stock_rate,
        #    lost_sales_pct are each their own independent group.)
        # Setting an entry on any one column in a group applies it to the whole group; setting
        # conflicting specs on two columns in the same group fails loudly at runtime.
        #
        # Example -- exclude NON-COMP from both in-stock metrics, WOS without NVROUT:
        #   "population_filters": {
        #       "in_stock_rate":         {"IS_COMP": {"exclude": ["no"]}},
        #       "weighted_instock_rate": {"IS_COMP": {"exclude": ["no"]}},
        #       "WOS":                   {"IS_NVROUT": {"exclude": ["yes"]}},
        #   },
        "population_filters": {},
    },
    # =============================================================================
    # OUTPUT & REPORTING
    # =============================================================================
    "output": {
        "save_outputs": True,
        "path_segments": ["analysis", "kpi_reports", "outputs"],
        "run_date": None,
        "save_mode": "initial",
        "allow_overwrite_existing": True,
        "recompute_comparisons_from_history": True,
    },
    "html_report": {
        "enabled": True,
        "filename": "kpi_report_{customer}_{report_end}.html",
        "report_title": "KPI Report",  # customize per client, e.g. "Acme Corp KPI Report"
        "output_path_segments": None,
        "metric_definitions": {},
        # Set to 3 or 4 to cap if the report gets too wide with many years in view.
        "weekly_display_weeks": 5,
        "monthly_display_months": 5,
        "quarterly_display_quarters": 5,
        "half_display_halves": 4,
        "yearly_display_years": None,
        # root id -> tab label; roots not listed fall back to "Overall" / the root id.
        "root_labels": {},
        # slice dimension name -> tab label, display only (saved outputs keep the dimension name),
        # e.g. {"brand": "Banner"}; dimensions not listed show their name title-cased.
        "dimension_labels": {},
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

    ifr = out.setdefault("item_family_rollup", {})
    if "KPI_ITEM_FAMILY_ROLLUP_DAILY_DATA" in os.environ:
        ifr["daily_data"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_DAILY_DATA"])
    if "KPI_ITEM_FAMILY_ROLLUP_LOST_SALES" in os.environ:
        ifr["lost_sales"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_LOST_SALES"])
    if "KPI_ITEM_FAMILY_ROLLUP_INVENTORY_WAREHOUSE" in os.environ:
        ifr["inventory_warehouse"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_INVENTORY_WAREHOUSE"])
    if "KPI_ITEM_FAMILY_ROLLUP_DEFINED_SCOPE" in os.environ:
        ifr["defined_scope"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_DEFINED_SCOPE"])
    if "KPI_ITEM_FAMILY_ROLLUP_GOODS_IN_TRANSIT" in os.environ:
        ifr["goods_in_transit"] = _parse_bool(os.environ["KPI_ITEM_FAMILY_ROLLUP_GOODS_IN_TRANSIT"])

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
    report_end_mode: str,
) -> Dict[str, Any]:
    report_start = _sunday_of_week(datetime.date(as_of.year, 1, 1))
    report_end = as_of if report_end_mode == "latest_day" else _last_completed_saturday(as_of)
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
        "goods_in_transit": bool(item_family_rollup_cfg.get("goods_in_transit", True)),
    }

    dc_instock_cfg = cfg.get("dc_instock", {}) or {}
    dc_instock_enabled = bool(dc_instock_cfg.get("enabled", False))
    dc_instock_stock_threshold = dc_instock_cfg.get("stock_threshold", 0)

    report_end_mode = rw["report_end"]
    if report_end_mode not in ("as_of", "complete_month", "latest_day"):
        raise ValueError(
            f"reporting_window.report_end must be 'as_of', 'complete_month' or 'latest_day'; got {report_end_mode!r}"
        )
    window = _resolve_report_window(
        datetime.date.fromisoformat(rw["as_of_date"]),
        run_min,
        report_end_mode,
    )

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
        # DC blocks of the same snapshot, read only when dc_solution_id is set.
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
    blocked_scope["dc_solution_id"] = blocked_scope_cfg["dc_solution_id"]
    if blocked_scope["dc_solution_id"] is not None:
        if type(blocked_scope["dc_solution_id"]) is not int:
            raise ValueError("blocked_scope.dc_solution_id must be an integer or None")
        if blocked_scope["path"] is None:
            raise ValueError("blocked_scope.dc_solution_id requires blocked_scope.ui_parameters_path")

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
    if report_end_mode == "latest_day" and not instock_daily["enabled"]:
        raise ValueError(
            "reporting_window.report_end='latest_day' requires instock_daily.enabled=True: the YTD cut at the "
            "latest day splits the daily in-stock frame's fiscal week, which weekly in-stock sources cannot do"
        )

    inventory_git_cfg = cfg["inventory_git"]
    inventory_git_shift_days = inventory_git_cfg["git_date_shift_days"]
    if inventory_git_shift_days is not None and type(inventory_git_shift_days) is not int:
        raise ValueError("inventory_git.git_date_shift_days must be an integer or None")
    inventory_git_requested = set(inventory_git_cfg["metrics"])
    unknown_inventory_git_metrics = sorted(inventory_git_requested - set(INVENTORY_GIT_METRICS_ALL))
    if unknown_inventory_git_metrics:
        raise ValueError(
            f"inventory_git.metrics has unknown names {unknown_inventory_git_metrics}; "
            f"allowed: {list(INVENTORY_GIT_METRICS_ALL)}"
        )
    inventory_git_metrics = [m for m in INVENTORY_GIT_METRICS_ALL if m in inventory_git_requested]
    if inventory_git_metrics and inventory_git_shift_days is None:
        raise ValueError("inventory_git.metrics is set but inventory_git.git_date_shift_days is None; set an integer")
    if inventory_git_metrics and not cfg["fiscal_calendar"]["use_fiscal_calendar"]:
        raise ValueError("inventory_git.metrics requires fiscal_calendar.use_fiscal_calendar=True")

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
    # meaningful when comparable_pairs.enabled=True; resolves to an empty list when disabled (no
    # need to force a non-empty kinds list on an off feature).
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

    # Grain of the comparable same-pairs population itself (NOT defined_scope.grain -- see the
    # comparable_pairs config block). Validated the same way defined_scope.grain is, above.
    comparable_pairs_grain = comparable_pairs_cfg.get("grain", "product_store")
    valid_comparable_grains = {"product", "product_store"}
    if comparable_pairs_grain not in valid_comparable_grains:
        raise ValueError(
            f"comparable_pairs.grain must be one of {sorted(valid_comparable_grains)}; "
            f"got {comparable_pairs_grain!r}"
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
        "INSTOCK_SOURCE_ENABLED": instock_source_enabled,
        "INSTOCK_SOURCE_COLUMN_MAP": instock_source_column_map,
        "ITEM_FAMILY_COLUMN_MAP": item_family_column_map,
        "ITEM_FAMILY_ROLLUP": item_family_rollup,
        "SCOPE_SOURCE": scope_source,
        "BLOCKED_SCOPE": blocked_scope,
        "INSTOCK_DAILY": instock_daily,
        "INVENTORY_GIT": {"git_date_shift_days": inventory_git_shift_days, "metrics": inventory_git_metrics},
        "DC_INSTOCK_ENABLED": dc_instock_enabled,
        "DC_INSTOCK_STOCK_THRESHOLD": dc_instock_stock_threshold,
        "DC_INSTOCK_GIT_DATE_SHIFT_DAYS": dc_instock_git_date_shift_days,
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

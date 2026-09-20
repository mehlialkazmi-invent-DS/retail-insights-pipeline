"""Gated 'comparable pairs' (like-for-like) comparisons — ytd / yoy / quarter.

Each enabled kind (see config.py's comparable_pairs.kinds / COMPARABLE_KINDS_ALL) recomputes
metrics over only the pairs present in EVERY qualifying year for that kind -- not just the two
years of a given link -- then compares each consecutive-year link within that shared population:

  ytd     — pairs present in every window year, compared on each year's elapsed (fully-closed-
            months) window, chained across every consecutive pair of years.
  yoy     — pairs present in every window year, compared on the full window year (including a
            partial first/last year, same accepted behaviour as the regular non-comparable
            Annual/YoY tab), chained across every consecutive pair of years (not just the latest
            two, unlike the regular non-comparable YoY comparison in comparisons.py).
  quarter — computed independently PER QUARTER NUMBER: for quarter Q, only years where Q falls
            entirely inside the report window count (see _complete_quarter_years -- a partial
            quarter at either window boundary is excluded outright, since REPORT_END_DATE is a
            week boundary that essentially never aligns to a quarter boundary). A pair must be
            present in quarter Q of every one of those years; each quarter number has its own
            fully independent population and year-chain.

The same-pairs population's own grain is comparable_pairs.grain (config.py), NOT defined_scope.grain:

  "product_store" (default) — (product_id, store_id) pairs present in every qualifying year, used
            under EVERY defined_scope.grain: scoped_daily comes straight from daily-data and is
            store-level whatever the scope grain, so like-for-like still means the same pairs
            present in every year regardless of how the report's own scope is defined.
  "product" — product_ids present in every qualifying year; every store of a qualifying product is
            kept, so store-estate churn is NOT isolated. Use only when a pair-level intersection
            leaves too small a population to be meaningful.

Frames with no store_id of their own are restricted to that universe's distinct products instead
(as is every frame under grain="product"); dc_daily/dc_inst each keep their own independent
(product_id, warehouse_id) universe under both grains. See _restrict_frames's docstring.

Produced for a kind only with >=2 qualifying years (quarter: >=2 years with that quarter number),
gracefully skipped otherwise. Gated overall by comparable_pairs.enabled, per-kind by
comparable_pairs.kinds.

comparable_kpi_long rows carry link_prior_year/link_current_year so the incremental-save row key
(period_type, period, root, dimension, dimension_value, link_prior_year, link_current_year)
doesn't collide across links -- a year appears as "current" in one link and "prior" in the next,
both sharing the same restricted population, and the link tag is what tells those rows apart.
Quarter rows also carry a plain (non-key) quarter_number column for the HTML renderer to group by
-- period_type ("quarter") + period ("2024-Q1"-style) already disambiguate the row key on their own.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.comparisons import _comparison_dimensions, _comparison_roots, _series_groups, build_comparison_long
from kpi_pipeline.context import KPIContext
from kpi_pipeline.kpi_long import _filter_frames_for_dimension, _period_frames, _period_label
from kpi_pipeline.metrics import build_kpi_table

_PAIR_KEYS = ["product_id", "store_id"]
_PRODUCT_KEYS = ["product_id"]
_DC_PAIR_KEYS = ["product_id", "warehouse_id"]
_RESTRICT_FRAMES = ("scoped_daily", "inst_data", "lost_base", "scope_pairs", "scope_pair_weeks")

# config.py's comparable_pairs.grain -> the key columns the store-side same-pairs universe is
# intersected on. The DC universe (_DC_PAIR_KEYS) is not configurable: warehouse inventory has no
# store dimension at all, so there is nothing for a product-grain setting to drop from it.
_GRAIN_PAIR_KEYS: Dict[str, List[str]] = {
    "product_store": _PAIR_KEYS,
    "product": _PRODUCT_KEYS,
}

# kind -> (kpi_long period_type this kind's rows are tagged with, ctx save attr, ctx display attr)
_KIND_CTX_ATTRS: Dict[str, Tuple[str, str, str]] = {
    "ytd": ("ytd", "comparable_comparison_ytd", "comparable_ytd_display"),
    "yoy": ("annual", "comparable_comparison_yoy", "comparable_yoy_display"),
    "quarter": ("quarter", "comparable_comparison_quarter", "comparable_quarter_display"),
}


def _restrict_frames(
    period_frames: Dict[str, DataFrame],
    comparable_keys: DataFrame,
    dc_comparable_keys: DataFrame,
    dc_inst_comparable_keys: DataFrame,
    pair_keys: List[str],
) -> Dict[str, DataFrame]:
    """Restrict every frame to the years' common pairs/products, and dc_daily to its own
    common (product, warehouse) pairs.

    ``pair_keys`` is the store-side universe's own key columns, resolved once from
    comparable_pairs.grain (_GRAIN_PAIR_KEYS): _PAIR_KEYS for "product_store", _PRODUCT_KEYS for
    "product". Under "product" EVERY frame is restricted by product alone -- scoped_daily keeps
    all of a qualifying product's store rows (its rows still carry store_id, they are simply not
    required to match a pair), which is the whole point of that setting.

    Under the default "product_store", like-for-like is pair-level under every defined_scope.grain,
    not only the store-level ones: scoped_daily is store-level straight from daily_data whatever
    the scope grain (see build_scoped_daily's has_store=False path, which restricts by product but
    leaves every store's rows intact), so the same pairs can be required in every year even for a
    product-grain report. Keeping the restriction pair-level under all scope grains is what makes
    comparable genuinely like-for-like rather than something that silently weakens with the scope
    configuration.

    Frames carrying no store_id of their own -- inst_data/lost_base when lost_sales_source has no
    store_col, plus scope_pairs/scope_pair_weeks when neither scope nor lost-sales has a store
    dimension -- are restricted to the pair universe's DISTINCT PRODUCTS instead. Collapsing to
    distinct products first is what stops that join fanning their rows out one-per-store; their
    values stay product-level totals, only the product universe is made like-for-like.

    dc_daily carries no store_id (DC/warehouse inventory has no store dimension at all), so it can
    never share the pair keys above -- it gets its own independent (product_id, warehouse_id)
    same-pairs restriction instead, exactly mirroring total_inventory_wos_ytd.ipynb's two
    independent same-pairs design (product x store from daily-data, product x warehouse from
    inventory_warehouse, each restricted on its own terms rather than one restriction forced onto
    both). dc_inst shares dc_daily's (product_id, warehouse_id) grain, id space and inventory
    source but still gets its own all-years intersection: dc_inst 0-fills every day from a pair's
    first stocked day to the report window's end, so a pair that stopped being stocked partway
    through still has rows (all of them stockouts) in the later years where dc_daily has none.
    Its per-year universe is therefore a superset of dc_daily's, and reusing dc_comparable_keys
    would delete exactly the sustained stockouts dc_in_stock_rate exists to surface.
    """
    out = dict(period_frames)
    comparable_products = comparable_keys.select(*_PRODUCT_KEYS).distinct()
    store_level_universe = pair_keys == _PAIR_KEYS
    for key in _RESTRICT_FRAMES:
        frame = out.get(key)
        if frame is None:
            continue
        pair_level = store_level_universe and "store_id" in frame.columns
        out[key] = frame.join(
            comparable_keys if pair_level else comparable_products,
            on=_PAIR_KEYS if pair_level else _PRODUCT_KEYS,
            how="inner",
        )
    out["dc_daily"] = out["dc_daily"].join(dc_comparable_keys, on=_DC_PAIR_KEYS, how="inner")
    out["dc_inst"] = out["dc_inst"].join(dc_inst_comparable_keys, on=_DC_PAIR_KEYS, how="inner")
    return out


def _complete_quarter_years(ctx: KPIContext, quarter: int) -> List[int]:
    """Years for which fiscal quarter ``quarter`` falls ENTIRELY within the report window
    ([EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE]) -- every one of that quarter's real
    calendar weeks is available, not a partial slice truncated at either window boundary.

    REPORT_END_DATE is a week boundary (last completed Saturday), never quarter-aligned, so the
    latest year's occurrence of whichever quarter is currently in progress is virtually always
    partial -- comparing it against a prior year's FULL quarter would silently produce a wildly
    wrong "like-for-like" delta (a quarter 6 weeks in compared to a full 13-week quarter). The
    window's own start can truncate the earliest year's quarter the same way if run_min_date
    doesn't fall on a quarter boundary.

    Delegates to fiscal.complete_fiscal_periods -- NOT ctx.fiscal_week directly. ctx.fiscal_week is
    itself clipped to [EFFECTIVE_REPORT_START_DATE, REPORT_END_DATE] (see
    fiscal.build_fiscal_cal_and_week_from_upload), so a week_start_date/week_end_date queried from
    it can never fall outside the window in the first place -- any quarter present at all would
    trivially pass a bounds check like "q_start >= start and q_end <= end" whether it's really
    complete or not. fiscal.complete_fiscal_periods re-reads the calendar unclipped specifically to
    avoid this; a quarter that's genuinely fully closed but happens to have all-zero
    sales/inventory still counts, and a quarter still in progress never counts even if its partial
    weeks already have real data.
    """
    from kpi_pipeline.fiscal import complete_fiscal_periods

    complete = complete_fiscal_periods(ctx, "Fiscal_Quarter").filter(F.col("Fiscal_Quarter") == quarter)
    return sorted(r["Year"] for r in complete.select("Year").distinct().collect())


def _intersect_years(frame: DataFrame, years: Sequence[int], key_cols: List[str]) -> DataFrame:
    """Intersect ``frame``'s distinct ``key_cols`` across every one of ``years`` -- ``frame``
    must already carry exactly the rows that should count for each year (e.g. pre-filtered to one
    quarter number for the quarter kind)."""
    year_frames = [frame.filter(F.col("Year") == y).select(*key_cols).distinct() for y in years]
    common = year_frames[0]
    for yf in year_frames[1:]:
        common = common.intersect(yf)
    return common.cache()


def _comparable_period_rows(
    ctx: KPIContext,
    frames: Dict[str, DataFrame],
    period_filter,
    metric_cols: List[str],
    period_type: str,
    period_col: str,
) -> pd.DataFrame:
    """kpi_long-shaped rows (every root x cut, mirrors kpi_long.build_kpi_long) matching
    ``period_filter``, tagged with ``period_type`` (the kpi_long period_type this kind renders
    under -- "ytd"/"annual"/"quarter") and labelled via kpi_long._period_label(period_type, ...)."""
    value_filters = ctx.settings.get("SLICE_VALUE_FILTERS", {}) or {}
    cuts: List[Tuple[str, List[str]]] = [("overall", [])] + [(d, [d]) for d in ctx.cut_dimensions]
    roots: List[Optional[Dict[str, str]]] = [None] + list(ctx.root_definitions)
    rows: List[dict] = []
    for root_def in roots:
        if root_def is None:
            root_name, rf = "overall", frames
        else:
            root_name = root_def["root"]
            rf = _filter_frames_for_dimension(
                frames, root_def["dim_col"], {root_def["dim_col"]: [root_def["value"]]}
            )
        for cut_name, gk in cuts:
            sf = _filter_frames_for_dimension(rf, gk[0], value_filters) if gk else rf
            tbl = build_kpi_table(ctx, sf, period_col, gk, period_filter)
            for _, r in tbl.iterrows():
                rec = {
                    "period_type": period_type,
                    "period": _period_label(period_type, r),
                    "root": root_name,
                    "dimension": cut_name,
                    "dimension_value": ("ALL" if not gk else r[gk[0]]),
                }
                for m in metric_cols:
                    rec[m] = r.get(m)
                rows.append(rec)
    return pd.DataFrame(
        rows, columns=["period_type", "period", "root", "dimension", "dimension_value"] + metric_cols
    )


def _comparisons_for_link(
    ctx: KPIContext,
    link_rows: pd.DataFrame,
    prior_year: int,
    current_year: int,
    metric_cols: List[str],
    comparison_type: str,
    period_key_fn,
    display_label_fn,
    change_label_fn,
) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    """Build (overall display, list of per-root-per-dimension save frames) for ONE
    consecutive-year link from its already-computed kpi_long-shaped rows (every root x cut).

    ``period_key_fn``/``display_label_fn``/``change_label_fn`` take a year and return: the exact
    string in the ``period`` column to match (period_key_fn), the prior/current column header
    text (display_label_fn), and the delta column header text (change_label_fn, called with
    current_year only)."""
    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()
    prior_label, current_label = display_label_fn(prior_year), display_label_fn(current_year)
    change_label = change_label_fn(current_year)
    for root in _comparison_roots(ctx):
        root_rows = link_rows[link_rows["root"] == root]
        for dimension in _comparison_dimensions(ctx):
            dim_rows = root_rows[root_rows["dimension"] == dimension]
            for dval, grp in _series_groups(dim_rows, dimension):
                prior_match = grp[grp["period"] == period_key_fn(prior_year)]
                current_match = grp[grp["period"] == period_key_fn(current_year)]
                if prior_match.empty or current_match.empty:
                    continue
                disp, save = build_comparison_long(
                    ctx,
                    prior_label,
                    current_label,
                    change_label,
                    prior_match.iloc[0].to_dict(),
                    current_match.iloc[0].to_dict(),
                    metric_cols,
                    comparison_type,
                    root,
                    dimension,
                    dval,
                )
                if not save.empty:
                    save_parts.append(save)
                if root == "overall" and dimension == "overall" and dval == "ALL" and not disp.empty:
                    display = disp
    return display, save_parts


def _period_key_fns(comparison_type: str, quarter: Optional[int] = None):
    """(period_key_fn, display_label_fn, change_label_fn) for one comparable kind."""
    if comparison_type == "ytd":
        return (
            lambda y: f"YTD-{y}",
            lambda y: f"{y} YTD",
            lambda y: f"YTD {y}",
        )
    if comparison_type == "yoy":
        return (
            lambda y: str(y),
            lambda y: str(y),
            lambda y: f"YoY {y}",
        )
    if comparison_type == "quarter":
        return (
            lambda y: f"{y}-Q{quarter}",
            lambda y: f"{y} Q{quarter}",
            lambda y: f"Q{quarter} {y}",
        )
    raise ValueError(f"unknown comparable comparison_type {comparison_type!r}")


def _build_comparable_kind(
    ctx: KPIContext,
    comparison_type: str,
    metric_cols: List[str],
    quarter: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute one comparable kind's (kpi_long rows, comparison save rows, overall display) —
    empty frames if there are fewer than 2 qualifying years or 0 common pairs. ``quarter`` is
    required (and only meaningful) for comparison_type="quarter".

    The store-side universe's keys come from comparable_pairs.grain, resolved ONCE here and
    threaded through _intersect_years/_restrict_frames. comparable_pair_count therefore counts
    common pairs under grain="product_store" and common PRODUCTS under grain="product"."""
    period_type, _, _ = _KIND_CTX_ATTRS[comparison_type]
    pair_keys = _GRAIN_PAIR_KEYS[ctx.settings["COMPARABLE_PAIRS_GRAIN"]]
    period_col = "period_key" if comparison_type == "quarter" else "Year"
    pf = _period_frames(ctx, ctx.hybrid_frames, period_type)

    def _q(frame: DataFrame) -> DataFrame:
        return frame.filter(F.col("Fiscal_Quarter") == quarter) if comparison_type == "quarter" else frame

    scoped_daily_pop = _q(pf["scoped_daily"])
    dc_daily_pop = _q(pf["dc_daily"])
    dc_inst_pop = _q(pf["dc_inst"])

    years = sorted(r["Year"] for r in scoped_daily_pop.select("Year").distinct().collect())
    if comparison_type == "quarter":
        # Drop any year whose occurrence of this quarter is only partially inside the report
        # window (see _complete_quarter_years) -- never compare a partial quarter to a full one.
        complete_years = set(_complete_quarter_years(ctx, quarter))
        years = [y for y in years if y in complete_years]
    if len(years) < 2:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    common_keys = _intersect_years(scoped_daily_pop, years, pair_keys)
    dc_common_keys = _intersect_years(dc_daily_pop, years, _DC_PAIR_KEYS)
    dc_inst_common_keys = _intersect_years(dc_inst_pop, years, _DC_PAIR_KEYS)
    pair_count = common_keys.count()
    if pair_count == 0:
        common_keys.unpersist()
        dc_common_keys.unpersist()
        dc_inst_common_keys.unpersist()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    restricted = _restrict_frames(pf, common_keys, dc_common_keys, dc_inst_common_keys, pair_keys)

    kpi_parts: List[pd.DataFrame] = []
    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()

    for prior_year, current_year in zip(years, years[1:]):
        period_filter = F.col("Year").isin([prior_year, current_year])
        if comparison_type == "quarter":
            period_filter = period_filter & (F.col("Fiscal_Quarter") == quarter)
        rows = _comparable_period_rows(ctx, restricted, period_filter, metric_cols, period_type, period_col)

        tagged = rows.copy()
        tagged.insert(0, "comparison_type", comparison_type)
        tagged["comparable_pair_count"] = pair_count
        tagged["link_prior_year"] = prior_year
        tagged["link_current_year"] = current_year
        if comparison_type == "quarter":
            tagged["quarter_number"] = quarter
        kpi_parts.append(tagged)

        period_key_fn, display_label_fn, change_label_fn = _period_key_fns(comparison_type, quarter)
        disp, parts = _comparisons_for_link(
            ctx, rows, prior_year, current_year, metric_cols,
            comparison_type, period_key_fn, display_label_fn, change_label_fn,
        )
        if comparison_type == "quarter":
            for p in parts:
                p["quarter_number"] = quarter
        save_parts.extend(parts)
        if not disp.empty:
            display = disp  # latest link's overall display wins, full detail is in the save table

    common_keys.unpersist()
    dc_common_keys.unpersist()
    dc_inst_common_keys.unpersist()

    kpi_long_rows = pd.concat(kpi_parts, ignore_index=True) if kpi_parts else pd.DataFrame()
    comparison_rows = pd.concat(save_parts, ignore_index=True) if save_parts else pd.DataFrame()
    return kpi_long_rows, comparison_rows, display


def build_comparable_pairs(ctx: KPIContext) -> None:
    """Populate comparable_kpi_long + comparable_comparison_{kind} for every kind in
    comparable_pairs.kinds, when comparable_pairs.enabled=True.

    Each kind's pair universe is computed ONCE, as the intersection across every qualifying year
    (not per link) -- a pair/product must be present in ALL of those years to count at all. Every
    consecutive-year link within a kind is then compared over that same shared population.
    """
    ctx.comparable_kpi_long = pd.DataFrame()
    for _, save_attr, display_attr in _KIND_CTX_ATTRS.values():
        setattr(ctx, save_attr, pd.DataFrame())
        setattr(ctx, display_attr, pd.DataFrame())

    if not ctx.settings.get("COMPARABLE_PAIRS_ENABLED", False):
        print("comparable pairs: skipped (comparable_pairs.enabled=False)")
        return
    if ctx.hybrid_frames is None:
        raise RuntimeError("comparable_pairs.enabled=True but pipeline frames are missing — run build_kpis first.")

    kinds = ctx.settings.get("COMPARABLE_KINDS") or []
    if not kinds:
        print("comparable pairs: skipped (comparable_pairs.kinds is empty)")
        return

    metric_cols = ctx.settings["METRIC_COLS"]
    # What comparable_pair_count actually counts under the configured comparable_pairs.grain.
    unit = "pairs" if ctx.settings["COMPARABLE_PAIRS_GRAIN"] == "product_store" else "products"
    kpi_long_parts: List[pd.DataFrame] = []
    summary: List[str] = []

    for kind in kinds:
        period_type, save_attr, display_attr = _KIND_CTX_ATTRS[kind]

        if kind != "quarter":
            kpi_rows, save_rows, display = _build_comparable_kind(ctx, kind, metric_cols)
            if not kpi_rows.empty:
                kpi_long_parts.append(kpi_rows)
            setattr(ctx, save_attr, save_rows)
            setattr(ctx, display_attr, display)
            pair_count = int(kpi_rows["comparable_pair_count"].iloc[0]) if not kpi_rows.empty else 0
            summary.append(f"{kind}={pair_count} {unit}" if pair_count else f"{kind}=n/a")
            continue

        # quarter: fully independent per quarter number. Fiscal_Quarter is already a plain column
        # on scoped_daily (from build_scoped_daily's fiscal_week join) -- no need to build the
        # "quarter"-period-framed variant (_with_period_key) just to read it.
        quarters = sorted(
            r["Fiscal_Quarter"]
            for r in ctx.hybrid_frames["scoped_daily"].select("Fiscal_Quarter").distinct().collect()
        )
        q_kpi_parts: List[pd.DataFrame] = []
        q_save_parts: List[pd.DataFrame] = []
        q_display = pd.DataFrame()
        q_summary = []
        for q in quarters:
            kpi_rows, save_rows, display = _build_comparable_kind(ctx, "quarter", metric_cols, quarter=q)
            if not kpi_rows.empty:
                q_kpi_parts.append(kpi_rows)
                pair_count = int(kpi_rows["comparable_pair_count"].iloc[0])
                q_summary.append(f"Q{q}={pair_count} {unit}")
            if not save_rows.empty:
                q_save_parts.append(save_rows)
            if not display.empty:
                q_display = display  # last quarter with data wins for the top-level display slot
        if q_kpi_parts:
            kpi_long_parts.append(pd.concat(q_kpi_parts, ignore_index=True))
        setattr(ctx, save_attr, pd.concat(q_save_parts, ignore_index=True) if q_save_parts else pd.DataFrame())
        setattr(ctx, display_attr, q_display)
        summary.append("quarter=(" + ", ".join(q_summary) + ")" if q_summary else "quarter=n/a")

    ctx.comparable_kpi_long = pd.concat(kpi_long_parts, ignore_index=True) if kpi_long_parts else pd.DataFrame()
    print("comparable pairs:", " | ".join(summary) if summary else "(none)")


def rebuild_comparable_kind_from_saved_rows(
    ctx: KPIContext, merged: pd.DataFrame, comparison_type: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Rebuild one comparable kind's comparison (display, save) from an already-computed,
    link-tagged comparable_kpi_long — e.g. reloaded after an incremental merge onto saved
    history. Pure pandas; no Spark recomputation needed since each link's rows already carry
    that link's own pair-restricted metric values."""
    metric_cols = ctx.settings["METRIC_COLS"]
    rows = merged[merged["comparison_type"] == comparison_type]
    if rows.empty:
        return pd.DataFrame(), pd.DataFrame()

    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()

    if comparison_type != "quarter":
        links = (
            rows[["link_prior_year", "link_current_year"]]
            .drop_duplicates()
            .sort_values(["link_current_year", "link_prior_year"])
        )
        period_key_fn, display_label_fn, change_label_fn = _period_key_fns(comparison_type)
        for _, link in links.iterrows():
            prior_year, current_year = int(link["link_prior_year"]), int(link["link_current_year"])
            link_rows = rows[(rows["link_prior_year"] == prior_year) & (rows["link_current_year"] == current_year)]
            disp, parts = _comparisons_for_link(
                ctx, link_rows, prior_year, current_year, metric_cols,
                comparison_type, period_key_fn, display_label_fn, change_label_fn,
            )
            save_parts.extend(parts)
            if not disp.empty:
                display = disp
    else:
        for q in sorted(rows["quarter_number"].dropna().unique()):
            q = int(q)
            q_rows = rows[rows["quarter_number"] == q]
            links = (
                q_rows[["link_prior_year", "link_current_year"]]
                .drop_duplicates()
                .sort_values(["link_current_year", "link_prior_year"])
            )
            period_key_fn, display_label_fn, change_label_fn = _period_key_fns("quarter", q)
            for _, link in links.iterrows():
                prior_year, current_year = int(link["link_prior_year"]), int(link["link_current_year"])
                link_rows = q_rows[
                    (q_rows["link_prior_year"] == prior_year) & (q_rows["link_current_year"] == current_year)
                ]
                disp, parts = _comparisons_for_link(
                    ctx, link_rows, prior_year, current_year, metric_cols,
                    "quarter", period_key_fn, display_label_fn, change_label_fn,
                )
                for p in parts:
                    p["quarter_number"] = q
                save_parts.extend(parts)
                if not disp.empty:
                    display = disp

    save_all = pd.concat(save_parts, ignore_index=True) if save_parts else pd.DataFrame()
    return display, save_all

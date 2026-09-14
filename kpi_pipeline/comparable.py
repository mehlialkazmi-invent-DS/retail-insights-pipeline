"""Gated 'comparable pairs' (like-for-like) YTD comparison.

Recomputes YTD metrics over only the (product_id, store_id) pairs present in EVERY year of the
run window — e.g. with 2024/2025/2026 all in scope, a pair must be present in all three years to
count at all. That single population is then compared across each consecutive-year link (2024 YTD
vs 2025 YTD, 2025 YTD vs 2026 YTD, ...), so every link shares the exact same restricted universe.
Isolates like-for-like movement from mix shifts caused by newly listed or closed pairs.

Under a product-grain defined_scope (defined_scope.grain="product", no store_col anywhere), the
same mechanism restricts by product_id alone instead of (product_id, store_id) -- the report's
own scope is already store-agnostic there, so a comparable restriction on store identity would
contradict that. See build_comparable_pairs's has_store / _restrict_frames's docstring.

Comparable is YTD-only — there is no comparable YoY/QoQ/MoM/WoW. Pair-level data only exists for
the current run window, so a comparable comparison is produced only when the window spans at least
2 years. Gated by comparable_pairs.enabled — a no-op otherwise.

Each link's rows in comparable_kpi_long still carry link_prior_year/link_current_year, kept so
incremental save's row key (period_type, period, dimension, dimension_value) doesn't collide across
links -- a given year appears in up to two links (as current in one, prior in the next), and even
though both links now share the same restricted population, the merge key still needs the tag to
tell those rows apart.
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


def _restrict_frames(
    period_frames: Dict[str, DataFrame],
    comparable_keys: DataFrame,
    join_keys: List[str],
    dc_comparable_keys: DataFrame,
) -> Dict[str, DataFrame]:
    """Restrict every frame to the years' common keys -- (product, store) pairs when the report's
    own scope grain is store-level, or just products when it's product-grain (join_keys picked by
    the caller from ctx.scope_keys, not per-frame). scoped_daily is always store-level under the
    hood (straight from daily_data) regardless of grain, but under product-grain the report's own
    semantics are store-agnostic (every store counts for an in-scope product, see
    build_scoped_daily's has_store=False path) -- restricting it by product only, ignoring which
    stores carried it in either year, keeps this comparable-pairs restriction consistent with
    that same "don't care which store" intent, not a stricter pair match the rest of a
    product-grain report never applies.

    dc_daily carries no store_id (DC/warehouse inventory has no store dimension at all), so it can
    never share the join_keys above -- it gets its own independent (product_id, warehouse_id)
    same-pairs restriction instead, exactly mirroring total_inventory_wos_ytd.ipynb's two
    independent same-pairs design (product x store from daily-data, product x warehouse from
    inventory_warehouse, each restricted on its own terms rather than one restriction forced onto
    both).
    """
    out = dict(period_frames)
    for key in _RESTRICT_FRAMES:
        if key in out and out[key] is not None:
            out[key] = out[key].join(comparable_keys, on=join_keys, how="inner")
    out["dc_daily"] = out["dc_daily"].join(dc_comparable_keys, on=_DC_PAIR_KEYS, how="inner")
    return out


def _comparable_period_rows(
    ctx: KPIContext,
    frames: Dict[str, DataFrame],
    years: Sequence[int],
    metric_cols: List[str],
) -> pd.DataFrame:
    """kpi_long-shaped YTD rows (every root x cut, mirrors kpi_long.build_kpi_long) for the given
    two years."""
    period_filter = F.col("Year").isin(list(years))
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
            tbl = build_kpi_table(ctx, sf, "Year", gk, period_filter)
            for _, r in tbl.iterrows():
                rec = {
                    "period_type": "ytd",
                    "period": _period_label("ytd", r),
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
) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    """Build (overall display, list of per-root-per-dimension save frames) for ONE
    consecutive-year link from its already-computed kpi_long-shaped rows (every root x cut)."""
    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()
    prior_label, current_label = f"{prior_year} YTD", f"{current_year} YTD"
    for root in _comparison_roots(ctx):
        root_rows = link_rows[link_rows["root"] == root]
        for dimension in _comparison_dimensions(ctx):
            dim_rows = root_rows[root_rows["dimension"] == dimension]
            for dval, grp in _series_groups(dim_rows, dimension):
                prior_match = grp[grp["period"] == f"YTD-{prior_year}"]
                current_match = grp[grp["period"] == f"YTD-{current_year}"]
                if prior_match.empty or current_match.empty:
                    continue
                disp, save = build_comparison_long(
                    ctx,
                    prior_label,
                    current_label,
                    f"YTD {current_year}",
                    prior_match.iloc[0].to_dict(),
                    current_match.iloc[0].to_dict(),
                    metric_cols,
                    "ytd",
                    root,
                    dimension,
                    dval,
                )
                if not save.empty:
                    save_parts.append(save)
                if root == "overall" and dimension == "overall" and dval == "ALL" and not disp.empty:
                    display = disp
    return display, save_parts


def build_comparable_pairs(ctx: KPIContext) -> None:
    """Populate comparable_kpi_long + comparable_comparison_ytd when comparable_pairs.enabled=True.

    The pair universe is computed ONCE, as the intersection across every year in the run window
    (not per link) -- a pair/product must be present in ALL of those years to count at all. Every
    consecutive-year link is then compared over that same shared population, unlike the regular
    (non-comparable) YTD comparison, which compares the full scope with no pair restriction.
    """
    ctx.comparable_comparison_ytd = pd.DataFrame()
    ctx.comparable_ytd_display = pd.DataFrame()
    ctx.comparable_kpi_long = pd.DataFrame()

    if not ctx.settings.get("COMPARABLE_PAIRS_ENABLED", False):
        print("comparable pairs: skipped (comparable_pairs.enabled=False)")
        return
    if ctx.hybrid_frames is None:
        raise RuntimeError("comparable_pairs.enabled=True but pipeline frames are missing — run build_kpis first.")
    if "ytd" not in (ctx.settings.get("COMPARISON_KINDS") or ()):
        print("comparable pairs: skipped ('ytd' not in comparisons.enabled — comparable is YTD-only)")
        return

    metric_cols = ctx.settings["METRIC_COLS"]
    pf = _period_frames(ctx, ctx.hybrid_frames, "ytd")
    scoped_daily = pf["scoped_daily"]

    # Join key for the year-over-year intersection follows the report's own scope grain
    # (ctx.scope_keys), not scoped_daily's own (always store-level) columns -- see
    # _restrict_frames's docstring for why product-grain needs product-only matching here too.
    has_store = "store_id" in ctx.scope_keys
    join_keys = _PAIR_KEYS if has_store else _PRODUCT_KEYS
    unit = "pairs" if has_store else "products"

    years = sorted(r["Year"] for r in scoped_daily.select("Year").distinct().collect())
    if len(years) < 2:
        print("comparable pairs: ytd=n/a (<2 years in window)")
        return

    dc_daily = pf["dc_daily"]

    # Same-pairs universe computed ONCE, as the intersection across EVERY year in the window --
    # a pair/product must survive all of years (not just one link's two) to count for any link.
    year_key_frames = [scoped_daily.filter(F.col("Year") == y).select(*join_keys).distinct() for y in years]
    common_keys = year_key_frames[0]
    for frame in year_key_frames[1:]:
        common_keys = common_keys.intersect(frame)
    common_keys = common_keys.cache()
    pair_count = common_keys.count()

    # DC's own (product_id, warehouse_id) same-pairs universe, independent of the store-side
    # common_keys above -- see _restrict_frames's docstring. Same all-years intersection.
    dc_year_key_frames = [dc_daily.filter(F.col("Year") == y).select(*_DC_PAIR_KEYS).distinct() for y in years]
    dc_common_keys = dc_year_key_frames[0]
    for frame in dc_year_key_frames[1:]:
        dc_common_keys = dc_common_keys.intersect(frame)
    dc_common_keys = dc_common_keys.cache()

    if pair_count == 0:
        common_keys.unpersist()
        dc_common_keys.unpersist()
        print(f"comparable pairs: 0 common {unit} across all years {years}")
        return

    # Restricted once, reused for every link -- every link shares this same population.
    restricted = _restrict_frames(pf, common_keys, join_keys, dc_common_keys)

    kpi_parts: List[pd.DataFrame] = []
    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()
    counts: List[str] = []

    for prior_year, current_year in zip(years, years[1:]):
        rows = _comparable_period_rows(ctx, restricted, [prior_year, current_year], metric_cols)

        tagged = rows.copy()
        tagged.insert(0, "comparison_type", "ytd")
        # Column name kept as comparable_pair_count for schema stability even under
        # product-grain, where it's actually a product count (unit == "products" above). Same
        # value on every link now, since all links share one all-years-restricted population.
        tagged["comparable_pair_count"] = pair_count
        tagged["link_prior_year"] = prior_year
        tagged["link_current_year"] = current_year
        kpi_parts.append(tagged)

        disp, parts = _comparisons_for_link(ctx, rows, prior_year, current_year, metric_cols)
        save_parts.extend(parts)
        if not disp.empty:
            display = disp  # latest link's overall display wins, full detail is in the save table

        counts.append(f"{prior_year}-{current_year}={pair_count} {unit}")

    common_keys.unpersist()
    dc_common_keys.unpersist()

    ctx.comparable_comparison_ytd = pd.concat(save_parts, ignore_index=True) if save_parts else pd.DataFrame()
    ctx.comparable_ytd_display = display
    ctx.comparable_kpi_long = pd.concat(kpi_parts, ignore_index=True) if kpi_parts else pd.DataFrame()
    print("comparable pairs:", " | ".join(counts) if counts else "(none)")


def rebuild_comparable_ytd_from_saved_rows(ctx: KPIContext, merged: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Rebuild the comparable-YTD (display, save) comparison from an already-computed,
    link-tagged comparable_kpi_long — e.g. reloaded after an incremental merge onto saved
    history. Pure pandas; no Spark recomputation needed since each link's rows already carry
    that link's own pair-restricted metric values."""
    metric_cols = ctx.settings["METRIC_COLS"]
    rows = merged[merged["comparison_type"] == "ytd"]
    if rows.empty:
        return pd.DataFrame(), pd.DataFrame()

    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()
    links = (
        rows[["link_prior_year", "link_current_year"]]
        .drop_duplicates()
        .sort_values(["link_current_year", "link_prior_year"])
    )
    for _, link in links.iterrows():
        prior_year, current_year = int(link["link_prior_year"]), int(link["link_current_year"])
        link_rows = rows[(rows["link_prior_year"] == prior_year) & (rows["link_current_year"] == current_year)]
        disp, parts = _comparisons_for_link(ctx, link_rows, prior_year, current_year, metric_cols)
        save_parts.extend(parts)
        if not disp.empty:
            display = disp

    save_all = pd.concat(save_parts, ignore_index=True) if save_parts else pd.DataFrame()
    return display, save_all

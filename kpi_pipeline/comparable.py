"""Comparable pairs (like-for-like): ytd / yoy / quarter / half comparisons over a fixed pair universe.

Each enabled kind (comparable_pairs.kinds) recomputes the metrics over only the pairs present in EVERY
qualifying year of that kind, then compares each consecutive-year link within that one population:

  ytd     each year's YTD window (closed fiscal months; with report_end="latest_day" days 1..K of the
          fiscal year, years in ctx.ytd_years only).
  yoy     the full window year (a partial first / last year included, as on the Annual tab; with
          "latest_day" complete fiscal years only), chained across every consecutive year pair.
  quarter per quarter number Q, independently: only years whose Q lies entirely inside the window
          (_complete_period_years); a pair must be present in Q of each of them.
  half    like quarter, per half number (H1 = Q1-Q2, H2 = Q3-Q4).

The universe's grain is comparable_pairs.grain: "product_store" (default, under every scope.grain,
since scoped_daily is store-level anyway) or "product" (every store of a qualifying product; store churn
is not isolated). Frames without store_id are restricted to the universe's products; dc_daily / dc_inst
each get their own (product_id, warehouse_id) universe (_restrict_frames).

A kind needs >= 2 qualifying years (quarter / half: per number) and is skipped otherwise. Rows carry
link_prior_year / link_current_year so the save key stays unique across links (a year is "current" in one
link and "prior" in the next); quarter / half rows also carry quarter_number / half_number for the HTML
report.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_pipeline.comparisons import _comparison_dimensions, _comparison_roots, _series_groups, build_comparison_long
from kpi_pipeline.context import KPIContext
from kpi_pipeline.kpi_long import _METRIC_FRAMES, _period_frames, kpi_long_frame, kpi_rows

_PAIR_KEYS = ["product_id", "store_id"]
_PRODUCT_KEYS = ["product_id"]
_DC_PAIR_KEYS = ["product_id", "warehouse_id"]
_RESTRICT_FRAMES = ("scoped_daily", "inst_data", "lost_base", "scope_pairs", "scope_pair_weeks")

# comparable_pairs.grain -> the store-side universe's key columns. The DC universe (_DC_PAIR_KEYS) is fixed.
_GRAIN_PAIR_KEYS: Dict[str, List[str]] = {
    "product_store": _PAIR_KEYS,
    "product": _PRODUCT_KEYS,
}

# kind -> (kpi_long period_type this kind's rows are tagged with, ctx save attr, ctx display attr)
_KIND_CTX_ATTRS: Dict[str, Tuple[str, str, str]] = {
    "ytd": ("ytd", "comparable_comparison_ytd", "comparable_ytd_display"),
    "yoy": ("annual", "comparable_comparison_yoy", "comparable_yoy_display"),
    "quarter": ("quarter", "comparable_comparison_quarter", "comparable_quarter_display"),
    "half": ("half", "comparable_comparison_half", "comparable_half_display"),
}


@dataclass(frozen=True)
class _NumberedKind:
    """A comparable kind computed independently per period number (quarter 1-4, half 1-2)."""

    period_col: str  # kpi_long._period_frames key column carrying "<Year>-<number>"
    number_col: str  # fiscal_week column holding the period number
    tag_col: str  # plain column the number is tagged on in comparable_kpi_long / the comparison rows
    prefix: str  # "Q" / "H" in period labels


_NUMBERED_KINDS: Dict[str, _NumberedKind] = {
    "quarter": _NumberedKind("period_key", "Fiscal_Quarter", "quarter_number", "Q"),
    "half": _NumberedKind("half_key", "Fiscal_Half", "half_number", "H"),
}

# Builds per kind the run's progress plans (one per quarter / half number); each builds one KPI table per
# root x cut. Builds that turn out to be skipped are taken off the plan (ctx.progress.skip).
PLANNED_BUILDS: Dict[str, int] = {"ytd": 1, "yoy": 1, "quarter": 4, "half": 2}


def _restrict_frames(
    period_frames: Dict[str, DataFrame],
    comparable_keys: DataFrame,
    dc_comparable_keys: DataFrame,
    dc_inst_comparable_keys: DataFrame,
    pair_keys: List[str],
) -> Dict[str, DataFrame]:
    """Restrict the frames to the common universe of every qualifying year.

    Store-side frames join ``comparable_keys`` on ``pair_keys`` (comparable_pairs.grain); a frame without
    store_id (inst_data / lost_base from a store-less source, scope frames without stores), and every frame
    under grain "product", joins the universe's distinct products instead, so its rows are not fanned out
    per store. dc_daily and dc_inst join their own (product_id, warehouse_id) universes: dc_inst 0-fills
    every day from a pair's first stocked day, so a pair that stopped being stocked still has (stockout)
    rows where dc_daily has none, and sharing dc_daily's universe would delete exactly those stockouts.
    """
    out = dict(period_frames)
    comparable_products = comparable_keys.select(*_PRODUCT_KEYS).distinct()
    store_level_universe = pair_keys == _PAIR_KEYS
    for key in _RESTRICT_FRAMES:
        frame = out[key]
        pair_level = store_level_universe and "store_id" in frame.columns
        out[key] = frame.join(
            comparable_keys if pair_level else comparable_products,
            on=_PAIR_KEYS if pair_level else _PRODUCT_KEYS,
            how="inner",
        )
    out["dc_daily"] = out["dc_daily"].join(dc_comparable_keys, on=_DC_PAIR_KEYS, how="inner")
    out["dc_inst"] = out["dc_inst"].join(dc_inst_comparable_keys, on=_DC_PAIR_KEYS, how="inner")
    return out


def _complete_period_years(ctx: KPIContext, kind: str, number: int) -> List[int]:
    """Years whose fiscal quarter / half ``number`` lies entirely inside the report window, from
    ctx.complete_fiscal_periods (built from the unclipped calendar; ctx.fiscal_week is clipped to the window
    and would make every period look complete). A partial period is never compared to a full one."""
    number_col = _NUMBERED_KINDS[kind].number_col
    complete = ctx.complete_fiscal_periods[number_col].filter(F.col(number_col) == number)
    return sorted(r["Year"] for r in complete.select("Year").distinct().collect())


def _intersect_years(frame: DataFrame, years: Sequence[int], key_cols: List[str]) -> DataFrame:
    """Cached distinct ``key_cols`` present in ``frame`` in every one of ``years`` (``frame`` already holds
    only the rows that count for each year), in one pass: a key is in every year when its rows of
    ``years`` span ``len(years)`` distinct years."""
    return (
        frame.filter(F.col("Year").isin(list(years)))
        .groupBy(*key_cols)
        .agg(F.countDistinct("Year").alias("_years"))
        .filter(F.col("_years") == len(years))
        .select(*key_cols)
        .cache()
    )


def _comparable_period_rows(
    ctx: KPIContext,
    frames: Dict[str, DataFrame],
    period_filter,
    period_type: str,
    period_col: str,
) -> pd.DataFrame:
    """kpi_long rows (every root x cut) of the ``period_filter`` rows, tagged ``period_type``."""
    return kpi_long_frame(ctx, kpi_rows(ctx, frames, period_type, period_col, period_filter))


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
    """(overall display, per root x dimension save frames) of one consecutive-year link, from its
    kpi_long-shaped rows. ``period_key_fn`` gives a year's ``period`` value, ``display_label_fn`` its column
    header, ``change_label_fn`` (current year) the change column header."""
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


def _period_key_fns(comparison_type: str, number: Optional[int] = None):
    """(period_key_fn, display_label_fn, change_label_fn) of one kind (``number``: quarter / half number)."""
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
    if comparison_type in _NUMBERED_KINDS:
        prefix = _NUMBERED_KINDS[comparison_type].prefix
        return (
            lambda y: f"{y}-{prefix}{number}",
            lambda y: f"{y} {prefix}{number}",
            lambda y: f"{prefix}{number} {y}",
        )
    raise ValueError(f"unknown comparable comparison_type {comparison_type!r}")


def _build_comparable_kind(
    ctx: KPIContext,
    comparison_type: str,
    metric_cols: List[str],
    number: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """(kpi_long rows, comparison save rows, overall display) of one kind (``number`` for quarter / half);
    empty frames with fewer than 2 qualifying years or no common pair. The years come from
    kpi_long._period_frames for the kind's period type, so they follow report_end. comparable_pair_count
    counts pairs, or products under grain "product"."""
    period_type, _, _ = _KIND_CTX_ATTRS[comparison_type]
    pair_keys = _GRAIN_PAIR_KEYS[ctx.settings["COMPARABLE_PAIRS_GRAIN"]]
    numbered = _NUMBERED_KINDS.get(comparison_type)
    build_label = f"{comparison_type} {numbered.prefix}{number}" if numbered else comparison_type
    ctx.progress.section = f"comparable {build_label}"
    period_col = numbered.period_col if numbered else "Year"
    pf = _period_frames(ctx, ctx.hybrid_frames, period_type)

    def _in_number(frame: DataFrame) -> DataFrame:
        return frame.filter(F.col(numbered.number_col) == number) if numbered else frame

    # A pair is present in a year on real rows only (a GIT-only day does not count); pair_days "unblocked"
    # (default) also leaves blocked days out (dc_inst: dc_unblocked_days). Metrics then gate blocked days
    # per blocked_scope.metrics as on the other tabs.
    unblocked_only = ctx.settings["COMPARABLE_PAIRS_PAIR_DAYS"] == "unblocked"
    present = F.col("has_daily_row") & ~F.col("is_blocked") if unblocked_only else F.col("has_daily_row")
    dc_present = F.col("has_inventory_row") & ~F.col("is_blocked") if unblocked_only else F.col("has_inventory_row")
    scoped_daily_pop = _in_number(pf["scoped_daily"]).filter(present)
    dc_daily_pop = _in_number(pf["dc_daily"]).filter(dc_present)
    dc_inst_pop = _in_number(pf["dc_inst"])
    if unblocked_only:
        dc_inst_pop = dc_inst_pop.filter(F.col("dc_unblocked_days") > 0)

    years = sorted(r["Year"] for r in scoped_daily_pop.select("Year").distinct().collect())
    if numbered:
        complete_years = set(_complete_period_years(ctx, comparison_type, number))
        years = [y for y in years if y in complete_years]
    if len(years) < 2:
        ctx.progress.skip(1, f"{build_label}: fewer than 2 qualifying years")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    common_keys = _intersect_years(scoped_daily_pop, years, pair_keys)
    dc_common_keys = _intersect_years(dc_daily_pop, years, _DC_PAIR_KEYS)
    dc_inst_common_keys = _intersect_years(dc_inst_pop, years, _DC_PAIR_KEYS)
    pair_count = common_keys.count()
    if pair_count == 0:
        ctx.progress.skip(1, f"{build_label}: no common pair")
        common_keys.unpersist()
        dc_common_keys.unpersist()
        dc_inst_common_keys.unpersist()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    restricted = _restrict_frames(pf, common_keys, dc_common_keys, dc_inst_common_keys, pair_keys)

    kpi_parts: List[pd.DataFrame] = []
    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()

    # Every year of the kind computed once: each aggregation groups by period_col or a finer key holding Year,
    # so a year's metrics read only its own rows, and a link takes its two years' rows. The restricted rows
    # are cached once, so every root x cut aggregation reads them instead of re-joining the full frames to
    # the comparable keys; compute_kpis / build_kpi_table apply period_filter again, a no-op on them.
    period_filter = F.col("Year").isin(years)
    if numbered:
        period_filter = period_filter & (F.col(numbered.number_col) == number)
    kind_frames = {key: restricted[key].filter(period_filter).cache() for key in _METRIC_FRAMES}
    rows = _comparable_period_rows(ctx, {**restricted, **kind_frames}, period_filter, period_type, period_col)
    for frame in [*kind_frames.values(), common_keys, dc_common_keys, dc_inst_common_keys]:
        frame.unpersist()

    period_key_fn, display_label_fn, change_label_fn = _period_key_fns(comparison_type, number)
    for prior_year, current_year in zip(years, years[1:]):
        link_rows = rows[rows["period"].isin([period_key_fn(prior_year), period_key_fn(current_year)])]
        link_rows = link_rows.reset_index(drop=True)

        tagged = link_rows.copy()
        tagged.insert(0, "comparison_type", comparison_type)
        tagged["comparable_pair_count"] = pair_count
        tagged["link_prior_year"] = prior_year
        tagged["link_current_year"] = current_year
        if numbered:
            tagged[numbered.tag_col] = number
        kpi_parts.append(tagged)

        disp, parts = _comparisons_for_link(
            ctx, link_rows, prior_year, current_year, metric_cols,
            comparison_type, period_key_fn, display_label_fn, change_label_fn,
        )
        if numbered:
            for p in parts:
                p[numbered.tag_col] = number
        save_parts.extend(parts)
        if not disp.empty:
            display = disp  # latest link's overall display wins, full detail is in the save table

    kpi_long_rows = pd.concat(kpi_parts, ignore_index=True) if kpi_parts else pd.DataFrame()
    comparison_rows = pd.concat(save_parts, ignore_index=True) if save_parts else pd.DataFrame()
    return kpi_long_rows, comparison_rows, display


def build_comparable_pairs(ctx: KPIContext) -> None:
    """Set ctx.comparable_kpi_long and comparable_comparison_<kind> / comparable_<kind>_display for every
    kind in comparable_pairs.kinds (when comparable_pairs.enabled). Each kind's universe is the
    intersection over all its qualifying years, shared by every link of the kind."""
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
    unit = "pairs" if ctx.settings["COMPARABLE_PAIRS_GRAIN"] == "product_store" else "products"
    kpi_long_parts: List[pd.DataFrame] = []
    summary: List[str] = []

    for kind in kinds:
        period_type, save_attr, display_attr = _KIND_CTX_ATTRS[kind]

        if kind not in _NUMBERED_KINDS:
            kpi_rows, save_rows, display = _build_comparable_kind(ctx, kind, metric_cols)
            if not kpi_rows.empty:
                kpi_long_parts.append(kpi_rows)
            setattr(ctx, save_attr, save_rows)
            setattr(ctx, display_attr, display)
            pair_count = int(kpi_rows["comparable_pair_count"].iloc[0]) if not kpi_rows.empty else 0
            summary.append(f"{kind}={pair_count} {unit}" if pair_count else f"{kind}=n/a")
            continue

        # quarter / half: independent per period number (a plain column on scoped_daily).
        numbered = _NUMBERED_KINDS[kind]
        numbers = sorted(
            r[numbered.number_col]
            for r in ctx.hybrid_frames["scoped_daily"].select(numbered.number_col).distinct().collect()
        )
        ctx.progress.skip(PLANNED_BUILDS[kind] - len(numbers), f"{kind}: numbers without data")
        n_kpi_parts: List[pd.DataFrame] = []
        n_save_parts: List[pd.DataFrame] = []
        n_display = pd.DataFrame()
        n_summary = []
        for n in numbers:
            kpi_rows, save_rows, display = _build_comparable_kind(ctx, kind, metric_cols, number=n)
            if not kpi_rows.empty:
                n_kpi_parts.append(kpi_rows)
                pair_count = int(kpi_rows["comparable_pair_count"].iloc[0])
                n_summary.append(f"{numbered.prefix}{n}={pair_count} {unit}")
            if not save_rows.empty:
                n_save_parts.append(save_rows)
            if not display.empty:
                n_display = display  # last number with data wins for the top-level display slot
        if n_kpi_parts:
            kpi_long_parts.append(pd.concat(n_kpi_parts, ignore_index=True))
        setattr(ctx, save_attr, pd.concat(n_save_parts, ignore_index=True) if n_save_parts else pd.DataFrame())
        setattr(ctx, display_attr, n_display)
        summary.append(f"{kind}=(" + ", ".join(n_summary) + ")" if n_summary else f"{kind}=n/a")

    ctx.comparable_kpi_long = pd.concat(kpi_long_parts, ignore_index=True) if kpi_long_parts else pd.DataFrame()
    print("comparable pairs:", " | ".join(summary) if summary else "(none)")


def rebuild_comparable_kind_from_saved_rows(
    ctx: KPIContext, merged: pd.DataFrame, comparison_type: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """One kind's comparison (display, save) rebuilt in pandas from a link-tagged comparable_kpi_long (e.g.
    the merged saved history): each link's rows already hold that link's restricted metric values."""
    metric_cols = ctx.settings["METRIC_COLS"]
    rows = merged[merged["comparison_type"] == comparison_type]
    if rows.empty:
        return pd.DataFrame(), pd.DataFrame()

    save_parts: List[pd.DataFrame] = []
    display = pd.DataFrame()

    if comparison_type not in _NUMBERED_KINDS:
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
        tag_col = _NUMBERED_KINDS[comparison_type].tag_col
        for n in sorted(rows[tag_col].dropna().unique()):
            n = int(n)
            n_rows = rows[rows[tag_col] == n]
            links = (
                n_rows[["link_prior_year", "link_current_year"]]
                .drop_duplicates()
                .sort_values(["link_current_year", "link_prior_year"])
            )
            period_key_fn, display_label_fn, change_label_fn = _period_key_fns(comparison_type, n)
            for _, link in links.iterrows():
                prior_year, current_year = int(link["link_prior_year"]), int(link["link_current_year"])
                link_rows = n_rows[
                    (n_rows["link_prior_year"] == prior_year) & (n_rows["link_current_year"] == current_year)
                ]
                disp, parts = _comparisons_for_link(
                    ctx, link_rows, prior_year, current_year, metric_cols,
                    comparison_type, period_key_fn, display_label_fn, change_label_fn,
                )
                for p in parts:
                    p[tag_col] = n
                save_parts.extend(parts)
                if not disp.empty:
                    display = disp

    save_all = pd.concat(save_parts, ignore_index=True) if save_parts else pd.DataFrame()
    return display, save_all

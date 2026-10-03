"""Orchestrates the full KPI pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from IPython.display import display
from pyspark.sql import SparkSession

from kpi_pipeline.comparable import _KIND_CTX_ATTRS as COMPARABLE_KIND_ATTRS
from kpi_pipeline.comparable import build_comparable_pairs
from kpi_pipeline.comparisons import _KIND_CTX_ATTRS as COMPARISON_KIND_ATTRS
from kpi_pipeline.comparisons import _selected_comparison_kinds, build_comparisons, build_scope_diff
from kpi_pipeline.context import KPIContext
from kpi_pipeline.fiscal import apply_report_end_mode, build_fiscal_and_products, build_fiscal_week_only
from kpi_pipeline.html_report import render_kpi_html
from kpi_pipeline.io import build_save_plan, load_saved_outputs, save_outputs
from kpi_pipeline.kpi_long import build_kpi_long, trim_periods_to_recent
from kpi_pipeline.pipeline import build_pipeline_frames
from kpi_pipeline.scope import (
    apply_scope_adjustments,
    build_blocked_days,
    build_dc_blocked_days,
    build_dc_scope,
    build_defined_scope,
    build_hybrid_scope,
    scope_summary_by_origin,
)
from kpi_pipeline.scope_debug import scope_universe_counts


def _write_text_to_datastore(spark: SparkSession, path: str, content: str) -> None:
    """Write text to a local path, DBFS-backed mount (/dbfs/mnt/...), or via dbutils."""
    candidates: list[str] = []
    if path.startswith("/mnt/"):
        candidates.append("/dbfs" + path)
    candidates.append(path)

    last_exc: Optional[Exception] = None
    for candidate in candidates:
        try:
            out = Path(candidate)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
            print(f"HTML report also saved to datastore: {candidate}")
            return
        except Exception as exc:
            last_exc = exc

    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        dbfs_uri = path if path.startswith("dbfs:") else f"dbfs:{path}"
        parent = dbfs_uri.rsplit("/", 1)[0]
        dbutils.fs.mkdirs(parent)
        dbutils.fs.put(dbfs_uri, content, overwrite=True)
        print(f"HTML report also saved to datastore: {path}")
        return
    except Exception as exc:
        last_exc = exc

    raise OSError(f"could not save HTML to datastore path {path!r}") from last_exc


def _show(title: str, table: Optional[pd.DataFrame]) -> None:
    """Print ``title`` and display ``table`` inline (Databricks renders it mid-cell), or note it is empty."""
    print(f"=== {title} ===")
    if table is None or table.empty:
        print("(no rows)")
    else:
        display(table)


class KPIRunner:
    """Run KPI generation from materialized config settings."""

    def __init__(self, spark: SparkSession, settings: Dict[str, Any]):
        self.ctx = KPIContext(spark=spark, settings=settings)
        self._scopes_ready = False

    @property
    def settings(self) -> Dict[str, Any]:
        return self.ctx.settings

    def _reset_run_caches(self) -> None:
        self.ctx.daily_data_raw = None
        self.ctx.daily_data_excluded_days = None
        self.ctx.lost_sales_weekly_base = None
        self.ctx.instock_weekly_base = None
        self.ctx.item_family_raw = None
        self.ctx.inventory_warehouse_rolled = None

    def print_config_summary(self) -> None:
        s = self.settings
        print("CUSTOMER:", s["CUSTOMER"])
        print("AS_OF_DATE:", s["AS_OF_DATE"], "| run_week:", s["RUN_WEEK_START_DATE"], "->", s["RUN_WEEK_END_DATE"])
        print(
            "REPORT_END_DATE:",
            s["REPORT_END_DATE"],
            "| report_end:",
            s["REPORT_END_MODE"],
            "(complete_month cuts it back when build_dimensions runs; latest_day = as_of_date itself)",
        )
        print("REPORT_START_DATE (Sun):", s["REPORT_START_DATE"], "| RUN_MIN_DATE (Sun):", s["RUN_MIN_DATE"])
        print(
            "EFFECTIVE window:",
            s["EFFECTIVE_REPORT_START_DATE"],
            "->",
            s["REPORT_END_DATE"],
            "| fiscal:",
            s["USE_FISCAL_CALENDAR"],
        )
        print("RUN_MODE:", s.get("RUN_MODE", "full"))
        print("SCOPE MODE:", "hybrid" if s["USE_HYBRID_SCOPE"] else "defined only")
        print("RUN_SCOPE_DIFF:", s.get("RUN_SCOPE_DIFF", False))
        if s["USE_HYBRID_SCOPE"] or s.get("RUN_SCOPE_DIFF", False):
            print(
                "score scope: p",
                int(s["SCOPE_MIN_PERCENTILE"] * 100),
                "AND rule; skip filter when pair weeks <=",
                s["SCOPE_MIN_WEEKS_FOR_FILTER"],
            )
        print("SCOPE SOURCE:", s["SCOPE_SOURCE"])
        print("DEFINED_SCOPE path:", s["DEFINED_SCOPE"]["path"])
        print("BLOCKED_SCOPE:", s["BLOCKED_SCOPE"])
        print("INSTOCK:", s["INSTOCK_METHOD"], "|", s["INSTOCK_DAILY"])
        print("GOODS_IN_TRANSIT:", s["GOODS_IN_TRANSIT"])
        print("SLICE_DIMENSIONS:", s["SLICE_DIMENSIONS"])
        print("COMPARISONS:", s.get("COMPARISON_KINDS", ["yoy", "ytd"]))
        if s["SAVE_OUTPUTS"]:
            print(
                "SAVE_OUTPUTS: True | mode:",
                s["OUTPUT_SAVE_MODE"],
                "| run_date:",
                s["OUTPUT_RUN_DATE"],
                "| allow overwrite:",
                s["ALLOW_OVERWRITE_EXISTING"],
                "| root:",
                s["PATH_OUTPUT_ROOT"],
            )
        else:
            print("SAVE_OUTPUTS: False")

    def preview_save_plan(self, fund_paste) -> Optional[object]:
        if not self.settings["SAVE_OUTPUTS"]:
            print("SAVE_OUTPUTS is False — no save plan.")
            return None
        if self.ctx.kpi_long is None:
            print("Run the pipeline first (runner.run) before previewing the save plan.")
            return None
        plan = build_save_plan(self.ctx, fund_paste)
        plan.print_summary()
        self.ctx.save_plan = plan
        return plan

    def _infer_active_slices_from_kpi_long(self) -> None:
        """html_only: infer the cut dimensions (configured order first, then extras found) and the root names
        from the loaded kpi_long; rendering reads only each root's name."""
        if self.ctx.kpi_long is None or self.ctx.kpi_long.empty:
            self.ctx.active_slice_dimensions = []
            self.ctx.cut_dimensions = []
            self.ctx.root_definitions = []
            return
        configured = list(self.settings.get("SLICE_DIMENSIONS") or [])
        data_dims = set(self.ctx.kpi_long["dimension"].unique()) - {"overall"}
        active = [d for d in configured if d in data_dims]
        for d in sorted(data_dims):
            if d not in active:
                active.append(d)
        self.ctx.active_slice_dimensions = active
        self.ctx.cut_dimensions = active

        if "root" in self.ctx.kpi_long.columns:
            data_roots = set(self.ctx.kpi_long["root"].unique()) - {"overall"}
            self.ctx.root_definitions = [{"root": r, "dim_col": None, "value": None} for r in sorted(data_roots)]
        else:
            self.ctx.root_definitions = []

        print("CUT_DIMENSIONS (inferred):", active, "| ROOTS (inferred):", ["overall"] + [r["root"] for r in self.ctx.root_definitions])

    def run_html_only(self, fund_paste=None) -> KPIContext:
        """Load saved Delta outputs and prepare ctx for HTML report only (no pipeline compute)."""
        if fund_paste is None:
            raise ValueError("fund_paste is required for html_only mode (to resolve output paths).")
        apply_report_end_mode(self.ctx)
        load_saved_outputs(self.ctx, fund_paste)
        self._infer_active_slices_from_kpi_long()
        build_fiscal_week_only(self.ctx)
        self.ctx.kpi_long_display = trim_periods_to_recent(self.ctx.kpi_long, self.ctx)
        print("html_only: skipped pipeline — loaded saved outputs for HTML report.")
        return self.ctx

    def run(self, fund_paste=None, save: bool = True) -> KPIContext:
        if self.settings.get("RUN_MODE", "full") == "html_only":
            return self.run_html_only(fund_paste=fund_paste)
        if not self._scopes_ready:
            self.prepare_scopes(fund_paste=fund_paste)
        self._scopes_ready = False
        self.build_kpis()
        self.build_comparisons()
        self.build_comparable_pairs()
        self.build_scope_comparison()
        if save:
            if fund_paste is not None:
                save_outputs(self.ctx, fund_paste)
            else:
                print(
                    "WARNING: save=True but fund_paste was not provided — "
                    "output save skipped. Pass fund_paste=fund.paste or call save_outputs() separately."
                )
        return self.ctx

    def prepare_scopes(self, fund_paste=None) -> None:
        """Reset the run caches, then build the dimensions and scopes. The next run() reuses them (the
        notebook's Scope debug cell), so they are not built twice; a later run() builds them again."""
        self._reset_run_caches()
        self.build_dimensions()
        self.build_scopes(fund_paste=fund_paste)
        self._scopes_ready = True

    def build_dimensions(self) -> None:
        apply_report_end_mode(self.ctx)
        build_fiscal_and_products(self.ctx)

    def build_scopes(self, fund_paste=None) -> None:
        build_defined_scope(self.ctx)
        build_blocked_days(self.ctx)
        build_dc_scope(self.ctx)
        build_dc_blocked_days(self.ctx)
        build_hybrid_scope(self.ctx)
        apply_scope_adjustments(self.ctx, fund_paste=fund_paste)

    def latest_overall_kpis(self) -> pd.DataFrame:
        """kpi_long's overall rows (root and dimension "overall") of the latest period of each period_type,
        metric columns only."""
        kpi_long = self.ctx.kpi_long
        overall = kpi_long[(kpi_long["root"] == "overall") & (kpi_long["dimension"] == "overall")]
        latest = overall.groupby("period_type")["period"].transform("max")
        return overall[overall["period"] == latest][
            ["period_type", "period"] + self.settings["METRIC_COLS"]
        ].reset_index(drop=True)

    def build_kpis(self) -> None:
        self.ctx.hybrid_frames = build_pipeline_frames(self.ctx, self.ctx.hybrid_scope_keys)
        self.ctx.kpi_long = build_kpi_long(self.ctx, self.ctx.hybrid_frames)
        print("kpi_long shape:", self.ctx.kpi_long.shape)
        print(
            "slices:",
            self.ctx.kpi_long["dimension"].unique().tolist(),
            "| periods:",
            self.ctx.kpi_long["period_type"].unique().tolist(),
        )
        _show("kpi_long — overall, latest period of each period type", self.latest_overall_kpis())

    def build_comparisons(self) -> None:
        build_comparisons(self.ctx)
        # Trimmed copy for the HTML report only: ctx.kpi_long stays full for the save step.
        self.ctx.kpi_long_display = trim_periods_to_recent(self.ctx.kpi_long, self.ctx)
        for kind in _selected_comparison_kinds(self.ctx):
            _show(f"{kind.upper()} — overall", getattr(self.ctx, COMPARISON_KIND_ATTRS[kind][1]))

    def build_comparable_pairs(self) -> None:
        build_comparable_pairs(self.ctx)
        if not self.settings.get("COMPARABLE_PAIRS_ENABLED", False):
            return
        for kind in self.settings.get("COMPARABLE_KINDS") or []:
            _show(f"Comparable {kind} — overall (latest link)", getattr(self.ctx, COMPARABLE_KIND_ATTRS[kind][2]))

    def build_scope_comparison(self) -> None:
        if not self.settings.get("RUN_SCOPE_DIFF", False):
            self.ctx.scope_diff = None
            print("scope diff: skipped (scope.run_scope_diff=False)")
            return
        if self.ctx.score_only_scope_keys is None:
            raise RuntimeError(
                "scope.run_scope_diff=True but score scope was not computed — "
                "check build_hybrid_scope and RUN_SCOPE_DIFF settings."
            )
        self.ctx.defined_frames = build_pipeline_frames(self.ctx, self.ctx.defined_scope_keys)
        self.ctx.score_frames = build_pipeline_frames(self.ctx, self.ctx.score_only_scope_keys)
        build_scope_diff(self.ctx)

    def build_html_report(self, local_dir: "str | Path | None" = ".") -> Optional[str]:
        """Render the HTML report into ``local_dir`` (also copied to html_report's datastore path when set);
        returns the path, or None when html_report.enabled is False or the pipeline has not run."""
        if not self.settings.get("HTML_REPORT_ENABLED", True):
            print("HTML report disabled (html_report.enabled=False).")
            return None
        if self.ctx.kpi_long is None:
            print("Run the pipeline first (runner.run) before generating the HTML report.")
            return None

        filename = self.settings["HTML_REPORT_FILENAME_TEMPLATE"].format(
            customer=self.settings["CUSTOMER"], report_end=self.settings["REPORT_END_DATE"]
        )
        out_path = Path(local_dir) / filename
        written = render_kpi_html(
            self.ctx,
            out_path,
            report_title=self.settings.get("HTML_REPORT_TITLE"),
            metric_definitions=self.settings.get("HTML_REPORT_METRIC_DEFS") or {},
        )

        datastore_dir = self.settings["HTML_REPORT_OUTPUT_DIR"]
        if datastore_dir:
            datastore_path = f"{datastore_dir.rstrip('/')}/{filename}"
            try:
                _write_text_to_datastore(
                    self.ctx.spark,
                    datastore_path,
                    Path(written).read_text(encoding="utf-8"),
                )
            except Exception as exc:
                print(f"Warning: could not save HTML to datastore path ({exc}).")

        return written

    def scope_debug_summary(self):
        """Distinct product / store / pair counts of the final scope, overall and per slice: a pre-flight
        check after build_dimensions and build_scopes, before the expensive build_kpis."""
        return scope_universe_counts(self.ctx)

    def hybrid_scope_summary(self):
        return scope_summary_by_origin(self.ctx.hybrid_scope_keys)

    def scope_before_adjustments_summary(self):
        if self.ctx.scope_before_adjustments is None:
            return None
        return scope_summary_by_origin(self.ctx.scope_before_adjustments)

    def scope_adjustment_steps_table(self):
        if not self.ctx.scope_adjustment_steps:
            return None
        return pd.DataFrame(self.ctx.scope_adjustment_steps)

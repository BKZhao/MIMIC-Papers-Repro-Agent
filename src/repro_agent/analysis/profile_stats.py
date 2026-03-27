from __future__ import annotations

import json
import math
import warnings
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from lifelines import CoxPHFitter, KaplanMeierFitter
from lifelines.exceptions import ConvergenceError
from lifelines.plotting import add_at_risk_counts
from lifelines.statistics import multivariate_logrank_test, proportional_hazard_test
from matplotlib.gridspec import GridSpecFromSubplotSpec
from matplotlib.patches import FancyBboxPatch, Rectangle
from patsy import build_design_matrices, dmatrix
from scipy.stats import chi2, chi2_contingency, f_oneway, kruskal, shapiro
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score, roc_curve

from ..paper.profiles import EndpointProfile, PaperExecutionProfile, SubgroupProfile, get_paper_execution_profile
from .trajectory_stats import run_trajectory_profile_stats_workflow


QUARTILES: tuple[str, ...] = ("Q1", "Q2", "Q3", "Q4")
STRING_COLUMNS: tuple[str, ...] = ("gender", "race", "insurance", "marital_status")
DEFAULT_PALETTE: dict[str, str] = {
    "Q1": "#1b4d6b",
    "Q2": "#3b8ea5",
    "Q3": "#c97c1a",
    "Q4": "#8c2d19",
}
STROKE_TYG_PAPER_PALETTE: dict[str, str] = {
    "Q1": "#f2a93b",
    "Q2": "#f06d8b",
    "Q3": "#b38adf",
    "Q4": "#7aa6d8",
}


@dataclass(frozen=True)
class ProfileStatsRunResult:
    profile_key: str
    analysis_dataset_rel: str
    row_count: int
    outputs: list[str]
    metrics: dict[str, float | None]


def _is_stroke_tyg_profile(profile: PaperExecutionProfile) -> bool:
    return str(profile.key).strip() == "mimic_tyg_stroke_nondiabetic"


def _resolve_quartile_palette(profile: PaperExecutionProfile) -> dict[str, str]:
    if _is_stroke_tyg_profile(profile):
        return STROKE_TYG_PAPER_PALETTE
    return DEFAULT_PALETTE


def _stroke_tyg_endpoint_title(endpoint_key: str, *, figure_kind: str, panel: str) -> str:
    km_titles = {
        "icu": "ICU mortality",
        "in_hospital": "In-hospital mortality",
        "day_30": "30-day mortality",
        "day_90": "90-day mortality",
        "day_180": "180-day mortality",
        "year_1": "1-year mortality",
    }
    compact_titles = {
        "icu": "ICU stay",
        "in_hospital": "In-hospital stay",
        "day_30": "30-day",
        "day_90": "90-day",
        "day_180": "180-day",
        "year_1": "1 year",
    }
    if figure_kind == "km":
        label = km_titles.get(endpoint_key, endpoint_key.replace("_", " ").title())
    else:
        label = compact_titles.get(endpoint_key, endpoint_key.replace("_", " ").title())
    return f"({panel}) {label}".strip()


def run_profile_stats(
    *,
    project_root: Path,
    profile_key: str,
    analysis_dataset_rel: str,
    missingness_rel: str = "",
    artifact_subdir: str = "",
    execution_environment_dataset_version: str = "",
    execution_year_window: str = "",
) -> ProfileStatsRunResult:
    profile = get_paper_execution_profile(profile_key)
    if profile is None:
        raise ValueError(f"Unknown paper execution profile: {profile_key}")

    if profile.analysis_family == "trajectory_survival":
        result = run_trajectory_profile_stats_workflow(
            project_root=project_root,
            profile=profile,
            analysis_dataset_rel=analysis_dataset_rel,
            missingness_rel=missingness_rel,
            artifact_subdir=artifact_subdir,
            execution_environment_dataset_version=execution_environment_dataset_version,
            execution_year_window=execution_year_window,
        )
        return ProfileStatsRunResult(
            profile_key=profile_key,
            analysis_dataset_rel=analysis_dataset_rel,
            row_count=int(result["row_count"]),
            outputs=list(result["outputs"]),
            metrics=dict(result["metrics"]),
        )

    if profile.analysis_family == "multi_endpoint_quartile_survival":
        result = run_multi_endpoint_profile_stats_workflow(
            project_root=project_root,
            profile=profile,
            analysis_dataset_rel=analysis_dataset_rel,
            missingness_rel=missingness_rel,
            artifact_subdir=artifact_subdir,
            execution_environment_dataset_version=execution_environment_dataset_version,
            execution_year_window=execution_year_window,
        )
        return ProfileStatsRunResult(
            profile_key=profile_key,
            analysis_dataset_rel=analysis_dataset_rel,
            row_count=int(result["row_count"]),
            outputs=list(result["outputs"]),
            metrics=dict(result["metrics"]),
        )

    analysis_dataset_path = (project_root / analysis_dataset_rel).resolve()
    if not analysis_dataset_path.exists():
        raise FileNotFoundError(f"Analysis dataset not found: {analysis_dataset_path}")

    raw_df = _load_profile_frame(analysis_dataset_path, profile)
    model_df, imputation_manifest = _prepare_model_frame(raw_df, profile)
    requested_output_kinds = _resolve_requested_output_kinds(profile)

    shared_dir = project_root / "shared"
    results_dir = project_root / "results"
    if artifact_subdir.strip():
        subdir = Path(artifact_subdir.strip())
        shared_dir = shared_dir / subdir
        results_dir = results_dir / subdir
    shared_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    baseline_path = shared_dir / f"{profile_key}_baseline_table.csv"
    baseline_md_path = shared_dir / f"{profile_key}_baseline_table.md"
    cox_path = shared_dir / f"{profile_key}_cox_models.csv"
    cox_md_path = shared_dir / f"{profile_key}_cox_models.md"
    subgroup_path = shared_dir / f"{profile_key}_subgroup_analysis.csv"
    subgroup_md_path = shared_dir / f"{profile_key}_subgroup_analysis.md"
    km_summary_path = shared_dir / f"{profile_key}_km_summary.json"
    rcs_curve_path = shared_dir / f"{profile_key}_rcs_curve.csv"
    rcs_summary_path = shared_dir / f"{profile_key}_rcs_summary.json"
    roc_summary_path = shared_dir / f"{profile_key}_roc_summary.json"
    model_ready_path = shared_dir / f"{profile_key}_analysis_dataset_model_ready.csv"
    stats_summary_path = shared_dir / f"{profile_key}_stats_summary.json"
    km_plot_path = results_dir / f"{profile_key}_km.png"
    rcs_plot_path = results_dir / f"{profile_key}_rcs.png"
    roc_plot_path = results_dir / f"{profile_key}_roc.png"
    subgroup_plot_path = results_dir / f"{profile_key}_subgroup_forest.png"

    if not _wants_any_output(requested_output_kinds, "baseline_table"):
        _remove_if_exists(baseline_path, baseline_md_path)
    if not _wants_any_output(requested_output_kinds, "cox_results_table"):
        _remove_if_exists(cox_path, cox_md_path)
    if not _wants_any_output(requested_output_kinds, "km_figure"):
        _remove_if_exists(km_summary_path, km_plot_path)
    if not _wants_any_output(requested_output_kinds, "rcs_figure"):
        _remove_if_exists(rcs_curve_path, rcs_summary_path, rcs_plot_path)
    if not _wants_any_output(requested_output_kinds, "roc_figure"):
        _remove_if_exists(roc_summary_path, roc_plot_path)
    if not _wants_any_output(requested_output_kinds, "subgroup_table", "subgroup_figure"):
        _remove_if_exists(subgroup_path, subgroup_md_path, subgroup_plot_path)

    model_df.to_csv(model_ready_path, index=False)

    baseline_df = pd.DataFrame()
    if _wants_any_output(requested_output_kinds, "baseline_table"):
        baseline_df = _build_baseline_table(raw_df, profile)
        baseline_df.to_csv(baseline_path, index=False)
        baseline_md_path.write_text(_dataframe_to_markdown(baseline_df), encoding="utf-8")

    cox_rows: list[dict[str, Any]] = []
    metrics: dict[str, float | None] = {}
    fitted_models: dict[str, dict[str, Any]] = {}
    if _wants_any_output(requested_output_kinds, "cox_results_table", "km_figure", "rcs_figure", "subgroup_table", "subgroup_figure"):
        for adjustment in profile.model_adjustments:
            continuous_result = _fit_continuous_cox(model_df, profile, adjustment.covariates, adjustment.name)
            cox_rows.extend(continuous_result["rows"])
            metrics[f"{adjustment.name}_continuous_hr"] = continuous_result["hazard_ratio"]
            fitted_models[f"{adjustment.name}_continuous"] = continuous_result

            quartile_result = _fit_quartile_cox(model_df, profile, adjustment.covariates, adjustment.name)
            cox_rows.extend(quartile_result["rows"])
            metrics[f"{adjustment.name}_q4_vs_q1_hr"] = quartile_result["q4_hazard_ratio"]
            fitted_models[f"{adjustment.name}_quartile"] = quartile_result

    cox_df = pd.DataFrame(
        cox_rows,
        columns=[
            "model_name",
            "model_type",
            "term",
            "hazard_ratio",
            "ci_lower_95",
            "ci_upper_95",
            "p_value",
            "n",
            "events",
            "covariates",
        ],
    )
    if _wants_any_output(requested_output_kinds, "cox_results_table"):
        cox_df.to_csv(cox_path, index=False)
        cox_md_path.write_text(_dataframe_to_markdown(cox_df), encoding="utf-8")

    km_result: dict[str, Any] = {}
    if _wants_any_output(requested_output_kinds, "km_figure"):
        km_result = _fit_km_by_quartile(model_df, profile, output_path=km_plot_path)
        km_summary_path.write_text(json.dumps(km_result, indent=2, ensure_ascii=False), encoding="utf-8")
        metrics["logrank_p_value"] = _to_optional_float(km_result.get("logrank_p_value"))

    primary_adjustment = profile.model_adjustments[-1] if profile.model_adjustments else None
    primary_covariates = primary_adjustment.covariates if primary_adjustment is not None else ()
    if _wants_any_output(requested_output_kinds, "rcs_figure"):
        rcs_result = _fit_rcs_model(
            model_df,
            profile,
            covariates=primary_covariates,
            model_name=primary_adjustment.name if primary_adjustment else "model_1",
        )
        rcs_result["curve"].to_csv(rcs_curve_path, index=False)
        rcs_summary_path.write_text(
            json.dumps(
                {
                    "profile": profile.as_dict(),
                    "overall_p_value": _round_or_none(rcs_result.get("overall_p_value"), digits=6),
                    "nonlinearity_p_value": _round_or_none(rcs_result.get("nonlinearity_p_value"), digits=6),
                    "reference_value": _round_or_none(rcs_result.get("reference_value"), digits=6),
                    "n": rcs_result.get("n"),
                    "events": rcs_result.get("events"),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        _plot_rcs_curve(rcs_result, profile, rcs_plot_path)
        metrics["rcs_overall_p_value"] = _to_optional_float(rcs_result.get("overall_p_value"))
        metrics["rcs_nonlinearity_p_value"] = _to_optional_float(rcs_result.get("nonlinearity_p_value"))

    if _wants_any_output(requested_output_kinds, "roc_figure"):
        roc_result = _fit_roc_curve(model_df, profile)
        roc_summary_path.write_text(json.dumps(roc_result, indent=2, ensure_ascii=False), encoding="utf-8")
        _plot_roc_curve(roc_result, profile, roc_plot_path)
        metrics["roc_auc"] = _to_optional_float(roc_result.get("auc"))
        metrics["roc_threshold"] = _to_optional_float(roc_result.get("best_threshold"))

    if _wants_any_output(requested_output_kinds, "subgroup_table", "subgroup_figure"):
        subgroup_df = _run_subgroup_analysis(
            model_df,
            profile,
            covariates=primary_covariates,
            overall_rows=(
                _build_overall_forest_row(
                    label="Crude",
                    model_name="model_1",
                    model_result=fitted_models.get("model_1_continuous"),
                    profile=profile,
                    df=model_df,
                ),
                _build_overall_forest_row(
                    label="Adjusted",
                    model_name=primary_adjustment.name if primary_adjustment is not None else "model_3",
                    model_result=fitted_models.get(f"{primary_adjustment.name}_continuous") if primary_adjustment is not None else None,
                    profile=profile,
                    df=model_df,
                ),
            ),
        )
        subgroup_df.to_csv(subgroup_path, index=False)
        subgroup_md_path.write_text(_dataframe_to_markdown(subgroup_df), encoding="utf-8")
        if _wants_any_output(requested_output_kinds, "subgroup_figure"):
            _plot_subgroup_forest(subgroup_df, profile, subgroup_plot_path)

    schoenfeld = _fit_schoenfeld_test(
        fitted_models.get(f"{primary_adjustment.name}_continuous") if primary_adjustment is not None else None,
        predictor_term=f"{profile.predictor_column}_z",
    )
    metrics["schoenfeld_p_value"] = _to_optional_float(schoenfeld.get("p_value"))

    missingness_path = (project_root / missingness_rel).resolve() if missingness_rel else None
    missingness_payload: dict[str, Any] = {}
    if missingness_path is not None and missingness_path.exists():
        missingness_payload = json.loads(missingness_path.read_text(encoding="utf-8"))

    artifact_paths = [
        baseline_path,
        baseline_md_path,
        cox_path,
        cox_md_path,
        subgroup_path,
        subgroup_md_path,
        km_summary_path,
        rcs_curve_path,
        rcs_summary_path,
        roc_summary_path,
        model_ready_path,
        stats_summary_path,
        km_plot_path,
        rcs_plot_path,
        roc_plot_path,
        subgroup_plot_path,
    ]
    materialized_artifacts = [
        str(path.relative_to(project_root))
        for path in artifact_paths
        if path.exists() or path == stats_summary_path
    ]

    stats_summary = {
        "profile": profile.as_dict(),
        "analysis_dataset_rel": analysis_dataset_rel,
        "artifact_subdir": artifact_subdir,
        "row_count": int(len(model_df)),
        "paper_target_dataset_version": profile.source_dataset_version,
        "execution_environment_dataset_version": execution_environment_dataset_version,
        "execution_year_window": execution_year_window or profile.execution_year_window,
        "source_dataset_version": profile.source_dataset_version,
        "notes": list(profile.notes),
        "imputation_manifest": imputation_manifest,
        "missingness_summary": missingness_payload.get("missingness", {}),
        "metrics": {key: _round_or_none(value, digits=6) for key, value in metrics.items()},
        "schoenfeld": schoenfeld,
        "artifacts": materialized_artifacts,
    }
    stats_summary_path.write_text(json.dumps(stats_summary, indent=2, ensure_ascii=False), encoding="utf-8")

    return ProfileStatsRunResult(
        profile_key=profile_key,
        analysis_dataset_rel=analysis_dataset_rel,
        row_count=int(len(model_df)),
        outputs=stats_summary["artifacts"],
        metrics=metrics,
    )


def run_multi_endpoint_profile_stats_workflow(
    *,
    project_root: Path,
    profile: PaperExecutionProfile,
    analysis_dataset_rel: str,
    missingness_rel: str = "",
    artifact_subdir: str = "",
    execution_environment_dataset_version: str = "",
    execution_year_window: str = "",
) -> dict[str, Any]:
    analysis_dataset_path = (project_root / analysis_dataset_rel).resolve()
    if not analysis_dataset_path.exists():
        raise FileNotFoundError(f"Analysis dataset not found: {analysis_dataset_path}")

    raw_df = _load_profile_frame(analysis_dataset_path, profile)
    requested_output_kinds = _resolve_requested_output_kinds(profile)

    shared_dir = project_root / "shared"
    results_dir = project_root / "results"
    if artifact_subdir.strip():
        subdir = Path(artifact_subdir.strip())
        shared_dir = shared_dir / subdir
        results_dir = results_dir / subdir
    shared_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    baseline_path = shared_dir / f"{profile.key}_baseline_table.csv"
    baseline_md_path = shared_dir / f"{profile.key}_baseline_table.md"
    cox_path = shared_dir / f"{profile.key}_cox_models.csv"
    cox_md_path = shared_dir / f"{profile.key}_cox_models.md"
    subgroup_path = shared_dir / f"{profile.key}_subgroup_analysis.csv"
    subgroup_md_path = shared_dir / f"{profile.key}_subgroup_analysis.md"
    km_summary_path = shared_dir / f"{profile.key}_km_summary.json"
    rcs_curve_path = shared_dir / f"{profile.key}_rcs_curve.csv"
    rcs_summary_path = shared_dir / f"{profile.key}_rcs_summary.json"
    stats_summary_path = shared_dir / f"{profile.key}_stats_summary.json"
    report_path = shared_dir / f"{profile.key}_reproduction_report.md"
    cohort_funnel_path = _resolve_cohort_funnel_path(
        project_root=project_root,
        analysis_dataset_rel=analysis_dataset_rel,
        artifact_subdir=artifact_subdir,
    )
    cohort_alignment_path = _resolve_cohort_alignment_path(
        project_root=project_root,
        analysis_dataset_rel=analysis_dataset_rel,
        artifact_subdir=artifact_subdir,
    )
    km_plot_path = results_dir / f"{profile.key}_km.png"
    rcs_plot_path = results_dir / f"{profile.key}_rcs.png"
    subgroup_plot_path = results_dir / f"{profile.key}_subgroup_forest.png"
    flowchart_plot_path = results_dir / f"{profile.key}_flowchart.png"

    if not _wants_any_output(requested_output_kinds, "baseline_table"):
        _remove_if_exists(baseline_path, baseline_md_path)
    if not _wants_any_output(requested_output_kinds, "cox_results_table"):
        _remove_if_exists(cox_path, cox_md_path)
    if not _wants_any_output(requested_output_kinds, "km_figure"):
        _remove_if_exists(km_summary_path, km_plot_path)
    if not _wants_any_output(requested_output_kinds, "rcs_figure"):
        _remove_if_exists(rcs_curve_path, rcs_summary_path, rcs_plot_path)
    if not _wants_any_output(requested_output_kinds, "subgroup_table", "subgroup_figure"):
        _remove_if_exists(subgroup_path, subgroup_md_path, subgroup_plot_path)
    if not _wants_any_output(requested_output_kinds, "cohort_flowchart_figure"):
        _remove_if_exists(flowchart_plot_path)

    baseline_df = pd.DataFrame()
    if _wants_any_output(requested_output_kinds, "baseline_table"):
        baseline_df = _build_baseline_table(raw_df, profile)
        baseline_df.to_csv(baseline_path, index=False)
        baseline_md_path.write_text(_dataframe_to_markdown(baseline_df), encoding="utf-8")

    primary_adjustment = profile.model_adjustments[-1] if profile.model_adjustments else None
    primary_covariates = primary_adjustment.covariates if primary_adjustment is not None else ()
    endpoint_summaries: list[dict[str, Any]] = []
    cox_rows: list[dict[str, Any]] = []
    km_results: list[dict[str, Any]] = []
    rcs_results: list[dict[str, Any]] = []
    subgroup_frames: list[pd.DataFrame] = []
    imputation_manifest: dict[str, Any] = {}
    metrics: dict[str, float | None] = {}

    for endpoint in _iter_endpoint_profiles(profile):
        if endpoint.event_column not in raw_df.columns or endpoint.duration_column not in raw_df.columns:
            endpoint_summaries.append(
                {
                    "key": endpoint.key,
                    "display_name": endpoint.display_name,
                    "panel": endpoint.figure_panel,
                    "status": "missing_columns",
                    "missing_columns": [
                        column
                        for column in (endpoint.event_column, endpoint.duration_column)
                        if column not in raw_df.columns
                    ],
                }
            )
            continue

        endpoint_profile = replace(
            profile,
            event_column=endpoint.event_column,
            duration_column=endpoint.duration_column,
            duration_unit=endpoint.duration_unit or profile.duration_unit,
            km_time_horizon=endpoint.km_time_horizon if endpoint.km_time_horizon is not None else profile.km_time_horizon,
        )
        model_df, endpoint_manifest = _prepare_model_frame(raw_df, endpoint_profile)
        imputation_manifest[endpoint.key] = endpoint_manifest
        endpoint_summary: dict[str, Any] = {
            "key": endpoint.key,
            "display_name": endpoint.display_name,
            "panel": endpoint.figure_panel,
            "status": "ready" if not model_df.empty else "empty_after_preparation",
            "event_column": endpoint.event_column,
            "duration_column": endpoint.duration_column,
            "row_count": int(len(model_df)),
            "events": int(model_df[endpoint.event_column].sum()) if not model_df.empty else 0,
        }
        if model_df.empty:
            endpoint_summaries.append(endpoint_summary)
            continue

        endpoint_fitted_models: dict[str, dict[str, Any]] = {}
        for adjustment in profile.model_adjustments:
            continuous_result = _fit_continuous_cox(model_df, endpoint_profile, adjustment.covariates, adjustment.name)
            for row in continuous_result["rows"]:
                row["endpoint_key"] = endpoint.key
                row["endpoint"] = endpoint.display_name
                row["panel"] = endpoint.figure_panel
            cox_rows.extend(continuous_result["rows"])
            endpoint_fitted_models[f"{adjustment.name}_continuous"] = continuous_result

            quartile_result = _fit_quartile_cox(model_df, endpoint_profile, adjustment.covariates, adjustment.name)
            for row in quartile_result["rows"]:
                row["endpoint_key"] = endpoint.key
                row["endpoint"] = endpoint.display_name
                row["panel"] = endpoint.figure_panel
            cox_rows.extend(quartile_result["rows"])
            endpoint_fitted_models[f"{adjustment.name}_quartile"] = quartile_result

            if adjustment == primary_adjustment:
                endpoint_summary["model_3_q4_vs_q1_hr"] = _round_or_none(
                    quartile_result.get("q4_hazard_ratio"),
                    digits=6,
                )
                metrics[f"{endpoint.key}_{adjustment.name}_q4_vs_q1_hr"] = _to_optional_float(
                    quartile_result.get("q4_hazard_ratio")
                )

        if _wants_any_output(requested_output_kinds, "km_figure"):
            km_result = _fit_km_summary_by_quartile(model_df, endpoint_profile)
            km_result["endpoint_key"] = endpoint.key
            km_result["endpoint"] = endpoint.display_name
            km_result["panel"] = endpoint.figure_panel
            km_results.append(km_result)
            endpoint_summary["logrank_p_value"] = km_result.get("logrank_p_value")
            metrics[f"{endpoint.key}_logrank_p_value"] = _to_optional_float(km_result.get("logrank_p_value"))

        if _wants_any_output(requested_output_kinds, "rcs_figure"):
            rcs_result = _fit_rcs_model(
                model_df,
                endpoint_profile,
                covariates=primary_covariates,
                model_name=primary_adjustment.name if primary_adjustment is not None else "model_3",
            )
            rcs_result["endpoint_key"] = endpoint.key
            rcs_result["endpoint"] = endpoint.display_name
            rcs_result["panel"] = endpoint.figure_panel
            rcs_results.append(rcs_result)
            endpoint_summary["rcs_overall_p_value"] = _round_or_none(rcs_result.get("overall_p_value"), digits=6)
            endpoint_summary["rcs_nonlinearity_p_value"] = _round_or_none(
                rcs_result.get("nonlinearity_p_value"),
                digits=6,
            )
            metrics[f"{endpoint.key}_rcs_overall_p_value"] = _to_optional_float(rcs_result.get("overall_p_value"))
            metrics[f"{endpoint.key}_rcs_nonlinearity_p_value"] = _to_optional_float(
                rcs_result.get("nonlinearity_p_value")
            )

        if _wants_any_output(requested_output_kinds, "subgroup_table", "subgroup_figure"):
            subgroup_df = _run_subgroup_analysis(
                model_df,
                endpoint_profile,
                covariates=primary_covariates,
                overall_rows=(
                    _build_overall_forest_row(
                        label="Crude",
                        model_name="model_1",
                        model_result=endpoint_fitted_models.get("model_1_continuous"),
                        profile=endpoint_profile,
                        df=model_df,
                    ),
                    _build_overall_forest_row(
                        label="Adjusted",
                        model_name=primary_adjustment.name if primary_adjustment is not None else "model_3",
                        model_result=(
                            endpoint_fitted_models.get(f"{primary_adjustment.name}_continuous")
                            if primary_adjustment is not None
                            else None
                        ),
                        profile=endpoint_profile,
                        df=model_df,
                    ),
                ),
            )
            if not subgroup_df.empty:
                subgroup_df.insert(0, "endpoint_key", endpoint.key)
                subgroup_df.insert(1, "endpoint", endpoint.display_name)
                subgroup_df.insert(2, "panel", endpoint.figure_panel)
                subgroup_frames.append(subgroup_df)

        endpoint_summaries.append(endpoint_summary)

    cox_df = pd.DataFrame(
        cox_rows,
        columns=[
            "endpoint_key",
            "endpoint",
            "panel",
            "model_name",
            "model_type",
            "term",
            "hazard_ratio",
            "ci_lower_95",
            "ci_upper_95",
            "p_value",
            "n",
            "events",
            "covariates",
        ],
    )
    if _wants_any_output(requested_output_kinds, "cox_results_table"):
        cox_df.to_csv(cox_path, index=False)
        cox_md_path.write_text(_dataframe_to_markdown(cox_df), encoding="utf-8")

    km_payload = {
        "profile": profile.as_dict(),
        "endpoints": km_results,
    }
    if _wants_any_output(requested_output_kinds, "km_figure"):
        km_summary_path.write_text(json.dumps(km_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        _plot_multi_endpoint_km(km_results, profile, km_plot_path)

    rcs_curve_frames = []
    rcs_payload_rows: list[dict[str, Any]] = []
    for item in rcs_results:
        curve = item.get("curve", pd.DataFrame())
        if not curve.empty:
            curve = curve.copy()
            curve.insert(0, "endpoint_key", item.get("endpoint_key", ""))
            curve.insert(1, "endpoint", item.get("endpoint", ""))
            curve.insert(2, "panel", item.get("panel", ""))
            rcs_curve_frames.append(curve)
        rcs_payload_rows.append(
            {
                "endpoint_key": item.get("endpoint_key", ""),
                "endpoint": item.get("endpoint", ""),
                "panel": item.get("panel", ""),
                "overall_p_value": _round_or_none(item.get("overall_p_value"), digits=6),
                "nonlinearity_p_value": _round_or_none(item.get("nonlinearity_p_value"), digits=6),
                "reference_value": _round_or_none(item.get("reference_value"), digits=6),
                "n": item.get("n"),
                "events": item.get("events"),
            }
        )
    if _wants_any_output(requested_output_kinds, "rcs_figure"):
        if rcs_curve_frames:
            pd.concat(rcs_curve_frames, ignore_index=True).to_csv(rcs_curve_path, index=False)
        rcs_summary_path.write_text(
            json.dumps({"profile": profile.as_dict(), "endpoints": rcs_payload_rows}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        _plot_multi_endpoint_rcs(rcs_results, profile, rcs_plot_path, raw_df=raw_df)

    subgroup_df = pd.concat(subgroup_frames, ignore_index=True) if subgroup_frames else pd.DataFrame()
    if _wants_any_output(requested_output_kinds, "subgroup_table", "subgroup_figure") and not subgroup_df.empty:
        subgroup_df.to_csv(subgroup_path, index=False)
        subgroup_md_path.write_text(_dataframe_to_markdown(subgroup_df), encoding="utf-8")
        if _wants_any_output(requested_output_kinds, "subgroup_figure"):
            _plot_multi_endpoint_subgroup_forest(subgroup_df, profile, subgroup_plot_path)

    if _wants_any_output(requested_output_kinds, "cohort_flowchart_figure") and cohort_funnel_path is not None:
        funnel_payload = json.loads(cohort_funnel_path.read_text(encoding="utf-8"))
        alignment_payload = (
            json.loads(cohort_alignment_path.read_text(encoding="utf-8"))
            if cohort_alignment_path is not None and cohort_alignment_path.exists()
            else {}
        )
        _plot_cohort_flowchart(funnel_payload, profile, flowchart_plot_path, alignment_payload=alignment_payload)

    missingness_path = (project_root / missingness_rel).resolve() if missingness_rel else None
    missingness_payload: dict[str, Any] = {}
    if missingness_path is not None and missingness_path.exists():
        missingness_payload = json.loads(missingness_path.read_text(encoding="utf-8"))

    materialized_artifacts: list[str] = []
    for path in (
        baseline_path,
        baseline_md_path,
        cox_path,
        cox_md_path,
        subgroup_path,
        subgroup_md_path,
        km_summary_path,
        rcs_curve_path,
        rcs_summary_path,
        stats_summary_path,
        report_path,
        flowchart_plot_path,
        km_plot_path,
        rcs_plot_path,
        subgroup_plot_path,
    ):
        if path.exists() or path == stats_summary_path:
            materialized_artifacts.append(str(path.relative_to(project_root)))

    stats_summary = {
        "profile": profile.as_dict(),
        "analysis_dataset_rel": analysis_dataset_rel,
        "artifact_subdir": artifact_subdir,
        "row_count": int(len(raw_df)),
        "paper_target_dataset_version": profile.source_dataset_version,
        "execution_environment_dataset_version": execution_environment_dataset_version,
        "execution_year_window": execution_year_window or profile.execution_year_window,
        "source_dataset_version": profile.source_dataset_version,
        "notes": list(profile.notes),
        "endpoint_metrics": endpoint_summaries,
        "imputation_manifest": imputation_manifest,
        "missingness_summary": missingness_payload.get("missingness", {}),
        "block_status": missingness_payload.get("block_status", {}),
        "metrics": {key: _round_or_none(value, digits=6) for key, value in metrics.items()},
        "artifacts": materialized_artifacts,
    }
    stats_summary_path.write_text(json.dumps(stats_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    report_path.write_text(
        _render_multi_endpoint_report(
            project_root=project_root,
            profile=profile,
            artifact_subdir=artifact_subdir,
            endpoint_summaries=endpoint_summaries,
            outputs=materialized_artifacts,
            execution_environment_dataset_version=execution_environment_dataset_version,
            execution_year_window=execution_year_window,
        ),
        encoding="utf-8",
    )
    if str(report_path.relative_to(project_root)) not in materialized_artifacts:
        materialized_artifacts.append(str(report_path.relative_to(project_root)))

    return {
        "row_count": int(len(raw_df)),
        "outputs": materialized_artifacts,
        "metrics": metrics,
    }


def _iter_endpoint_profiles(profile: PaperExecutionProfile) -> tuple[EndpointProfile, ...]:
    if profile.endpoint_profiles:
        return profile.endpoint_profiles
    return (
        EndpointProfile(
            key="primary",
            display_name=profile.title,
            event_column=profile.event_column,
            duration_column=profile.duration_column,
            km_time_horizon=profile.km_time_horizon,
            duration_unit=profile.duration_unit,
        ),
    )


def _fit_km_summary_by_quartile(df: pd.DataFrame, profile: PaperExecutionProfile) -> dict[str, Any]:
    work = df[[profile.duration_column, profile.event_column, profile.predictor_quartile_column]].dropna().copy()
    work = work[work[profile.duration_column] > 0].copy()
    if work.empty:
        return {"logrank_p_value": None, "groups": {}, "time_horizon": None, "xticks": [], "curves": {}}

    display_duration = (
        work[profile.duration_column] / 24.0
        if profile.duration_unit == "hours"
        else work[profile.duration_column]
    )
    work["display_duration"] = display_duration
    horizon = profile.km_time_horizon
    if horizon is None:
        horizon = float(np.ceil(work["display_duration"].quantile(0.95)))
    horizon = max(float(horizon), 1.0)
    if horizon <= 10:
        xticks = list(range(0, int(math.ceil(horizon)) + 1, 2)) or [0, int(math.ceil(horizon))]
    else:
        xticks = [0, 7, 14, 21, 28] if horizon <= 28.5 else list(np.linspace(0, horizon, 5, dtype=int))
    xticks = sorted(set(int(tick) for tick in xticks if 0 <= tick <= math.ceil(horizon)))

    groups_payload: dict[str, dict[str, Any]] = {}
    curves: dict[str, dict[str, Any]] = {}
    for quartile in QUARTILES:
        subset = work.loc[work[profile.predictor_quartile_column] == quartile]
        if subset.empty:
            continue
        kmf = KaplanMeierFitter()
        kmf.fit(subset["display_duration"], event_observed=subset[profile.event_column], label=quartile)
        survival = kmf.survival_function_.reset_index()
        timeline_col, survival_col = survival.columns[:2]
        groups_payload[quartile] = {
            "n": int(len(subset)),
            "events": int(subset[profile.event_column].sum()),
            "median_time": _round_or_none(kmf.median_survival_time_, digits=4),
        }
        curves[quartile] = {
            "timeline": [round(float(value), 6) for value in survival[timeline_col].tolist()],
            "survival": [round(float(value), 6) for value in survival[survival_col].tolist()],
        }

    logrank_p = None
    try:
        result = multivariate_logrank_test(
            event_durations=work["display_duration"],
            groups=work[profile.predictor_quartile_column],
            event_observed=work[profile.event_column],
        )
        logrank_p = float(result.p_value)
    except ValueError:
        pass

    return {
        "logrank_p_value": _round_or_none(logrank_p, digits=6),
        "groups": groups_payload,
        "curves": curves,
        "time_horizon": _round_or_none(horizon, digits=3),
        "xticks": xticks,
    }


def _plot_multi_endpoint_km(results: list[dict[str, Any]], profile: PaperExecutionProfile, output_path: Path) -> None:
    active_results = [item for item in results if item.get("curves")]
    if not active_results:
        return
    if _is_stroke_tyg_profile(profile):
        _plot_stroke_tyg_paper_km(active_results, profile, output_path)
        return
    ncols = 3
    nrows = math.ceil(len(active_results) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.3 * ncols, 4.3 * nrows), squeeze=False)
    axes_flat = axes.flatten()
    palette = _resolve_quartile_palette(profile)

    for index, result in enumerate(active_results):
        ax = axes_flat[index]
        for quartile in QUARTILES:
            curve = dict(result.get("curves", {})).get(quartile)
            if not curve:
                continue
            ax.plot(
                curve["timeline"],
                curve["survival"],
                color=palette[quartile],
                linewidth=2.0,
                label=quartile,
            )
        horizon = _to_optional_float(result.get("time_horizon")) or 1.0
        xticks = [int(value) for value in result.get("xticks", []) if value is not None]
        ax.set_xlim(0, horizon)
        if xticks:
            ax.set_xticks(xticks)
        ax.set_ylim(0.0, 1.02)
        ax.grid(axis="y", alpha=0.2)
        ax.set_title(f"{result.get('panel', '')}. {result.get('endpoint', '')}".strip(". "), fontsize=11)
        ax.set_xlabel("Time (days)")
        ax.set_ylabel("Survival probability")
        ax.text(
            0.98,
            0.96,
            f"Log-rank P = {_format_p_value(result.get('logrank_p_value'))}",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=8.5,
            bbox={"facecolor": "white", "edgecolor": "#dddddd", "alpha": 0.9, "pad": 3},
        )
        if index == 0:
            ax.legend(title="TyG quartile", loc="lower left", fontsize=8)

    for index in range(len(active_results), len(axes_flat)):
        axes_flat[index].axis("off")

    fig.suptitle(f"{profile.title}\nKaplan-Meier survival by TyG quartile", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _plot_multi_endpoint_rcs(
    results: list[dict[str, Any]],
    profile: PaperExecutionProfile,
    output_path: Path,
    *,
    raw_df: pd.DataFrame | None = None,
) -> None:
    active_results = [item for item in results if not item.get("curve", pd.DataFrame()).empty]
    if not active_results:
        return
    if _is_stroke_tyg_profile(profile):
        _plot_stroke_tyg_paper_rcs(active_results, profile, output_path, raw_df=raw_df)
        return
    ncols = 3
    nrows = math.ceil(len(active_results) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.3 * ncols, 4.3 * nrows), squeeze=False)
    axes_flat = axes.flatten()

    for index, result in enumerate(active_results):
        ax = axes_flat[index]
        curve = result["curve"]
        ax.plot(curve["x_value"], curve["hazard_ratio"], color="#184e77", linewidth=2.0)
        ax.fill_between(curve["x_value"], curve["ci_lower_95"], curve["ci_upper_95"], color="#76c893", alpha=0.22)
        ax.axhline(1.0, linestyle="--", color="#666666", linewidth=1.0)
        if result.get("reference_value") is not None:
            ax.axvline(float(result["reference_value"]), linestyle=":", color="#c1121f", linewidth=1.2)
        ax.set_title(f"{result.get('panel', '')}. {result.get('endpoint', '')}".strip(". "), fontsize=11)
        ax.set_xlabel("TyG index")
        ax.set_ylabel("Hazard ratio")
        ax.grid(axis="y", alpha=0.2)
        ax.text(
            0.03,
            0.96,
            "\n".join(
                [
                    f"Overall P = {_format_p_value(result.get('overall_p_value'))}",
                    f"P non-linear = {_format_p_value(result.get('nonlinearity_p_value'))}",
                ]
            ),
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=8.3,
            bbox={"facecolor": "white", "edgecolor": "#dddddd", "alpha": 0.9, "pad": 3},
        )

    for index in range(len(active_results), len(axes_flat)):
        axes_flat[index].axis("off")

    fig.suptitle(f"{profile.title}\nRestricted cubic spline across mortality endpoints", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _plot_multi_endpoint_subgroup_forest(df: pd.DataFrame, profile: PaperExecutionProfile, output_path: Path) -> None:
    if df.empty:
        return
    if _is_stroke_tyg_profile(profile):
        _plot_stroke_tyg_paper_subgroup(df, profile, output_path)
        return
    endpoint_order = [item.key for item in _iter_endpoint_profiles(profile)]
    endpoint_lookup = {item.key: item for item in _iter_endpoint_profiles(profile)}
    active_keys = [key for key in endpoint_order if key in set(df["endpoint_key"].astype(str))]
    if not active_keys:
        return

    ncols = 3
    nrows = math.ceil(len(active_keys) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.6 * ncols, 4.6 * nrows), squeeze=False)
    axes_flat = axes.flatten()

    for index, endpoint_key in enumerate(active_keys):
        endpoint = endpoint_lookup[endpoint_key]
        subset = df.loc[df["endpoint_key"].astype(str) == endpoint_key].copy()
        _render_subgroup_panel(
            axes_flat[index],
            subset,
            title=f"{endpoint.figure_panel}. {endpoint.display_name}".strip(". "),
        )

    for index in range(len(active_keys), len(axes_flat)):
        axes_flat[index].axis("off")

    fig.suptitle(f"{profile.title}\nSubgroup forest plots across mortality endpoints", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _render_subgroup_panel(ax: plt.Axes, df: pd.DataFrame, *, title: str) -> None:
    if df.empty:
        ax.axis("off")
        return

    display_labels: list[str] = []
    centers: list[float] = []
    lowers: list[float] = []
    uppers: list[float] = []
    p_interaction_rows: list[tuple[int, float]] = []
    y_positions: list[int] = []

    for _, row in df.iterrows():
        hr = _to_optional_float(row.get("hazard_ratio"))
        lo = _to_optional_float(row.get("ci_lower_95"))
        hi = _to_optional_float(row.get("ci_upper_95"))
        if hr is None or lo is None or hi is None:
            continue
        if str(row.get("row_type", "")) == "overall":
            label = f"Overall {row.get('level', '')}".strip()
        else:
            label = f"{row.get('subgroup', '')}: {row.get('level', '')}".strip(": ")
        display_labels.append(label)
        centers.append(hr)
        lowers.append(lo)
        uppers.append(hi)
        y_positions.append(len(display_labels))
        p_interaction = _to_optional_float(row.get("p_interaction"))
        if p_interaction is not None:
            p_interaction_rows.append((len(display_labels), p_interaction))

    if not display_labels:
        ax.axis("off")
        return

    y = np.arange(len(display_labels), 0, -1)
    x_values = centers
    error_left = np.array(centers) - np.array(lowers)
    error_right = np.array(uppers) - np.array(centers)
    x_min = min(lowers) * 0.85
    x_max = max(uppers) * 1.18
    x_min = min(x_min, 0.7)
    x_max = max(x_max, 1.4)

    ax.errorbar(
        x_values,
        y,
        xerr=[error_left, error_right],
        fmt="o",
        color="#184e77",
        ecolor="#184e77",
        elinewidth=1.4,
        capsize=2.8,
    )
    ax.axvline(1.0, color="#666666", linestyle="--", linewidth=1.0)
    ax.set_xlim(x_min, x_max)
    ax.set_yticks(y)
    ax.set_yticklabels(display_labels, fontsize=7)
    ax.set_xlabel("HR (95% CI)")
    ax.set_title(title, fontsize=11)
    ax.grid(axis="x", alpha=0.2)
    ax.tick_params(axis="x", labelsize=8)

    for row_index, p_value in p_interaction_rows:
        ax.text(
            x_max,
            len(display_labels) - row_index + 1,
            f"Pint={_format_p_value(p_value)}",
            fontsize=6.6,
            ha="right",
            va="center",
        )


def _render_multi_endpoint_report(
    *,
    project_root: Path,
    profile: PaperExecutionProfile,
    artifact_subdir: str,
    endpoint_summaries: list[dict[str, Any]],
    outputs: list[str],
    execution_environment_dataset_version: str,
    execution_year_window: str,
) -> str:
    alignment_rel = ""
    if artifact_subdir.strip():
        candidate = Path("shared") / artifact_subdir.strip() / "cohort_alignment.json"
        if (project_root / candidate).exists():
            alignment_rel = str(candidate)
    alignment_payload: dict[str, Any] = {}
    if alignment_rel:
        alignment_payload = json.loads((project_root / alignment_rel).read_text(encoding="utf-8"))
    actual_n = int(alignment_payload.get("actual", {}).get("n_final", 0) or 0)
    if actual_n <= 0:
        actual_n = max((int(item.get("row_count", 0) or 0) for item in endpoint_summaries), default=0)
    cohort_gap = actual_n - int(profile.expected_final_n or 0)

    lines = [
        f"# {profile.title} Reproduction Report",
        "",
        "## Paper Summary",
        f"- Paper target dataset version: {profile.source_dataset_version or 'unknown'}",
        f"- Execution environment dataset version: {execution_environment_dataset_version or 'unknown'}",
        f"- Execution year window: {execution_year_window or profile.execution_year_window or 'unknown'}",
        f"- Reported paper cohort size: {profile.expected_final_n or 'unknown'}",
        "- Primary analysis family: Cox regression, Kaplan-Meier, restricted cubic spline, and subgroup interaction analysis.",
        "",
        "## Reproduced Outputs",
    ]
    for item in endpoint_summaries:
        if item.get("status") != "ready":
            lines.append(
                f"- {item.get('panel', '')}. {item.get('display_name', '')}: status={item.get('status', 'unknown')}."
            )
            continue
        lines.append(
            "- "
            + f"{item.get('panel', '')}. {item.get('display_name', '')}: "
            + f"n={item.get('row_count', 0)}, events={item.get('events', 0)}, "
            + f"log-rank P={_format_p_value(item.get('logrank_p_value'))}, "
            + f"Model 3 Q4 vs Q1 HR={_format_numeric(item.get('model_3_q4_vs_q1_hr'), digits=3)}, "
            + f"RCS P non-linear={_format_p_value(item.get('rcs_nonlinearity_p_value'))}."
        )
    lines.extend(
        [
            "",
            "## Method Gap",
            "- Triglyceride and glucose availability is approximated using an admission-anchored first-day paired laboratory draw because fasting state is not explicitly encoded in MIMIC-IV.",
            "- Diabetes-history exclusion and insulin-treatment indicators are derived from diagnosis and prescription records, so they remain code- and documentation-dependent approximations.",
            "- The paper's MICE plus PSM sensitivity analyses are not yet executed in this profile; the current route focuses on the main survival analysis package.",
            "- IV-tPA and mechanical thrombectomy are approximated from medication and procedure coding rather than a paper-authored curated intervention definition.",
            "",
            "## Cohort Alignment Gap",
            f"- Reproduced cohort size: {actual_n}",
            f"- Paper cohort size: {profile.expected_final_n}",
            f"- Cohort difference: {cohort_gap:+d}",
        ]
    )
    if alignment_rel:
        lines.append(f"- Alignment artifact: `{alignment_rel}`")
    lines.extend(
        [
            "",
            "## Artifacts",
            *[f"- `{item}`" for item in outputs],
        ]
    )
    return "\n".join(lines) + "\n"


def _resolve_cohort_funnel_path(
    *,
    project_root: Path,
    analysis_dataset_rel: str,
    artifact_subdir: str,
) -> Path | None:
    candidates: list[Path] = []
    if artifact_subdir.strip():
        candidates.append(project_root / "shared" / artifact_subdir.strip() / "cohort_funnel.json")
    analysis_path = (project_root / analysis_dataset_rel).resolve()
    if analysis_path.parent.exists():
        candidates.append(analysis_path.parent / "cohort_funnel.json")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _resolve_cohort_alignment_path(
    *,
    project_root: Path,
    analysis_dataset_rel: str,
    artifact_subdir: str,
) -> Path | None:
    candidates: list[Path] = []
    if artifact_subdir.strip():
        candidates.append(project_root / "shared" / artifact_subdir.strip() / "cohort_alignment.json")
    analysis_path = (project_root / analysis_dataset_rel).resolve()
    if analysis_path.parent.exists():
        candidates.append(analysis_path.parent / "cohort_alignment.json")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _plot_cohort_flowchart(
    funnel_payload: dict[str, Any],
    profile: PaperExecutionProfile,
    output_path: Path,
    *,
    alignment_payload: dict[str, Any] | None = None,
) -> None:
    if _is_stroke_tyg_profile(profile):
        _plot_stroke_tyg_paper_flowchart(
            funnel_payload,
            profile,
            output_path,
            alignment_payload=alignment_payload or {},
        )
        return
    counts = funnel_payload.get("counts", {})
    if not isinstance(counts, dict) or not counts:
        return

    ordered_steps = [
        (key, value)
        for key, value in counts.items()
        if isinstance(value, (int, float))
    ]
    if not ordered_steps:
        return

    n_steps = len(ordered_steps)
    fig_height = max(8.0, 1.65 * n_steps + 1.8)
    fig, ax = plt.subplots(figsize=(10.5, fig_height))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    box_width = 0.72
    box_height = min(0.11, 0.74 / max(n_steps, 1))
    left = 0.14
    top = 0.92
    gap = min(0.045, (0.78 - n_steps * box_height) / max(n_steps - 1, 1)) if n_steps > 1 else 0.0

    fig.suptitle(
        f"{profile.title}\nParticipant Selection Flowchart",
        fontsize=14,
        y=0.98,
    )

    for index, (key, value) in enumerate(ordered_steps):
        y = top - (index + 1) * box_height - index * gap
        patch = FancyBboxPatch(
            (left, y),
            box_width,
            box_height,
            boxstyle="round,pad=0.012,rounding_size=0.02",
            facecolor="#f7fbff" if index == 0 else "#fff8ef",
            edgecolor="#2f4858",
            linewidth=1.5,
        )
        ax.add_patch(patch)
        step_text = f"{_humanize_funnel_step(key)}\n(n = {int(value)})"
        ax.text(
            left + box_width / 2.0,
            y + box_height / 2.0,
            step_text,
            ha="center",
            va="center",
            fontsize=10,
        )
        if index < n_steps - 1:
            next_y = top - (index + 2) * box_height - (index + 1) * gap
            ax.annotate(
                "",
                xy=(0.5, next_y + box_height + 0.003),
                xytext=(0.5, y - 0.003),
                arrowprops={"arrowstyle": "-|>", "lw": 1.4, "color": "#2f4858"},
            )

    note_lines = []
    if funnel_payload.get("profile", {}).get("expected_final_n"):
        note_lines.append(f"Paper target n = {funnel_payload['profile']['expected_final_n']}")
    if funnel_payload.get("n_output_rows") is not None:
        note_lines.append(f"Reproduced final n = {int(funnel_payload['n_output_rows'])}")
    if note_lines:
        ax.text(
            0.5,
            0.03,
            " | ".join(note_lines),
            ha="center",
            va="bottom",
            fontsize=9,
            color="#4b5563",
        )

    fig.tight_layout(rect=[0, 0.02, 1, 0.95])
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def _stroke_tyg_endpoint_sort_lookup(profile: PaperExecutionProfile) -> dict[str, int]:
    return {endpoint.key: index for index, endpoint in enumerate(_iter_endpoint_profiles(profile))}


def _stroke_tyg_sorted_results(
    results: list[dict[str, Any]],
    profile: PaperExecutionProfile,
) -> list[dict[str, Any]]:
    lookup = _stroke_tyg_endpoint_sort_lookup(profile)
    return sorted(
        results,
        key=lambda item: (
            lookup.get(str(item.get("endpoint_key", "")), 99),
            str(item.get("panel", "")),
            str(item.get("endpoint", "")),
        ),
    )


def _stroke_tyg_extract_quartile_counts(alignment_payload: dict[str, Any]) -> dict[str, int]:
    actual = alignment_payload.get("actual", {}) if isinstance(alignment_payload, dict) else {}
    quartiles = actual.get("tyg_quartile", {}) if isinstance(actual, dict) else {}
    result: dict[str, int] = {}
    if isinstance(quartiles, dict):
        for quartile in QUARTILES:
            value = quartiles.get(quartile)
            if isinstance(value, (int, float)):
                result[quartile] = int(value)
    return result


def _stroke_tyg_count(counts: dict[str, Any], key: str) -> int | None:
    value = counts.get(key)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _stroke_tyg_ordered_axes(
    fig: plt.Figure,
    active_items: list[dict[str, Any]],
) -> tuple[np.ndarray, list[plt.Axes]]:
    axes = fig.subplots(2, 3, squeeze=False)
    axes_flat = list(axes.flatten())
    for index in range(len(active_items), len(axes_flat)):
        axes_flat[index].axis("off")
    return axes, axes_flat


def _plot_stroke_tyg_paper_km(
    results: list[dict[str, Any]],
    profile: PaperExecutionProfile,
    output_path: Path,
) -> None:
    active_results = _stroke_tyg_sorted_results(results, profile)
    if not active_results:
        return

    palette = _resolve_quartile_palette(profile)
    fig = plt.figure(figsize=(12.6, 7.4))
    _, axes_flat = _stroke_tyg_ordered_axes(fig, active_results)

    legend_handles: list[Any] = []
    legend_labels: list[str] = []
    for index, result in enumerate(active_results):
        ax = axes_flat[index]
        for quartile in QUARTILES:
            curve = dict(result.get("curves", {})).get(quartile)
            if not curve:
                continue
            handle = ax.step(
                curve["timeline"],
                curve["survival"],
                where="post",
                color=palette[quartile],
                linewidth=1.9,
                label=quartile,
            )[0]
            if quartile not in legend_labels:
                legend_handles.append(handle)
                legend_labels.append(quartile)

        horizon = _to_optional_float(result.get("time_horizon")) or 1.0
        xticks = [int(value) for value in result.get("xticks", []) if value is not None]
        ax.set_xlim(0.0, horizon)
        if xticks:
            ax.set_xticks(xticks)
        ax.set_ylim(0.0, 1.02)
        ax.set_xlabel("Time (days)", fontsize=8.5)
        if index % 3 == 0:
            ax.set_ylabel("Survival probability", fontsize=8.5)
        else:
            ax.set_ylabel("")
        ax.set_title(
            _stroke_tyg_endpoint_title(
                str(result.get("endpoint_key", "")),
                figure_kind="km",
                panel=str(result.get("panel", "")),
            ),
            fontsize=10,
            loc="left",
            pad=5,
        )
        ax.text(
            0.04,
            0.96,
            f"Log-rank P = {_format_p_value(result.get('logrank_p_value'))}",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=7.8,
        )
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", alpha=0.15, linewidth=0.7)
        ax.tick_params(axis="both", labelsize=8)

    if legend_handles:
        fig.legend(
            legend_handles,
            legend_labels,
            loc="lower center",
            bbox_to_anchor=(0.5, 0.01),
            ncol=4,
            frameon=False,
            title="TyG quartile",
            fontsize=8.2,
            title_fontsize=8.2,
        )

    fig.subplots_adjust(left=0.08, right=0.985, top=0.96, bottom=0.12, wspace=0.22, hspace=0.34)
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def _plot_stroke_tyg_paper_rcs(
    results: list[dict[str, Any]],
    profile: PaperExecutionProfile,
    output_path: Path,
    *,
    raw_df: pd.DataFrame | None = None,
) -> None:
    active_results = _stroke_tyg_sorted_results(results, profile)
    if not active_results:
        return

    predictor_values = pd.Series(dtype=float)
    if raw_df is not None and profile.predictor_column in raw_df.columns:
        predictor_values = pd.to_numeric(raw_df[profile.predictor_column], errors="coerce").dropna()

    x_limits: list[float] = []
    y_limits: list[float] = [1.1]
    for item in active_results:
        curve = item.get("curve", pd.DataFrame())
        if curve.empty:
            continue
        x_limits.extend([float(curve["x_value"].min()), float(curve["x_value"].max())])
        y_limits.append(float(pd.to_numeric(curve["ci_upper_95"], errors="coerce").max()))
    if predictor_values.empty:
        predictor_values = pd.Series(x_limits, dtype=float)
    global_x_min = float(np.nanmin(predictor_values)) if len(predictor_values) else min(x_limits, default=0.0)
    global_x_max = float(np.nanmax(predictor_values)) if len(predictor_values) else max(x_limits, default=1.0)
    global_y_max = max(y_limits) * 1.08

    fig = plt.figure(figsize=(12.6, 7.6))
    _, axes_flat = _stroke_tyg_ordered_axes(fig, active_results)

    for index, result in enumerate(active_results):
        ax = axes_flat[index]
        curve = result.get("curve", pd.DataFrame())
        if curve.empty:
            ax.axis("off")
            continue

        hist_ax = ax.twinx()
        if len(predictor_values):
            bins = np.linspace(global_x_min, global_x_max, 20)
            hist_ax.hist(
                predictor_values,
                bins=bins,
                color="#bfd8ea",
                edgecolor="white",
                linewidth=0.6,
                alpha=0.7,
                zorder=0,
            )
        hist_ax.set_yticks([])
        hist_ax.spines["top"].set_visible(False)
        hist_ax.spines["right"].set_visible(False)
        hist_ax.spines["left"].set_visible(False)
        hist_ax.tick_params(axis="y", length=0)

        ax.set_zorder(2)
        ax.patch.set_alpha(0.0)
        ax.plot(curve["x_value"], curve["hazard_ratio"], color="#d24b68", linewidth=2.0, zorder=4)
        ax.fill_between(
            curve["x_value"],
            curve["ci_lower_95"],
            curve["ci_upper_95"],
            color="#f3b7c5",
            alpha=0.55,
            zorder=3,
        )
        ax.axhline(1.0, linestyle="--", color="#666666", linewidth=1.0, zorder=2)
        reference_value = _to_optional_float(result.get("reference_value"))
        if reference_value is not None:
            ax.axvline(reference_value, linestyle=":", color="#444444", linewidth=1.1, zorder=2)
        ax.set_xlim(global_x_min, global_x_max)
        ax.set_ylim(0.0, global_y_max)
        ax.set_xlabel("TyG index", fontsize=8.5)
        if index % 3 == 0:
            ax.set_ylabel("Hazard ratio", fontsize=8.5)
        else:
            ax.set_ylabel("")
        ax.set_title(
            _stroke_tyg_endpoint_title(
                str(result.get("endpoint_key", "")),
                figure_kind="rcs",
                panel=str(result.get("panel", "")),
            ),
            fontsize=10,
            loc="left",
            pad=5,
        )
        ax.text(
            0.04,
            0.96,
            "\n".join(
                [
                    f"P for overall = {_format_p_value(result.get('overall_p_value'))}",
                    f"P for non-linear = {_format_p_value(result.get('nonlinearity_p_value'))}",
                ]
            ),
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=7.5,
        )
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", alpha=0.12, linewidth=0.7)
        ax.tick_params(axis="both", labelsize=8)

    fig.subplots_adjust(left=0.08, right=0.985, top=0.96, bottom=0.1, wspace=0.22, hspace=0.34)
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def _stroke_tyg_display_rows(subset: pd.DataFrame) -> list[dict[str, Any]]:
    display_rows: list[dict[str, Any]] = []
    overall_rows = subset.loc[subset["row_type"].astype(str) == "overall"].copy()
    if not overall_rows.empty:
        adjusted = overall_rows.loc[overall_rows["level"].astype(str).str.lower() == "adjusted"]
        selected = adjusted.iloc[0] if not adjusted.empty else overall_rows.iloc[0]
        display_rows.append({"kind": "data", "subgroup_label": "Overall", "level_label": "", **selected.to_dict()})

    subgroup_rows = subset.loc[subset["row_type"].astype(str) == "subgroup_level"].copy()
    for subgroup_name, group in subgroup_rows.groupby("subgroup", sort=False):
        p_interaction = group.iloc[0].get("p_interaction")
        display_rows.append(
            {
                "kind": "header",
                "subgroup_label": str(subgroup_name),
                "p_interaction": p_interaction,
            }
        )
        for _, row in group.iterrows():
            display_rows.append(
                {
                    "kind": "data",
                    "subgroup_label": "",
                    "level_label": str(row.get("level", "")),
                    **row.to_dict(),
                }
            )
    return display_rows


def _plot_stroke_tyg_paper_subgroup(df: pd.DataFrame, profile: PaperExecutionProfile, output_path: Path) -> None:
    if df.empty:
        return

    endpoint_lookup = {item.key: item for item in _iter_endpoint_profiles(profile)}
    active_keys = [item.key for item in _iter_endpoint_profiles(profile) if item.key in set(df["endpoint_key"].astype(str))]
    if not active_keys:
        return

    numeric_values: list[float] = []
    filtered = df.loc[
        ~(
            (df["row_type"].astype(str) == "overall")
            & (df["level"].astype(str).str.lower() == "crude")
        )
    ].copy()
    for column in ("hazard_ratio", "ci_lower_95", "ci_upper_95"):
        numeric_values.extend(pd.to_numeric(filtered[column], errors="coerce").dropna().tolist())
    x_min = min(numeric_values) * 0.82 if numeric_values else 0.55
    x_max = max(numeric_values) * 1.18 if numeric_values else 1.75
    x_min = min(x_min, 0.7)
    x_max = max(x_max, 1.5)

    fig = plt.figure(figsize=(14.6, 9.6))
    outer = fig.add_gridspec(2, 3, left=0.035, right=0.988, top=0.972, bottom=0.07, wspace=0.14, hspace=0.20)

    for index, endpoint_key in enumerate(active_keys):
        endpoint = endpoint_lookup[endpoint_key]
        subset = filtered.loc[filtered["endpoint_key"].astype(str) == endpoint_key].copy()
        display_rows = _stroke_tyg_display_rows(subset)
        if not display_rows:
            continue

        inner = GridSpecFromSubplotSpec(1, 2, subplot_spec=outer[index], width_ratios=[0.79, 0.21], wspace=0.02)
        table_ax = fig.add_subplot(inner[0])
        forest_ax = fig.add_subplot(inner[1], sharey=table_ax)

        nrows = len(display_rows)
        header_y = nrows + 0.75

        table_ax.set_xlim(0.0, 1.0)
        table_ax.set_ylim(0.4, nrows + 1.0)
        table_ax.axis("off")
        table_ax.text(
            0.0,
            nrows + 0.96,
            _stroke_tyg_endpoint_title(endpoint.key, figure_kind="km", panel=endpoint.figure_panel),
            fontsize=9.2,
            fontweight="bold",
            va="bottom",
        )
        table_ax.text(0.01, header_y, "Subgroup", fontsize=6.9, fontweight="bold", va="bottom")
        table_ax.text(0.59, header_y, "No.", fontsize=6.9, fontweight="bold", va="bottom", ha="right")
        table_ax.text(0.84, header_y, "HR (95% CI)", fontsize=6.9, fontweight="bold", va="bottom", ha="right")
        table_ax.text(0.99, header_y, "P for interaction", fontsize=6.6, fontweight="bold", va="bottom", ha="right")

        forest_ax.set_ylim(0.4, nrows + 1.0)
        forest_ax.set_xlim(x_min, x_max)
        forest_ax.axvline(1.0, color="#666666", linestyle="--", linewidth=1.0)
        forest_ax.set_yticks([])
        forest_ax.tick_params(axis="x", labelsize=6.8)
        forest_ax.grid(axis="x", alpha=0.16, linewidth=0.7)
        forest_ax.spines["top"].set_visible(False)
        forest_ax.spines["right"].set_visible(False)
        forest_ax.spines["left"].set_visible(False)
        forest_ax.set_xlabel("HR", fontsize=7.1)

        for row_index, item in enumerate(display_rows):
            y = nrows - row_index
            if item.get("kind") == "header":
                table_ax.text(0.01, y, str(item.get("subgroup_label", "")), fontsize=6.8, fontweight="bold", va="center")
                p_interaction = item.get("p_interaction")
                if p_interaction not in (None, ""):
                    table_ax.text(0.99, y, _format_p_value(p_interaction), fontsize=6.6, va="center", ha="right")
                continue

            hr = _to_optional_float(item.get("hazard_ratio"))
            lo = _to_optional_float(item.get("ci_lower_95"))
            hi = _to_optional_float(item.get("ci_upper_95"))
            if hr is None or lo is None or hi is None:
                continue

            subgroup_label = str(item.get("subgroup_label", ""))
            level_label = str(item.get("level_label", ""))
            table_ax.text(0.01, y, subgroup_label, fontsize=6.6, va="center")
            table_ax.text(0.08, y, level_label, fontsize=6.6, va="center")
            table_ax.text(0.59, y, str(int(item.get("total", 0) or 0)), fontsize=6.6, va="center", ha="right")
            table_ax.text(
                0.84,
                y,
                f"{_format_numeric(hr, digits=2)} ({_format_numeric(lo, digits=2)}-{_format_numeric(hi, digits=2)})",
                fontsize=6.6,
                va="center",
                ha="right",
            )
            err_left = max(hr - lo, 0.0)
            err_right = max(hi - hr, 0.0)
            forest_ax.errorbar(
                hr,
                y,
                xerr=[[err_left], [err_right]],
                fmt="s",
                color="#b22222",
                markerfacecolor="#b22222",
                markeredgecolor="#b22222",
                ecolor="black",
                elinewidth=1.0,
                markersize=4.0,
                capsize=2.0,
            )

    for index in range(len(active_keys), 6):
        ax = fig.add_subplot(outer[index])
        ax.axis("off")

    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def _draw_stroke_tyg_flowchart_box(
    ax: plt.Axes,
    *,
    center_x: float,
    center_y: float,
    width: float,
    height: float,
    text: str,
    fontsize: float = 8.8,
) -> tuple[float, float, float, float]:
    left = center_x - width / 2.0
    bottom = center_y - height / 2.0
    patch = Rectangle((left, bottom), width, height, fill=False, edgecolor="black", linewidth=1.15)
    ax.add_patch(patch)
    ax.text(center_x, center_y, text, ha="center", va="center", fontsize=fontsize)
    return left, bottom, width, height


def _draw_stroke_tyg_flowchart_arrow(
    ax: plt.Axes,
    start: tuple[float, float],
    end: tuple[float, float],
) -> None:
    ax.annotate(
        "",
        xy=end,
        xytext=start,
        arrowprops={"arrowstyle": "-|>", "lw": 1.0, "color": "black", "shrinkA": 2, "shrinkB": 2},
    )


def _plot_stroke_tyg_paper_flowchart(
    funnel_payload: dict[str, Any],
    profile: PaperExecutionProfile,
    output_path: Path,
    *,
    alignment_payload: dict[str, Any],
) -> None:
    counts = funnel_payload.get("counts", {})
    if not isinstance(counts, dict) or not counts:
        return

    n_initial = _stroke_tyg_count(counts, "n_initial_first_icu")
    n_stroke = _stroke_tyg_count(counts, "n_after_primary_ischemic_stroke")
    n_los = _stroke_tyg_count(counts, "n_after_icu_los")
    n_non_diabetic = _stroke_tyg_count(counts, "n_after_non_diabetic_exclusion")
    n_final = _stroke_tyg_count(counts, "n_final_with_day1_tyg")
    if None in {n_initial, n_stroke, n_los, n_non_diabetic, n_final}:
        return

    non_stroke = max(n_initial - n_stroke, 0)
    los_excluded = max(n_stroke - n_los, 0)
    diabetes_excluded = max(n_los - n_non_diabetic, 0)
    missing_tyg = max(n_non_diabetic - n_final, 0)
    quartile_counts = _stroke_tyg_extract_quartile_counts(alignment_payload)

    fig, ax = plt.subplots(figsize=(11.8, 7.8))
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.axis("off")

    box_w = 0.28
    box_h = 0.082
    main_x = 0.48
    y_positions = {
        "initial": 0.85,
        "stroke_total": 0.68,
        "first_stroke": 0.51,
        "final": 0.34,
    }

    _draw_stroke_tyg_flowchart_box(
        ax,
        center_x=main_x,
        center_y=y_positions["initial"],
        width=box_w,
        height=box_h,
        text=f"Total ICU admissions in {profile.source_dataset_version}\n(n = {n_initial})",
        fontsize=8.4,
    )
    _draw_stroke_tyg_flowchart_box(
        ax,
        center_x=main_x,
        center_y=y_positions["stroke_total"],
        width=box_w,
        height=box_h,
        text=f"Total ischemic stroke ICU admissions\n(n = {n_stroke})",
        fontsize=8.4,
    )
    _draw_stroke_tyg_flowchart_box(
        ax,
        center_x=main_x,
        center_y=y_positions["first_stroke"],
        width=box_w,
        height=box_h,
        text=f"First ICU ischemic stroke admission\n(n = {n_stroke})",
        fontsize=8.4,
    )
    _draw_stroke_tyg_flowchart_box(
        ax,
        center_x=main_x,
        center_y=y_positions["final"],
        width=box_w,
        height=box_h,
        text=f"Final cohort\n(n = {n_final})",
        fontsize=8.6,
    )

    _draw_stroke_tyg_flowchart_box(
        ax,
        center_x=0.17,
        center_y=0.68,
        width=0.22,
        height=0.10,
        text=f"Non-ischemic stroke ICU admissions\n(n = {non_stroke})",
        fontsize=8.0,
    )
    _draw_stroke_tyg_flowchart_box(
        ax,
        center_x=0.80,
        center_y=0.50,
        width=0.29,
        height=0.19,
        text=(
            "Excluded\n"
            f"(1) TyG index missing at ICU admission (n = {missing_tyg})\n"
            f"(2) ICU length of stay < 3 h (n = {los_excluded})\n"
            f"(3) Diabetic history / medication (n = {diabetes_excluded})\n"
            "(4) Age < 18 years (n = 0)"
        ),
        fontsize=7.5,
    )

    _draw_stroke_tyg_flowchart_arrow(
        ax,
        (main_x, y_positions["initial"] - box_h / 2.0),
        (main_x, y_positions["stroke_total"] + box_h / 2.0),
    )
    _draw_stroke_tyg_flowchart_arrow(
        ax,
        (main_x, y_positions["stroke_total"] - box_h / 2.0),
        (main_x, y_positions["first_stroke"] + box_h / 2.0),
    )
    _draw_stroke_tyg_flowchart_arrow(
        ax,
        (main_x, y_positions["first_stroke"] - box_h / 2.0),
        (main_x, y_positions["final"] + box_h / 2.0),
    )
    _draw_stroke_tyg_flowchart_arrow(
        ax,
        (main_x - box_w / 2.0, y_positions["stroke_total"]),
        (0.17 + 0.22 / 2.0, 0.68),
    )
    _draw_stroke_tyg_flowchart_arrow(
        ax,
        (main_x + box_w / 2.0, y_positions["first_stroke"]),
        (0.80 - 0.29 / 2.0, 0.50),
    )

    quartile_xs = np.linspace(0.17, 0.79, 4)
    quartile_w = 0.16
    quartile_h = 0.075
    for quartile, center_x in zip(QUARTILES, quartile_xs, strict=True):
        count = quartile_counts.get(quartile)
        quartile_label = quartile.replace("Q", "Quartile ")
        label = f"{quartile_label}\n(n = {count})" if count is not None else quartile_label
        _draw_stroke_tyg_flowchart_box(
            ax,
            center_x=float(center_x),
            center_y=0.11,
            width=quartile_w,
            height=quartile_h,
            text=label,
            fontsize=7.6,
        )
        _draw_stroke_tyg_flowchart_arrow(
            ax,
            (main_x, y_positions["final"] - box_h / 2.0),
            (float(center_x), 0.11 + quartile_h / 2.0),
        )

    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def _humanize_funnel_step(key: str) -> str:
    labels = {
        "n_initial_first_icu": "First ICU admissions",
        "n_after_age": "Adults aged 18 years or older",
        "n_after_primary_ischemic_stroke": "Primary ischemic stroke admissions",
        "n_after_icu_los": "ICU length of stay >= 3 hours",
        "n_after_non_diabetic_exclusion": "After non-diabetic history exclusion",
        "n_final_with_day1_tyg": "Final cohort with ICU day-1 TyG data",
    }
    return labels.get(key, key.replace("n_after_", "").replace("_", " ").strip().title())


def _resolve_requested_output_kinds(profile: PaperExecutionProfile) -> set[str]:
    requested = {str(item).strip() for item in profile.outputs if str(item).strip()}
    if requested:
        return requested
    return {
        "baseline_table",
        "cox_results_table",
        "km_figure",
        "rcs_figure",
        "subgroup_table",
        "subgroup_figure",
        "roc_figure",
        "reproduction_report",
    }


def _wants_any_output(requested_output_kinds: set[str], *kinds: str) -> bool:
    return any(kind in requested_output_kinds for kind in kinds)


def _remove_if_exists(*paths: Path) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def _load_profile_frame(path: Path, profile: PaperExecutionProfile) -> pd.DataFrame:
    df = pd.read_csv(path)
    for column in df.columns:
        if column in STRING_COLUMNS or column == profile.predictor_quartile_column:
            df[column] = df[column].fillna("").astype(str).str.strip()
            continue
        try:
            df[column] = pd.to_numeric(df[column])
        except (TypeError, ValueError):
            pass

    if "gender" in df.columns:
        df["gender"] = df["gender"].map(_normalize_gender)
    if "race" in df.columns:
        df["race"] = df["race"].map(_collapse_race_categories)

    if profile.predictor_column not in df.columns:
        raise ValueError(f"Predictor column '{profile.predictor_column}' not found in {path}")
    if profile.event_column not in df.columns:
        raise ValueError(f"Event column '{profile.event_column}' not found in {path}")
    if profile.duration_column not in df.columns:
        raise ValueError(f"Duration column '{profile.duration_column}' not found in {path}")

    predictor = pd.to_numeric(df[profile.predictor_column], errors="coerce")
    df[profile.predictor_column] = predictor
    if profile.predictor_quartile_column not in df.columns or not df[profile.predictor_quartile_column].astype(str).str.strip().any():
        df[profile.predictor_quartile_column] = predictor.map(lambda value: _assign_quartile(value, profile))
    else:
        df[profile.predictor_quartile_column] = df[profile.predictor_quartile_column].map(_normalize_quartile)

    df = df[df[profile.predictor_quartile_column].isin(QUARTILES)].copy()
    df[profile.predictor_quartile_column] = pd.Categorical(
        df[profile.predictor_quartile_column],
        categories=QUARTILES,
        ordered=True,
    )
    df[profile.event_column] = pd.to_numeric(df[profile.event_column], errors="coerce")
    df[profile.duration_column] = pd.to_numeric(df[profile.duration_column], errors="coerce")
    return df


def _prepare_model_frame(df: pd.DataFrame, profile: PaperExecutionProfile) -> tuple[pd.DataFrame, dict[str, Any]]:
    covariate_columns = {
        column
        for adjustment in profile.model_adjustments
        for column in adjustment.covariates
        if column in df.columns
    }
    subgroup_columns = {item.column for item in profile.subgroups if item.column in df.columns}
    keep_columns = list(
        dict.fromkeys(
            column
            for column in (
                profile.predictor_column,
                profile.predictor_quartile_column,
                profile.event_column,
                profile.duration_column,
                *profile.baseline_continuous_columns,
                *profile.baseline_categorical_columns,
                *sorted(covariate_columns),
                *sorted(subgroup_columns),
            )
            if column in df.columns
        )
    )
    work = df[keep_columns].copy()
    work = work.replace([np.inf, -np.inf], np.nan)

    predictor = pd.to_numeric(work[profile.predictor_column], errors="coerce")
    predictor_mean = float(predictor.mean())
    predictor_std = float(predictor.std(ddof=0))
    if predictor_std > 0:
        work[f"{profile.predictor_column}_z"] = (predictor - predictor_mean) / predictor_std
    else:
        work[f"{profile.predictor_column}_z"] = predictor

    categorical_columns = [
        column
        for column in work.columns
        if column in STRING_COLUMNS or column == profile.predictor_quartile_column or work[column].dtype == object
    ]
    numeric_columns = [
        column
        for column in work.columns
        if column not in categorical_columns and column not in {profile.event_column, profile.duration_column}
    ]

    categorical_imputed: list[str] = []
    if categorical_columns:
        imputer = SimpleImputer(strategy="most_frequent")
        original_na = {column: int(work[column].isna().sum()) for column in categorical_columns}
        work[categorical_columns] = imputer.fit_transform(work[categorical_columns])
        categorical_imputed = [column for column, count in original_na.items() if count > 0]

    numeric_imputed: list[str] = []
    if numeric_columns:
        imputer = SimpleImputer(strategy="median")
        original_na = {column: int(pd.to_numeric(work[column], errors="coerce").isna().sum()) for column in numeric_columns}
        work[numeric_columns] = imputer.fit_transform(work[numeric_columns])
        numeric_imputed = [column for column, count in original_na.items() if count > 0]

    work = work.dropna(subset=[profile.event_column, profile.duration_column, profile.predictor_column]).copy()
    work = work[work[profile.duration_column] > 0].copy()
    return work, {
        "categorical_imputed_columns": categorical_imputed,
        "numeric_imputed_columns": numeric_imputed,
        "predictor_mean": _round_or_none(predictor_mean, digits=6),
        "predictor_std": _round_or_none(predictor_std, digits=6),
    }


def _build_baseline_table(df: pd.DataFrame, profile: PaperExecutionProfile) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    quartile_col = profile.predictor_quartile_column

    for column in profile.baseline_continuous_columns:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        groups = [values[df[quartile_col] == quartile].dropna() for quartile in QUARTILES]
        overall = values.dropna()
        if overall.empty:
            continue
        all_normal = all(_looks_normal(series) for series in groups if len(series) >= 3)
        formatter = _format_mean_sd if all_normal else _format_median_iqr
        test_name, p_value = _continuous_group_test(groups=groups, assume_normal=all_normal)
        row = {
            "variable": column,
            "level": "",
            "variable_type": "continuous",
            "test": test_name,
            "p_value": _round_or_none(p_value, digits=6),
            "overall": formatter(overall),
        }
        for quartile, series in zip(QUARTILES, groups, strict=True):
            row[quartile] = formatter(series)
        rows.append(row)

    for column in profile.baseline_categorical_columns:
        if column not in df.columns:
            continue
        series = df[column]
        if pd.api.types.is_numeric_dtype(series):
            series = pd.to_numeric(series, errors="coerce")
            contingency = pd.crosstab(series.fillna(-1).astype("Int64").astype(str), df[quartile_col])
        else:
            contingency = pd.crosstab(series.fillna("Missing").astype(str), df[quartile_col])
        if contingency.empty:
            continue
        levels = contingency.index.tolist()
        if set(levels).issubset({"0", "1"}) and "1" in levels:
            levels = ["1"]
        p_value = None
        if contingency.shape[0] > 1 and contingency.shape[1] > 1:
            try:
                _, p_value, _, _ = chi2_contingency(contingency)
            except ValueError:
                p_value = None
        for level in levels:
            denom = int(series.notna().sum())
            count = int(contingency.loc[level].sum()) if level in contingency.index else 0
            row = {
                "variable": column,
                "level": level,
                "variable_type": "categorical",
                "test": "chi_square",
                "p_value": _round_or_none(p_value, digits=6),
                "overall": _format_count_pct(count, denom),
            }
            for quartile in QUARTILES:
                quartile_denom = int((df[quartile_col] == quartile).sum())
                quartile_count = int(contingency.loc[level, quartile]) if quartile in contingency.columns else 0
                row[quartile] = _format_count_pct(quartile_count, quartile_denom)
            rows.append(row)

    return pd.DataFrame(rows, columns=["variable", "level", "variable_type", "test", "p_value", "overall", *QUARTILES])


def _fit_continuous_cox(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    covariates: tuple[str, ...],
    model_name: str,
) -> dict[str, Any]:
    predictor_term = f"{profile.predictor_column}_z"
    model_df = _build_design_matrix(
        df=df,
        duration_col=profile.duration_column,
        event_col=profile.event_column,
        predictor_mode="continuous_z",
        predictor_column=profile.predictor_column,
        quartile_column=profile.predictor_quartile_column,
        covariates=covariates,
    )
    fit = _fit_cox_model(model_df, profile.duration_column, profile.event_column)
    if fit is None or predictor_term not in fit.summary.index:
        return {"rows": [], "hazard_ratio": None, "fit": None, "model_df": model_df}
    summary_row = fit.summary.loc[predictor_term]
    return {
        "rows": [
            {
                "model_name": model_name,
                "model_type": "continuous_z",
                "term": predictor_term,
                "hazard_ratio": _round_or_none(float(summary_row["exp(coef)"]), digits=6),
                "ci_lower_95": _round_or_none(float(summary_row["exp(coef) lower 95%"]), digits=6),
                "ci_upper_95": _round_or_none(float(summary_row["exp(coef) upper 95%"]), digits=6),
                "p_value": _round_or_none(float(summary_row["p"]), digits=6),
                "n": int(len(model_df)),
                "events": int(model_df[profile.event_column].sum()),
                "covariates": list(covariates),
            }
        ],
        "hazard_ratio": float(summary_row["exp(coef)"]),
        "fit": fit,
        "model_df": model_df,
    }


def _fit_quartile_cox(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    covariates: tuple[str, ...],
    model_name: str,
) -> dict[str, Any]:
    model_df = _build_design_matrix(
        df=df,
        duration_col=profile.duration_column,
        event_col=profile.event_column,
        predictor_mode="quartile",
        predictor_column=profile.predictor_column,
        quartile_column=profile.predictor_quartile_column,
        covariates=covariates,
    )
    fit = _fit_cox_model(model_df, profile.duration_column, profile.event_column)
    if fit is None:
        return {"rows": [], "q4_hazard_ratio": None, "fit": None, "model_df": model_df}

    rows: list[dict[str, Any]] = []
    q4_hr: float | None = None
    label_map = {"quartile_Q2": "Q2_vs_Q1", "quartile_Q3": "Q3_vs_Q1", "quartile_Q4": "Q4_vs_Q1"}
    for term in ("quartile_Q2", "quartile_Q3", "quartile_Q4"):
        if term not in fit.summary.index:
            continue
        summary_row = fit.summary.loc[term]
        hazard_ratio = float(summary_row["exp(coef)"])
        if term == "quartile_Q4":
            q4_hr = hazard_ratio
        rows.append(
            {
                "model_name": model_name,
                "model_type": "quartile",
                "term": label_map[term],
                "hazard_ratio": _round_or_none(hazard_ratio, digits=6),
                "ci_lower_95": _round_or_none(float(summary_row["exp(coef) lower 95%"]), digits=6),
                "ci_upper_95": _round_or_none(float(summary_row["exp(coef) upper 95%"]), digits=6),
                "p_value": _round_or_none(float(summary_row["p"]), digits=6),
                "n": int(len(model_df)),
                "events": int(model_df[profile.event_column].sum()),
                "covariates": list(covariates),
            }
        )
    return {
        "rows": rows,
        "q4_hazard_ratio": q4_hr,
        "fit": fit,
        "model_df": model_df,
    }


def _build_design_matrix(
    *,
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    predictor_mode: str,
    predictor_column: str,
    quartile_column: str,
    covariates: tuple[str, ...],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = [df[[duration_col, event_col]].astype(float).reset_index(drop=True)]
    predictor_terms: list[str] = []

    if predictor_mode == "continuous_z":
        z_col = f"{predictor_column}_z"
        frames.append(pd.to_numeric(df[z_col], errors="coerce").to_frame(z_col).reset_index(drop=True))
        predictor_terms = [z_col]
    elif predictor_mode == "quartile":
        dummies = pd.get_dummies(df[quartile_column], prefix="quartile", drop_first=True, dtype=float)
        frames.append(dummies.reset_index(drop=True))
        predictor_terms = [name for name in ("quartile_Q2", "quartile_Q3", "quartile_Q4") if name in dummies.columns]
    else:
        raise ValueError(f"Unsupported predictor_mode: {predictor_mode}")

    for column in covariates:
        if column not in df.columns:
            continue
        series = df[column]
        if column in STRING_COLUMNS or series.dtype == object:
            dummies = pd.get_dummies(series.astype(str), prefix=column, drop_first=True, dtype=float)
            if not dummies.empty:
                frames.append(dummies.reset_index(drop=True))
        else:
            frames.append(pd.to_numeric(series, errors="coerce").to_frame(column).reset_index(drop=True))

    model_df = pd.concat(frames, axis=1)
    model_df = model_df.replace([np.inf, -np.inf], np.nan).dropna().copy()
    constant_cols = [
        column
        for column in model_df.columns
        if column not in {duration_col, event_col} and model_df[column].nunique(dropna=True) <= 1
    ]
    if constant_cols:
        model_df = model_df.drop(columns=constant_cols)
    return model_df


def _fit_km_by_quartile(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    *,
    output_path: Path,
) -> dict[str, Any]:
    work = df[[profile.duration_column, profile.event_column, profile.predictor_quartile_column]].dropna().copy()
    work = work[work[profile.duration_column] > 0].copy()
    if work.empty:
        return {"logrank_p_value": None, "groups": {}, "plot": str(output_path)}

    display_duration = (
        work[profile.duration_column] / 24.0
        if profile.duration_unit == "hours"
        else work[profile.duration_column]
    )
    work["display_duration"] = display_duration
    horizon = profile.km_time_horizon
    if horizon is None:
        horizon = float(np.ceil(work["display_duration"].quantile(0.95)))
    horizon = max(float(horizon), 1.0)
    if horizon <= 10:
        xticks = list(range(0, int(math.ceil(horizon)) + 1, 2)) or [0, int(math.ceil(horizon))]
    else:
        xticks = [0, 7, 14, 21, 28] if horizon <= 28.5 else list(np.linspace(0, horizon, 5, dtype=int))
    xticks = sorted(set(int(tick) for tick in xticks if 0 <= tick <= math.ceil(horizon)))

    fig, ax = plt.subplots(figsize=(8.4, 6.8))
    fitters: list[KaplanMeierFitter] = []
    groups_payload: dict[str, dict[str, Any]] = {}
    for quartile in QUARTILES:
        subset = work.loc[work[profile.predictor_quartile_column] == quartile]
        if subset.empty:
            continue
        kmf = KaplanMeierFitter()
        kmf.fit(subset["display_duration"], event_observed=subset[profile.event_column], label=quartile)
        kmf.plot_survival_function(ax=ax, ci_show=False, linewidth=2.1, color=DEFAULT_PALETTE[quartile])
        fitters.append(kmf)
        groups_payload[quartile] = {
            "n": int(len(subset)),
            "events": int(subset[profile.event_column].sum()),
            "median_time": _round_or_none(kmf.median_survival_time_, digits=4),
        }

    logrank_p = None
    try:
        result = multivariate_logrank_test(
            event_durations=work["display_duration"],
            groups=work[profile.predictor_quartile_column],
            event_observed=work[profile.event_column],
        )
        logrank_p = float(result.p_value)
    except ValueError:
        pass

    ax.set_title(f"{profile.title}\nKaplan-Meier survival by {profile.predictor_column.upper()} quartile")
    ax.set_xlabel(f"Time ({profile.duration_unit})")
    ax.set_ylabel("Survival probability")
    ax.set_xlim(0, horizon)
    ax.set_xticks(xticks)
    ax.set_ylim(0.0, 1.02)
    ax.grid(axis="y", alpha=0.2)
    ax.legend(title="Quartile", loc="lower left")
    ax.text(
        0.98,
        0.97,
        f"Log-rank P = {_format_p_value(logrank_p)}",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=10,
        bbox={"facecolor": "white", "edgecolor": "#dddddd", "alpha": 0.9, "pad": 4},
    )
    if fitters:
        add_at_risk_counts(*fitters, ax=ax, xticks=xticks, rows_to_show=["At risk"])
    fig.subplots_adjust(left=0.11, right=0.97, top=0.90, bottom=0.28)
    fig.savefig(output_path, dpi=170)
    plt.close(fig)

    return {
        "logrank_p_value": _round_or_none(logrank_p, digits=6),
        "groups": groups_payload,
        "plot": str(output_path),
        "time_horizon": _round_or_none(horizon, digits=3),
        "xticks": xticks,
    }


def _fit_rcs_model(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    *,
    covariates: tuple[str, ...],
    model_name: str,
) -> dict[str, Any]:
    work = df[[profile.duration_column, profile.event_column, profile.predictor_column, *[c for c in covariates if c in df.columns]]].copy()
    work = work.replace([np.inf, -np.inf], np.nan).dropna().copy()
    work = work[work[profile.duration_column] > 0].copy()
    if work.empty or work[profile.event_column].sum() <= 0:
        return {"curve": pd.DataFrame(), "overall_p_value": None, "nonlinearity_p_value": None, "reference_value": None, "n": 0, "events": 0}

    lower = float(work[profile.predictor_column].quantile(0.005))
    upper = float(work[profile.predictor_column].quantile(0.995))
    work = work[(work[profile.predictor_column] >= lower) & (work[profile.predictor_column] <= upper)].copy()
    if work.empty:
        return {"curve": pd.DataFrame(), "overall_p_value": None, "nonlinearity_p_value": None, "reference_value": None, "n": 0, "events": 0}

    base_frames = [work[[profile.duration_column, profile.event_column]].astype(float).reset_index(drop=True)]
    for column in covariates:
        if column not in work.columns:
            continue
        series = work[column]
        if column in STRING_COLUMNS or series.dtype == object:
            dummies = pd.get_dummies(series.astype(str), prefix=column, drop_first=True, dtype=float)
            if not dummies.empty:
                base_frames.append(dummies.reset_index(drop=True))
        else:
            base_frames.append(pd.to_numeric(series, errors="coerce").to_frame(column).reset_index(drop=True))
    base_df = pd.concat(base_frames, axis=1).replace([np.inf, -np.inf], np.nan).dropna().copy()

    basis = dmatrix("cr(x, df=4) - 1", {"x": work[profile.predictor_column]}, return_type="dataframe")
    basis.columns = [f"rcs_{index}" for index in range(basis.shape[1])]
    spline_df = pd.concat([base_df.reset_index(drop=True), basis.reset_index(drop=True)], axis=1).dropna().copy()
    predictor_terms = [column for column in basis.columns if column in spline_df.columns]
    fit = _fit_cox_model(spline_df, profile.duration_column, profile.event_column)
    if fit is None or not predictor_terms:
        return {"curve": pd.DataFrame(), "overall_p_value": None, "nonlinearity_p_value": None, "reference_value": None, "n": 0, "events": 0}

    linear_df = pd.concat(
        [
            base_df.reset_index(drop=True),
            pd.to_numeric(work[profile.predictor_column], errors="coerce").to_frame(profile.predictor_column).reset_index(drop=True),
        ],
        axis=1,
    ).dropna().copy()
    linear_fit = _fit_cox_model(linear_df, profile.duration_column, profile.event_column)

    design_info = basis.design_info
    x_grid = np.linspace(float(work[profile.predictor_column].quantile(0.05)), float(work[profile.predictor_column].quantile(0.95)), 200)
    grid_basis = pd.DataFrame(build_design_matrices([design_info], {"x": x_grid})[0], columns=basis.columns)[predictor_terms]
    reference_value = float(np.median(work[profile.predictor_column]))
    reference_basis = pd.DataFrame(build_design_matrices([design_info], {"x": [reference_value]})[0], columns=basis.columns)[predictor_terms]

    coef = fit.params_.loc[predictor_terms].to_numpy()
    cov = fit.variance_matrix_.loc[predictor_terms, predictor_terms].to_numpy()
    contrast = grid_basis.to_numpy() - reference_basis.to_numpy()
    log_hazard = contrast @ coef
    variance = np.einsum("ij,jk,ik->i", contrast, cov, contrast)
    std_error = np.sqrt(np.clip(variance, a_min=0.0, a_max=None))
    hazard_ratio = np.exp(log_hazard)
    ci_lower = np.exp(log_hazard - 1.96 * std_error)
    ci_upper = np.exp(log_hazard + 1.96 * std_error)

    overall_p = _extract_model_lrt_p_value(fit)
    nonlinearity_p = _likelihood_ratio_p_value(fit, linear_fit)
    curve = pd.DataFrame(
        {
            "x_value": np.round(x_grid, 6),
            "hazard_ratio": np.round(hazard_ratio, 6),
            "ci_lower_95": np.round(ci_lower, 6),
            "ci_upper_95": np.round(ci_upper, 6),
            "overall_p_value": _round_or_none(overall_p, digits=6),
            "nonlinearity_p_value": _round_or_none(nonlinearity_p, digits=6),
        }
    )
    return {
        "curve": curve,
        "overall_p_value": overall_p,
        "nonlinearity_p_value": nonlinearity_p,
        "reference_value": reference_value,
        "n": int(len(work)),
        "events": int(work[profile.event_column].sum()),
        "model_name": model_name,
    }


def _plot_rcs_curve(result: dict[str, Any], profile: PaperExecutionProfile, output_path: Path) -> None:
    curve = result.get("curve", pd.DataFrame())
    if curve.empty:
        return
    fig, ax = plt.subplots(figsize=(8.4, 6.8))
    ax.plot(curve["x_value"], curve["hazard_ratio"], color="#184e77", linewidth=2.2)
    ax.fill_between(curve["x_value"], curve["ci_lower_95"], curve["ci_upper_95"], color="#76c893", alpha=0.22)
    ax.axhline(1.0, linestyle="--", color="#666666", linewidth=1.0)
    ax.axvline(float(result["reference_value"]), linestyle=":", color="#c1121f", linewidth=1.4)
    ax.set_title(f"{profile.title}\nRestricted cubic spline for {profile.predictor_column.upper()}")
    ax.set_xlabel(profile.predictor_column.upper())
    ax.set_ylabel("Hazard ratio")
    ax.grid(axis="y", alpha=0.2)
    ax.text(
        0.03,
        0.97,
        "\n".join(
            [
                f"Overall P = {_format_p_value(result.get('overall_p_value'))}",
                f"P for nonlinearity = {_format_p_value(result.get('nonlinearity_p_value'))}",
                f"Reference = {_format_numeric(result.get('reference_value'), digits=2)}",
            ]
        ),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9.5,
        bbox={"facecolor": "white", "edgecolor": "#dddddd", "alpha": 0.9, "pad": 4},
    )
    fig.tight_layout()
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _fit_roc_curve(df: pd.DataFrame, profile: PaperExecutionProfile) -> dict[str, Any]:
    predictor = f"{profile.predictor_column}_z"
    work = df[[predictor, profile.event_column]].dropna().copy()
    work = work[work[profile.event_column].isin([0, 1])].copy()
    if work.empty or work[profile.event_column].nunique() < 2:
        return {"auc": None, "best_threshold": None, "fpr": [], "tpr": []}
    y_true = pd.to_numeric(work[profile.event_column], errors="coerce")
    scores = pd.to_numeric(work[predictor], errors="coerce")
    auc_value = float(roc_auc_score(y_true, scores))
    fpr, tpr, thresholds = roc_curve(y_true, scores)
    youden = tpr - fpr
    best_index = int(np.argmax(youden))
    return {
        "auc": _round_or_none(auc_value, digits=6),
        "best_threshold": _round_or_none(float(thresholds[best_index]), digits=6),
        "sensitivity": _round_or_none(float(tpr[best_index]), digits=6),
        "specificity": _round_or_none(float(1.0 - fpr[best_index]), digits=6),
        "fpr": [round(float(item), 6) for item in fpr],
        "tpr": [round(float(item), 6) for item in tpr],
    }


def _plot_roc_curve(result: dict[str, Any], profile: PaperExecutionProfile, output_path: Path) -> None:
    fpr = result.get("fpr", [])
    tpr = result.get("tpr", [])
    if not fpr or not tpr:
        return
    fig, ax = plt.subplots(figsize=(7.0, 7.0))
    ax.plot(fpr, tpr, color="#184e77", linewidth=2.2, label=f"AUC = {_format_numeric(result.get('auc'), digits=3)}")
    ax.plot([0, 1], [0, 1], linestyle="--", color="#888888", linewidth=1.0)
    ax.set_title(f"{profile.title}\nROC curve for {profile.predictor_column.upper()}")
    ax.set_xlabel("1 - Specificity")
    ax.set_ylabel("Sensitivity")
    ax.legend(loc="lower right")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _build_overall_forest_row(
    *,
    label: str,
    model_name: str,
    model_result: dict[str, Any] | None,
    profile: PaperExecutionProfile,
    df: pd.DataFrame,
) -> dict[str, Any] | None:
    if not model_result:
        return None
    rows = list(model_result.get("rows", []))
    if not rows:
        return None
    row = rows[0]
    return {
        "row_type": "overall",
        "subgroup": "Overall",
        "level": label,
        "model_name": model_name,
        "model_type": row.get("model_type", "continuous_z"),
        "term": row.get("term", f"{profile.predictor_column}_z"),
        "hazard_ratio": row.get("hazard_ratio"),
        "ci_lower_95": row.get("ci_lower_95"),
        "ci_upper_95": row.get("ci_upper_95"),
        "p_value": row.get("p_value"),
        "total": int(len(df)),
        "events": int(df[profile.event_column].sum()),
        "event_percent": _round_or_none(float(df[profile.event_column].mean()) * 100.0, digits=1),
        "p_interaction": None,
        "covariates": row.get("covariates", []),
    }


def _fit_interaction_p_value(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    subgroup: SubgroupProfile,
    covariates: tuple[str, ...],
) -> float | None:
    subgroup_frame = _build_interaction_subgroup_frame(df, subgroup)
    if subgroup_frame is None or subgroup_frame.empty:
        return None

    z_col = f"{profile.predictor_column}_z"
    base_frames = [
        df[[profile.duration_column, profile.event_column]].astype(float).reset_index(drop=True),
        pd.to_numeric(df[z_col], errors="coerce").to_frame(z_col).reset_index(drop=True),
        subgroup_frame.reset_index(drop=True),
    ]
    filtered_covariates = tuple(column for column in covariates if column != subgroup.column)
    for column in filtered_covariates:
        if column not in df.columns:
            continue
        series = df[column]
        if column in STRING_COLUMNS or series.dtype == object:
            dummies = pd.get_dummies(series.astype(str), prefix=column, drop_first=True, dtype=float)
            if not dummies.empty:
                base_frames.append(dummies.reset_index(drop=True))
        else:
            base_frames.append(pd.to_numeric(series, errors="coerce").to_frame(column).reset_index(drop=True))

    base_df = pd.concat(base_frames, axis=1).replace([np.inf, -np.inf], np.nan).dropna().copy()
    subgroup_terms = [column for column in subgroup_frame.columns if column in base_df.columns]
    if base_df.empty or not subgroup_terms:
        return None

    interaction_terms: list[str] = []
    full_df = base_df.copy()
    for term in subgroup_terms:
        interaction_name = f"{z_col}_x_{term}"
        full_df[interaction_name] = full_df[z_col] * full_df[term]
        interaction_terms.append(interaction_name)

    base_fit = _fit_cox_model(base_df, profile.duration_column, profile.event_column)
    full_fit = _fit_cox_model(full_df, profile.duration_column, profile.event_column)
    return _likelihood_ratio_p_value(full_fit, base_fit)


def _build_interaction_subgroup_frame(df: pd.DataFrame, subgroup: SubgroupProfile) -> pd.DataFrame | None:
    if subgroup.column not in df.columns:
        return None
    series = df[subgroup.column]
    if subgroup.kind == "cut" and subgroup.cut is not None:
        numeric = pd.to_numeric(series, errors="coerce")
        labels = np.where(numeric < subgroup.cut, subgroup.lower_label or f"<{subgroup.cut:g}", subgroup.upper_label or f">={subgroup.cut:g}")
        dummies = pd.get_dummies(pd.Series(labels), prefix=subgroup.column, drop_first=True, dtype=float)
        return dummies if not dummies.empty else None

    if pd.api.types.is_numeric_dtype(series):
        labels = pd.to_numeric(series, errors="coerce").fillna(-1).astype("Int64").astype(str)
    else:
        labels = series.astype(str)
    dummies = pd.get_dummies(labels, prefix=subgroup.column, drop_first=True, dtype=float)
    return dummies if not dummies.empty else None


def _run_subgroup_analysis(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    *,
    covariates: tuple[str, ...],
    overall_rows: tuple[dict[str, Any] | None, ...] = (),
) -> pd.DataFrame:
    if not profile.subgroups:
        return pd.DataFrame([row for row in overall_rows if row is not None])

    rows: list[dict[str, Any]] = []
    rows.extend(row for row in overall_rows if row is not None)
    for subgroup in profile.subgroups:
        interaction_p = _fit_interaction_p_value(df, profile, subgroup, covariates)
        first_level = True
        for level_label, subset in _iter_subgroup_levels(df, subgroup):
            if subset.empty or subset[profile.event_column].sum() < 10:
                continue
            result = _fit_continuous_cox(subset, profile, covariates, "subgroup_model_3")
            result_rows = list(result["rows"])
            if not result_rows:
                continue
            row = result_rows[0]
            rows.append(
                {
                    "row_type": "subgroup_level",
                    "subgroup": subgroup.display_name,
                    "level": level_label,
                    "model_name": row.get("model_name", "subgroup_model_3"),
                    "model_type": row.get("model_type", "continuous_z"),
                    "term": row.get("term", f"{profile.predictor_column}_z"),
                    "hazard_ratio": row.get("hazard_ratio"),
                    "ci_lower_95": row.get("ci_lower_95"),
                    "ci_upper_95": row.get("ci_upper_95"),
                    "p_value": row.get("p_value"),
                    "total": int(len(subset)),
                    "events": int(subset[profile.event_column].sum()),
                    "event_percent": _round_or_none(float(subset[profile.event_column].mean()) * 100.0, digits=1),
                    "p_interaction": _round_or_none(interaction_p, digits=6) if first_level else None,
                    "covariates": row.get("covariates", []),
                }
            )
            first_level = False
    return pd.DataFrame(rows)


def _plot_subgroup_forest(df: pd.DataFrame, profile: PaperExecutionProfile, output_path: Path) -> None:
    if df.empty:
        return

    display_rows: list[dict[str, Any]] = []
    overall_rows = df.loc[df["row_type"] == "overall"].copy() if "row_type" in df.columns else pd.DataFrame()
    if not overall_rows.empty:
        display_rows.append({"kind": "header", "label": "Overall", "p_interaction": None})
        for _, row in overall_rows.iterrows():
            display_rows.append({"kind": "data", **row.to_dict()})

    subgroup_rows = df.loc[df["row_type"] == "subgroup_level"].copy() if "row_type" in df.columns else df.copy()
    for subgroup_name, group in subgroup_rows.groupby("subgroup", sort=False):
        first = group.iloc[0]
        display_rows.append(
            {
                "kind": "header",
                "label": str(subgroup_name),
                "p_interaction": first.get("p_interaction"),
            }
        )
        for _, row in group.iterrows():
            display_rows.append({"kind": "data", **row.to_dict()})

    nrows = len(display_rows)
    if nrows == 0:
        return

    hazard_values = []
    for item in display_rows:
        if item.get("kind") != "data":
            continue
        for key in ("hazard_ratio", "ci_lower_95", "ci_upper_95"):
            value = _to_optional_float(item.get(key))
            if value is not None:
                hazard_values.append(value)
    x_min = min(hazard_values) * 0.85 if hazard_values else 0.7
    x_max = max(hazard_values) * 1.15 if hazard_values else 1.4
    x_min = min(x_min, 0.7)
    x_max = max(x_max, 1.4)

    fig = plt.figure(figsize=(13.5, max(6.5, 0.42 * nrows + 1.6)))
    left_ax = fig.add_axes([0.02, 0.08, 0.48, 0.84])
    forest_ax = fig.add_axes([0.50, 0.08, 0.28, 0.84], sharey=left_ax)
    right_ax = fig.add_axes([0.80, 0.08, 0.18, 0.84], sharey=left_ax)

    for axis in (left_ax, right_ax):
        axis.set_xlim(0, 1)
        axis.set_ylim(0.5, nrows + 0.9)
        axis.axis("off")

    forest_ax.set_ylim(0.5, nrows + 0.9)
    forest_ax.set_xlim(x_min, x_max)
    forest_ax.axvline(1.0, color="#666666", linestyle="--", linewidth=1.0)
    forest_ax.set_xlabel("Effect (95% CI)")
    forest_ax.set_yticks([])
    forest_ax.grid(axis="x", alpha=0.2)

    header_y = nrows + 0.45
    left_ax.text(0.01, header_y, "Subgroup", fontsize=10, fontweight="bold", va="bottom")
    left_ax.text(0.45, header_y, "Total", fontsize=10, fontweight="bold", va="bottom", ha="right")
    left_ax.text(0.63, header_y, "Event (%)", fontsize=10, fontweight="bold", va="bottom", ha="right")
    left_ax.text(0.99, header_y, "HR (95% CI)", fontsize=10, fontweight="bold", va="bottom", ha="right")
    right_ax.text(0.98, header_y, "P for interaction", fontsize=10, fontweight="bold", va="bottom", ha="right")

    for index, item in enumerate(display_rows):
        y = nrows - index
        if item.get("kind") == "header":
            left_ax.text(0.01, y, str(item.get("label", "")), fontsize=10, fontweight="bold", va="center")
            p_interaction = item.get("p_interaction")
            if p_interaction not in (None, ""):
                right_ax.text(0.98, y, _format_p_value(p_interaction), fontsize=9.5, va="center", ha="right")
            continue

        label = str(item.get("level", ""))
        left_ax.text(0.04, y, label, fontsize=9.5, va="center")
        left_ax.text(0.45, y, str(int(item.get("total", 0))), fontsize=9.5, va="center", ha="right")
        left_ax.text(
            0.63,
            y,
            _format_event_text(item.get("events"), item.get("event_percent")),
            fontsize=9.5,
            va="center",
            ha="right",
        )
        left_ax.text(
            0.99,
            y,
            _format_hr_ci_text(item.get("hazard_ratio"), item.get("ci_lower_95"), item.get("ci_upper_95")),
            fontsize=9.5,
            va="center",
            ha="right",
        )

        hr = _to_optional_float(item.get("hazard_ratio"))
        lo = _to_optional_float(item.get("ci_lower_95"))
        hi = _to_optional_float(item.get("ci_upper_95"))
        if hr is not None and lo is not None and hi is not None:
            forest_ax.plot([lo, hi], [y, y], color="#26a7d8", linewidth=1.8)
            forest_ax.scatter([hr], [y], s=36, marker="s", color="#15aabf", zorder=3)

    fig.suptitle(
        f"Subgroup analysis of the association between {profile.predictor_column.upper()} and 28-day mortality",
        fontsize=12,
        y=0.97,
    )
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _iter_subgroup_levels(df: pd.DataFrame, subgroup: SubgroupProfile) -> list[tuple[str, pd.DataFrame]]:
    if subgroup.column not in df.columns:
        return []
    series = df[subgroup.column]
    if subgroup.kind == "cut" and subgroup.cut is not None:
        numeric = pd.to_numeric(series, errors="coerce")
        lower = df.loc[numeric < subgroup.cut].copy()
        upper = df.loc[numeric >= subgroup.cut].copy()
        return [
            (subgroup.lower_label or f"<{subgroup.cut:g}", lower),
            (subgroup.upper_label or f">={subgroup.cut:g}", upper),
        ]
    levels = list(subgroup.levels) if subgroup.levels else sorted(series.dropna().astype(str).unique().tolist())
    results: list[tuple[str, pd.DataFrame]] = []
    for level in levels:
        if pd.api.types.is_numeric_dtype(series):
            subset = df.loc[pd.to_numeric(series, errors="coerce").fillna(-1).astype("Int64").astype(str) == level].copy()
        else:
            subset = df.loc[series.astype(str) == level].copy()
        results.append((level, subset))
    return results


def _fit_schoenfeld_test(model_result: dict[str, Any] | None, *, predictor_term: str) -> dict[str, Any]:
    if not model_result:
        return {"test_statistic": None, "p_value": None}
    fit = model_result.get("fit")
    model_df = model_result.get("model_df")
    if fit is None or model_df is None or predictor_term not in getattr(fit, "params_", pd.Series(dtype=float)).index:
        return {"test_statistic": None, "p_value": None}
    try:
        result = proportional_hazard_test(fit, model_df, time_transform="rank")
        summary = result.summary
        if predictor_term not in summary.index:
            return {"test_statistic": None, "p_value": None}
        row = summary.loc[predictor_term]
        return {
            "test_statistic": _round_or_none(row.get("test_statistic"), digits=6),
            "p_value": _round_or_none(row.get("p"), digits=6),
        }
    except (ValueError, TypeError, ZeroDivisionError):
        return {"test_statistic": None, "p_value": None}


def _fit_cox_model(df: pd.DataFrame, duration_col: str, event_col: str) -> CoxPHFitter | None:
    if df.empty or df[event_col].sum() <= 0:
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fitter = CoxPHFitter(penalizer=0.01)
            fitter.fit(df, duration_col=duration_col, event_col=event_col, show_progress=False)
            return fitter
    except (ConvergenceError, ValueError, ZeroDivisionError, np.linalg.LinAlgError):
        return None


def _assign_quartile(value: Any, profile: PaperExecutionProfile) -> str:
    number = _to_optional_float(value)
    bounds = profile.quartile_bounds or ()
    if number is None or len(bounds) != 3:
        return ""
    q1_max, q2_max, q3_max = bounds
    if number <= q1_max:
        return "Q1"
    if number <= q2_max:
        return "Q2"
    if number <= q3_max:
        return "Q3"
    return "Q4"


def _normalize_quartile(value: Any) -> str:
    text = str(value).strip().upper()
    if text in {"1", "2", "3", "4"}:
        return f"Q{text}"
    return text


def _normalize_gender(value: Any) -> str:
    text = str(value).strip().upper()
    if text in {"M", "MALE", "1"}:
        return "M"
    if text in {"F", "FEMALE", "0"}:
        return "F"
    return text


def _collapse_race_categories(value: Any) -> str:
    text = str(value).strip().upper()
    if not text:
        return "UNKNOWN"
    if text.startswith("WHITE"):
        return "WHITE"
    if text.startswith("BLACK"):
        return "BLACK"
    if "HISPANIC" in text or "LATINO" in text:
        return "HISPANIC_LATINO"
    if text.startswith("ASIAN"):
        return "ASIAN"
    return "OTHER"


def _looks_normal(series: pd.Series) -> bool:
    if len(series) < 3:
        return False
    try:
        sample = series.astype(float)
        if len(sample) > 5000:
            sample = sample.sample(5000, random_state=42)
        _, p_value = shapiro(sample)
        return bool(p_value > 0.05)
    except ValueError:
        return False


def _continuous_group_test(groups: list[pd.Series], assume_normal: bool) -> tuple[str, float | None]:
    valid_groups = [series.astype(float) for series in groups if len(series) > 0]
    if len(valid_groups) < 2:
        return "not_tested", None
    try:
        if assume_normal:
            _, p_value = f_oneway(*valid_groups)
            return "anova", float(p_value)
        _, p_value = kruskal(*valid_groups)
        return "kruskal_wallis", float(p_value)
    except ValueError:
        return "not_tested", None


def _format_mean_sd(series: pd.Series) -> str:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return ""
    return f"{values.mean():.2f} ± {values.std(ddof=1):.2f}"


def _format_median_iqr(series: pd.Series) -> str:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return ""
    q1 = values.quantile(0.25)
    q3 = values.quantile(0.75)
    return f"{values.median():.2f} ({q1:.2f}, {q3:.2f})"


def _format_count_pct(count: int, denom: int) -> str:
    if denom <= 0:
        return f"{count} (0.0%)"
    return f"{count} ({(count / denom) * 100.0:.1f}%)"


def _dataframe_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "| status |\n| --- |\n| empty |\n"

    headers = [str(column) for column in df.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in df.itertuples(index=False, name=None):
        cells: list[str] = []
        for value in row:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                text = ""
            else:
                text = str(value)
            text = text.replace("|", "\\|").replace("\n", "<br>")
            cells.append(text)
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def _format_event_text(events: Any, event_percent: Any) -> str:
    event_count = int(_to_optional_float(events) or 0)
    percent = _to_optional_float(event_percent)
    if percent is None:
        return str(event_count)
    return f"{event_count} ({percent:.1f})"


def _format_hr_ci_text(hr: Any, lower: Any, upper: Any) -> str:
    hr_value = _to_optional_float(hr)
    lower_value = _to_optional_float(lower)
    upper_value = _to_optional_float(upper)
    if hr_value is None or lower_value is None or upper_value is None:
        return "NA"
    return f"{hr_value:.2f} ({lower_value:.2f}-{upper_value:.2f})"


def _extract_model_lrt_p_value(fit: CoxPHFitter | None) -> float | None:
    if fit is None:
        return None
    try:
        return float(fit.log_likelihood_ratio_test().p_value)
    except (AttributeError, TypeError, ValueError):
        return None


def _likelihood_ratio_p_value(full_fit: CoxPHFitter | None, reduced_fit: CoxPHFitter | None) -> float | None:
    if full_fit is None or reduced_fit is None:
        return None
    try:
        stat = 2.0 * (float(full_fit.log_likelihood_) - float(reduced_fit.log_likelihood_))
        df_diff = int(len(full_fit.params_) - len(reduced_fit.params_))
        if stat < 0 or df_diff <= 0:
            return None
        return float(chi2.sf(stat, df_diff))
    except (AttributeError, TypeError, ValueError):
        return None


def _to_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _round_or_none(value: Any, digits: int) -> float | None:
    number = _to_optional_float(value)
    if number is None:
        return None
    return round(number, digits)


def _format_p_value(value: Any) -> str:
    number = _to_optional_float(value)
    if number is None:
        return "NA"
    if number < 0.001:
        return "<0.001"
    return f"{number:.3f}"


def _format_numeric(value: Any, digits: int = 2) -> str:
    number = _to_optional_float(value)
    if number is None:
        return "NA"
    return f"{number:.{digits}f}"

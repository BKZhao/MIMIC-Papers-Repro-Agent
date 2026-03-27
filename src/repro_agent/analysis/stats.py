from __future__ import annotations

import json
import math
import warnings
from dataclasses import dataclass
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
from lifelines.statistics import multivariate_logrank_test
from scipy.stats import chi2, chi2_contingency, f_oneway, kruskal, shapiro
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import IterativeImputer, SimpleImputer

from ..paper.contract import assign_paper_tyg_quartile, build_paper_alignment_contract

try:
    from patsy import build_design_matrices, dmatrix
except ImportError:  # pragma: no cover - optional dependency fallback
    build_design_matrices = None
    dmatrix = None


QUARTILES: tuple[str, ...] = ("Q1", "Q2", "Q3", "Q4")

COHORT_NUMERIC_COLUMNS: tuple[str, ...] = (
    "age",
    "sofa_score",
    "admit_to_icu_hours",
    "hospital_survival_hours",
    "icu_survival_hours",
    "icu_los_hours",
    "hospital_los_hours",
    "tyg_index",
    "hospital_mortality",
    "icu_mortality",
)

BASELINE_CONTINUOUS_COLUMNS: tuple[str, ...] = (
    "age",
    "sofa_score",
    "admit_to_icu_hours",
    "icu_los_hours",
    "hospital_los_hours",
    "tyg_index",
)

BASELINE_CATEGORICAL_COLUMNS: tuple[str, ...] = ("sex",)

CATEGORICAL_MODEL_COLUMNS: tuple[str, ...] = ("gender", "race", "insurance", "marital_status")
BINARY_MODEL_COLUMNS: tuple[str, ...] = (
    "hypertension",
    "type2_diabetes",
    "heart_failure",
    "myocardial_infarction",
    "malignant_tumor",
    "chronic_renal_failure",
    "acute_renal_failure",
    "cirrhosis",
    "hepatitis",
    "tuberculosis",
    "pneumonia",
    "copd",
    "hyperlipidemia",
    "stroke",
    "continuous_renal_replacement_therapy",
    "mechanical_ventilation",
    "in_hospital_mortality",
    "icu_mortality",
    "sepsis3_flag",
)
MODEL2_COVARIATES: tuple[str, ...] = ("age", "gender", "height_cm", "weight_kg", "race")
MODEL3_COVARIATES: tuple[str, ...] = (
    *MODEL2_COVARIATES,
    "insurance",
    "marital_status",
    "white_blood_cell_count",
    "red_blood_cell_count",
    "hemoglobin_count",
    "rdw",
    "albumin",
    "chloride",
    "alanine_aminotransferase",
    "aspartate_aminotransferase",
    "sofa_score",
    "apache_iii_score",
    "saps_ii_score",
    "oasis_score",
    "charlson_score",
    "gcs_score",
    "hypertension",
    "type2_diabetes",
    "heart_failure",
    "myocardial_infarction",
    "malignant_tumor",
    "chronic_renal_failure",
    "acute_renal_failure",
    "stroke",
    "hyperlipidemia",
    "copd",
)
WIDE_BASELINE_CONTINUOUS_COLUMNS: tuple[str, ...] = (
    "age",
    "height_cm",
    "weight_kg",
    "bmi",
    "white_blood_cell_count",
    "red_blood_cell_count",
    "hemoglobin_count",
    "rdw",
    "albumin",
    "chloride",
    "alanine_aminotransferase",
    "aspartate_aminotransferase",
    "sofa_score",
    "apache_iii_score",
    "saps_ii_score",
    "oasis_score",
    "charlson_score",
    "gcs_score",
    "blood_glucose",
    "hba1c",
    "triglycerides",
    "tyg_index",
)
WIDE_BASELINE_CATEGORICAL_COLUMNS: tuple[str, ...] = (
    "gender",
    "race",
    "insurance",
    "marital_status",
    "hypertension",
    "type2_diabetes",
    "heart_failure",
    "myocardial_infarction",
    "malignant_tumor",
    "chronic_renal_failure",
    "acute_renal_failure",
    "stroke",
    "hyperlipidemia",
    "copd",
    "continuous_renal_replacement_therapy",
    "mechanical_ventilation",
)
MODELING_REQUIRED_COLUMNS: tuple[str, ...] = (
    "subject_id",
    "hadm_id",
    "stay_id",
    "gender",
    "race",
    "insurance",
    "marital_status",
    "age",
    "height_cm",
    "weight_kg",
    "bmi",
    "white_blood_cell_count",
    "red_blood_cell_count",
    "hemoglobin_count",
    "rdw",
    "albumin",
    "chloride",
    "alanine_aminotransferase",
    "aspartate_aminotransferase",
    "sofa_score",
    "apache_iii_score",
    "saps_ii_score",
    "oasis_score",
    "charlson_score",
    "gcs_score",
    "hypertension",
    "type2_diabetes",
    "heart_failure",
    "myocardial_infarction",
    "malignant_tumor",
    "chronic_renal_failure",
    "acute_renal_failure",
    "stroke",
    "hyperlipidemia",
    "copd",
    "continuous_renal_replacement_therapy",
    "mechanical_ventilation",
    "hospital_survival_hours",
    "icu_survival_hours",
    "hospital_los_hours",
    "icu_los_hours",
    "in_hospital_mortality",
    "icu_mortality",
    "tyg_index",
    "tyg_quartile",
)
NON_MODEL_IMPUTE_COLUMNS: tuple[str, ...] = (
    "subject_id",
    "hadm_id",
    "stay_id",
    "hospital_survival_hours",
    "icu_survival_hours",
    "hospital_los_hours",
    "icu_los_hours",
    "in_hospital_mortality",
    "icu_mortality",
    "tyg_index",
    "tyg_quartile",
)

FIGURE2_MAX_DAYS = 36
FIGURE2_XTICKS_DAYS: tuple[int, ...] = (0, 6, 12, 18, 24, 30, 36)

BASELINE_PERCENT_TARGETS: dict[str, tuple[str, Any]] = {
    "gender_male_pct": ("gender", "M"),
    "type2_diabetes_pct": ("type2_diabetes", 1),
    "hypertension_pct": ("hypertension", 1),
    "heart_failure_pct": ("heart_failure", 1),
    "myocardial_infarction_pct": ("myocardial_infarction", 1),
    "malignant_tumor_pct": ("malignant_tumor", 1),
    "chronic_renal_failure_pct": ("chronic_renal_failure", 1),
    "acute_renal_failure_pct": ("acute_renal_failure", 1),
    "cirrhosis_pct": ("cirrhosis", 1),
    "hepatitis_pct": ("hepatitis", 1),
    "tuberculosis_pct": ("tuberculosis", 1),
    "pneumonia_pct": ("pneumonia", 1),
    "stroke_pct": ("stroke", 1),
    "hyperlipidemia_pct": ("hyperlipidemia", 1),
    "copd_pct": ("copd", 1),
    "continuous_renal_replacement_therapy_pct": ("continuous_renal_replacement_therapy", 1),
}


@dataclass(frozen=True)
class StatsRunResult:
    outputs: list[str]
    metrics: dict[str, float | None]
    analysis_mode: str
    cohort_n: int


def run_stats_analysis(
    project_root: Path,
    cohort_rel: str,
    targets: list[dict[str, Any]],
    analysis_dataset_rel: str = "shared/analysis_dataset.csv",
    missingness_rel: str = "shared/analysis_missingness.json",
    contract_rel: str = "shared/paper_alignment_contract.json",
) -> StatsRunResult:
    analysis_dataset_path = project_root / analysis_dataset_rel
    missingness_path = project_root / missingness_rel
    contract_path = project_root / contract_rel
    contract = build_paper_alignment_contract()
    if contract_path.exists():
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    if analysis_dataset_path.exists() and missingness_path.exists():
        return _run_wide_stats_analysis(
            project_root=project_root,
            analysis_dataset_path=analysis_dataset_path,
            missingness_path=missingness_path,
            contract=contract,
            targets=targets,
        )
    return run_cohort_stats_analysis(project_root=project_root, cohort_rel=cohort_rel, targets=targets)


def run_cohort_stats_analysis(
    project_root: Path,
    cohort_rel: str,
    targets: list[dict[str, Any]],
) -> StatsRunResult:
    cohort_path = project_root / cohort_rel
    if not cohort_path.exists():
        raise FileNotFoundError(f"Cohort file not found: {cohort_path}")

    df = _load_cohort_frame(cohort_path)
    contract = build_paper_alignment_contract(project_root=project_root)
    shared_dir = project_root / "shared"
    results_dir = project_root / "results"
    shared_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    baseline_path = shared_dir / "baseline_table.csv"
    cox_path = shared_dir / "cox_models.csv"
    km_summary_path = shared_dir / "km_summary.json"
    rcs_curve_path = shared_dir / "rcs_curve.csv"
    stats_summary_path = shared_dir / "stats_summary.json"
    results_table_path = shared_dir / "results_table.csv"
    km_hospital_panel_path = results_dir / ".km_hospital_tyg_panel.png"
    km_icu_panel_path = results_dir / ".km_icu_tyg_panel.png"
    km_figure2_path = results_dir / "km_tyg_figure2.png"
    legacy_km_paths = (
        results_dir / "km_hospital_tyg.png",
        results_dir / "km_icu_tyg.png",
    )
    render_km_figure = _should_render_paper_figure(contract, "figure2")
    _remove_paths_if_exist(km_hospital_panel_path, km_icu_panel_path, *legacy_km_paths)
    if not render_km_figure:
        _remove_paths_if_exist(km_figure2_path)

    baseline_df = _build_baseline_table(df)
    baseline_df.to_csv(baseline_path, index=False)

    hospital_duration_col = _resolve_duration_column(df, "hospital_survival_hours", "hospital_los_hours")
    icu_duration_col = _resolve_duration_column(df, "icu_survival_hours", "icu_los_hours")

    km_hospital = _fit_km_by_quartile(
        df=df,
        duration_col=hospital_duration_col,
        event_col="hospital_mortality",
        title="A. Cumulative survival during hospitalization",
        output_path=km_hospital_panel_path,
        endpoint="hospital",
        contract=contract,
    )
    km_icu = _fit_km_by_quartile(
        df=df,
        duration_col=icu_duration_col,
        event_col="icu_mortality",
        title="B. Cumulative survival during ICU stay",
        output_path=km_icu_panel_path,
        endpoint="icu",
        contract=contract,
    )
    km_artifacts: list[str] = []
    if render_km_figure and _compose_km_figure_from_panel_images(
        [km_hospital_panel_path, km_icu_panel_path],
        output_path=km_figure2_path,
        contract=contract,
    ):
        km_artifacts.append("results/km_tyg_figure2.png")
    _remove_paths_if_exist(km_hospital_panel_path, km_icu_panel_path)

    cox_rows: list[dict[str, Any]] = []
    metrics: dict[str, float | None] = {
        "in_hospital_q4_m1_hr": None,
        "icu_q4_m1_hr": None,
        "icu_q4_m3_hr": None,
        "rcs_inflection": None,
    }

    hospital_m1 = _fit_unadjusted_quartile_cox(df, hospital_duration_col, "hospital_mortality", endpoint="hospital")
    cox_rows.extend(hospital_m1["rows"])
    metrics["in_hospital_q4_m1_hr"] = hospital_m1["q4_hr"]

    icu_m1 = _fit_unadjusted_quartile_cox(df, icu_duration_col, "icu_mortality", endpoint="icu")
    cox_rows.extend(icu_m1["rows"])
    metrics["icu_q4_m1_hr"] = icu_m1["q4_hr"]

    hospital_cont = _fit_unadjusted_continuous_cox(df, hospital_duration_col, "hospital_mortality", endpoint="hospital")
    cox_rows.extend(hospital_cont["rows"])
    icu_cont = _fit_unadjusted_continuous_cox(df, icu_duration_col, "icu_mortality", endpoint="icu")
    cox_rows.extend(icu_cont["rows"])

    cox_df = pd.DataFrame(cox_rows)
    if not cox_df.empty:
        cox_df.to_csv(cox_path, index=False)
    else:
        pd.DataFrame(
            columns=[
                "endpoint",
                "model",
                "term",
                "hazard_ratio",
                "ci_lower_95",
                "ci_upper_95",
                "p_value",
                "n",
                "events",
            ]
        ).to_csv(cox_path, index=False)

    rcs_frames: list[pd.DataFrame] = []
    hospital_rcs = _fit_rcs_curve(df, hospital_duration_col, "hospital_mortality", endpoint="hospital")
    if not hospital_rcs["curve"].empty:
        rcs_frames.append(hospital_rcs["curve"])

    icu_rcs = _fit_rcs_curve(df, icu_duration_col, "icu_mortality", endpoint="icu")
    if not icu_rcs["curve"].empty:
        rcs_frames.append(icu_rcs["curve"])

    inflection_candidates = [value for value in (hospital_rcs["inflection"], icu_rcs["inflection"]) if value is not None]
    if inflection_candidates:
        metrics["rcs_inflection"] = float(np.mean(inflection_candidates))

    if rcs_frames:
        pd.concat(rcs_frames, ignore_index=True).to_csv(rcs_curve_path, index=False)
    else:
        pd.DataFrame(columns=["endpoint", "tyg_index", "hazard_ratio", "reference_tyg_index"]).to_csv(
            rcs_curve_path,
            index=False,
        )

    km_payload = {
        "analysis_mode": "cohort_only",
        "hospital": km_hospital,
        "icu": km_icu,
    }
    km_summary_path.write_text(json.dumps(km_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    stats_summary = {
        "analysis_mode": "cohort_only",
        "cohort_n": int(len(df)),
        "metrics": {key: _round_or_none(value, digits=6) for key, value in metrics.items()},
        "logrank_p_values": {
            "hospital": _round_or_none(km_hospital.get("logrank_p_value"), digits=6),
            "icu": _round_or_none(km_icu.get("logrank_p_value"), digits=6),
        },
        "survival_duration_columns": {
            "hospital": hospital_duration_col,
            "icu": icu_duration_col,
        },
        "artifacts": [
            "shared/baseline_table.csv",
            "shared/cox_models.csv",
            "shared/km_summary.json",
            "shared/rcs_curve.csv",
            *km_artifacts,
            "shared/results_table.csv",
        ],
        "notes": [
            "Current stats stage uses shared/cohort.csv only.",
            "Model 2/3 adjusted analyses still require the wide analysis dataset.",
        ],
    }
    stats_summary_path.write_text(json.dumps(stats_summary, indent=2, ensure_ascii=False), encoding="utf-8")

    results_rows = _build_results_rows(targets=targets, metrics=metrics)
    pd.DataFrame(results_rows).to_csv(results_table_path, index=False)

    return StatsRunResult(
        outputs=[
            "shared/baseline_table.csv",
            "shared/cox_models.csv",
            "shared/km_summary.json",
            "shared/rcs_curve.csv",
            "shared/stats_summary.json",
            *km_artifacts,
            "shared/results_table.csv",
        ],
        metrics=metrics,
        analysis_mode="cohort_only",
        cohort_n=int(len(df)),
    )


def _run_wide_stats_analysis(
    project_root: Path,
    analysis_dataset_path: Path,
    missingness_path: Path,
    contract: dict[str, Any],
    targets: list[dict[str, Any]],
) -> StatsRunResult:
    df = _load_analysis_frame(analysis_dataset_path)
    missingness_payload = json.loads(missingness_path.read_text(encoding="utf-8"))

    shared_dir = project_root / "shared"
    results_dir = project_root / "results"
    shared_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    baseline_path = shared_dir / "baseline_table.csv"
    cox_path = shared_dir / "cox_models.csv"
    km_summary_path = shared_dir / "km_summary.json"
    rcs_curve_path = shared_dir / "rcs_curve.csv"
    rcs_summary_path = shared_dir / "rcs_summary.json"
    subgroup_path = shared_dir / "subgroup_analysis.csv"
    model_ready_path = shared_dir / "analysis_dataset_model_ready.csv"
    manifest_path = shared_dir / "modeling_manifest.json"
    diagnostics_path = shared_dir / "paper_alignment_diagnostics.json"
    stats_summary_path = shared_dir / "stats_summary.json"
    results_table_path = shared_dir / "results_table.csv"
    km_hospital_panel_path = results_dir / ".km_hospital_tyg_panel.png"
    km_icu_panel_path = results_dir / ".km_icu_tyg_panel.png"
    km_figure2_path = results_dir / "km_tyg_figure2.png"
    legacy_km_paths = (
        results_dir / "km_hospital_tyg.png",
        results_dir / "km_icu_tyg.png",
    )
    rcs_plot_path = results_dir / "rcs_tyg_mortality.png"
    render_km_figure = _should_render_paper_figure(contract, "figure2")
    render_rcs_figure = _should_render_paper_figure(contract, "figure3")
    _remove_paths_if_exist(km_hospital_panel_path, km_icu_panel_path, *legacy_km_paths)
    if not render_km_figure:
        _remove_paths_if_exist(km_figure2_path)
    if not render_rcs_figure:
        _remove_paths_if_exist(rcs_plot_path)

    modeling_df, modeling_manifest = _prepare_modeling_frame(df, missingness_payload)
    imputed_df, imputation_manifest = _impute_modeling_frame(modeling_df)
    model_ready_path.write_text(imputed_df.to_csv(index=False), encoding="utf-8")
    for frame in (modeling_df, imputed_df):
        if "tyg_index" in frame.columns:
            frame["tyg_quartile"] = frame["tyg_index"].map(assign_paper_tyg_quartile)
            frame["tyg_quartile"] = pd.Categorical(frame["tyg_quartile"], categories=QUARTILES, ordered=True)

    manifest = {
        **modeling_manifest,
        **imputation_manifest,
        "analysis_mode": "paper_aligned",
        "row_count": int(len(imputed_df)),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    baseline_df = _build_baseline_table_for_columns(
        df=df,
        continuous_columns=[column for column in WIDE_BASELINE_CONTINUOUS_COLUMNS if column in df.columns],
        categorical_columns=[column for column in WIDE_BASELINE_CATEGORICAL_COLUMNS if column in df.columns],
    )
    baseline_df.to_csv(baseline_path, index=False)

    hospital_duration_col = _resolve_duration_column(imputed_df, "hospital_survival_hours", "hospital_los_hours")
    icu_duration_col = _resolve_duration_column(imputed_df, "icu_survival_hours", "icu_los_hours")

    km_hospital = _fit_km_by_quartile(
        df=imputed_df,
        duration_col=hospital_duration_col,
        event_col="in_hospital_mortality",
        title="A. Cumulative survival during hospitalization",
        output_path=km_hospital_panel_path,
        endpoint="hospital",
        contract=contract,
    )
    km_icu = _fit_km_by_quartile(
        df=imputed_df,
        duration_col=icu_duration_col,
        event_col="icu_mortality",
        title="B. Cumulative survival during ICU stay",
        output_path=km_icu_panel_path,
        endpoint="icu",
        contract=contract,
    )
    km_artifacts: list[str] = []
    if render_km_figure and _compose_km_figure_from_panel_images(
        [km_hospital_panel_path, km_icu_panel_path],
        output_path=km_figure2_path,
        contract=contract,
    ):
        km_artifacts.append("results/km_tyg_figure2.png")
    _remove_paths_if_exist(km_hospital_panel_path, km_icu_panel_path)

    available_model2 = [column for column in MODEL2_COVARIATES if column in imputed_df.columns]
    available_model3 = [column for column in MODEL3_COVARIATES if column in imputed_df.columns]

    cox_rows: list[dict[str, Any]] = []
    metrics: dict[str, float | None] = {
        "in_hospital_q4_m1_hr": None,
        "icu_q4_m1_hr": None,
        "icu_q4_m3_hr": None,
        "rcs_inflection": None,
    }

    endpoints = (
        ("hospital", hospital_duration_col, "in_hospital_mortality"),
        ("icu", icu_duration_col, "icu_mortality"),
    )
    rcs_results: list[dict[str, Any]] = []
    adjusted_inflections: list[float] = []

    for endpoint, duration_col, event_col in endpoints:
        m1_quartile = _fit_unadjusted_quartile_cox(imputed_df, duration_col, event_col, endpoint=endpoint)
        cox_rows.extend(m1_quartile["rows"])
        if endpoint == "hospital":
            metrics["in_hospital_q4_m1_hr"] = m1_quartile["q4_hr"]
        elif endpoint == "icu":
            metrics["icu_q4_m1_hr"] = m1_quartile["q4_hr"]

        cox_rows.extend(
            _fit_unadjusted_continuous_cox(imputed_df, duration_col, event_col, endpoint=endpoint).get("rows", [])
        )

        m2_quartile = _fit_adjusted_quartile_cox(
            df=imputed_df,
            duration_col=duration_col,
            event_col=event_col,
            endpoint=endpoint,
            model_label="cox_m2_quartile_adjusted",
            covariates=available_model2,
        )
        cox_rows.extend(m2_quartile["rows"])

        cox_rows.extend(
            _fit_adjusted_continuous_cox(
                df=imputed_df,
                duration_col=duration_col,
                event_col=event_col,
                endpoint=endpoint,
                model_label="cox_m2_continuous_adjusted",
                covariates=available_model2,
            )["rows"]
        )

        m3_quartile = _fit_adjusted_quartile_cox(
            df=imputed_df,
            duration_col=duration_col,
            event_col=event_col,
            endpoint=endpoint,
            model_label="cox_m3_quartile_adjusted",
            covariates=available_model3,
        )
        cox_rows.extend(m3_quartile["rows"])
        if endpoint == "icu":
            metrics["icu_q4_m3_hr"] = m3_quartile["q4_hr"]

        cox_rows.extend(
            _fit_adjusted_continuous_cox(
                df=imputed_df,
                duration_col=duration_col,
                event_col=event_col,
                endpoint=endpoint,
                model_label="cox_m3_continuous_adjusted",
                covariates=available_model3,
            )["rows"]
        )

        unadjusted_rcs = _fit_rcs_model(
            df=imputed_df,
            duration_col=duration_col,
            event_col=event_col,
            endpoint=endpoint,
            covariates=[],
            model_label="cox_m1_rcs_unadjusted",
        )
        if not unadjusted_rcs["curve"].empty:
            rcs_results.append(unadjusted_rcs)

        adjusted_rcs = _fit_rcs_model(
            df=imputed_df,
            duration_col=duration_col,
            event_col=event_col,
            endpoint=endpoint,
            covariates=available_model3,
            model_label="cox_m3_rcs_adjusted",
        )
        if not adjusted_rcs["curve"].empty:
            rcs_results.append(adjusted_rcs)
        if adjusted_rcs["inflection"] is not None:
            adjusted_inflections.append(adjusted_rcs["inflection"])

    if adjusted_inflections:
        metrics["rcs_inflection"] = float(np.mean(adjusted_inflections))

    cox_df = pd.DataFrame(cox_rows)
    if not cox_df.empty:
        cox_df.to_csv(cox_path, index=False)
    else:
        pd.DataFrame(
            columns=[
                "endpoint",
                "model",
                "term",
                "hazard_ratio",
                "ci_lower_95",
                "ci_upper_95",
                "p_value",
                "n",
                "events",
            ]
        ).to_csv(cox_path, index=False)

    rcs_frames = [result["curve"] for result in rcs_results if not result["curve"].empty]
    if rcs_frames:
        pd.concat(rcs_frames, ignore_index=True).to_csv(rcs_curve_path, index=False)
    else:
        pd.DataFrame(
            columns=[
                "endpoint",
                "model",
                "tyg_index",
                "hazard_ratio",
                "ci_lower_95",
                "ci_upper_95",
                "reference_tyg_index",
                "overall_p_value",
                "nonlinearity_p_value",
            ]
        ).to_csv(
            rcs_curve_path,
            index=False,
        )

    subgroup_df = _run_subgroup_analyses(imputed_df, available_model3)
    subgroup_df.to_csv(subgroup_path, index=False)

    rcs_summary = _build_rcs_summary_payload(rcs_results, contract)
    rcs_summary_path.write_text(json.dumps(rcs_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    if render_rcs_figure:
        _plot_rcs_panels(rcs_results=rcs_results, output_path=rcs_plot_path, contract=contract)

    km_payload = {
        "analysis_mode": "paper_aligned",
        "hospital": km_hospital,
        "icu": km_icu,
        "figure_target": dict(contract.get("figure_targets", {})).get("figure2", {}),
    }
    km_summary_path.write_text(json.dumps(km_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    alignment_diagnostics = _build_paper_alignment_diagnostics(
        project_root=project_root,
        contract=contract,
        cohort_n=int(len(imputed_df)),
        baseline_df=df,
        metrics=metrics,
        km_results={"hospital": km_hospital, "icu": km_icu},
        rcs_results=rcs_results,
    )
    diagnostics_path.write_text(json.dumps(alignment_diagnostics, indent=2, ensure_ascii=False), encoding="utf-8")

    stats_summary = {
        "analysis_mode": "paper_aligned",
        "cohort_n": int(len(imputed_df)),
        "metrics": {key: _round_or_none(value, digits=6) for key, value in metrics.items()},
        "logrank_p_values": {
            "hospital": _round_or_none(km_hospital.get("logrank_p_value"), digits=6),
            "icu": _round_or_none(km_icu.get("logrank_p_value"), digits=6),
        },
        "survival_duration_columns": {
            "hospital": hospital_duration_col,
            "icu": icu_duration_col,
        },
        "km_target_p_values": contract.get("km_targets", {}),
        "rcs_panel_summary": rcs_summary.get("panels", []),
        "model2_covariates": available_model2,
        "model3_covariates": available_model3,
        "dropped_above_50_missing": modeling_manifest["dropped_columns_above_50_missing"],
        "retained_required_above_50_missing": modeling_manifest["retained_required_above_50_missing"],
        "alignment_summary": alignment_diagnostics.get("summary", {}),
        "artifacts": [
            "shared/analysis_dataset_model_ready.csv",
            "shared/modeling_manifest.json",
            "shared/baseline_table.csv",
            "shared/cox_models.csv",
            "shared/subgroup_analysis.csv",
            "shared/km_summary.json",
            "shared/rcs_curve.csv",
            "shared/rcs_summary.json",
            "shared/paper_alignment_diagnostics.json",
            "shared/stats_summary.json",
            *km_artifacts,
            *(["results/rcs_tyg_mortality.png"] if render_rcs_figure else []),
            "shared/results_table.csv",
        ],
    }
    stats_summary_path.write_text(json.dumps(stats_summary, indent=2, ensure_ascii=False), encoding="utf-8")

    results_rows = _build_results_rows(targets=targets, metrics=metrics)
    pd.DataFrame(results_rows).to_csv(results_table_path, index=False)

    return StatsRunResult(
        outputs=[
            "shared/analysis_dataset_model_ready.csv",
            "shared/modeling_manifest.json",
            "shared/baseline_table.csv",
            "shared/cox_models.csv",
            "shared/subgroup_analysis.csv",
            "shared/km_summary.json",
            "shared/rcs_curve.csv",
            "shared/rcs_summary.json",
            "shared/paper_alignment_diagnostics.json",
            "shared/stats_summary.json",
            *km_artifacts,
            *(["results/rcs_tyg_mortality.png"] if render_rcs_figure else []),
            "shared/results_table.csv",
        ],
        metrics=metrics,
        analysis_mode="paper_aligned",
        cohort_n=int(len(imputed_df)),
    )


def _load_analysis_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    string_columns = {
        "gender",
        "race",
        "insurance",
        "marital_status",
        "tyg_quartile",
        "suspected_infection_time",
        "sofa_time",
    }
    for column in df.columns:
        if column not in string_columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in ("insurance", "marital_status"):
        if column in df.columns:
            df[column] = df[column].fillna("Unknown").astype(str).str.strip()

    if "gender" in df.columns:
        df["gender"] = df["gender"].map(_normalize_gender)

    if "race" in df.columns:
        df["race"] = df["race"].fillna("Unknown").astype(str).map(_collapse_race_categories)

    if "tyg_index" in df.columns:
        df["tyg_quartile"] = df["tyg_index"].map(assign_paper_tyg_quartile)
    else:
        df["tyg_quartile"] = df["tyg_quartile"].map(_normalize_quartile)
    df = df[df["tyg_quartile"].isin(QUARTILES)].copy()
    df["tyg_quartile"] = pd.Categorical(df["tyg_quartile"], categories=QUARTILES, ordered=True)

    for column in BINARY_MODEL_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _collapse_race_categories(value: str) -> str:
    text = value.strip().upper()
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


def _prepare_modeling_frame(
    df: pd.DataFrame,
    missingness_payload: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    missingness = missingness_payload.get("missingness", {})
    above_50_missing = set(missingness.get("columns_above_50_percent_missing", []))
    required_columns = {column for column in MODELING_REQUIRED_COLUMNS if column in df.columns}
    retained_required_above_50 = sorted(column for column in above_50_missing if column in required_columns)

    modeling_columns = [column for column in MODELING_REQUIRED_COLUMNS if column in df.columns]
    modeling_df = df[modeling_columns].copy()

    dropped_columns = sorted(
        column for column in above_50_missing if column in df.columns and column not in modeling_df.columns
    )
    manifest = {
        "missing_threshold_used_for_optional_features": 0.50,
        "reported_columns_above_30_missing": missingness.get("columns_above_30_percent_missing", []),
        "reported_columns_above_50_missing": missingness.get("columns_above_50_percent_missing", []),
        "dropped_columns_above_50_missing": dropped_columns,
        "retained_required_above_50_missing": retained_required_above_50,
    }
    return modeling_df, manifest


def _impute_modeling_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = df.copy()

    categorical_cols = [column for column in CATEGORICAL_MODEL_COLUMNS if column in work.columns]
    binary_cols = [column for column in BINARY_MODEL_COLUMNS if column in work.columns]
    protected_cols = {column for column in NON_MODEL_IMPUTE_COLUMNS if column in work.columns}
    numeric_cols = [
        column
        for column in work.columns
        if column not in protected_cols
        and column not in categorical_cols
        and column not in binary_cols
        and pd.api.types.is_numeric_dtype(work[column])
    ]

    categorical_imputed: list[str] = []
    if categorical_cols:
        cat_imputer = SimpleImputer(strategy="most_frequent")
        work[categorical_cols] = cat_imputer.fit_transform(work[categorical_cols])
        categorical_imputed = [column for column in categorical_cols if df[column].isna().any()]

    binary_imputed: list[str] = []
    if binary_cols:
        bin_imputer = SimpleImputer(strategy="most_frequent")
        work[binary_cols] = bin_imputer.fit_transform(work[binary_cols])
        for column in binary_cols:
            work[column] = pd.to_numeric(work[column], errors="coerce").round().clip(lower=0, upper=1)
        binary_imputed = [column for column in binary_cols if df[column].isna().any()]

    numeric_impute_cols = [column for column in numeric_cols if work[column].isna().any()]
    if numeric_impute_cols:
        estimator = RandomForestRegressor(
            n_estimators=50,
            max_depth=8,
            min_samples_leaf=5,
            n_jobs=-1,
            random_state=42,
        )
        numeric_imputer = IterativeImputer(
            estimator=estimator,
            max_iter=5,
            random_state=42,
            initial_strategy="median",
            skip_complete=True,
        )
        work[numeric_impute_cols] = numeric_imputer.fit_transform(work[numeric_impute_cols])

    manifest = {
        "categorical_imputed_columns": categorical_imputed,
        "binary_imputed_columns": binary_imputed,
        "numeric_imputed_columns": numeric_impute_cols,
    }
    return work, manifest


def _load_cohort_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    for column in COHORT_NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "sex" in df.columns:
        df["sex"] = df["sex"].map(_normalize_gender)

    if "tyg_index" in df.columns:
        df["tyg_quartile"] = df["tyg_index"].map(assign_paper_tyg_quartile)
    else:
        df["tyg_quartile"] = df["tyg_quartile"].map(_normalize_quartile)
    df = df[df["tyg_quartile"].isin(QUARTILES)].copy()
    df["tyg_quartile"] = pd.Categorical(df["tyg_quartile"], categories=QUARTILES, ordered=True)
    return df


def _normalize_quartile(value: Any) -> str:
    text = str(value).strip().upper()
    if text in {"1", "2", "3", "4"}:
        return f"Q{text}"
    return text


def _normalize_gender(value: Any) -> str:
    text = str(value).strip().upper()
    if text in {"M", "MALE"}:
        return "M"
    if text in {"F", "FEMALE"}:
        return "F"
    return "UNKNOWN"


def _build_baseline_table(df: pd.DataFrame) -> pd.DataFrame:
    return _build_baseline_table_for_columns(
        df=df,
        continuous_columns=[column for column in BASELINE_CONTINUOUS_COLUMNS if column in df.columns],
        categorical_columns=[column for column in BASELINE_CATEGORICAL_COLUMNS if column in df.columns],
    )


def _build_baseline_table_for_columns(
    df: pd.DataFrame,
    continuous_columns: list[str],
    categorical_columns: list[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for column in continuous_columns:
        groups = [df.loc[df["tyg_quartile"] == quartile, column].dropna() for quartile in QUARTILES]
        overall = df[column].dropna()
        non_empty_groups = [series for series in groups if len(series) > 0]
        if not non_empty_groups:
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

    for column in categorical_columns:
        series = df[column].fillna("Missing").astype(str)
        contingency = pd.crosstab(series, df["tyg_quartile"])
        if contingency.empty:
            continue
        p_value = math.nan
        if contingency.shape[0] > 1 and contingency.shape[1] > 1:
            try:
                _, p_value, _, _ = chi2_contingency(contingency)
            except ValueError:
                p_value = math.nan
        levels = contingency.index.tolist()
        if set(levels).issubset({"0", "1"}) and "1" in levels:
            levels = ["1"]
        for level in levels:
            row = {
                "variable": column,
                "level": level,
                "variable_type": "categorical",
                "test": "chi_square",
                "p_value": _round_or_none(p_value, digits=6),
                "overall": _format_count_pct(int((series == level).sum()), int(series.notna().sum())),
            }
            for quartile in QUARTILES:
                denom = int((df["tyg_quartile"] == quartile).sum())
                count = int(contingency.loc[level, quartile]) if quartile in contingency.columns else 0
                row[quartile] = _format_count_pct(count, denom)
            rows.append(row)

    ordered_columns = ["variable", "level", "variable_type", "test", "p_value", "overall", *QUARTILES]
    return pd.DataFrame(rows, columns=ordered_columns)


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
    if series.empty:
        return ""
    values = series.astype(float)
    return f"{values.mean():.2f} ± {values.std(ddof=1):.2f}"


def _format_median_iqr(series: pd.Series) -> str:
    if series.empty:
        return ""
    values = series.astype(float)
    q1 = values.quantile(0.25)
    q3 = values.quantile(0.75)
    return f"{values.median():.2f} ({q1:.2f}, {q3:.2f})"


def _format_count_pct(count: int, denom: int) -> str:
    if denom <= 0:
        return f"{count} (0.0%)"
    return f"{count} ({(count / denom) * 100.0:.1f}%)"


def _fit_km_by_quartile(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    title: str,
    output_path: Path,
    endpoint: str | None = None,
    contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    work = df[[duration_col, event_col, "tyg_quartile"]].dropna().copy()
    work = work[work[duration_col] > 0].copy()
    if work.empty:
        return {"logrank_p_value": None, "groups": {}, "plot": str(output_path)}

    work["duration_days"] = pd.to_numeric(work[duration_col], errors="coerce") / 24.0
    work = work[work["duration_days"] > 0].copy()
    if work.empty:
        return {"logrank_p_value": None, "groups": {}, "plot": str(output_path)}

    figure2_target = dict(dict(contract or {}).get("figure_targets", {})).get("figure2", {})
    max_days = int(figure2_target.get("x_axis_max_days", FIGURE2_MAX_DAYS) or FIGURE2_MAX_DAYS)
    xticks = [tick for tick in FIGURE2_XTICKS_DAYS if tick <= max_days]

    fig, ax = plt.subplots(figsize=(8.5, 7.0))
    palette = {"Q1": "#1b4d6b", "Q2": "#3b8ea5", "Q3": "#c97c1a", "Q4": "#8c2d19"}
    fitters: list[KaplanMeierFitter] = []
    group_payload: dict[str, dict[str, Any]] = {}
    for quartile in QUARTILES:
        subset = work.loc[work["tyg_quartile"] == quartile]
        if subset.empty:
            continue
        kmf = KaplanMeierFitter()
        kmf.fit(subset["duration_days"], event_observed=subset[event_col], label=quartile)
        kmf.plot_survival_function(ax=ax, ci_show=False, linewidth=2.0, color=palette.get(quartile))
        fitters.append(kmf)
        group_payload[quartile] = {
            "n": int(len(subset)),
            "events": int(subset[event_col].sum()),
            "median_time_days": _round_or_none(kmf.median_survival_time_, digits=4),
            "number_at_risk": {str(tick): int((subset["duration_days"] >= tick).sum()) for tick in xticks},
        }

    try:
        logrank = multivariate_logrank_test(
            event_durations=work["duration_days"],
            groups=work["tyg_quartile"],
            event_observed=work[event_col],
        )
        p_value = float(logrank.p_value)
    except ValueError:
        p_value = None

    ax.set_title(title)
    ax.set_xlabel("Time (days)")
    ax.set_ylabel("Cumulative survival probability")
    ax.set_xlim(0, max_days)
    ax.set_xticks(xticks)
    ax.set_ylim(0.0, 1.02)
    ax.grid(axis="y", alpha=0.2)
    ax.legend(title="TyG quartile", loc="lower left")
    if figure2_target.get("show_logrank_p_value", True):
        ax.text(
            0.98,
            0.97,
            f"Log-rank P = {_format_p_value(p_value)}",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=10,
            bbox={"facecolor": "white", "edgecolor": "#dddddd", "alpha": 0.9, "pad": 4},
        )
    if fitters:
        add_at_risk_counts(*fitters, ax=ax, xticks=xticks, rows_to_show=["At risk"])
    fig.subplots_adjust(left=0.11, right=0.97, top=0.9, bottom=0.28)
    fig.savefig(output_path, dpi=160)
    plt.close(fig)

    return {
        "logrank_p_value": _round_or_none(p_value, digits=6),
        "groups": group_payload,
        "plot": str(output_path),
        "display_max_days": max_days,
        "display_xticks_days": xticks,
        "endpoint": endpoint,
    }


def _should_render_paper_figure(contract: dict[str, Any] | None, figure_key: str) -> bool:
    figure_targets = dict(dict(contract or {}).get("figure_targets", {}))
    if not figure_targets:
        return True
    return figure_key in figure_targets


def _compose_km_figure_from_panel_images(
    panel_paths: list[Path],
    output_path: Path,
    contract: dict[str, Any] | None = None,
) -> bool:
    existing_paths = [path for path in panel_paths if path.exists()]
    if not existing_paths:
        return False

    figure2_target = dict(dict(contract or {}).get("figure_targets", {})).get("figure2", {})
    panel_specs = list(figure2_target.get("panels", []))
    x_axis_unit = str(figure2_target.get("x_axis_unit", "days")).strip()

    panel_count = len(existing_paths)
    fig, axes = plt.subplots(1, panel_count, figsize=(7.2 * panel_count, 7.6))
    axes_array = np.atleast_1d(axes)

    for index, (ax, panel_path) in enumerate(zip(axes_array, existing_paths, strict=True)):
        image = plt.imread(panel_path)
        ax.imshow(image)
        ax.set_axis_off()
        if index < len(panel_specs):
            title = str(panel_specs[index].get("title", "")).strip()
            if title:
                ax.set_title(title, fontsize=11, pad=8)

    footer = []
    if x_axis_unit:
        footer.append(f"Time unit: {x_axis_unit}")
    if figure2_target.get("show_logrank_p_value", True):
        footer.append("Log-rank p-value shown in each panel")
    if footer:
        fig.text(0.5, 0.02, " | ".join(footer), ha="center", va="bottom", fontsize=10)

    fig.tight_layout(rect=[0.0, 0.05, 1.0, 1.0])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=170)
    plt.close(fig)
    return True


def _remove_paths_if_exist(*paths: Path) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            continue


def _fit_unadjusted_quartile_cox(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    endpoint: str,
) -> dict[str, Any]:
    work = df[[duration_col, event_col, "tyg_quartile"]].dropna().copy()
    work = work[work[duration_col] > 0].copy()
    quartile_dummies = pd.get_dummies(work["tyg_quartile"], prefix="tyg", drop_first=True, dtype=float)
    work = pd.concat([work[[duration_col, event_col]].astype(float), quartile_dummies], axis=1)

    if work.empty or work[event_col].sum() <= 0:
        return {"rows": [], "q4_hr": None}

    fit = _fit_cox_model(work, duration_col=duration_col, event_col=event_col)
    if fit is None:
        return {"rows": [], "q4_hr": None}

    rows: list[dict[str, Any]] = []
    q4_hr: float | None = None
    for term, label in (("tyg_Q2", "Q2_vs_Q1"), ("tyg_Q3", "Q3_vs_Q1"), ("tyg_Q4", "Q4_vs_Q1")):
        if term not in fit.summary.index:
            continue
        summary_row = fit.summary.loc[term]
        hazard_ratio = float(summary_row["exp(coef)"])
        if term == "tyg_Q4":
            q4_hr = hazard_ratio
        rows.append(
            {
                "endpoint": endpoint,
                "model": "cox_m1_quartile_unadjusted",
                "term": label,
                "hazard_ratio": _round_or_none(hazard_ratio, digits=6),
                "ci_lower_95": _round_or_none(float(summary_row["exp(coef) lower 95%"]), digits=6),
                "ci_upper_95": _round_or_none(float(summary_row["exp(coef) upper 95%"]), digits=6),
                "p_value": _round_or_none(float(summary_row["p"]), digits=6),
                "n": int(len(work)),
                "events": int(work[event_col].sum()),
            }
        )
    return {"rows": rows, "q4_hr": q4_hr}


def _fit_unadjusted_continuous_cox(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    endpoint: str,
) -> dict[str, Any]:
    work = df[[duration_col, event_col, "tyg_index"]].dropna().copy()
    work = work[work[duration_col] > 0].copy()
    if work.empty or work[event_col].sum() <= 0:
        return {"rows": []}

    fit = _fit_cox_model(work.astype(float), duration_col=duration_col, event_col=event_col)
    if fit is None or "tyg_index" not in fit.summary.index:
        return {"rows": []}

    summary_row = fit.summary.loc["tyg_index"]
    return {
        "rows": [
            {
                "endpoint": endpoint,
                "model": "cox_m1_continuous_unadjusted",
                "term": "tyg_index",
                "hazard_ratio": _round_or_none(float(summary_row["exp(coef)"]), digits=6),
                "ci_lower_95": _round_or_none(float(summary_row["exp(coef) lower 95%"]), digits=6),
                "ci_upper_95": _round_or_none(float(summary_row["exp(coef) upper 95%"]), digits=6),
                "p_value": _round_or_none(float(summary_row["p"]), digits=6),
                "n": int(len(work)),
                "events": int(work[event_col].sum()),
            }
        ]
    }


def _fit_adjusted_quartile_cox(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    endpoint: str,
    model_label: str,
    covariates: list[str],
) -> dict[str, Any]:
    model_df, predictor_terms = _build_cox_design_frame(
        df=df,
        duration_col=duration_col,
        event_col=event_col,
        predictor_mode="quartile",
        covariates=covariates,
    )
    if model_df.empty or not predictor_terms or model_df[event_col].sum() <= 0:
        return {"rows": [], "q4_hr": None}

    fit = _fit_cox_model(model_df, duration_col=duration_col, event_col=event_col)
    if fit is None:
        return {"rows": [], "q4_hr": None}

    rows: list[dict[str, Any]] = []
    q4_hr: float | None = None
    label_map = {"tyg_Q2": "Q2_vs_Q1", "tyg_Q3": "Q3_vs_Q1", "tyg_Q4": "Q4_vs_Q1"}
    for term in predictor_terms:
        if term not in fit.summary.index:
            continue
        summary_row = fit.summary.loc[term]
        hazard_ratio = float(summary_row["exp(coef)"])
        if term == "tyg_Q4":
            q4_hr = hazard_ratio
        rows.append(
            {
                "endpoint": endpoint,
                "model": model_label,
                "term": label_map.get(term, term),
                "hazard_ratio": _round_or_none(hazard_ratio, digits=6),
                "ci_lower_95": _round_or_none(float(summary_row["exp(coef) lower 95%"]), digits=6),
                "ci_upper_95": _round_or_none(float(summary_row["exp(coef) upper 95%"]), digits=6),
                "p_value": _round_or_none(float(summary_row["p"]), digits=6),
                "n": int(len(model_df)),
                "events": int(model_df[event_col].sum()),
            }
        )
    return {"rows": rows, "q4_hr": q4_hr}


def _fit_adjusted_continuous_cox(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    endpoint: str,
    model_label: str,
    covariates: list[str],
) -> dict[str, Any]:
    model_df, predictor_terms = _build_cox_design_frame(
        df=df,
        duration_col=duration_col,
        event_col=event_col,
        predictor_mode="continuous",
        covariates=covariates,
    )
    if model_df.empty or "tyg_index" not in predictor_terms or model_df[event_col].sum() <= 0:
        return {"rows": []}

    fit = _fit_cox_model(model_df, duration_col=duration_col, event_col=event_col)
    if fit is None or "tyg_index" not in fit.summary.index:
        return {"rows": []}

    summary_row = fit.summary.loc["tyg_index"]
    return {
        "rows": [
            {
                "endpoint": endpoint,
                "model": model_label,
                "term": "tyg_index",
                "hazard_ratio": _round_or_none(float(summary_row["exp(coef)"]), digits=6),
                "ci_lower_95": _round_or_none(float(summary_row["exp(coef) lower 95%"]), digits=6),
                "ci_upper_95": _round_or_none(float(summary_row["exp(coef) upper 95%"]), digits=6),
                "p_value": _round_or_none(float(summary_row["p"]), digits=6),
                "n": int(len(model_df)),
                "events": int(model_df[event_col].sum()),
            }
        ]
    }


def _build_cox_design_frame(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    predictor_mode: str,
    covariates: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    required = [duration_col, event_col]
    if predictor_mode in {"continuous", "spline"}:
        required.append("tyg_index")
    else:
        required.append("tyg_quartile")

    available_covariates = [column for column in covariates if column in df.columns and column not in required]
    work = df[required + available_covariates].copy()
    work = work.dropna(subset=[duration_col, event_col])
    work = work[work[duration_col] > 0].copy()
    if work.empty:
        return pd.DataFrame(), []

    frames: list[pd.DataFrame] = [work[[duration_col, event_col]].astype(float).reset_index(drop=True)]
    predictor_terms: list[str] = []

    if predictor_mode == "continuous":
        frames.append(pd.to_numeric(work["tyg_index"], errors="coerce").to_frame("tyg_index").reset_index(drop=True))
        predictor_terms = ["tyg_index"]
    elif predictor_mode == "quartile":
        quartile_dummies = pd.get_dummies(work["tyg_quartile"], prefix="tyg", drop_first=True, dtype=float)
        frames.append(quartile_dummies.reset_index(drop=True))
        predictor_terms = [term for term in ("tyg_Q2", "tyg_Q3", "tyg_Q4") if term in quartile_dummies.columns]
    elif predictor_mode == "spline":
        if dmatrix is None:
            return pd.DataFrame(), []
        spline_basis = dmatrix("cr(x, df=4) - 1", {"x": work["tyg_index"]}, return_type="dataframe")
        spline_basis.columns = [f"rcs_{index}" for index in range(spline_basis.shape[1])]
        frames.append(spline_basis.reset_index(drop=True))
        predictor_terms = list(spline_basis.columns)
    else:
        raise ValueError(f"Unsupported predictor_mode: {predictor_mode}")

    for column in available_covariates:
        if column in CATEGORICAL_MODEL_COLUMNS:
            dummies = pd.get_dummies(work[column].astype(str), prefix=column, drop_first=True, dtype=float)
            if not dummies.empty:
                frames.append(dummies.reset_index(drop=True))
        else:
            frames.append(pd.to_numeric(work[column], errors="coerce").to_frame(column).reset_index(drop=True))

    model_df = pd.concat(frames, axis=1)
    model_df = model_df.replace([np.inf, -np.inf], np.nan).dropna()
    constant_cols = [
        column
        for column in model_df.columns
        if column not in {duration_col, event_col} and model_df[column].nunique(dropna=True) <= 1
    ]
    if constant_cols:
        model_df = model_df.drop(columns=constant_cols)
    predictor_terms = [term for term in predictor_terms if term in model_df.columns]
    return model_df, predictor_terms


def _fit_adjusted_rcs_curve(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    endpoint: str,
    covariates: list[str],
    model_label: str,
) -> dict[str, Any]:
    return _fit_rcs_model(
        df=df,
        duration_col=duration_col,
        event_col=event_col,
        endpoint=endpoint,
        covariates=covariates,
        model_label=model_label,
    )


def _run_subgroup_analyses(df: pd.DataFrame, covariates: list[str]) -> pd.DataFrame:
    work = df.copy()
    if "age" in work.columns:
        work["age_group"] = np.where(work["age"] <= 70, "<=70", ">70")
    if "bmi" in work.columns:
        work["bmi_group"] = pd.cut(
            work["bmi"],
            bins=[-np.inf, 27.4, 31.2, np.inf],
            labels=["<27.4", "27.4-31.2", ">=31.2"],
        )

    subgroup_specs = (
        ("age_group", ["<=70", ">70"]),
        ("gender", sorted(work["gender"].dropna().astype(str).unique().tolist()) if "gender" in work.columns else []),
        ("bmi_group", ["<27.4", "27.4-31.2", ">=31.2"]),
        ("hypertension", ["0", "1"]),
        ("type2_diabetes", ["0", "1"]),
        ("heart_failure", ["0", "1"]),
        ("continuous_renal_replacement_therapy", ["0", "1"]),
        ("mechanical_ventilation", ["0", "1"]),
    )

    rows: list[dict[str, Any]] = []
    for subgroup, levels in subgroup_specs:
        if subgroup not in work.columns:
            continue
        series = work[subgroup]
        for level in levels:
            if pd.api.types.is_numeric_dtype(series):
                subset = work.loc[series.astype("Int64").astype(str) == level].copy()
            else:
                subset = work.loc[series.astype(str) == level].copy()
            if subset.empty:
                continue
            for endpoint, event_col, preferred_duration, fallback_duration in (
                ("hospital", "in_hospital_mortality", "hospital_survival_hours", "hospital_los_hours"),
                ("icu", "icu_mortality", "icu_survival_hours", "icu_los_hours"),
            ):
                duration_col = _resolve_duration_column(subset, preferred_duration, fallback_duration)
                if event_col not in subset.columns or subset[event_col].sum() < 10:
                    continue
                fit = _fit_adjusted_continuous_cox(
                    df=subset,
                    duration_col=duration_col,
                    event_col=event_col,
                    endpoint=endpoint,
                    model_label="subgroup_m3_continuous_adjusted",
                    covariates=covariates,
                )
                for row in fit["rows"]:
                    rows.append(
                        {
                            "subgroup": subgroup,
                            "level": level,
                            **row,
                        }
                    )

    if not rows:
        return pd.DataFrame(
            columns=[
                "subgroup",
                "level",
                "endpoint",
                "model",
                "term",
                "hazard_ratio",
                "ci_lower_95",
                "ci_upper_95",
                "p_value",
                "n",
                "events",
            ]
        )
    return pd.DataFrame(rows)


def _fit_rcs_curve(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    endpoint: str,
) -> dict[str, Any]:
    return _fit_rcs_model(
        df=df,
        duration_col=duration_col,
        event_col=event_col,
        endpoint=endpoint,
        covariates=[],
        model_label="cox_m1_rcs_unadjusted",
    )


def _fit_rcs_model(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    endpoint: str,
    covariates: list[str],
    model_label: str,
) -> dict[str, Any]:
    if dmatrix is None or build_design_matrices is None:
        return {"curve": pd.DataFrame(), "inflection": None}

    work, available_covariates = _prepare_rcs_base_frame(
        df=df,
        duration_col=duration_col,
        event_col=event_col,
        covariates=covariates,
    )
    if work.empty or work[event_col].sum() <= 0:
        return {"curve": pd.DataFrame(), "inflection": None}

    base_frames: list[pd.DataFrame] = [work[[duration_col, event_col]].astype(float).reset_index(drop=True)]
    base_frames.extend(_encode_covariate_frames(work, available_covariates))
    base_df = _finalize_design_matrix(pd.concat(base_frames, axis=1), duration_col=duration_col, event_col=event_col)
    if base_df.empty:
        return {"curve": pd.DataFrame(), "inflection": None}

    linear_df = _finalize_design_matrix(
        pd.concat(
            [
                base_df.reset_index(drop=True),
                work["tyg_index"].astype(float).reset_index(drop=True).to_frame("tyg_index"),
            ],
            axis=1,
        ),
        duration_col=duration_col,
        event_col=event_col,
    )

    basis = dmatrix("cr(x, df=4) - 1", {"x": work["tyg_index"]}, return_type="dataframe")
    basis.columns = [f"rcs_{index}" for index in range(basis.shape[1])]
    predictor_terms = list(basis.columns)
    model_df = _finalize_design_matrix(
        pd.concat([base_df.reset_index(drop=True), basis.reset_index(drop=True)], axis=1),
        duration_col=duration_col,
        event_col=event_col,
    )
    predictor_terms = [term for term in predictor_terms if term in model_df.columns]
    if not predictor_terms:
        return {"curve": pd.DataFrame(), "inflection": None}

    fit = _fit_cox_model(model_df, duration_col=duration_col, event_col=event_col)
    if fit is None:
        return {"curve": pd.DataFrame(), "inflection": None}

    design_info = basis.design_info
    x_grid = np.linspace(work["tyg_index"].quantile(0.05), work["tyg_index"].quantile(0.95), 200)
    grid_basis = pd.DataFrame(
        build_design_matrices([design_info], {"x": x_grid})[0],
        columns=basis.columns,
    )[predictor_terms]
    ref_x = 8.9 if float(work["tyg_index"].min()) <= 8.9 <= float(work["tyg_index"].max()) else float(work["tyg_index"].median())
    ref_basis = pd.DataFrame(
        build_design_matrices([design_info], {"x": [ref_x]})[0],
        columns=basis.columns,
    )[predictor_terms]

    coef = fit.params_.loc[predictor_terms].to_numpy()
    cov_matrix = fit.variance_matrix_.loc[predictor_terms, predictor_terms].to_numpy()
    contrast = grid_basis.to_numpy() - ref_basis.to_numpy()
    log_partial_hazard = contrast @ coef
    variance = np.einsum("ij,jk,ik->i", contrast, cov_matrix, contrast)
    std_error = np.sqrt(np.clip(variance, a_min=0.0, a_max=None))
    hazard_ratio = np.exp(log_partial_hazard)
    ci_lower = np.exp(log_partial_hazard - 1.96 * std_error)
    ci_upper = np.exp(log_partial_hazard + 1.96 * std_error)

    overall_p_value = _extract_model_lrt_p_value(fit)
    linear_fit = _fit_cox_model(linear_df, duration_col=duration_col, event_col=event_col) if not linear_df.empty else None
    nonlinearity_p_value = _likelihood_ratio_p_value(fit, linear_fit)

    curve = pd.DataFrame(
        {
            "endpoint": endpoint,
            "model": model_label,
            "tyg_index": np.round(x_grid, 6),
            "hazard_ratio": np.round(hazard_ratio, 6),
            "ci_lower_95": np.round(ci_lower, 6),
            "ci_upper_95": np.round(ci_upper, 6),
            "reference_tyg_index": round(ref_x, 6),
            "overall_p_value": _round_or_none(overall_p_value, digits=6),
            "nonlinearity_p_value": _round_or_none(nonlinearity_p_value, digits=6),
        }
    )
    inflection = float(curve.loc[curve["hazard_ratio"].idxmin(), "tyg_index"])
    return {
        "curve": curve,
        "inflection": inflection,
        "overall_p_value": overall_p_value,
        "nonlinearity_p_value": nonlinearity_p_value,
        "n": int(len(work)),
        "events": int(work[event_col].sum()),
        "endpoint": endpoint,
        "model": model_label,
        "reference_tyg_index": ref_x,
    }


def _prepare_rcs_base_frame(
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    covariates: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    required = [duration_col, event_col, "tyg_index"]
    available_covariates = [column for column in covariates if column in df.columns and column not in required]
    work = df[required + available_covariates].copy()
    work = work.replace([np.inf, -np.inf], np.nan).dropna()
    work = work[work[duration_col] > 0].copy()
    return work, available_covariates


def _encode_covariate_frames(work: pd.DataFrame, covariates: list[str]) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for column in covariates:
        if column in CATEGORICAL_MODEL_COLUMNS:
            dummies = pd.get_dummies(work[column].astype(str), prefix=column, drop_first=True, dtype=float)
            if not dummies.empty:
                frames.append(dummies.reset_index(drop=True))
        else:
            frames.append(pd.to_numeric(work[column], errors="coerce").to_frame(column).reset_index(drop=True))
    return frames


def _finalize_design_matrix(df: pd.DataFrame, duration_col: str, event_col: str) -> pd.DataFrame:
    model_df = df.replace([np.inf, -np.inf], np.nan).dropna().copy()
    constant_cols = [
        column
        for column in model_df.columns
        if column not in {duration_col, event_col} and model_df[column].nunique(dropna=True) <= 1
    ]
    if constant_cols:
        model_df = model_df.drop(columns=constant_cols)
    return model_df


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


def _build_rcs_summary_payload(rcs_results: list[dict[str, Any]], contract: dict[str, Any]) -> dict[str, Any]:
    target_lookup = {
        ("hospital", "cox_m1_rcs_unadjusted"): dict(contract.get("rcs_targets", {})).get("hospital_unadjusted", {}),
        ("hospital", "cox_m3_rcs_adjusted"): dict(contract.get("rcs_targets", {})).get("hospital_adjusted", {}),
        ("icu", "cox_m1_rcs_unadjusted"): dict(contract.get("rcs_targets", {})).get("icu_unadjusted", {}),
        ("icu", "cox_m3_rcs_adjusted"): dict(contract.get("rcs_targets", {})).get("icu_adjusted", {}),
    }
    panels: list[dict[str, Any]] = []
    for result in rcs_results:
        key = (str(result.get("endpoint")), str(result.get("model")))
        targets = target_lookup.get(key, {})
        panels.append(
            {
                "endpoint": key[0],
                "model": key[1],
                "n": result.get("n"),
                "events": result.get("events"),
                "inflection": _round_or_none(result.get("inflection"), digits=6),
                "overall_p_value": _round_or_none(result.get("overall_p_value"), digits=6),
                "nonlinearity_p_value": _round_or_none(result.get("nonlinearity_p_value"), digits=6),
                "target": {
                    "overall_p_value": targets.get("overall_p_value"),
                    "nonlinearity_p_value": targets.get("nonlinearity_p_value"),
                    "inflection": targets.get("inflection"),
                },
            }
        )
    return {
        "figure_target": dict(contract.get("figure_targets", {})).get("figure3", {}),
        "panels": panels,
    }


def _plot_rcs_panels(
    rcs_results: list[dict[str, Any]],
    output_path: Path,
    contract: dict[str, Any],
) -> None:
    panel_specs = list(dict(contract.get("figure_targets", {})).get("figure3", {}).get("panels", []))
    if not panel_specs:
        panel_specs = [
            {"panel": "a", "endpoint": "hospital", "model": "cox_m1_rcs_unadjusted"},
            {"panel": "b", "endpoint": "hospital", "model": "cox_m3_rcs_adjusted"},
            {"panel": "c", "endpoint": "icu", "model": "cox_m1_rcs_unadjusted"},
            {"panel": "d", "endpoint": "icu", "model": "cox_m3_rcs_adjusted"},
        ]

    result_lookup = {(str(item.get("endpoint")), str(item.get("model"))): item for item in rcs_results}
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 9.5), sharex=True)
    axes_flat = list(axes.flatten())

    for ax, spec in zip(axes_flat, panel_specs):
        endpoint = str(spec.get("endpoint", ""))
        model = str(spec.get("model", ""))
        panel = str(spec.get("panel", "")).lower()
        result = result_lookup.get((endpoint, model))
        if result is None or result["curve"].empty:
            ax.text(0.5, 0.5, "Curve unavailable", ha="center", va="center")
            ax.set_axis_off()
            continue

        curve = result["curve"]
        ax.plot(curve["tyg_index"], curve["hazard_ratio"], color="#184e77", linewidth=2.2)
        ax.fill_between(
            curve["tyg_index"],
            curve["ci_lower_95"],
            curve["ci_upper_95"],
            color="#76c893",
            alpha=0.22,
        )
        ax.axhline(1.0, linestyle="--", color="#666666", linewidth=1.0)
        inflection = result.get("inflection")
        if inflection is not None:
            ax.axvline(float(inflection), linestyle=":", color="#c1121f", linewidth=1.4)

        endpoint_label = "in-hospital mortality" if endpoint == "hospital" else "ICU mortality"
        model_label = "univariate analysis" if model == "cox_m1_rcs_unadjusted" else "multivariate analysis"
        ax.set_title(f"({panel}) {endpoint_label}, {model_label}")
        ax.set_xlabel("TyG index")
        ax.set_ylabel("Hazard ratio")
        ax.grid(axis="y", alpha=0.2)
        ax.text(
            0.03,
            0.97,
            "\n".join(
                [
                    f"Overall P = {_format_p_value(result.get('overall_p_value'))}",
                    f"P for nonlinearity = {_format_p_value(result.get('nonlinearity_p_value'))}",
                    f"Inflection = {_format_numeric(result.get('inflection'), digits=2)}",
                ]
            ),
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=9.5,
            bbox={"facecolor": "white", "edgecolor": "#dddddd", "alpha": 0.9, "pad": 4},
        )

    for ax in axes_flat[len(panel_specs):]:
        ax.set_axis_off()

    figure_note = dict(contract.get("figure_targets", {})).get("figure3", {}).get("caption_note", "")
    if figure_note:
        fig.suptitle(
            "Fig. 3. RCS regression for TyG and mortality\n"
            + figure_note,
            fontsize=12,
            y=0.98,
        )
    fig.tight_layout(rect=[0.0, 0.0, 1.0, 0.94])
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _fit_cox_model(df: pd.DataFrame, duration_col: str, event_col: str) -> CoxPHFitter | None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fitter = CoxPHFitter(penalizer=0.01)
            fitter.fit(df, duration_col=duration_col, event_col=event_col, show_progress=False)
            return fitter
    except (ConvergenceError, ValueError, ZeroDivisionError, np.linalg.LinAlgError):
        return None


def _resolve_duration_column(df: pd.DataFrame, preferred: str, fallback: str) -> str:
    if preferred in df.columns and pd.to_numeric(df[preferred], errors="coerce").notna().any():
        return preferred
    return fallback


def _build_paper_alignment_diagnostics(
    project_root: Path,
    contract: dict[str, Any],
    cohort_n: int,
    baseline_df: pd.DataFrame,
    metrics: dict[str, float | None],
    km_results: dict[str, dict[str, Any]],
    rcs_results: list[dict[str, Any]],
) -> dict[str, Any]:
    cohort_rows = _build_cohort_alignment_rows(project_root=project_root, contract=contract, fallback_n=cohort_n, df=baseline_df)
    baseline_rows = _build_baseline_alignment_rows(df=baseline_df, contract=contract)
    supplement_rows = _build_supplement_baseline_alignment_rows(df=baseline_df, contract=contract)
    cox_table_rows = _build_cox_table_alignment_rows(project_root=project_root, contract=contract)
    metric_rows = _build_metric_alignment_rows(metrics=metrics, contract=contract)
    km_rows = _build_km_alignment_rows(km_results=km_results, contract=contract)
    rcs_rows = _build_rcs_alignment_rows(rcs_results=rcs_results, contract=contract)

    sections = {
        "cohort_alignment": _section_payload(cohort_rows),
        "baseline_alignment": _section_payload(baseline_rows),
        "supplement_baseline_alignment": _section_payload(supplement_rows),
        "cox_table_alignment": _section_payload(cox_table_rows),
        "metric_alignment": _section_payload(metric_rows),
        "km_alignment": _section_payload(km_rows),
        "rcs_alignment": _section_payload(rcs_rows),
    }
    return {
        **sections,
        "summary": {
            "cohort_n": cohort_n,
            "section_fail_counts": {name: payload["summary"]["fail"] for name, payload in sections.items()},
            "section_warn_counts": {name: payload["summary"]["warn"] for name, payload in sections.items()},
            "baseline_mean_percent_deviation": _mean_percent_deviation(baseline_rows),
            "supplement_baseline_mean_percent_deviation": _mean_percent_deviation(supplement_rows),
            "cox_table_mean_percent_deviation": _mean_percent_deviation(cox_table_rows),
            "metric_mean_percent_deviation": _mean_percent_deviation(metric_rows),
        },
    }


def _build_cohort_alignment_rows(
    project_root: Path,
    contract: dict[str, Any],
    fallback_n: int,
    df: pd.DataFrame,
) -> list[dict[str, Any]]:
    cohort_path = project_root / "shared" / "cohort_alignment.json"
    cohort_targets = dict(contract.get("cohort_targets", {}))
    rows: list[dict[str, Any]] = []
    if cohort_path.exists():
        payload = json.loads(cohort_path.read_text(encoding="utf-8"))
        actual = dict(payload.get("actual", {}))
        target = dict(payload.get("target", {}))
        rows.append(_count_alignment_row("n_final", actual.get("n_final"), target.get("n_final")))
        rows.append(_count_alignment_row("n_hospital_death", actual.get("n_hospital_death"), target.get("n_hospital_death")))
        rows.append(_count_alignment_row("n_icu_death", actual.get("n_icu_death"), target.get("n_icu_death")))
        actual_q = dict(actual.get("tyg_quartile_counts", {}))
        target_q = dict(target.get("tyg_quartile_counts", {}))
        for quartile in QUARTILES:
            rows.append(
                _count_alignment_row(
                    f"tyg_{quartile}_n",
                    actual_q.get(quartile),
                    target_q.get(quartile),
                )
            )
        return rows

    rows.append(_count_alignment_row("n_final", fallback_n, cohort_targets.get("final_n")))
    if "in_hospital_mortality" in df.columns:
        rows.append(
            _count_alignment_row(
                "n_hospital_death",
                pd.to_numeric(df["in_hospital_mortality"], errors="coerce").fillna(0).sum(),
                cohort_targets.get("in_hospital_mortality_n"),
            )
        )
    if "icu_mortality" in df.columns:
        rows.append(
            _count_alignment_row(
                "n_icu_death",
                pd.to_numeric(df["icu_mortality"], errors="coerce").fillna(0).sum(),
                cohort_targets.get("icu_mortality_n"),
            )
        )
    actual_quartiles = df["tyg_quartile"].astype(str).value_counts() if "tyg_quartile" in df.columns else pd.Series(dtype=int)
    target_q = dict(cohort_targets.get("tyg_quartile_target_counts", {}))
    for quartile in QUARTILES:
        rows.append(
            _count_alignment_row(
                f"tyg_{quartile}_n",
                int(actual_quartiles.get(quartile, 0)),
                target_q.get(quartile),
            )
        )
    return rows


def _build_baseline_alignment_rows(df: pd.DataFrame, contract: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric, spec in dict(contract.get("baseline_targets", {})).items():
        target = _to_optional_float(spec.get("target"))
        actual = _extract_baseline_actual(df, metric)
        deviation = _percent_deviation(target, actual) if target is not None and actual is not None else None
        rows.append(
            {
                "metric": metric,
                "target": target,
                "actual": actual,
                "deviation_percent": _round_or_none(deviation, digits=4),
                "status": _grade_deviation(deviation, pass_threshold=10, warn_threshold=20) if deviation is not None else "missing",
                "kind": str(spec.get("kind", "")),
                "source_file": str(spec.get("source_file", "")),
                "source_label": str(spec.get("source_label", metric)),
            }
        )
    return rows


def _build_supplement_baseline_alignment_rows(df: pd.DataFrame, contract: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric, spec in dict(contract.get("supplement_baseline_targets", {})).items():
        target = _to_optional_float(spec.get("target"))
        actual = _extract_baseline_actual(df, metric)
        deviation = _percent_deviation(target, actual) if target is not None and actual is not None else None
        rows.append(
            {
                "metric": metric,
                "target": target,
                "actual": actual,
                "deviation_percent": _round_or_none(deviation, digits=4),
                "status": _grade_deviation(deviation, pass_threshold=10, warn_threshold=20) if deviation is not None else "missing",
                "kind": str(spec.get("kind", "")),
                "source_file": str(spec.get("source_file", "")),
                "source_label": str(spec.get("source_label", metric)),
            }
        )
    return rows


def _build_cox_table_alignment_rows(project_root: Path, contract: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    target_rows = list(contract.get("cox_table_targets", []))
    cox_path = project_root / "shared" / "cox_models.csv"
    actual_df = pd.read_csv(cox_path) if cox_path.exists() else pd.DataFrame()
    for item in target_rows:
        endpoint = str(item.get("endpoint", ""))
        model = str(item.get("model", ""))
        term = str(item.get("term", ""))
        actual_row = _lookup_cox_actual_row(actual_df, endpoint=endpoint, model=model, term=term)

        target_hr = _to_optional_float(item.get("hazard_ratio"))
        actual_hr = _to_optional_float(actual_row.get("hazard_ratio")) if actual_row is not None else None
        hr_deviation = _percent_deviation(target_hr, actual_hr) if target_hr is not None and actual_hr is not None else None
        rows.append(
            {
                "metric": f"{endpoint}_{model}_{term}_hazard_ratio",
                "target": target_hr,
                "actual": actual_hr,
                "deviation_percent": _round_or_none(hr_deviation, digits=4),
                "status": _grade_deviation(hr_deviation, pass_threshold=10, warn_threshold=20) if hr_deviation is not None else "missing",
                "source_file": str(item.get("source_file", "")),
                "source_label": str(item.get("source_label", term)),
            }
        )

        target_p = _to_optional_float(item.get("p_value"))
        actual_p = _to_optional_float(actual_row.get("p_value")) if actual_row is not None else None
        p_status, p_deviation = _grade_p_value_target(target_p, actual_p)
        rows.append(
            {
                "metric": f"{endpoint}_{model}_{term}_p_value",
                "target": target_p,
                "actual": actual_p,
                "deviation_percent": _round_or_none(p_deviation, digits=4),
                "status": p_status,
                "source_file": str(item.get("source_file", "")),
                "source_label": str(item.get("source_label", term)),
            }
        )
    return rows


def _build_metric_alignment_rows(metrics: dict[str, float | None], contract: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in list(contract.get("metric_targets", [])):
        metric = str(item.get("metric", "unknown"))
        target = _to_optional_float(item.get("target"))
        actual = metrics.get(metric)
        deviation = _percent_deviation(target, actual) if target is not None and actual is not None else None
        rows.append(
            {
                "metric": metric,
                "target": target,
                "actual": actual,
                "deviation_percent": _round_or_none(deviation, digits=4),
                "status": _grade_deviation(deviation) if deviation is not None else "missing",
                "source_file": str(item.get("source_file", "")),
            }
        )
    return rows


def _lookup_cox_actual_row(
    df: pd.DataFrame,
    *,
    endpoint: str,
    model: str,
    term: str,
) -> pd.Series | None:
    if df.empty:
        return None
    required_columns = {"endpoint", "model", "term"}
    if not required_columns.issubset(df.columns):
        return None
    mask = (
        df["endpoint"].astype(str).eq(endpoint)
        & df["model"].astype(str).eq(model)
        & df["term"].astype(str).eq(term)
    )
    matches = df.loc[mask]
    if matches.empty:
        return None
    return matches.iloc[0]


def _build_km_alignment_rows(km_results: dict[str, dict[str, Any]], contract: dict[str, Any]) -> list[dict[str, Any]]:
    km_targets = dict(contract.get("km_targets", {}))
    rows: list[dict[str, Any]] = []
    mapping = {
        "hospital_logrank_p_value": "hospital",
        "icu_logrank_p_value": "icu",
    }
    for metric, endpoint in mapping.items():
        target = _to_optional_float(km_targets.get(metric))
        actual = _to_optional_float(km_results.get(endpoint, {}).get("logrank_p_value"))
        status, deviation = _grade_p_value_target(target, actual)
        rows.append(
            {
                "metric": metric,
                "target": target,
                "actual": actual,
                "deviation_percent": _round_or_none(deviation, digits=4),
                "status": status,
            }
        )
    return rows


def _build_rcs_alignment_rows(rcs_results: list[dict[str, Any]], contract: dict[str, Any]) -> list[dict[str, Any]]:
    target_lookup = {
        ("hospital", "cox_m1_rcs_unadjusted"): dict(contract.get("rcs_targets", {})).get("hospital_unadjusted", {}),
        ("hospital", "cox_m3_rcs_adjusted"): dict(contract.get("rcs_targets", {})).get("hospital_adjusted", {}),
        ("icu", "cox_m1_rcs_unadjusted"): dict(contract.get("rcs_targets", {})).get("icu_unadjusted", {}),
        ("icu", "cox_m3_rcs_adjusted"): dict(contract.get("rcs_targets", {})).get("icu_adjusted", {}),
    }
    rows: list[dict[str, Any]] = []
    for result in rcs_results:
        key = (str(result.get("endpoint")), str(result.get("model")))
        targets = target_lookup.get(key, {})
        for field in ("overall_p_value", "nonlinearity_p_value", "inflection"):
            target = _to_optional_float(targets.get(field))
            actual = _to_optional_float(result.get(field))
            if field.endswith("p_value"):
                status, deviation = _grade_p_value_target(target, actual)
            else:
                deviation = _percent_deviation(target, actual) if target is not None and actual is not None else None
                status = _grade_deviation(deviation, pass_threshold=5, warn_threshold=10) if deviation is not None else "missing"
            rows.append(
                {
                    "metric": f"{key[0]}_{key[1]}_{field}",
                    "target": target,
                    "actual": actual,
                    "deviation_percent": _round_or_none(deviation, digits=4),
                    "status": status,
                }
            )
    return rows


def _count_alignment_row(metric: str, actual: Any, target: Any) -> dict[str, Any]:
    actual_num = _to_optional_float(actual)
    target_num = _to_optional_float(target)
    deviation = None
    if actual_num is not None and target_num is not None and target_num != 0:
        deviation = abs(actual_num - target_num) / abs(target_num) * 100.0
    status = "missing"
    if actual_num is not None and target_num is not None:
        if int(round(actual_num)) == int(round(target_num)):
            status = "pass"
        elif abs(actual_num - target_num) <= 2:
            status = "warn"
        else:
            status = "fail"
    return {
        "metric": metric,
        "target": target_num,
        "actual": actual_num,
        "deviation_percent": _round_or_none(deviation, digits=4),
        "status": status,
    }


def _extract_baseline_actual(df: pd.DataFrame, metric: str) -> float | None:
    if metric in BASELINE_PERCENT_TARGETS:
        column, positive_value = BASELINE_PERCENT_TARGETS[metric]
        if column not in df.columns:
            return None
        series = df[column]
        if column == "gender":
            normalized = series.map(_normalize_gender)
            denom = normalized.notna().sum()
            if denom <= 0:
                return None
            return float((normalized == str(positive_value)).sum() / denom * 100.0)
        values = pd.to_numeric(series, errors="coerce")
        denom = values.notna().sum()
        if denom <= 0:
            return None
        return float((values == float(positive_value)).sum() / denom * 100.0)

    if metric not in df.columns:
        return None
    values = pd.to_numeric(df[metric], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _section_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "total": len(rows),
        "pass": sum(1 for row in rows if row.get("status") == "pass"),
        "warn": sum(1 for row in rows if row.get("status") == "warn"),
        "fail": sum(1 for row in rows if row.get("status") == "fail"),
        "missing": sum(1 for row in rows if row.get("status") == "missing"),
    }
    return {"summary": summary, "rows": rows}


def _mean_percent_deviation(rows: list[dict[str, Any]]) -> float | None:
    values = [float(row["deviation_percent"]) for row in rows if row.get("deviation_percent") is not None]
    if not values:
        return None
    return round(float(np.mean(values)), 4)


def _grade_p_value_target(target: float | None, actual: float | None) -> tuple[str, float | None]:
    if target is None or actual is None:
        return "missing", None
    if target <= 0.0015 and actual <= 0.0015:
        return "pass", 0.0
    deviation = abs(actual - target) * 100.0
    if deviation <= 1.0:
        return "pass", deviation
    if deviation <= 5.0:
        return "warn", deviation
    return "fail", deviation


def _percent_deviation(target: float | None, actual: float | None) -> float | None:
    if target is None or actual is None:
        return None
    if target == 0:
        return 0.0 if actual == 0 else 100.0
    return abs(actual - target) / abs(target) * 100.0


def _grade_deviation(
    deviation_percent: float | None,
    pass_threshold: float = 5.0,
    warn_threshold: float = 10.0,
) -> str:
    if deviation_percent is None:
        return "missing"
    if deviation_percent <= pass_threshold:
        return "pass"
    if deviation_percent <= warn_threshold:
        return "warn"
    return "fail"


def _to_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        number = float(text)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


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


def _build_results_rows(
    targets: list[dict[str, Any]],
    metrics: dict[str, float | None],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in targets:
        metric = str(item.get("metric", "unknown_metric"))
        target = _to_float(item.get("target"), default=0.0)
        reproduced = metrics.get(metric)
        rows.append(
            {
                "metric": metric,
                "target": f"{target:.6f}",
                "reproduced": "" if reproduced is None else f"{reproduced:.6f}",
                "model": _metric_model_name(metric),
                "notes": _metric_note(metric, reproduced),
            }
        )
    return rows


def _metric_model_name(metric: str) -> str:
    if metric in {"in_hospital_q4_m1_hr", "icu_q4_m1_hr"}:
        return "cox_m1_quartile_unadjusted"
    if metric == "rcs_inflection":
        return "cox_m3_rcs_adjusted"
    if metric == "icu_q4_m3_hr":
        return "cox_m3_quartile_adjusted"
    return "cohort_only_analysis"


def _metric_note(metric: str, reproduced: float | None) -> str:
    if reproduced is None and metric == "icu_q4_m3_hr":
        return "requires_full_analysis_dataset"
    if reproduced is None:
        return "metric_not_available"
    if metric == "rcs_inflection":
        return "mean_of_adjusted_hospital_and_icu_spline_minima"
    if metric == "icu_q4_m3_hr":
        return "derived_from_imputed_wide_dataset"
    return "derived_from_stats_workflow"


def _round_or_none(value: Any, digits: int) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(number, digits)


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

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
import statsmodels.api as sm
import statsmodels.formula.api as smf
from lifelines import CoxPHFitter
from lifelines.exceptions import ConvergenceError
from lifelines.utils import concordance_index
from scipy.stats import chi2_contingency, fisher_exact, mannwhitneyu, shapiro, ttest_ind
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss, roc_auc_score, roc_curve

from ..contracts import ModelSpec, OutputSpec, TaskContract, VariableRole


@dataclass(frozen=True)
class BinaryOutcomeRunResult:
    analysis_dataset_rel: str
    row_count: int
    outputs: list[str]
    metrics: dict[str, float | None]


def run_binary_outcome_analysis_workflow(
    *,
    project_root: Path,
    contract: TaskContract,
    analysis_dataset_rel: str,
    artifact_subdir: str = "",
    missingness_rel: str = "",
) -> BinaryOutcomeRunResult:
    analysis_dataset_path = (project_root / analysis_dataset_rel).resolve()
    if not analysis_dataset_path.exists():
        raise FileNotFoundError(f"Analysis dataset not found: {analysis_dataset_path}")

    raw_df = pd.read_csv(analysis_dataset_path)
    if raw_df.empty:
        raise ValueError("Analysis dataset is empty")

    logistic_models = [item for item in contract.models if item.family == "logistic_regression"]
    cox_models = [item for item in contract.models if item.family == "cox_regression"]
    if not logistic_models:
        logistic_models = [_fallback_logistic_model(contract)]

    outcome_column = _resolve_primary_outcome(contract=contract, models=(logistic_models or cox_models), df=raw_df)
    df = raw_df.copy()
    df[outcome_column] = _normalize_binary_outcome(df[outcome_column], outcome_column)
    modeling_df, preprocessing_summary = _apply_paper_aligned_preprocessing(
        df=df,
        contract=contract,
        models=[*logistic_models, *cox_models],
        outcome_column=outcome_column,
    )

    shared_dir = project_root / "shared"
    results_dir = project_root / "results"
    if artifact_subdir.strip():
        subdir = Path(artifact_subdir.strip())
        shared_dir = shared_dir / subdir
        results_dir = results_dir / subdir
    shared_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    baseline_path = shared_dir / "baseline_table.csv"
    baseline_md_path = shared_dir / "baseline_table.md"
    logistic_path = shared_dir / "logistic_models.csv"
    logistic_md_path = shared_dir / "logistic_models.md"
    cox_path = shared_dir / "cox_results_table.csv"
    cox_md_path = shared_dir / "cox_results_table.md"
    cox_summary_path = shared_dir / "cox_summary.json"
    roc_summary_path = shared_dir / "roc_summary.json"
    calibration_summary_path = shared_dir / "calibration_summary.json"
    dca_summary_path = shared_dir / "dca_summary.json"
    nomogram_summary_path = shared_dir / "nomogram_summary.json"
    train_validation_summary_path = shared_dir / "train_validation_summary.json"
    distribution_summary_path = shared_dir / "distribution_summary.json"
    heatmap_summary_path = shared_dir / "heatmap_summary.json"
    heatmap_matrix_path = shared_dir / "heatmap_matrix.csv"
    stats_summary_path = shared_dir / "stats_summary.json"
    report_path = shared_dir / "reproduction_report.md"
    roc_plot_path = results_dir / "roc.png"
    calibration_plot_path = results_dir / "calibration_curve.png"
    decision_curve_plot_path = results_dir / "decision_curve.png"
    nomogram_plot_path = results_dir / "nomogram.png"
    distribution_plot_path = results_dir / "distribution.png"
    heatmap_plot_path = results_dir / "heatmap.png"
    requested_output_kinds = _resolve_requested_output_kinds(contract)

    baseline_columns = _resolve_baseline_columns(
        contract=contract,
        models=[*logistic_models, *cox_models],
        df=df,
        outcome_column=outcome_column,
    )
    baseline_df = _build_binary_baseline_table(df=df, columns=baseline_columns, outcome_column=outcome_column)
    baseline_df.to_csv(baseline_path, index=False)
    baseline_md_path.write_text(_dataframe_to_markdown(baseline_df), encoding="utf-8")

    logistic_rows: list[dict[str, Any]] = []
    cox_rows: list[dict[str, Any]] = []
    cox_models_payload: list[dict[str, Any]] = []
    roc_models: list[dict[str, Any]] = []
    probability_models: list[dict[str, Any]] = []
    model_summaries: list[dict[str, Any]] = []
    cox_model_summaries: list[dict[str, Any]] = []
    metrics: dict[str, float | None] = {}

    for model in logistic_models:
        fitted = _fit_logistic_model(df=modeling_df, contract=contract, model=model, outcome_column=outcome_column)
        logistic_rows.extend(fitted["rows"])
        if fitted["roc"] is not None:
            roc_models.append(fitted["roc"])
            metrics[f"{model.name}_roc_auc"] = fitted["roc"]["auc"]
        probability_models.append(
            {
                "model_name": model.name,
                "auc": fitted["roc"]["auc"] if fitted["roc"] is not None else None,
                "observed": np.asarray(fitted["observed"], dtype=float),
                "predicted": np.asarray(fitted["predicted"], dtype=float),
                "n_used": fitted["n_used"],
                "event_count": fitted["event_count"],
            }
        )
        model_summaries.append(
            {
                "model_name": model.name,
                "family": model.family,
                "formula": fitted["formula"],
                "n_used": fitted["n_used"],
                "event_count": fitted["event_count"],
                "dropped_missing": fitted["dropped_missing"],
                "exposure_variables": list(fitted["exposure_variables"]),
                "control_variables": list(fitted["control_variables"]),
            }
        )

    for model in cox_models:
        fitted = _fit_cox_regression_model(df=modeling_df, contract=contract, model=model, outcome_column=outcome_column)
        cox_rows.extend(fitted["rows"])
        c_index = _to_optional_float(fitted.get("concordance_index"))
        metrics[f"{model.name}_c_index"] = c_index
        cox_models_payload.append(
            {
                "model_name": model.name,
                "concordance_index": _round_or_none(c_index),
                "n_used": fitted["n_used"],
                "events": fitted["event_count"],
                "duration_column": fitted["duration_column"],
                "event_column": fitted["event_column"],
            }
        )
        cox_model_summaries.append(
            {
                "model_name": model.name,
                "family": model.family,
                "formula": fitted["formula"],
                "n_used": fitted["n_used"],
                "event_count": fitted["event_count"],
                "dropped_missing": fitted["dropped_missing"],
                "duration_column": fitted["duration_column"],
                "event_column": fitted["event_column"],
                "concordance_index": _round_or_none(c_index),
                "exposure_variables": list(fitted["exposure_variables"]),
                "control_variables": list(fitted["control_variables"]),
            }
        )

    comparator_models = _build_score_comparator_models(
        df=modeling_df,
        outcome_column=outcome_column,
        score_columns=("apsiii", "sapsii", "oasis"),
    )
    for item in comparator_models:
        if item.get("roc") is not None:
            roc_models.append(item["roc"])
            metrics[f'{item["model_name"]}_roc_auc'] = _to_optional_float(item["roc"].get("auc"))
        probability_models.append(
            {
                "model_name": item["model_name"],
                "auc": item["roc"]["auc"] if item.get("roc") is not None else None,
                "observed": np.asarray(item["observed"], dtype=float),
                "predicted": np.asarray(item["predicted"], dtype=float),
                "n_used": item["n_used"],
                "event_count": item["event_count"],
                "is_comparator": True,
                "source_column": item.get("source_column", ""),
            }
        )

    logistic_df = pd.DataFrame(
        logistic_rows,
        columns=[
            "model_name",
            "term",
            "term_group",
            "odds_ratio",
            "ci_lower_95",
            "ci_upper_95",
            "p_value",
            "n_used",
            "events",
            "formula",
        ],
    )
    logistic_df.to_csv(logistic_path, index=False)
    logistic_md_path.write_text(_dataframe_to_markdown(logistic_df), encoding="utf-8")

    cox_df = pd.DataFrame(
        cox_rows,
        columns=[
            "model_name",
            "term",
            "term_group",
            "hazard_ratio",
            "ci_lower_95",
            "ci_upper_95",
            "p_value",
            "n_used",
            "events",
            "duration_column",
            "event_column",
            "formula",
        ],
    )
    cox_summary = _build_cox_summary(cox_models_payload)
    if cox_models:
        cox_df.to_csv(cox_path, index=False)
        cox_md_path.write_text(_dataframe_to_markdown(cox_df), encoding="utf-8")
        cox_summary_path.write_text(json.dumps(cox_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    metrics["best_cox_c_index"] = _to_optional_float(cox_summary.get("best_model_c_index"))

    train_validation_summary = _build_train_validation_summary(
        df=modeling_df,
        contract=contract,
        logistic_models=logistic_models,
        cox_models=cox_models,
        outcome_column=outcome_column,
    )
    if train_validation_summary.get("enabled", False):
        train_validation_summary_path.write_text(
            json.dumps(train_validation_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        metrics["best_validation_auc"] = _to_optional_float(train_validation_summary.get("best_validation_auc"))
        metrics["best_validation_c_index"] = _to_optional_float(train_validation_summary.get("best_validation_c_index"))

    roc_summary = _build_roc_summary(roc_models)
    roc_summary_path.write_text(json.dumps(roc_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    _plot_roc_curves(roc_models=roc_models, output_path=roc_plot_path)
    metrics["best_roc_auc"] = _to_optional_float(roc_summary.get("best_model_auc"))

    calibration_summary: dict[str, Any] = {}
    primary_probability_model = logistic_models[0].name if logistic_models else ""
    if _wants_any_output(requested_output_kinds, "calibration_figure"):
        calibration_summary = _build_calibration_summary(
            probability_models,
            preferred_model_name=primary_probability_model,
        )
        calibration_summary_path.write_text(
            json.dumps(calibration_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        _plot_calibration_curve(calibration_summary=calibration_summary, output_path=calibration_plot_path)
        metrics["best_brier_score"] = _to_optional_float(calibration_summary.get("brier_score"))

    dca_summary: dict[str, Any] = {}
    if _wants_any_output(requested_output_kinds, "decision_curve_figure"):
        dca_summary = _build_decision_curve_summary(
            probability_models,
            preferred_model_name=primary_probability_model,
        )
        dca_summary_path.write_text(json.dumps(dca_summary, indent=2, ensure_ascii=False), encoding="utf-8")
        _plot_decision_curve(dca_summary=dca_summary, output_path=decision_curve_plot_path)
        metrics["best_decision_curve_net_benefit"] = _to_optional_float(dca_summary.get("best_net_benefit"))

    nomogram_summary: dict[str, Any] = {}
    if _wants_any_output(requested_output_kinds, "nomogram_figure"):
        nomogram_summary = _build_nomogram_figure_summary(
            df=modeling_df,
            cox_df=cox_df,
            outcome_column=outcome_column,
            output_path=nomogram_plot_path,
        )
        nomogram_summary_path.write_text(
            json.dumps(nomogram_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    distribution_summary: dict[str, Any] = {}
    if _wants_any_output(requested_output_kinds, "distribution_figure"):
        distribution_summary = _build_distribution_figure_summary(
            df=df,
            contract=contract,
            outcome_column=outcome_column,
            output_path=distribution_plot_path,
        )
        distribution_summary_path.write_text(
            json.dumps(distribution_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    heatmap_summary: dict[str, Any] = {}
    if _wants_any_output(requested_output_kinds, "heatmap_figure"):
        heatmap_summary = _build_heatmap_figure_summary(
            df=df,
            contract=contract,
            outcome_column=outcome_column,
            output_path=heatmap_plot_path,
            matrix_output_path=heatmap_matrix_path,
        )
        heatmap_summary_path.write_text(
            json.dumps(heatmap_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    missingness_payload: dict[str, Any] = {}
    if missingness_rel.strip():
        missingness_path = (project_root / missingness_rel).resolve()
        if missingness_path.exists():
            missingness_payload = json.loads(missingness_path.read_text(encoding="utf-8"))

    artifact_paths = [
        baseline_path,
        baseline_md_path,
        logistic_path,
        logistic_md_path,
        roc_summary_path,
        stats_summary_path,
        report_path,
        roc_plot_path,
    ]
    if cox_models:
        artifact_paths.extend([cox_path, cox_md_path, cox_summary_path])
    if nomogram_summary:
        artifact_paths.extend([nomogram_summary_path, nomogram_plot_path])
    if train_validation_summary.get("enabled", False):
        artifact_paths.append(train_validation_summary_path)
    if calibration_summary:
        artifact_paths.extend([calibration_summary_path, calibration_plot_path])
    if dca_summary:
        artifact_paths.extend([dca_summary_path, decision_curve_plot_path])
    if distribution_summary:
        artifact_paths.extend([distribution_summary_path, distribution_plot_path])
    if heatmap_summary:
        artifact_paths.extend([heatmap_summary_path, heatmap_plot_path])
        if heatmap_matrix_path.exists():
            artifact_paths.append(heatmap_matrix_path)

    outputs = [
        str(path.relative_to(project_root))
        for path in artifact_paths
        if path.exists() or path in {stats_summary_path, report_path}
    ]
    stats_summary = {
        "title": contract.title,
        "task_id": contract.task_id,
        "analysis_dataset_rel": analysis_dataset_rel,
        "artifact_subdir": artifact_subdir,
        "row_count": int(len(modeling_df)),
        "outcome_column": outcome_column,
        "baseline_columns": baseline_columns,
        "paper_required_methods": list(contract.meta.get("paper_required_methods", [])),
        "paper_target_dataset_version": str(contract.meta.get("paper_target_dataset_version", contract.dataset.version)),
        "execution_environment_dataset_version": str(
            contract.meta.get("execution_environment_dataset_version", contract.meta.get("configured_dataset_version", contract.dataset.version))
        ),
        "requested_output_kinds": sorted(requested_output_kinds),
        "preprocessing_summary": preprocessing_summary,
        "comparator_models": comparator_models,
        "model_summaries": model_summaries,
        "cox_model_summaries": cox_model_summaries,
        "cox_summary": cox_summary,
        "nomogram_summary": nomogram_summary,
        "train_validation_summary": train_validation_summary,
        "calibration_summary": calibration_summary,
        "decision_curve_summary": dca_summary,
        "distribution_summary": distribution_summary,
        "heatmap_summary": heatmap_summary,
        "missingness_summary": missingness_payload.get("missingness", {}),
        "metrics": {key: _round_or_none(value) for key, value in metrics.items()},
        "outputs": outputs,
    }
    stats_summary_path.write_text(json.dumps(stats_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    report_path.write_text(
        _render_binary_outcome_report(
            contract=contract,
            outcome_column=outcome_column,
            baseline_rows=len(baseline_df),
            logistic_df=logistic_df,
            cox_df=cox_df,
            cox_summary=cox_summary,
            nomogram_summary=nomogram_summary,
            roc_summary=roc_summary,
            calibration_summary=calibration_summary,
            dca_summary=dca_summary,
            distribution_summary=distribution_summary,
            heatmap_summary=heatmap_summary,
            stats_summary=stats_summary,
        ),
        encoding="utf-8",
    )

    return BinaryOutcomeRunResult(
        analysis_dataset_rel=analysis_dataset_rel,
        row_count=int(len(modeling_df)),
        outputs=outputs,
        metrics=metrics,
    )


def _fallback_logistic_model(contract: TaskContract) -> ModelSpec:
    exposures = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE]
    outcomes = [item.name for item in contract.variables if item.role == VariableRole.OUTCOME]
    controls = [item.name for item in contract.variables if item.role == VariableRole.CONTROL]
    return ModelSpec(
        name="model_1",
        family="logistic_regression",
        exposure_variables=exposures,
        outcome_variables=outcomes,
        control_variables=controls,
        description="Fallback logistic model synthesized from TaskContract roles.",
    )


def _resolve_primary_outcome(*, contract: TaskContract, models: list[ModelSpec], df: pd.DataFrame) -> str:
    for model in models:
        for value in model.outcome_variables:
            if value in df.columns:
                return value
    for variable in contract.variables:
        if variable.role == VariableRole.OUTCOME and variable.name in df.columns:
            return variable.name
    raise ValueError("No outcome variable from the TaskContract was found in the analysis dataset")


def _normalize_binary_outcome(series: pd.Series, column_name: str) -> pd.Series:
    if series.dropna().empty:
        raise ValueError(f"Outcome column {column_name} contains only missing values")
    non_null = series.dropna()
    unique = list(pd.unique(non_null))
    if all(isinstance(value, (bool, np.bool_)) for value in unique):
        return series.astype("Int64")
    if set(unique).issubset({0, 1, 0.0, 1.0}):
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if len(unique) != 2:
        raise ValueError(f"Outcome column {column_name} is not binary: observed values={unique[:8]}")
    ordered = sorted(str(value) for value in unique)
    mapping = {ordered[0]: 0, ordered[1]: 1}
    return series.map(lambda value: mapping.get(str(value)) if pd.notna(value) else pd.NA).astype("Int64")


def _resolve_baseline_columns(
    *,
    contract: TaskContract,
    models: list[ModelSpec],
    df: pd.DataFrame,
    outcome_column: str,
) -> list[str]:
    ordered: list[str] = []
    for variable in contract.variables:
        if variable.role in {VariableRole.EXPOSURE, VariableRole.CONTROL, VariableRole.SUBGROUP} and variable.name in df.columns:
            ordered.append(variable.name)
    for model in models:
        ordered.extend([value for value in model.exposure_variables if value in df.columns])
        ordered.extend([value for value in model.control_variables if value in df.columns])
    deduped: list[str] = []
    seen: set[str] = set()
    for value in ordered:
        normalized = str(value).strip()
        if not normalized or normalized == outcome_column or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _collect_modeling_predictor_columns(
    *,
    contract: TaskContract,
    models: list[ModelSpec],
    df: pd.DataFrame,
    outcome_column: str,
) -> list[str]:
    candidates: list[str] = []
    for variable in contract.variables:
        if variable.role in {VariableRole.EXPOSURE, VariableRole.CONTROL} and variable.name in df.columns:
            candidates.append(variable.name)
    for model in models:
        candidates.extend([value for value in model.exposure_variables if value in df.columns])
        candidates.extend([value for value in model.control_variables if value in df.columns])
    return [column for column in _dedupe(candidates) if column in df.columns and column != outcome_column]


def _apply_paper_aligned_preprocessing(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    models: list[ModelSpec],
    outcome_column: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    threshold = float(contract.meta.get("missingness_exclusion_threshold", 0.20) or 0.20)
    threshold = max(0.0, min(1.0, threshold))
    predictor_columns = _collect_modeling_predictor_columns(
        contract=contract,
        models=models,
        df=df,
        outcome_column=outcome_column,
    )
    missing_ratios = {
        column: float(df[column].isna().mean()) if column in df.columns else 0.0
        for column in predictor_columns
    }
    dropped_columns = [column for column, ratio in missing_ratios.items() if ratio > threshold]
    retained_columns = [column for column in predictor_columns if column not in dropped_columns]

    work = df.copy()
    if dropped_columns:
        work = work.drop(columns=dropped_columns, errors="ignore")

    imputation: list[dict[str, Any]] = []
    for column in retained_columns:
        if column not in work.columns:
            continue
        series = work[column]
        missing_count = int(series.isna().sum())
        if missing_count == 0:
            continue

        if _is_numeric_series(series):
            fill_value = pd.to_numeric(series, errors="coerce").median(skipna=True)
            if pd.isna(fill_value):
                fill_value = 0.0
            work[column] = pd.to_numeric(series, errors="coerce").fillna(float(fill_value))
            imputation.append(
                {
                    "column": column,
                    "strategy": "median",
                    "fill_value": float(fill_value),
                    "missing_count": missing_count,
                }
            )
            continue

        mode = series.mode(dropna=True)
        fill_value_obj: Any = mode.iloc[0] if not mode.empty else "Missing"
        work[column] = series.fillna(fill_value_obj)
        imputation.append(
            {
                "column": column,
                "strategy": "mode",
                "fill_value": str(fill_value_obj),
                "missing_count": missing_count,
            }
        )

    # Outcome still must be observed for supervised fitting.
    work = work.loc[work[outcome_column].notna()].copy()
    work[outcome_column] = pd.to_numeric(work[outcome_column], errors="coerce").astype("Int64")

    summary = {
        "missingness_exclusion_threshold": _round_or_none(threshold),
        "predictor_candidates": predictor_columns,
        "dropped_predictors_over_threshold": dropped_columns,
        "retained_predictors": [column for column in retained_columns if column in work.columns],
        "imputation_applied": imputation,
        "row_count_before_preprocessing": int(len(df)),
        "row_count_after_preprocessing": int(len(work)),
    }
    return work, summary


def _build_binary_baseline_table(*, df: pd.DataFrame, columns: list[str], outcome_column: str) -> pd.DataFrame:
    group_zero = df[df[outcome_column] == 0]
    group_one = df[df[outcome_column] == 1]
    rows: list[dict[str, Any]] = []
    for column in columns:
        series = df[column]
        if _is_numeric_series(series):
            p_value, test_name = _continuous_group_test(group_zero[column], group_one[column])
            rows.append(
                {
                    "variable": column,
                    "level": "",
                    "overall": _format_continuous(series),
                    "outcome_0": _format_continuous(group_zero[column]),
                    "outcome_1": _format_continuous(group_one[column]),
                    "p_value": _round_or_none(p_value),
                    "test": test_name,
                }
            )
            continue

        contingency = pd.crosstab(df[column].fillna("Missing"), df[outcome_column])
        p_value, test_name = _categorical_group_test(contingency)
        levels = list(pd.Series(df[column].fillna("Missing")).value_counts(dropna=False).index)
        for index, level in enumerate(levels):
            overall_n = int((df[column].fillna("Missing") == level).sum())
            group0_n = int((group_zero[column].fillna("Missing") == level).sum())
            group1_n = int((group_one[column].fillna("Missing") == level).sum())
            rows.append(
                {
                    "variable": column if index == 0 else "",
                    "level": str(level),
                    "overall": _format_count_pct(overall_n, len(df)),
                    "outcome_0": _format_count_pct(group0_n, len(group_zero)),
                    "outcome_1": _format_count_pct(group1_n, len(group_one)),
                    "p_value": _round_or_none(p_value) if index == 0 else None,
                    "test": test_name if index == 0 else "",
                }
            )
    return pd.DataFrame(rows)


def _fit_logistic_model(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    model: ModelSpec,
    outcome_column: str,
) -> dict[str, Any]:
    exposure_variables = [value for value in model.exposure_variables if value in df.columns]
    if not exposure_variables:
        exposure_variables = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE and item.name in df.columns]
    control_variables = [value for value in model.control_variables if value in df.columns]

    selected_columns = [outcome_column, *exposure_variables, *control_variables]
    selected_columns = [value for value in _dedupe(selected_columns) if value in df.columns]
    model_df = df[selected_columns].copy().dropna()
    if model_df.empty:
        raise ValueError(f"Model {model.name} has no complete-case rows after selecting {selected_columns}")
    if model_df[outcome_column].nunique(dropna=True) != 2:
        raise ValueError(f"Model {model.name} does not have a binary outcome after complete-case filtering")

    formula_terms = [
        _formula_term(column, model_df[column])
        for column in [*exposure_variables, *control_variables]
        if column in model_df.columns
    ]
    if not formula_terms:
        raise ValueError(f"Model {model.name} has no predictor terms available in the analysis dataset")
    formula = f'{_quote_name(outcome_column)} ~ ' + " + ".join(formula_terms)

    fitted = smf.glm(
        formula=formula,
        data=model_df,
        family=sm.families.Binomial(),
    ).fit()

    confint = fitted.conf_int()
    rows: list[dict[str, Any]] = []
    for term, estimate in fitted.params.items():
        if term == "Intercept":
            continue
        ci_lower, ci_upper = confint.loc[term]
        rows.append(
            {
                "model_name": model.name,
                "term": term,
                "term_group": _term_group(term, exposure_variables),
                "odds_ratio": _safe_exp(float(estimate)),
                "ci_lower_95": _safe_exp(float(ci_lower)),
                "ci_upper_95": _safe_exp(float(ci_upper)),
                "p_value": float(fitted.pvalues.loc[term]),
                "n_used": int(len(model_df)),
                "events": int(model_df[outcome_column].sum()),
                "formula": formula,
            }
        )

    predictions = fitted.predict(model_df)
    auc_value = None
    fpr: list[float] = []
    tpr: list[float] = []
    thresholds: list[float] = []
    if model_df[outcome_column].nunique(dropna=True) == 2:
        auc_value = float(roc_auc_score(model_df[outcome_column], predictions))
        fpr_array, tpr_array, threshold_array = roc_curve(model_df[outcome_column], predictions)
        fpr = [float(value) for value in fpr_array]
        tpr = [float(value) for value in tpr_array]
        thresholds = [float(value) for value in threshold_array]

    return {
        "rows": rows,
        "formula": formula,
        "n_used": int(len(model_df)),
        "event_count": int(model_df[outcome_column].sum()),
        "dropped_missing": int(len(df) - len(model_df)),
        "exposure_variables": exposure_variables,
        "control_variables": control_variables,
        "observed": model_df[outcome_column].astype(int).tolist(),
        "predicted": [float(value) for value in predictions],
        "roc": {
            "model_name": model.name,
            "auc": auc_value,
            "fpr": fpr,
            "tpr": tpr,
            "thresholds": thresholds,
            "n_used": int(len(model_df)),
            "events": int(model_df[outcome_column].sum()),
        }
        if auc_value is not None
        else None,
    }


def _fit_cox_regression_model(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    model: ModelSpec,
    outcome_column: str,
) -> dict[str, Any]:
    exposure_variables = [value for value in model.exposure_variables if value in df.columns]
    if not exposure_variables:
        exposure_variables = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE and item.name in df.columns]
    control_variables = [value for value in model.control_variables if value in df.columns]
    duration_column = _resolve_cox_duration_column(df=df, contract=contract, model=model)
    event_column = outcome_column

    model_df = _build_cox_design_matrix(
        df=df,
        duration_column=duration_column,
        event_column=event_column,
        exposure_variables=exposure_variables,
        control_variables=control_variables,
    )
    if model_df.empty:
        raise ValueError(f"Model {model.name} has no complete-case rows after selecting Cox predictors")
    if model_df[event_column].nunique(dropna=True) != 2:
        raise ValueError(f"Model {model.name} does not retain both event and non-event rows for Cox fitting")
    if float(model_df[event_column].sum()) <= 0:
        raise ValueError(f"Model {model.name} has zero events after complete-case filtering")
    predictor_columns = [column for column in model_df.columns if column not in {duration_column, event_column}]
    if not predictor_columns:
        raise ValueError(f"Model {model.name} has no predictor columns available for Cox fitting")
    formula = f"CoxPH({duration_column}, {event_column}) ~ " + " + ".join(predictor_columns)

    fit: CoxPHFitter | None = None
    fit_error: str = ""
    for penalizer in (0.01, 0.05, 0.1):
        try:
            candidate = CoxPHFitter(penalizer=penalizer)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                candidate.fit(model_df, duration_col=duration_column, event_col=event_column, show_progress=False)
            fit = candidate
            break
        except (ConvergenceError, ValueError, ZeroDivisionError, np.linalg.LinAlgError) as exc:
            fit_error = str(exc)
            continue

    if fit is None:
        detail = fit_error or "unknown convergence error"
        raise ValueError(f"Model {model.name} failed Cox fitting: {detail}")

    rows: list[dict[str, Any]] = []
    for term, summary_row in fit.summary.iterrows():
        hazard_ratio = _to_optional_float(summary_row.get("exp(coef)"))
        ci_lower = _to_optional_float(summary_row.get("exp(coef) lower 95%"))
        ci_upper = _to_optional_float(summary_row.get("exp(coef) upper 95%"))
        p_value = _to_optional_float(summary_row.get("p"))
        if hazard_ratio is None:
            continue
        rows.append(
            {
                "model_name": model.name,
                "term": str(term),
                "term_group": _term_group(str(term), exposure_variables),
                "hazard_ratio": hazard_ratio,
                "ci_lower_95": ci_lower,
                "ci_upper_95": ci_upper,
                "p_value": p_value,
                "n_used": int(len(model_df)),
                "events": int(model_df[event_column].sum()),
                "duration_column": duration_column,
                "event_column": event_column,
                "formula": formula,
            }
        )

    return {
        "rows": rows,
        "formula": formula,
        "n_used": int(len(model_df)),
        "event_count": int(model_df[event_column].sum()),
        "dropped_missing": int(len(df) - len(model_df)),
        "duration_column": duration_column,
        "event_column": event_column,
        "concordance_index": _to_optional_float(getattr(fit, "concordance_index_", None)),
        "exposure_variables": exposure_variables,
        "control_variables": control_variables,
    }


def _resolve_cox_duration_column(*, df: pd.DataFrame, contract: TaskContract, model: ModelSpec) -> str:
    explicit = str(model.time_variable or "").strip()
    if explicit and explicit.lower() not in {"none", "null", "na", "n/a"} and explicit in df.columns:
        numeric = pd.to_numeric(df[explicit], errors="coerce")
        if numeric.notna().any():
            return explicit

    for variable in contract.variables:
        if variable.role != VariableRole.TIME:
            continue
        if variable.name not in df.columns:
            continue
        numeric = pd.to_numeric(df[variable.name], errors="coerce")
        if numeric.notna().any():
            return variable.name

    canonical_candidates = [
        "time_to_event_28d_days",
        "time_to_event_28d",
        "time_to_event_days",
        "survival_time_days",
        "followup_days",
        "followup_time_days",
        "icu_los_days",
        "los_days",
    ]
    for column in canonical_candidates:
        if column not in df.columns:
            continue
        numeric = pd.to_numeric(df[column], errors="coerce")
        if numeric.notna().any():
            return column

    for column in df.columns:
        lowered = column.lower()
        if "time" not in lowered:
            continue
        if all(token not in lowered for token in ("event", "follow", "day", "hour", "survival", "los")):
            continue
        numeric = pd.to_numeric(df[column], errors="coerce")
        if numeric.notna().any():
            return column

    raise ValueError("No valid time-to-event column found for Cox regression models")


def _build_cox_design_matrix(
    *,
    df: pd.DataFrame,
    duration_column: str,
    event_column: str,
    exposure_variables: list[str],
    control_variables: list[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    duration = pd.to_numeric(df[duration_column], errors="coerce").to_frame(duration_column)
    event = pd.to_numeric(df[event_column], errors="coerce").to_frame(event_column)
    frames.extend([duration, event])

    for column in _dedupe([*exposure_variables, *control_variables]):
        if column not in df.columns:
            continue
        series = df[column]
        if pd.api.types.is_bool_dtype(series) or pd.api.types.is_object_dtype(series) or isinstance(series.dtype, pd.CategoricalDtype):
            dummies = pd.get_dummies(series.fillna("Missing").astype(str), prefix=column, drop_first=True, dtype=float)
            if not dummies.empty:
                frames.append(dummies)
        else:
            frames.append(pd.to_numeric(series, errors="coerce").to_frame(column))

    model_df = pd.concat(frames, axis=1)
    model_df = model_df.replace([np.inf, -np.inf], np.nan).dropna().copy()
    if model_df.empty:
        return model_df
    model_df = model_df.loc[model_df[duration_column] > 0].copy()
    if model_df.empty:
        return model_df
    model_df[event_column] = (model_df[event_column] > 0).astype(int)

    constant_columns = [
        column
        for column in model_df.columns
        if column not in {duration_column, event_column} and model_df[column].nunique(dropna=True) <= 1
    ]
    if constant_columns:
        model_df = model_df.drop(columns=constant_columns)
    return model_df


def _build_train_validation_summary(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    logistic_models: list[ModelSpec],
    cox_models: list[ModelSpec],
    outcome_column: str,
) -> dict[str, Any]:
    options = _resolve_train_validation_options(contract)
    if not options["enabled"]:
        return {
            "enabled": False,
            "train_fraction": options["train_fraction"],
            "seed": options["seed"],
            "logistic_models": [],
            "cox_models": [],
        }

    logistic_payload: list[dict[str, Any]] = []
    for model in logistic_models:
        try:
            payload = _fit_logistic_train_validation_metrics(
                df=df,
                contract=contract,
                model=model,
                outcome_column=outcome_column,
                train_fraction=options["train_fraction"],
                seed=options["seed"],
            )
        except ValueError as exc:
            payload = {"model_name": model.name, "family": model.family, "error": str(exc)}
        logistic_payload.append(payload)

    cox_payload: list[dict[str, Any]] = []
    for model in cox_models:
        try:
            payload = _fit_cox_train_validation_metrics(
                df=df,
                contract=contract,
                model=model,
                outcome_column=outcome_column,
                train_fraction=options["train_fraction"],
                seed=options["seed"],
            )
        except ValueError as exc:
            payload = {"model_name": model.name, "family": model.family, "error": str(exc)}
        cox_payload.append(payload)

    validation_auc_values = [
        _to_optional_float(item.get("validation_auc"))
        for item in logistic_payload
        if isinstance(item, dict)
    ]
    validation_auc_values = [value for value in validation_auc_values if value is not None]
    validation_cindex_values = [
        _to_optional_float(item.get("validation_c_index"))
        for item in cox_payload
        if isinstance(item, dict)
    ]
    validation_cindex_values = [value for value in validation_cindex_values if value is not None]

    return {
        "enabled": True,
        "train_fraction": options["train_fraction"],
        "validation_fraction": _round_or_none(1.0 - options["train_fraction"]),
        "seed": options["seed"],
        "stratified_by": outcome_column,
        "logistic_models": logistic_payload,
        "cox_models": cox_payload,
        "best_validation_auc": _round_or_none(max(validation_auc_values)) if validation_auc_values else None,
        "best_validation_c_index": _round_or_none(max(validation_cindex_values)) if validation_cindex_values else None,
    }


def _resolve_train_validation_options(contract: TaskContract) -> dict[str, Any]:
    evidence = contract.meta.get("paper_evidence", {})
    evidence_text = ""
    if isinstance(evidence, dict):
        evidence_text = " ".join(
            str(item)
            for item in [
                *(evidence.get("result_targets", []) or []),
                *(evidence.get("cohort_logic", []) if isinstance(evidence.get("cohort_logic"), list) else [evidence.get("cohort_logic", "")]),
            ]
            if str(item).strip()
        ).lower()
    default_enabled = "validation" in evidence_text or "training" in evidence_text
    enabled = bool(contract.meta.get("train_validation_split", default_enabled))
    train_fraction = float(contract.meta.get("train_fraction", 0.70) or 0.70)
    train_fraction = max(0.50, min(0.90, train_fraction))
    seed = int(contract.meta.get("train_validation_seed", 42) or 42)
    return {"enabled": enabled, "train_fraction": train_fraction, "seed": seed}


def _fit_logistic_train_validation_metrics(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    model: ModelSpec,
    outcome_column: str,
    train_fraction: float,
    seed: int,
) -> dict[str, Any]:
    exposure_variables = [value for value in model.exposure_variables if value in df.columns]
    if not exposure_variables:
        exposure_variables = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE and item.name in df.columns]
    control_variables = [value for value in model.control_variables if value in df.columns]
    selected_columns = [outcome_column, *exposure_variables, *control_variables]
    selected_columns = [value for value in _dedupe(selected_columns) if value in df.columns]
    model_df = df[selected_columns].copy().dropna().reset_index(drop=True)
    if model_df.empty:
        raise ValueError("No complete rows available for train/validation logistic split")
    if model_df[outcome_column].nunique(dropna=True) != 2:
        raise ValueError("Outcome is not binary after preprocessing in train/validation split")

    train_idx, valid_idx = _stratified_train_validation_indices(
        outcome=model_df[outcome_column],
        train_fraction=train_fraction,
        seed=seed,
    )
    if not train_idx or not valid_idx:
        raise ValueError("Unable to create non-empty train and validation splits")

    formula_terms = [
        _formula_term(column, model_df[column])
        for column in [*exposure_variables, *control_variables]
        if column in model_df.columns
    ]
    if not formula_terms:
        raise ValueError("No predictor terms available for train/validation logistic split")
    formula = f'{_quote_name(outcome_column)} ~ ' + " + ".join(formula_terms)

    train_df = model_df.loc[train_idx].copy()
    valid_df = model_df.loc[valid_idx].copy()
    fitted = smf.glm(formula=formula, data=train_df, family=sm.families.Binomial()).fit()
    train_pred = fitted.predict(train_df)
    valid_pred = fitted.predict(valid_df)

    return {
        "model_name": model.name,
        "family": model.family,
        "n_train": int(len(train_df)),
        "n_validation": int(len(valid_df)),
        "events_train": int(train_df[outcome_column].sum()),
        "events_validation": int(valid_df[outcome_column].sum()),
        "train_auc": _safe_binary_auc(train_df[outcome_column], train_pred),
        "validation_auc": _safe_binary_auc(valid_df[outcome_column], valid_pred),
        "train_brier_score": _safe_brier(train_df[outcome_column], train_pred),
        "validation_brier_score": _safe_brier(valid_df[outcome_column], valid_pred),
        "formula": formula,
    }


def _fit_cox_train_validation_metrics(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    model: ModelSpec,
    outcome_column: str,
    train_fraction: float,
    seed: int,
) -> dict[str, Any]:
    exposure_variables = [value for value in model.exposure_variables if value in df.columns]
    if not exposure_variables:
        exposure_variables = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE and item.name in df.columns]
    control_variables = [value for value in model.control_variables if value in df.columns]
    duration_column = _resolve_cox_duration_column(df=df, contract=contract, model=model)
    event_column = outcome_column

    model_df = _build_cox_design_matrix(
        df=df,
        duration_column=duration_column,
        event_column=event_column,
        exposure_variables=exposure_variables,
        control_variables=control_variables,
    ).reset_index(drop=True)
    if model_df.empty:
        raise ValueError("No rows available for train/validation Cox split")
    if model_df[event_column].nunique(dropna=True) != 2:
        raise ValueError("Event column is not binary after preprocessing in Cox split")

    train_idx, valid_idx = _stratified_train_validation_indices(
        outcome=model_df[event_column],
        train_fraction=train_fraction,
        seed=seed,
    )
    if not train_idx or not valid_idx:
        raise ValueError("Unable to create non-empty train and validation splits for Cox model")
    train_df = model_df.loc[train_idx].copy()
    valid_df = model_df.loc[valid_idx].copy()

    fit: CoxPHFitter | None = None
    fit_error = ""
    for penalizer in (0.01, 0.05, 0.1):
        try:
            candidate = CoxPHFitter(penalizer=penalizer)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                candidate.fit(train_df, duration_col=duration_column, event_col=event_column, show_progress=False)
            fit = candidate
            break
        except (ConvergenceError, ValueError, ZeroDivisionError, np.linalg.LinAlgError) as exc:
            fit_error = str(exc)
            continue
    if fit is None:
        raise ValueError(f"Cox train/validation fit failed: {fit_error or 'unknown convergence error'}")

    validation_c_index: float | None = None
    if len(valid_df) > 0:
        try:
            risk = fit.predict_partial_hazard(valid_df).to_numpy().reshape(-1)
            validation_c_index = _to_optional_float(
                concordance_index(
                    valid_df[duration_column].to_numpy(),
                    -risk,
                    valid_df[event_column].to_numpy(),
                )
            )
        except Exception:  # noqa: BLE001
            validation_c_index = None

    return {
        "model_name": model.name,
        "family": model.family,
        "n_train": int(len(train_df)),
        "n_validation": int(len(valid_df)),
        "events_train": int(train_df[event_column].sum()),
        "events_validation": int(valid_df[event_column].sum()),
        "duration_column": duration_column,
        "event_column": event_column,
        "train_c_index": _to_optional_float(getattr(fit, "concordance_index_", None)),
        "validation_c_index": validation_c_index,
    }


def _stratified_train_validation_indices(
    *,
    outcome: pd.Series,
    train_fraction: float,
    seed: int,
) -> tuple[list[int], list[int]]:
    clean = pd.to_numeric(outcome, errors="coerce").fillna(0).astype(int).reset_index(drop=True)
    train_idx: list[int] = []
    valid_idx: list[int] = []
    rng = np.random.default_rng(seed)
    for value in sorted(clean.unique()):
        class_idx = np.where(clean.to_numpy() == value)[0]
        if len(class_idx) == 0:
            continue
        shuffled = rng.permutation(class_idx)
        n_train = int(round(len(shuffled) * train_fraction))
        if len(shuffled) >= 2:
            n_train = max(1, min(len(shuffled) - 1, n_train))
        train_idx.extend(int(item) for item in shuffled[:n_train])
        valid_idx.extend(int(item) for item in shuffled[n_train:])

    train_idx = sorted(train_idx)
    valid_idx = sorted(valid_idx)
    return train_idx, valid_idx


def _safe_binary_auc(observed: pd.Series | np.ndarray, predicted: pd.Series | np.ndarray) -> float | None:
    aligned = pd.DataFrame(
        {
            "y": pd.to_numeric(pd.Series(observed), errors="coerce"),
            "p": pd.to_numeric(pd.Series(predicted), errors="coerce"),
        }
    ).dropna()
    if aligned.empty or aligned["y"].nunique(dropna=True) < 2:
        return None
    return _to_optional_float(roc_auc_score(aligned["y"], aligned["p"]))


def _safe_brier(observed: pd.Series | np.ndarray, predicted: pd.Series | np.ndarray) -> float | None:
    y_true = pd.to_numeric(pd.Series(observed), errors="coerce")
    y_score = pd.to_numeric(pd.Series(predicted), errors="coerce")
    aligned = pd.DataFrame({"y": y_true, "p": y_score}).dropna()
    if aligned.empty:
        return None
    return _to_optional_float(brier_score_loss(aligned["y"], aligned["p"]))


def _canonicalize_column_token(value: str) -> str:
    return "".join(char for char in str(value).lower() if char.isalnum())


def _resolve_score_column(df: pd.DataFrame, score_name: str) -> str:
    normalized_columns = {
        _canonicalize_column_token(column): column
        for column in df.columns
    }
    alias_map = {
        "apsiii": ("apsiii", "apacheiiiscore", "apacheiii", "aps3", "aps_iii"),
        "sapsii": ("sapsii", "sapsiiscore", "saps2", "saps_ii"),
        "oasis": ("oasis", "oasisscore", "oasis_score"),
    }
    aliases = alias_map.get(
        _canonicalize_column_token(score_name),
        (score_name,),
    )
    for alias in aliases:
        matched = normalized_columns.get(_canonicalize_column_token(alias))
        if matched:
            return matched
    return ""


def _fit_single_score_probability_model(
    *,
    df: pd.DataFrame,
    outcome_column: str,
    score_column: str,
    model_name: str,
) -> dict[str, Any] | None:
    if outcome_column not in df.columns or score_column not in df.columns:
        return None

    aligned = pd.DataFrame(
        {
            "outcome": pd.to_numeric(df[outcome_column], errors="coerce"),
            "score": pd.to_numeric(df[score_column], errors="coerce"),
        }
    ).dropna()
    if aligned.empty or aligned["outcome"].nunique(dropna=True) != 2:
        return None
    if aligned["score"].nunique(dropna=True) <= 1:
        return None

    x_design = sm.add_constant(aligned["score"], has_constant="add")
    try:
        fitted = sm.GLM(aligned["outcome"], x_design, family=sm.families.Binomial()).fit()
        predicted = np.asarray(fitted.predict(x_design), dtype=float)
    except Exception:  # noqa: BLE001
        score = aligned["score"].to_numpy(dtype=float)
        score_min = float(np.nanmin(score))
        score_max = float(np.nanmax(score))
        if not np.isfinite(score_min) or not np.isfinite(score_max) or score_max <= score_min:
            return None
        predicted = (score - score_min) / (score_max - score_min)

    observed = aligned["outcome"].astype(int).to_numpy(dtype=float)
    predicted = np.clip(predicted, 1e-6, 1.0 - 1e-6)
    auc_value = _safe_binary_auc(observed, predicted)

    roc_payload: dict[str, Any] | None = None
    if auc_value is not None:
        fpr_array, tpr_array, threshold_array = roc_curve(observed, predicted)
        roc_payload = {
            "model_name": model_name,
            "auc": _round_or_none(auc_value),
            "fpr": [float(item) for item in fpr_array],
            "tpr": [float(item) for item in tpr_array],
            "thresholds": [float(item) for item in threshold_array],
            "n_used": int(len(aligned)),
            "events": int(observed.sum()),
            "source_column": score_column,
            "is_comparator": True,
        }

    return {
        "model_name": model_name,
        "source_column": score_column,
        "n_used": int(len(aligned)),
        "event_count": int(observed.sum()),
        "observed": observed.tolist(),
        "predicted": [float(item) for item in predicted],
        "roc": roc_payload,
    }


def _build_score_comparator_models(
    *,
    df: pd.DataFrame,
    outcome_column: str,
    score_columns: tuple[str, ...] = ("apsiii", "sapsii", "oasis"),
) -> list[dict[str, Any]]:
    comparator_models: list[dict[str, Any]] = []
    used_columns: set[str] = set()
    for raw_name in score_columns:
        resolved_column = _resolve_score_column(df, raw_name)
        if not resolved_column or resolved_column in used_columns:
            continue
        used_columns.add(resolved_column)
        model_name = str(raw_name).upper()
        fitted = _fit_single_score_probability_model(
            df=df,
            outcome_column=outcome_column,
            score_column=resolved_column,
            model_name=model_name,
        )
        if fitted is not None:
            comparator_models.append(fitted)
    return comparator_models


def _resolve_nomogram_source_column(term: str, df_columns: list[str]) -> str:
    if term in df_columns:
        return term
    for column in df_columns:
        if term.startswith(f"{column}_"):
            return column
    if term.startswith("C(") and ")." in term:
        inside = term.split("C(", 1)[1].split(")", 1)[0].replace('Q("', "").replace('")', "")
        if inside in df_columns:
            return inside
    if "_" in term:
        prefix = term.split("_", 1)[0]
        if prefix in df_columns:
            return prefix
    return ""


def _plot_nomogram_points(
    *,
    model_name: str,
    outcome_column: str,
    prevalence: float,
    terms: list[dict[str, Any]],
    risk_scale: float,
    output_path: Path,
) -> None:
    fig = plt.figure(figsize=(8.0, 5.8), constrained_layout=True)
    grid = fig.add_gridspec(nrows=2, ncols=1, height_ratios=[3.3, 1.2], hspace=0.35)
    ax_terms = fig.add_subplot(grid[0, 0])
    ax_risk = fig.add_subplot(grid[1, 0])

    ordered_terms = list(reversed(terms))
    y_positions = np.arange(len(ordered_terms))
    term_labels = [item["display_term"] for item in ordered_terms]
    points = [float(item["points"]) for item in ordered_terms]
    colors = ["#0072B2" if item.get("term_group") == "exposure" else "#009E73" for item in ordered_terms]

    ax_terms.barh(y_positions, points, color=colors, alpha=0.85)
    for idx, item in enumerate(ordered_terms):
        hr = item.get("hazard_ratio")
        hr_text = f"HR={hr:.2f}" if hr is not None else "HR=n/a"
        ax_terms.text(
            min(float(item["points"]) + 1.5, 102.0),
            idx,
            hr_text,
            va="center",
            fontsize=8,
            color="#333333",
        )
    ax_terms.set_yticks(y_positions)
    ax_terms.set_yticklabels(term_labels, fontsize=8)
    ax_terms.set_xlim(0, 105)
    ax_terms.set_xlabel("Nomogram points (max contributor = 100)")
    ax_terms.set_title(f"Nomogram-style predictor points ({model_name})")
    ax_terms.grid(alpha=0.2, axis="x")
    ax_terms.spines["top"].set_visible(False)
    ax_terms.spines["right"].set_visible(False)

    total_points = np.linspace(0, 100, 101)
    base_odds = prevalence / max(1.0 - prevalence, 1e-6)
    odds = base_odds * np.exp((total_points / 100.0) * risk_scale)
    predicted_risk = odds / (1.0 + odds)
    ax_risk.plot(total_points, predicted_risk, color="#D55E00", linewidth=2.2)
    ax_risk.set_xlim(0, 100)
    ax_risk.set_ylim(0, 1)
    ax_risk.set_xlabel("Total points")
    ax_risk.set_ylabel("Predicted event probability")
    ax_risk.set_title(f"Approximate risk mapping for {outcome_column}")
    ax_risk.grid(alpha=0.2)
    ax_risk.spines["top"].set_visible(False)
    ax_risk.spines["right"].set_visible(False)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _build_nomogram_figure_summary(
    *,
    df: pd.DataFrame,
    cox_df: pd.DataFrame,
    outcome_column: str,
    output_path: Path,
) -> dict[str, Any]:
    if cox_df.empty:
        _render_placeholder_figure(output_path=output_path, title="Nomogram", message="No Cox coefficients available")
        return {"generated": False, "reason": "cox_results_empty"}

    working = cox_df.copy()
    working["hazard_ratio"] = pd.to_numeric(working["hazard_ratio"], errors="coerce")
    working["n_used"] = pd.to_numeric(working["n_used"], errors="coerce")
    working = working.dropna(subset=["hazard_ratio"])
    working = working.loc[working["hazard_ratio"] > 0].copy()
    if working.empty:
        _render_placeholder_figure(output_path=output_path, title="Nomogram", message="No valid hazard ratios")
        return {"generated": False, "reason": "no_valid_hazard_ratios"}

    model_rank = (
        working.groupby("model_name", as_index=False)
        .agg(n_used=("n_used", "max"), term_count=("term", "count"))
        .sort_values(["n_used", "term_count"], ascending=False)
    )
    selected_model = str(model_rank.iloc[0]["model_name"])
    selected = working.loc[working["model_name"] == selected_model].copy()
    if selected.empty:
        _render_placeholder_figure(output_path=output_path, title="Nomogram", message="No model terms available")
        return {"generated": False, "reason": "no_model_terms"}

    df_columns = list(df.columns)
    term_rows: list[dict[str, Any]] = []
    for _, row in selected.iterrows():
        term = str(row["term"])
        hr = _to_optional_float(row["hazard_ratio"])
        if hr is None or hr <= 0:
            continue
        beta = float(np.log(hr))
        source_column = _resolve_nomogram_source_column(term, df_columns)

        effect_span = 1.0
        if source_column and source_column in df.columns:
            source_values = pd.to_numeric(df[source_column], errors="coerce")
            if source_values.notna().any() and _is_numeric_series(df[source_column]):
                q05 = _to_optional_float(source_values.quantile(0.05))
                q95 = _to_optional_float(source_values.quantile(0.95))
                if q05 is not None and q95 is not None:
                    effect_span = max(float(q95 - q05), 1e-3)

        effect_size = abs(beta) * effect_span
        term_rows.append(
            {
                "term": term,
                "display_term": term.replace("_", " "),
                "term_group": str(row.get("term_group") or "control"),
                "source_column": source_column,
                "hazard_ratio": _round_or_none(hr),
                "beta": _round_or_none(beta),
                "effect_span": _round_or_none(effect_span),
                "effect_size": effect_size,
            }
        )

    if not term_rows:
        _render_placeholder_figure(output_path=output_path, title="Nomogram", message="No plottable predictors")
        return {"generated": False, "reason": "no_plottable_predictors", "model_name": selected_model}

    max_effect = max(item["effect_size"] for item in term_rows)
    if not np.isfinite(max_effect) or max_effect <= 0:
        max_effect = 1.0
    for item in term_rows:
        item["points"] = _round_or_none((item["effect_size"] / max_effect) * 100.0)
    term_rows = sorted(term_rows, key=lambda item: float(item.get("points") or 0.0), reverse=True)[:10]

    prevalence = _to_optional_float(pd.to_numeric(df[outcome_column], errors="coerce").mean()) or 0.5
    prevalence = min(max(prevalence, 1e-4), 1.0 - 1e-4)
    mean_abs_beta = float(np.mean([abs(float(item.get("beta") or 0.0)) for item in term_rows]))
    risk_scale = float(np.clip(2.0 * mean_abs_beta, 0.6, 4.0))

    _plot_nomogram_points(
        model_name=selected_model,
        outcome_column=outcome_column,
        prevalence=prevalence,
        terms=term_rows,
        risk_scale=risk_scale,
        output_path=output_path,
    )
    return {
        "generated": True,
        "model_name": selected_model,
        "term_count": len(term_rows),
        "outcome_column": outcome_column,
        "event_prevalence": _round_or_none(prevalence),
        "risk_scale": _round_or_none(risk_scale),
        "terms": [
            {
                "term": item["term"],
                "source_column": item["source_column"],
                "term_group": item["term_group"],
                "hazard_ratio": item["hazard_ratio"],
                "beta": item["beta"],
                "points": item["points"],
            }
            for item in term_rows
        ],
    }


def _prepare_probability_model(item: dict[str, Any]) -> dict[str, Any] | None:
    observed = pd.to_numeric(pd.Series(item.get("observed", [])), errors="coerce")
    predicted = pd.to_numeric(pd.Series(item.get("predicted", [])), errors="coerce")
    aligned = pd.DataFrame({"observed": observed, "predicted": predicted}).dropna()
    if aligned.empty or aligned["observed"].nunique(dropna=True) < 2:
        return None

    payload = dict(item)
    payload["observed"] = aligned["observed"].astype(int).to_numpy(dtype=float)
    payload["predicted"] = np.clip(aligned["predicted"].to_numpy(dtype=float), 1e-6, 1.0 - 1e-6)
    auc_value = _to_optional_float(payload.get("auc"))
    if auc_value is None:
        auc_value = _safe_binary_auc(payload["observed"], payload["predicted"])
    payload["auc"] = auc_value
    return payload


def _select_probability_model(
    probability_models: list[dict[str, Any]],
    *,
    preferred_model_name: str = "",
) -> dict[str, Any] | None:
    prepared = [item for item in (_prepare_probability_model(model) for model in probability_models) if item is not None]
    if not prepared:
        return None

    preferred_name = str(preferred_model_name or "").strip()
    if preferred_name:
        for item in prepared:
            if str(item.get("model_name", "")).strip() == preferred_name:
                return item
    return max(prepared, key=lambda item: float(item.get("auc") or -1.0))


def _build_calibration_summary(
    probability_models: list[dict[str, Any]],
    preferred_model_name: str = "",
) -> dict[str, Any]:
    selected = _select_probability_model(probability_models, preferred_model_name=preferred_model_name)
    if selected is None:
        return {}

    observed = np.asarray(selected["observed"], dtype=float)
    predicted = np.asarray(selected["predicted"], dtype=float)
    bin_count = int(min(10, max(4, len(predicted) // 25 or 4)))
    fraction_positive, mean_predicted = calibration_curve(
        observed,
        predicted,
        n_bins=bin_count,
        strategy="quantile",
    )
    return {
        "model_name": selected["model_name"],
        "auc": _round_or_none(selected.get("auc")),
        "brier_score": _round_or_none(float(brier_score_loss(observed, predicted))),
        "bin_count": bin_count,
        "observed_event_rate": _round_or_none(float(np.mean(observed))),
        "predicted_mean_probability": _round_or_none(float(np.mean(predicted))),
        "calibration_points": [
            {
                "mean_predicted_probability": _round_or_none(float(x)),
                "fraction_positive": _round_or_none(float(y)),
            }
            for x, y in zip(mean_predicted, fraction_positive, strict=False)
        ],
    }


def _plot_calibration_curve(*, calibration_summary: dict[str, Any], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(5.4, 4.4))
    points = calibration_summary.get("calibration_points", [])
    if not points:
        ax.text(0.5, 0.5, "No calibration-ready model", ha="center", va="center", fontsize=12)
        ax.set_axis_off()
    else:
        x = [float(item["mean_predicted_probability"]) for item in points]
        y = [float(item["fraction_positive"]) for item in points]
        ax.plot([0, 1], [0, 1], linestyle="--", color="#999999", linewidth=1.2, label="Ideal")
        ax.plot(x, y, marker="o", linewidth=2, color="#1b4d6b", label=calibration_summary.get("model_name", "Model"))
        ax.set_xlabel("Predicted probability")
        ax.set_ylabel("Observed event rate")
        ax.set_title("Calibration Curve")
        ax.legend(frameon=False, loc="upper left")
        ax.grid(alpha=0.2)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _build_decision_curve_summary(
    probability_models: list[dict[str, Any]],
    preferred_model_name: str = "",
) -> dict[str, Any]:
    selected = _select_probability_model(probability_models, preferred_model_name=preferred_model_name)
    if selected is None:
        return {}

    observed = np.asarray(selected["observed"], dtype=float)
    predicted = np.asarray(selected["predicted"], dtype=float)
    thresholds = np.linspace(0.05, 0.95, 19)
    n = max(len(observed), 1)
    prevalence = float(np.mean(observed))
    model_points: list[dict[str, float]] = []
    best_net_benefit: float | None = None
    best_threshold: float | None = None

    for threshold in thresholds:
        predicted_positive = predicted >= threshold
        true_positive = float(np.sum((predicted_positive == 1) & (observed == 1)))
        false_positive = float(np.sum((predicted_positive == 1) & (observed == 0)))
        harm_weight = float(threshold / (1.0 - threshold))
        model_net_benefit = (true_positive / n) - (false_positive / n) * harm_weight
        treat_all_net_benefit = prevalence - (1.0 - prevalence) * harm_weight
        if best_net_benefit is None or model_net_benefit > best_net_benefit:
            best_net_benefit = model_net_benefit
            best_threshold = float(threshold)
        model_points.append(
            {
                "threshold": _round_or_none(float(threshold)),
                "model_net_benefit": _round_or_none(model_net_benefit),
                "treat_all_net_benefit": _round_or_none(treat_all_net_benefit),
                "treat_none_net_benefit": 0.0,
            }
        )

    return {
        "model_name": selected["model_name"],
        "auc": _round_or_none(selected.get("auc")),
        "prevalence": _round_or_none(prevalence),
        "best_threshold": _round_or_none(best_threshold),
        "best_net_benefit": _round_or_none(best_net_benefit),
        "threshold_points": model_points,
    }


def _plot_decision_curve(*, dca_summary: dict[str, Any], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(5.6, 4.4))
    points = dca_summary.get("threshold_points", [])
    if not points:
        ax.text(0.5, 0.5, "No DCA-ready model", ha="center", va="center", fontsize=12)
        ax.set_axis_off()
    else:
        thresholds = [float(item["threshold"]) for item in points]
        model_net_benefit = [float(item["model_net_benefit"]) for item in points]
        treat_all = [float(item["treat_all_net_benefit"]) for item in points]
        treat_none = [float(item["treat_none_net_benefit"]) for item in points]
        ax.plot(thresholds, model_net_benefit, linewidth=2.2, color="#1b4d6b", label=dca_summary.get("model_name", "Model"))
        ax.plot(thresholds, treat_all, linewidth=1.8, linestyle="--", color="#c97c1a", label="Treat all")
        ax.plot(thresholds, treat_none, linewidth=1.5, linestyle=":", color="#666666", label="Treat none")
        ax.set_xlabel("Threshold probability")
        ax.set_ylabel("Net benefit")
        ax.set_title("Decision Curve Analysis")
        ax.legend(frameon=False, loc="best")
        ax.grid(alpha=0.2)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _build_distribution_figure_summary(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    outcome_column: str,
    output_path: Path,
) -> dict[str, Any]:
    selected_column = _select_distribution_column(contract=contract, df=df, outcome_column=outcome_column)
    if not selected_column:
        _render_placeholder_figure(output_path=output_path, title="Distribution Figure", message="No suitable variable found")
        return {"selected_column": "", "plot_type": "placeholder", "style_hints": []}

    style_hints = _style_hints_for_output(contract, "distribution_figure")
    is_numeric = _is_numeric_series(df[selected_column])
    if is_numeric:
        plot_type = "violin_plot" if "violin_plot" in style_hints else "box_plot"
        _plot_numeric_distribution(
            series=df[selected_column],
            outcome=df[outcome_column],
            output_path=output_path,
            variable_name=selected_column,
            plot_type=plot_type,
            overlay_points=("raw_points_overlay" in style_hints),
        )
    else:
        plot_type = "grouped_bar"
        _plot_categorical_distribution(
            series=df[selected_column],
            outcome=df[outcome_column],
            output_path=output_path,
            variable_name=selected_column,
        )
    return {
        "selected_column": selected_column,
        "plot_type": plot_type,
        "style_hints": style_hints,
    }


def _build_heatmap_figure_summary(
    *,
    df: pd.DataFrame,
    contract: TaskContract,
    outcome_column: str,
    output_path: Path,
    matrix_output_path: Path,
) -> dict[str, Any]:
    candidate_columns = _select_heatmap_columns(contract=contract, df=df, outcome_column=outcome_column)
    if len(candidate_columns) < 2:
        _render_placeholder_figure(output_path=output_path, title="Heatmap", message="Not enough numeric variables")
        return {"columns": candidate_columns, "matrix_written": False}

    corr = df[candidate_columns].apply(pd.to_numeric, errors="coerce").corr()
    corr.to_csv(matrix_output_path, index=True)

    fig, ax = plt.subplots(figsize=(0.8 * len(candidate_columns) + 2.4, 0.8 * len(candidate_columns) + 1.8))
    image = ax.imshow(corr.to_numpy(), cmap="cividis", vmin=-1, vmax=1)
    ax.set_xticks(range(len(candidate_columns)))
    ax.set_xticklabels(candidate_columns, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(candidate_columns)))
    ax.set_yticklabels(candidate_columns, fontsize=8)
    ax.set_title("Correlation Heatmap")
    for i in range(len(candidate_columns)):
        for j in range(len(candidate_columns)):
            ax.text(j, i, f"{corr.iloc[i, j]:.2f}", ha="center", va="center", color="white", fontsize=7)
    fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return {"columns": candidate_columns, "matrix_written": True}


def _build_cox_summary(cox_models: list[dict[str, Any]]) -> dict[str, Any]:
    def _score(item: dict[str, Any]) -> float:
        return float(_to_optional_float(item.get("concordance_index")) or -1.0)

    best = max(cox_models, key=_score, default=None)
    return {
        "model_count": len(cox_models),
        "best_model": best.get("model_name") if best is not None else "",
        "best_model_c_index": _to_optional_float(best.get("concordance_index")) if best is not None else None,
        "models": cox_models,
    }


def _build_roc_summary(roc_models: list[dict[str, Any]]) -> dict[str, Any]:
    best = max(roc_models, key=lambda item: float(item.get("auc") or 0.0), default=None)
    return {
        "model_count": len(roc_models),
        "best_model": best.get("model_name") if best is not None else "",
        "best_model_auc": float(best.get("auc")) if best is not None and best.get("auc") is not None else None,
        "models": roc_models,
    }


def _plot_roc_curves(*, roc_models: list[dict[str, Any]], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6, 5))
    if not roc_models:
        ax.text(0.5, 0.5, "No ROC-ready models", ha="center", va="center", fontsize=12)
        ax.set_axis_off()
    else:
        ordered_models = sorted(roc_models, key=lambda item: float(item.get("auc") or -1.0), reverse=True)
        has_score_comparators = any(
            str(item.get("model_name", "")).upper() in {"APSIII", "SAPSII", "OASIS"}
            for item in ordered_models
        )
        for model in ordered_models:
            model_name = str(model.get("model_name", "Model"))
            is_score_comparator = model_name.upper() in {"APSIII", "SAPSII", "OASIS"}
            line_style = "--" if is_score_comparator else "-"
            line_width = 1.9 if is_score_comparator else 2.3
            ax.plot(
                model["fpr"],
                model["tpr"],
                linewidth=line_width,
                linestyle=line_style,
                label=f'{model_name} (AUC={float(model["auc"]):.3f})',
            )
        ax.plot([0, 1], [0, 1], linestyle="--", color="#999999", linewidth=1.5, label="Chance")
        ax.set_xlabel("False positive rate")
        ax.set_ylabel("True positive rate")
        if has_score_comparators:
            ax.set_title("ROC Curves (Primary model vs severity scores)")
        else:
            ax.set_title("ROC Curves")
        ax.legend(frameon=False, loc="lower right")
        ax.grid(alpha=0.2)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _render_binary_outcome_report(
    *,
    contract: TaskContract,
    outcome_column: str,
    baseline_rows: int,
    logistic_df: pd.DataFrame,
    cox_df: pd.DataFrame,
    cox_summary: dict[str, Any],
    nomogram_summary: dict[str, Any],
    roc_summary: dict[str, Any],
    calibration_summary: dict[str, Any],
    dca_summary: dict[str, Any],
    distribution_summary: dict[str, Any],
    heatmap_summary: dict[str, Any],
    stats_summary: dict[str, Any],
) -> str:
    exposure_variables = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE]
    preprocessing_summary = stats_summary.get("preprocessing_summary", {})
    split_summary = stats_summary.get("train_validation_summary", {})
    comparator_models = stats_summary.get("comparator_models", [])
    comparator_names = [str(item.get("model_name")) for item in comparator_models if str(item.get("model_name", "")).strip()]
    lines = [
        f"# Binary Outcome Reproduction Report: {contract.title}",
        "",
        "## Execution Mode",
        "- Backend: hybrid_binary_runner",
        "- Posture: LLM-guided contract + deterministic local logistic/Cox execution",
        "",
        "## Study Frame",
        f"- Outcome column: {outcome_column}",
        f"- Exposure variables: {', '.join(exposure_variables) or 'not specified'}",
        f"- Analysis dataset rows: {stats_summary['row_count']}",
        "",
        "## Produced Outputs",
        *[f"- {item}" for item in stats_summary["outputs"]],
        "",
        "## Model Summary",
        f"- Logistic rows exported: {len(logistic_df)}",
        f"- Cox rows exported: {len(cox_df)}",
        f"- Cox-ready models: {cox_summary.get('model_count', 0)}",
        f"- Best Cox model: {cox_summary.get('best_model') or 'n/a'}",
        f"- Best Cox c-index: {cox_summary.get('best_model_c_index') if cox_summary.get('best_model_c_index') is not None else 'n/a'}",
        f"- ROC-ready models: {roc_summary.get('model_count', 0)}",
        f"- Best ROC model: {roc_summary.get('best_model') or 'n/a'}",
        f"- Best ROC AUC: {roc_summary.get('best_model_auc') if roc_summary.get('best_model_auc') is not None else 'n/a'}",
        f"- Train/validation split enabled: {bool(split_summary.get('enabled', False))}",
        f"- Best validation AUC: {split_summary.get('best_validation_auc') if split_summary.get('best_validation_auc') is not None else 'n/a'}",
        f"- Best validation C-index: {split_summary.get('best_validation_c_index') if split_summary.get('best_validation_c_index') is not None else 'n/a'}",
        f"- Comparator score models: {', '.join(comparator_names) if comparator_names else 'none'}",
        f"- Calibration executed: {bool(calibration_summary)}",
        f"- Decision curve executed: {bool(dca_summary)}",
        f"- Nomogram figure executed: {bool(nomogram_summary)}",
        f"- Distribution figure executed: {bool(distribution_summary)}",
        f"- Heatmap executed: {bool(heatmap_summary)}",
        f"- Missingness exclusion threshold: {preprocessing_summary.get('missingness_exclusion_threshold', 'n/a')}",
        f"- Dropped predictors (>threshold): {', '.join(preprocessing_summary.get('dropped_predictors_over_threshold', [])) or 'none'}",
        "",
        "## Notes",
        "- Logistic and Cox models run on rows available after the paper-aligned missingness preprocessing stage.",
        "- Before fitting, predictors with missing ratio above threshold are excluded and the remaining missing predictor values are imputed (median for numeric, mode for categorical).",
        "- Odds ratios and confidence intervals are computed locally with statsmodels GLM (binomial family) for logistic models.",
        "- Cox hazard ratios and confidence intervals are computed locally with lifelines CoxPHFitter.",
        "- Calibration and decision-curve outputs now prioritize the primary logistic model in the contract, and only fall back to best-AUC alternatives when needed.",
        "- ROC output includes score-based comparators (APSIII, SAPSII, OASIS) when those columns exist in the analysis dataset.",
        "- Distribution and heatmap figures are paper-guided secondary visualization artifacts; they do not claim paper-identical panel semantics unless the paper contract specifies them explicitly.",
        "- Nomogram figure is a Cox-coefficient-derived point-allocation approximation; it preserves predictor ranking intent but is not guaranteed to be publication-identical to paper-specific R nomogram implementations.",
        f"- Baseline table rows exported: {baseline_rows}",
        "",
    ]
    return "\n".join(lines) + "\n"


def _resolve_requested_output_kinds(contract: TaskContract) -> set[str]:
    requested = {
        str(item.name or item.kind).strip()
        for item in contract.outputs
        if str(item.name or item.kind).strip()
    }
    return requested or {"baseline_table", "model_results_table", "roc_figure", "reproduction_report"}


def _wants_any_output(requested_output_kinds: set[str], *kinds: str) -> bool:
    return any(kind in requested_output_kinds for kind in kinds)


def _select_best_probability_model(probability_models: list[dict[str, Any]]) -> dict[str, Any] | None:
    return _select_probability_model(probability_models, preferred_model_name="")


def _select_distribution_column(*, contract: TaskContract, df: pd.DataFrame, outcome_column: str) -> str:
    candidate_columns: list[str] = []
    for variable in contract.variables:
        if variable.role in {VariableRole.EXPOSURE, VariableRole.CONTROL, VariableRole.SUBGROUP} and variable.name in df.columns:
            candidate_columns.append(variable.name)
    candidate_columns = [column for column in _dedupe(candidate_columns) if column != outcome_column]
    numeric_candidates = [column for column in candidate_columns if _is_numeric_series(df[column])]
    if numeric_candidates:
        return numeric_candidates[0]
    return candidate_columns[0] if candidate_columns else ""


def _select_heatmap_columns(*, contract: TaskContract, df: pd.DataFrame, outcome_column: str) -> list[str]:
    candidate_columns: list[str] = []
    for variable in contract.variables:
        if variable.role in {VariableRole.EXPOSURE, VariableRole.CONTROL, VariableRole.OUTCOME, VariableRole.SUBGROUP} and variable.name in df.columns:
            candidate_columns.append(variable.name)
    numeric_candidates = [
        column
        for column in _dedupe(candidate_columns)
        if column != outcome_column and _is_numeric_series(df[column])
    ]
    if outcome_column in df.columns and _is_numeric_series(df[outcome_column]):
        numeric_candidates.append(outcome_column)
    return numeric_candidates[:8]


def _style_hints_for_output(contract: TaskContract, output_name: str) -> list[str]:
    for output in contract.outputs:
        if str(output.name or output.kind).strip() != output_name:
            continue
        hints = output.options.get("paper_style_hints", [])
        if isinstance(hints, list):
            return [str(item).strip() for item in hints if str(item).strip()]
    return []


def _plot_numeric_distribution(
    *,
    series: pd.Series,
    outcome: pd.Series,
    output_path: Path,
    variable_name: str,
    plot_type: str,
    overlay_points: bool,
) -> None:
    clean = pd.DataFrame({"value": pd.to_numeric(series, errors="coerce"), "outcome": pd.to_numeric(outcome, errors="coerce")}).dropna()
    group_zero = clean.loc[clean["outcome"] == 0, "value"].to_numpy()
    group_one = clean.loc[clean["outcome"] == 1, "value"].to_numpy()
    fig, ax = plt.subplots(figsize=(5.2, 4.2))
    if plot_type == "violin_plot":
        parts = ax.violinplot([group_zero, group_one], positions=[1, 2], showmeans=True, showextrema=False)
        for body, color in zip(parts["bodies"], ("#3b8ea5", "#c97c1a"), strict=False):
            body.set_facecolor(color)
            body.set_alpha(0.6)
    else:
        box = ax.boxplot([group_zero, group_one], patch_artist=True, widths=0.5)
        for patch, color in zip(box["boxes"], ("#3b8ea5", "#c97c1a"), strict=False):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
    if overlay_points:
        rng = np.random.default_rng(42)
        for xpos, values, color in ((1, group_zero, "#1b4d6b"), (2, group_one, "#8c2d19")):
            if len(values) == 0:
                continue
            jitter = rng.normal(loc=xpos, scale=0.04, size=len(values))
            ax.scatter(jitter, values, s=10, alpha=0.35, color=color, edgecolors="none")
    ax.set_xticks([1, 2])
    ax.set_xticklabels(["Outcome 0", "Outcome 1"])
    ax.set_ylabel(variable_name)
    ax.set_title(f"{variable_name} by outcome group")
    ax.grid(alpha=0.15, axis="y")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _plot_categorical_distribution(
    *,
    series: pd.Series,
    outcome: pd.Series,
    output_path: Path,
    variable_name: str,
) -> None:
    clean = pd.DataFrame({"value": series.fillna("Missing").astype(str), "outcome": pd.to_numeric(outcome, errors="coerce")}).dropna()
    summary = (
        clean.groupby(["value", "outcome"]).size().unstack(fill_value=0).sort_values(by=list(clean["outcome"].dropna().unique()), ascending=False)
    )
    fig, ax = plt.subplots(figsize=(max(5.2, 0.8 * len(summary.index)), 4.2))
    positions = np.arange(len(summary.index))
    zero_counts = summary.get(0.0, pd.Series(index=summary.index, data=0)).to_numpy()
    one_counts = summary.get(1.0, pd.Series(index=summary.index, data=0)).to_numpy()
    ax.bar(positions, zero_counts, color="#3b8ea5", alpha=0.8, label="Outcome 0")
    ax.bar(positions, one_counts, bottom=zero_counts, color="#c97c1a", alpha=0.8, label="Outcome 1")
    ax.set_xticks(positions)
    ax.set_xticklabels(summary.index.tolist(), rotation=45, ha="right")
    ax.set_ylabel("Count")
    ax.set_title(f"{variable_name} distribution by outcome")
    ax.legend(frameon=False)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _render_placeholder_figure(*, output_path: Path, title: str, message: str) -> None:
    fig, ax = plt.subplots(figsize=(5.2, 4.2))
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=12)
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _continuous_group_test(group_zero: pd.Series, group_one: pd.Series) -> tuple[float | None, str]:
    zero = pd.to_numeric(group_zero, errors="coerce").dropna()
    one = pd.to_numeric(group_one, errors="coerce").dropna()
    if zero.empty or one.empty:
        return None, "insufficient_data"
    use_t_test = _looks_normal(zero) and _looks_normal(one)
    if use_t_test:
        _, p_value = ttest_ind(zero, one, equal_var=False)
        return float(p_value), "welch_t_test"
    _, p_value = mannwhitneyu(zero, one, alternative="two-sided")
    return float(p_value), "mann_whitney"


def _categorical_group_test(contingency: pd.DataFrame) -> tuple[float | None, str]:
    if contingency.empty or contingency.shape[1] != 2:
        return None, "insufficient_data"
    if contingency.shape == (2, 2):
        _, fisher_p_value = fisher_exact(contingency.to_numpy())
        return float(fisher_p_value), "fisher_exact"
    _, p_value, _, _ = chi2_contingency(contingency.to_numpy())
    return float(p_value), "chi_square"


def _looks_normal(series: pd.Series) -> bool:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if len(clean) < 3:
        return False
    sampled = clean.iloc[:5000]
    _, p_value = shapiro(sampled)
    return bool(p_value > 0.05)


def _is_numeric_series(series: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(series)


def _formula_term(column: str, series: pd.Series) -> str:
    if (
        pd.api.types.is_bool_dtype(series)
        or pd.api.types.is_object_dtype(series)
        or isinstance(series.dtype, pd.CategoricalDtype)
    ):
        return f"C({_quote_name(column)})"
    return _quote_name(column)


def _term_group(term: str, exposure_variables: list[str]) -> str:
    for variable in exposure_variables:
        if variable in term:
            return "exposure"
    return "control"


def _quote_name(value: str) -> str:
    escaped = value.replace('"', '\\"')
    return f'Q("{escaped}")'


def _format_continuous(series: pd.Series) -> str:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return "NA"
    return f"{clean.mean():.2f} ± {clean.std(ddof=1):.2f}"


def _format_count_pct(count: int, total: int) -> str:
    if total <= 0:
        return f"{count} (NA)"
    return f"{count} ({(count / total) * 100:.1f}%)"


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(value)


def _dataframe_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows available._\n"
    rendered = df.fillna("").astype(str)
    columns = list(rendered.columns)
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    rows = [
        "| " + " | ".join(str(value) for value in row) + " |"
        for row in rendered.itertuples(index=False, name=None)
    ]
    return "\n".join([header, separator, *rows]) + "\n"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _safe_exp(value: float, clip: float = 50.0) -> float:
    clipped = max(min(float(value), clip), -clip)
    return float(np.exp(clipped))

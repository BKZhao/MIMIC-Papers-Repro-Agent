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
from lifelines.statistics import multivariate_logrank_test, proportional_hazard_test
from patsy import build_design_matrices, dmatrix
from scipy.stats import chi2, chi2_contingency, f_oneway, kruskal, shapiro
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score, roc_curve

from .paper_profiles import PaperExecutionProfile, SubgroupProfile, get_paper_execution_profile


QUARTILES: tuple[str, ...] = ("Q1", "Q2", "Q3", "Q4")
STRING_COLUMNS: tuple[str, ...] = ("gender", "race", "insurance", "marital_status")
DEFAULT_PALETTE: dict[str, str] = {
    "Q1": "#1b4d6b",
    "Q2": "#3b8ea5",
    "Q3": "#c97c1a",
    "Q4": "#8c2d19",
}


@dataclass(frozen=True)
class ProfileStatsRunResult:
    profile_key: str
    analysis_dataset_rel: str
    row_count: int
    outputs: list[str]
    metrics: dict[str, float | None]


def run_profile_stats(
    *,
    project_root: Path,
    profile_key: str,
    analysis_dataset_rel: str,
    missingness_rel: str = "",
    artifact_subdir: str = "",
) -> ProfileStatsRunResult:
    profile = get_paper_execution_profile(profile_key)
    if profile is None:
        raise ValueError(f"Unknown paper execution profile: {profile_key}")

    analysis_dataset_path = (project_root / analysis_dataset_rel).resolve()
    if not analysis_dataset_path.exists():
        raise FileNotFoundError(f"Analysis dataset not found: {analysis_dataset_path}")

    raw_df = _load_profile_frame(analysis_dataset_path, profile)
    model_df, imputation_manifest = _prepare_model_frame(raw_df, profile)

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

    model_df.to_csv(model_ready_path, index=False)

    baseline_df = _build_baseline_table(raw_df, profile)
    baseline_df.to_csv(baseline_path, index=False)
    baseline_md_path.write_text(_dataframe_to_markdown(baseline_df), encoding="utf-8")

    cox_rows: list[dict[str, Any]] = []
    metrics: dict[str, float | None] = {}
    fitted_models: dict[str, dict[str, Any]] = {}
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
    cox_df.to_csv(cox_path, index=False)
    cox_md_path.write_text(_dataframe_to_markdown(cox_df), encoding="utf-8")

    km_result = _fit_km_by_quartile(model_df, profile, output_path=km_plot_path)
    km_summary_path.write_text(json.dumps(km_result, indent=2, ensure_ascii=False), encoding="utf-8")
    metrics["logrank_p_value"] = _to_optional_float(km_result.get("logrank_p_value"))

    primary_adjustment = profile.model_adjustments[-1] if profile.model_adjustments else None
    primary_covariates = primary_adjustment.covariates if primary_adjustment is not None else ()
    rcs_result = _fit_rcs_model(model_df, profile, covariates=primary_covariates, model_name=primary_adjustment.name if primary_adjustment else "model_1")
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

    roc_result = _fit_roc_curve(model_df, profile)
    roc_summary_path.write_text(json.dumps(roc_result, indent=2, ensure_ascii=False), encoding="utf-8")
    _plot_roc_curve(roc_result, profile, roc_plot_path)
    metrics["roc_auc"] = _to_optional_float(roc_result.get("auc"))
    metrics["roc_threshold"] = _to_optional_float(roc_result.get("best_threshold"))

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

    stats_summary = {
        "profile": profile.as_dict(),
        "analysis_dataset_rel": analysis_dataset_rel,
        "artifact_subdir": artifact_subdir,
        "row_count": int(len(model_df)),
        "source_dataset_version": profile.source_dataset_version,
        "notes": list(profile.notes),
        "imputation_manifest": imputation_manifest,
        "missingness_summary": missingness_payload.get("missingness", {}),
        "metrics": {key: _round_or_none(value, digits=6) for key, value in metrics.items()},
        "schoenfeld": schoenfeld,
        "artifacts": [
            str(path.relative_to(project_root))
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
                roc_summary_path,
                model_ready_path,
                stats_summary_path,
                km_plot_path,
                rcs_plot_path,
                roc_plot_path,
                subgroup_plot_path,
            )
        ],
    }
    stats_summary_path.write_text(json.dumps(stats_summary, indent=2, ensure_ascii=False), encoding="utf-8")

    return ProfileStatsRunResult(
        profile_key=profile_key,
        analysis_dataset_rel=analysis_dataset_rel,
        row_count=int(len(model_df)),
        outputs=stats_summary["artifacts"],
        metrics=metrics,
    )


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

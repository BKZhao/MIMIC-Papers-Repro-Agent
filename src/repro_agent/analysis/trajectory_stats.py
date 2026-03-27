from __future__ import annotations

import json
import math
import warnings
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
from scipy.stats import chi2_contingency, kruskal
from sklearn.impute import SimpleImputer

from ..paper.profiles import ModelAdjustmentProfile, PaperExecutionProfile
from .trajectory import fit_trajectory_classes


TRAJECTORY_PALETTE: dict[str, str] = {
    "class_1": "#1b4d6b",
    "class_2": "#2e7d32",
    "class_3": "#9c27b0",
    "class_4": "#c62828",
    "class_5": "#ef6c00",
    "class_6": "#6d4c41",
}


def run_trajectory_profile_stats_workflow(
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

    df = pd.read_csv(analysis_dataset_path)
    panel_columns = [column for column in profile.trajectory_panel_columns if column in df.columns]
    if len(panel_columns) != len(profile.trajectory_panel_columns):
        missing = [column for column in profile.trajectory_panel_columns if column not in df.columns]
        raise ValueError(f"Trajectory analysis dataset is missing hourly panel columns: {missing}")

    fit = fit_trajectory_classes(
        df,
        hour_columns=panel_columns,
        n_components=max(profile.trajectory_class_count, 2),
    )
    classified_df = fit.classified_df.copy()
    group_column = profile.group_column or profile.predictor_column or "heart_rate_trajectory_class"
    classes = sorted(classified_df[group_column].dropna().astype(str).unique(), key=_class_sort_key)
    if len(classes) < 2:
        raise ValueError("Trajectory backend produced fewer than two classes; downstream survival analysis is not meaningful")

    shared_dir = project_root / "shared"
    results_dir = project_root / "results"
    if artifact_subdir.strip():
        subdir = Path(artifact_subdir.strip())
        shared_dir = shared_dir / subdir
        results_dir = results_dir / subdir
    shared_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    assignments_path = shared_dir / f"{profile.key}_trajectory_assignments.csv"
    summary_path = shared_dir / f"{profile.key}_trajectory_table.csv"
    summary_md_path = shared_dir / f"{profile.key}_trajectory_table.md"
    backend_summary_path = shared_dir / f"{profile.key}_trajectory_backend_summary.json"
    model_ready_path = shared_dir / f"{profile.key}_analysis_dataset_model_ready.csv"
    baseline_path = shared_dir / f"{profile.key}_baseline_table.csv"
    baseline_md_path = shared_dir / f"{profile.key}_baseline_table.md"
    cox_path = shared_dir / f"{profile.key}_cox_models.csv"
    cox_md_path = shared_dir / f"{profile.key}_cox_models.md"
    km_summary_path = shared_dir / f"{profile.key}_km_summary.json"
    stats_summary_path = shared_dir / f"{profile.key}_stats_summary.json"
    report_path = shared_dir / f"{profile.key}_reproduction_report.md"
    trajectory_plot_path = results_dir / f"{profile.key}_trajectory.png"
    km_plot_path = results_dir / f"{profile.key}_km.png"

    fit.summary_df.to_csv(summary_path, index=False)
    summary_md_path.write_text(_dataframe_to_markdown(fit.summary_df), encoding="utf-8")
    assignments_path.write_text(classified_df.to_csv(index=False), encoding="utf-8")
    backend_summary_path.write_text(json.dumps(fit.metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    baseline_df = _build_baseline_table(classified_df, profile, classes=classes)
    baseline_df.to_csv(baseline_path, index=False)
    baseline_md_path.write_text(_dataframe_to_markdown(baseline_df), encoding="utf-8")

    cox_df, cox_metrics = _run_cox_models(classified_df, profile, classes=classes, group_column=group_column)
    cox_df.to_csv(cox_path, index=False)
    cox_md_path.write_text(_dataframe_to_markdown(cox_df), encoding="utf-8")

    km_summary = _fit_km_by_class(classified_df, profile, classes=classes, group_column=group_column, output_path=km_plot_path)
    km_summary_path.write_text(json.dumps(km_summary, indent=2, ensure_ascii=False), encoding="utf-8")

    _plot_trajectory_profiles(fit.summary_df, profile, output_path=trajectory_plot_path)

    classified_df.to_csv(model_ready_path, index=False)

    missingness_payload: dict[str, Any] = {}
    missingness_path = (project_root / missingness_rel).resolve() if missingness_rel else None
    if missingness_path is not None and missingness_path.exists():
        missingness_payload = json.loads(missingness_path.read_text(encoding="utf-8"))
    cohort_alignment_payload: dict[str, Any] = {}
    cohort_alignment_path = shared_dir / "cohort_alignment.json"
    if cohort_alignment_path.exists():
        cohort_alignment_payload = json.loads(cohort_alignment_path.read_text(encoding="utf-8"))
    resolved_execution_environment_dataset_version = (
        execution_environment_dataset_version
        or str(cohort_alignment_payload.get("execution_environment_dataset_version", "")).strip()
    )
    resolved_execution_year_window = (
        execution_year_window
        or str(cohort_alignment_payload.get("execution_year_window", "")).strip()
        or profile.execution_year_window
    )

    metrics: dict[str, float | None] = {
        "class_count": float(len(classes)),
        "reference_class_n": float(len(classified_df.loc[classified_df[group_column] == "class_1"])),
        "logrank_p_value": _to_optional_float(km_summary.get("logrank_p_value")),
        **cox_metrics,
    }
    stats_summary = {
        "profile": profile.as_dict(),
        "analysis_dataset_rel": analysis_dataset_rel,
        "artifact_subdir": artifact_subdir,
        "row_count": int(len(classified_df)),
        "class_count": len(classes),
        "classes": classes,
        "paper_target_dataset_version": profile.source_dataset_version,
        "execution_environment_dataset_version": resolved_execution_environment_dataset_version,
        "execution_year_window": resolved_execution_year_window,
        "trajectory_backend": fit.metadata,
        "fidelity": "method_aligned_not_paper_identical",
        "method_gap": (
            "Paper-required method is LGMM. This workflow uses a local Python mixture backend to derive trajectory "
            "classes before KM and Cox analysis."
        ),
        "source_strategy": missingness_payload.get("source_strategy", {}),
        "block_status": missingness_payload.get("block_status", {}),
        "cohort_gap": {
            "paper_reported_n": int(profile.expected_final_n),
            "reproduced_n": int(len(classified_df)),
            "delta_n": int(len(classified_df) - int(profile.expected_final_n)),
        },
        "missingness_summary": missingness_payload.get("missingness", {}),
        "metrics": {key: _round_or_none(value, digits=6) for key, value in metrics.items()},
        "artifacts": [
            str(path.relative_to(project_root))
            for path in (
                assignments_path,
                summary_path,
                summary_md_path,
                backend_summary_path,
                model_ready_path,
                baseline_path,
                baseline_md_path,
                cox_path,
                cox_md_path,
                km_summary_path,
                stats_summary_path,
                report_path,
                trajectory_plot_path,
                km_plot_path,
            )
        ],
    }
    stats_summary_path.write_text(json.dumps(stats_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    report_path.write_text(
        _build_report_markdown(
            profile=profile,
            row_count=len(classified_df),
            classes=classes,
            km_summary=km_summary,
            cox_df=cox_df,
            backend_metadata=fit.metadata,
            missingness_payload=missingness_payload,
            cohort_alignment_payload=cohort_alignment_payload,
            execution_environment_dataset_version=resolved_execution_environment_dataset_version,
            execution_year_window=resolved_execution_year_window,
        ),
        encoding="utf-8",
    )
    return {
        "row_count": int(len(classified_df)),
        "outputs": stats_summary["artifacts"],
        "metrics": metrics,
    }


def _build_baseline_table(df: pd.DataFrame, profile: PaperExecutionProfile, *, classes: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    group_column = profile.group_column or profile.predictor_column
    grouped = {label: df.loc[df[group_column] == label].copy() for label in classes}

    for column in profile.baseline_continuous_columns:
        if column not in df.columns:
            continue
        non_empty = [group[column].dropna() for group in grouped.values() if column in group.columns and group[column].notna().any()]
        p_value = kruskal(*non_empty).pvalue if len(non_empty) >= 2 else None
        row = {"variable": column, "summary_type": "continuous", "level": ""}
        for label, group in grouped.items():
            row[label] = _format_median_iqr(group[column])
        row["overall"] = _format_median_iqr(df[column])
        row["p_value"] = _format_p_value(p_value)
        rows.append(row)

    for column in profile.baseline_categorical_columns:
        if column not in df.columns:
            continue
        raw = df[column].fillna("")
        unique_values = [value for value in sorted(raw.astype(str).str.strip().unique()) if value != ""]
        if not unique_values:
            continue
        binary_like = set(unique_values).issubset({"0", "1", "False", "True", "false", "true"})
        levels = ["1"] if binary_like and "1" in unique_values else unique_values
        contingency_values = []
        for label in classes:
            series = grouped[label][column].fillna("").astype(str).str.strip()
            contingency_values.append([int((series == value).sum()) for value in unique_values])
        try:
            p_value = chi2_contingency(np.array(contingency_values)).pvalue if len(classes) >= 2 else None
        except ValueError:
            p_value = None
        for index, value in enumerate(levels):
            row = {
                "variable": column,
                "summary_type": "categorical",
                "level": value,
            }
            for label, group in grouped.items():
                series = group[column].fillna("").astype(str).str.strip()
                row[label] = _format_count_pct(int((series == value).sum()), len(series))
            overall_series = df[column].fillna("").astype(str).str.strip()
            row["overall"] = _format_count_pct(int((overall_series == value).sum()), len(overall_series))
            row["p_value"] = _format_p_value(p_value) if index == 0 else ""
            rows.append(row)
    return pd.DataFrame(rows)


def _run_cox_models(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    *,
    classes: list[str],
    group_column: str,
) -> tuple[pd.DataFrame, dict[str, float | None]]:
    rows: list[dict[str, Any]] = []
    metrics: dict[str, float | None] = {}
    for adjustment in profile.model_adjustments:
        result = _fit_class_cox_model(
            df=df,
            profile=profile,
            adjustment=adjustment,
            classes=classes,
            group_column=group_column,
        )
        rows.extend(result["rows"])
        for key, value in result["metrics"].items():
            metrics[key] = value
    return pd.DataFrame(rows), metrics


def _fit_class_cox_model(
    *,
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    adjustment: ModelAdjustmentProfile,
    classes: list[str],
    group_column: str,
) -> dict[str, Any]:
    duration_col = profile.duration_column
    event_col = profile.event_column
    keep_columns = [duration_col, event_col, group_column, *adjustment.covariates]
    keep_columns = [column for column in keep_columns if column in df.columns]
    frame = df.loc[:, keep_columns].copy()
    frame = frame.dropna(subset=[duration_col, event_col, group_column])
    if frame.empty:
        return {"rows": [], "metrics": {}}

    frame[group_column] = pd.Categorical(frame[group_column].astype(str), categories=classes, ordered=True)
    categorical_columns = [group_column]
    for column in adjustment.covariates:
        if column not in frame.columns:
            continue
        if frame[column].dtype == object or column in {"gender", "race", "marital_status"}:
            categorical_columns.append(column)

    numeric_columns = [
        column
        for column in frame.columns
        if column not in categorical_columns and column not in {duration_col, event_col}
    ]
    if numeric_columns:
        numeric_imputer = SimpleImputer(strategy="median")
        frame.loc[:, numeric_columns] = numeric_imputer.fit_transform(frame.loc[:, numeric_columns])
    other_categoricals = [column for column in categorical_columns if column != group_column]
    for column in other_categoricals:
        frame[column] = frame[column].fillna("").astype(str).replace({"": "unknown"})
    frame = pd.get_dummies(frame, columns=categorical_columns, drop_first=True)

    fit = _fit_cox(frame, duration_col=duration_col, event_col=event_col)
    if fit is None:
        return {"rows": [], "metrics": {f"{adjustment.name}_global_p_value": None}}

    summary = fit.summary.reset_index().rename(columns={"covariate": "term"})
    group_prefix = f"{group_column}_"
    group_rows = summary.loc[summary["term"].astype(str).str.startswith(group_prefix)].copy()
    results: list[dict[str, Any]] = []
    metrics: dict[str, float | None] = {
        f"{adjustment.name}_global_p_value": _to_optional_float(getattr(fit.log_likelihood_ratio_test(), "p_value", None))
    }
    for _, row in group_rows.iterrows():
        term = str(row["term"])
        class_name = term.split(group_prefix, 1)[-1]
        hr = float(np.exp(row["coef"]))
        lower = float(np.exp(row["coef lower 95%"]))
        upper = float(np.exp(row["coef upper 95%"]))
        p_value = _to_optional_float(row["p"])
        results.append(
            {
                "model_name": adjustment.name,
                "term": f"{class_name} vs {profile.reference_group or 'class_1'}",
                "hazard_ratio": round(hr, 6),
                "ci_lower_95": round(lower, 6),
                "ci_upper_95": round(upper, 6),
                "p_value": _round_or_none(p_value, digits=6),
                "n": int(fit._n_examples),
                "events": int(frame[event_col].sum()),
                "covariates": ", ".join(adjustment.covariates),
            }
        )
        metrics[f"{adjustment.name}_{class_name}_hr"] = hr
    return {"rows": results, "metrics": metrics}


def _fit_km_by_class(
    df: pd.DataFrame,
    profile: PaperExecutionProfile,
    *,
    classes: list[str],
    group_column: str,
    output_path: Path,
) -> dict[str, Any]:
    duration_col = profile.duration_column
    event_col = profile.event_column
    km_fitters: list[KaplanMeierFitter] = []
    horizon = float(profile.km_time_horizon) if profile.km_time_horizon is not None else float(df[duration_col].max())
    xticks = list(np.linspace(0.0, horizon, num=7))
    fig, ax = plt.subplots(figsize=(10, 7))
    for label in classes:
        subset = df.loc[df[group_column] == label].copy()
        if subset.empty:
            continue
        kmf = KaplanMeierFitter(label=f"{label} (n={len(subset)})")
        kmf.fit(subset[duration_col], event_observed=subset[event_col])
        kmf.plot_survival_function(ax=ax, ci_show=False, color=TRAJECTORY_PALETTE.get(label, None), linewidth=2.0)
        km_fitters.append(kmf)

    if not km_fitters:
        raise ValueError("No trajectory classes were available for KM plotting")

    add_at_risk_counts(*km_fitters, ax=ax, xticks=xticks, rows_to_show=["At risk"])
    ax.set_title(f"{profile.title}\n30-day survival by trajectory class")
    ax.set_xlabel("Days since ICU admission")
    ax.set_ylabel("Survival probability")
    ax.set_xlim(0, horizon)
    ax.set_xticks(xticks)
    ax.set_ylim(0.0, 1.02)
    ax.grid(axis="y", alpha=0.2, linestyle="--")
    ax.legend(frameon=False, fontsize=9, loc="lower left")
    fig.subplots_adjust(left=0.10, right=0.98, top=0.90, bottom=0.30)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=220)
    plt.close(fig)

    logrank = multivariate_logrank_test(df[duration_col], df[group_column], df[event_col])
    class_summary = {
        label: {
            "n": int(len(df.loc[df[group_column] == label])),
            "events": int(df.loc[df[group_column] == label, event_col].sum()),
        }
        for label in classes
    }
    return {
        "logrank_p_value": _round_or_none(logrank.p_value, digits=6),
        "test_statistic": _round_or_none(logrank.test_statistic, digits=6),
        "classes": class_summary,
    }


def _plot_trajectory_profiles(summary_df: pd.DataFrame, profile: PaperExecutionProfile, *, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 6))
    hour_columns = [column for column in profile.trajectory_panel_columns if column in summary_df.columns]
    x = np.arange(1, len(hour_columns) + 1)
    for _, row in summary_df.iterrows():
        label = str(row["heart_rate_trajectory_class"])
        y = [float(row[column]) for column in hour_columns]
        ax.plot(
            x,
            y,
            marker="o",
            linewidth=2.0,
            color=TRAJECTORY_PALETTE.get(label, None),
            label=f"{label} (n={int(row['n'])})",
        )
    ax.set_title(f"{profile.title}: trajectory class profiles")
    ax.set_xlabel("Hourly window after ICU admission")
    ax.set_ylabel("Heart rate (bpm)")
    ax.set_xticks(x)
    ax.grid(alpha=0.2, linestyle="--")
    ax.legend(frameon=False, fontsize=9)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def _build_report_markdown(
    *,
    profile: PaperExecutionProfile,
    row_count: int,
    classes: list[str],
    km_summary: dict[str, Any],
    cox_df: pd.DataFrame,
    backend_metadata: dict[str, Any],
    missingness_payload: dict[str, Any],
    cohort_alignment_payload: dict[str, Any],
    execution_environment_dataset_version: str,
    execution_year_window: str,
) -> str:
    paper_target_n = int(profile.expected_final_n)
    delta_n = row_count - paper_target_n
    source_strategy = dict(missingness_payload.get("source_strategy", {}))
    block_status = dict(missingness_payload.get("block_status", {}))
    partial_fidelity_notes = [
        str(item).strip()
        for item in missingness_payload.get("partial_fidelity_notes", [])
        if str(item).strip()
    ]
    lines = [
        f"# {profile.title}",
        "",
        "## Paper Summary",
        "- Route: LLM-orchestrated, deterministic-tool execution",
        f"- Paper target dataset version: {profile.source_dataset_version or 'unknown'}",
        f"- Execution environment dataset version: {execution_environment_dataset_version or 'unknown'}",
        f"- Execution year window: {execution_year_window or 'unknown'}",
        f"- Paper reported cohort n: {paper_target_n}",
        f"- Paper required trajectory classes: {max(profile.trajectory_class_count, 0)}",
        "- Paper-required clustering method: LGMM",
        "",
        "## Reproduced Outputs",
        f"- Reproduced cohort/analysis rows: {row_count}",
        f"- Trajectory classes produced: {len(classes)}",
        f"- Reference class: {backend_metadata.get('reference_class', profile.reference_group or 'class_1')}",
        f"- Engine backend: {backend_metadata.get('backend', 'unknown')}",
        f"- Log-rank p-value: {_format_p_value(km_summary.get('logrank_p_value'))}",
        "",
        "### Cox Highlights",
    ]
    if cox_df.empty:
        lines.append("- Cox model did not return comparable class effects.")
    else:
        top_rows = cox_df.loc[cox_df["model_name"] == "model_3"].copy()
        if top_rows.empty:
            top_rows = cox_df
        for _, row in top_rows.head(5).iterrows():
            lines.append(
                "- "
                + f"{row['term']}: HR {_format_numeric(row['hazard_ratio'])} "
                + f"({_format_numeric(row['ci_lower_95'])}, {_format_numeric(row['ci_upper_95'])}), "
                + f"p={_format_p_value(row['p_value'])}"
            )
    lines.extend(
        [
            "",
            "## Method Gap",
            "- Fidelity: method-aligned, not paper-identical.",
            "- Paper-required method: LGMM.",
            "- Engine-supported backend: Python-only trajectory mixture route.",
            "- Missing-data handling: local staged dataset build plus deterministic model-time imputation where required.",
            "- Source strategy: derived-first with raw fallback policy.",
        ]
    )
    if source_strategy:
        lines.append(f"- Source strategy detail: {json.dumps(source_strategy, ensure_ascii=False)}")
    if partial_fidelity_notes:
        lines.append("- Partial-fidelity notes:")
        lines.extend(f"  - {item}" for item in partial_fidelity_notes[:6])
    lines.extend(
        [
            "",
            "## Cohort and Alignment Gap",
            f"- Current cohort vs paper: {row_count} vs {paper_target_n} (delta {delta_n:+d}).",
            "- Multiple ICU stay interpretation may differ from the paper's operational definition.",
            "- Hourly heart-rate completeness may differ because this engine uses a deterministic 10-hour panel rule.",
            "- Derived vitalsign and first-day derived tables may differ from raw-event semantics used in the paper workflow.",
            "- First-day covariate availability and local missingness handling can change the adjusted Cox estimates.",
            "- LGMM vs Python backend remains the main method difference even when downstream KM/Cox is executable.",
        ]
    )
    if cohort_alignment_payload:
        lines.append(f"- Cohort alignment detail: {json.dumps(cohort_alignment_payload, ensure_ascii=False)}")
    if block_status:
        lines.append("- Block diagnostics:")
        for block_name, payload in list(block_status.items())[:8]:
            if not isinstance(payload, dict):
                continue
            lines.append(
                "  - "
                + f"{block_name}: status={payload.get('status', 'unknown')}, "
                + f"source={payload.get('source', 'unknown')}, "
                + f"error={payload.get('error', '') or 'none'}"
            )
    lines.extend(
        [
            "",
            "## Notes",
            "- This report is generated from local SQL and Python statistics artifacts.",
            "- Real execution artifacts should be interpreted together with the fidelity and cohort-gap sections above.",
        ]
    )
    return "\n".join(lines) + "\n"


def _fit_cox(df: pd.DataFrame, *, duration_col: str, event_col: str) -> CoxPHFitter | None:
    if df.empty:
        return None
    for penalizer in (0.05, 0.1, 0.2):
        fitter = CoxPHFitter(penalizer=penalizer)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                fitter.fit(df, duration_col=duration_col, event_col=event_col)
            return fitter
        except (ConvergenceError, ValueError, ZeroDivisionError):
            continue
    return None


def _format_median_iqr(series: pd.Series) -> str:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return ""
    q1 = numeric.quantile(0.25)
    median = numeric.median()
    q3 = numeric.quantile(0.75)
    return f"{median:.2f} ({q1:.2f}, {q3:.2f})"


def _format_count_pct(count: int, denom: int) -> str:
    if denom <= 0:
        return ""
    return f"{count} ({(count / denom) * 100:.1f}%)"


def _dataframe_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows produced._\n"
    header = "| " + " | ".join(str(column) for column in df.columns) + " |"
    divider = "| " + " | ".join("---" for _ in df.columns) + " |"
    body = [
        "| " + " | ".join("" if pd.isna(value) else str(value) for value in row) + " |"
        for row in df.itertuples(index=False, name=None)
    ]
    return "\n".join([header, divider, *body]) + "\n"


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, int, np.floating, np.integer)):
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _round_or_none(value: Any, digits: int) -> float | None:
    numeric = _to_optional_float(value)
    if numeric is None:
        return None
    return round(numeric, digits)


def _format_p_value(value: Any) -> str:
    numeric = _to_optional_float(value)
    if numeric is None:
        return ""
    if numeric < 0.001:
        return "<0.001"
    return f"{numeric:.3f}"


def _format_numeric(value: Any, digits: int = 2) -> str:
    numeric = _to_optional_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.{digits}f}"


def _class_sort_key(value: str) -> tuple[int, str]:
    text = str(value).strip()
    if text.startswith("class_"):
        suffix = text.split("_", 1)[-1]
        try:
            return (int(suffix), text)
        except ValueError:
            pass
    return (9999, text)

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler


@dataclass(frozen=True)
class TrajectoryFitResult:
    classified_df: pd.DataFrame
    summary_df: pd.DataFrame
    metadata: dict[str, Any]


def fit_trajectory_classes(
    df: pd.DataFrame,
    *,
    hour_columns: tuple[str, ...] | list[str],
    n_components: int = 6,
    reference_target_bpm: float = 80.0,
    random_state: int = 42,
) -> TrajectoryFitResult:
    columns = [str(column).strip() for column in hour_columns if str(column).strip()]
    if not columns:
        raise ValueError("Trajectory fitting requires non-empty hour_columns")

    panel = df.loc[:, columns].apply(pd.to_numeric, errors="coerce")
    if panel.isnull().any().any():
        missing = [column for column in columns if panel[column].isnull().any()]
        raise ValueError(f"Trajectory fitting requires complete hourly panel data. Missing columns: {missing}")
    if len(panel) < n_components:
        raise ValueError(f"Trajectory fitting requires at least {n_components} rows, got {len(panel)}")

    matrix = panel.to_numpy(dtype=float)
    feature_frame = _build_quadratic_feature_frame(panel)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(feature_frame.to_numpy(dtype=float))

    backend = "gaussian_mixture_quadratic_features"
    backend_notes: list[str] = []
    probabilities: np.ndarray | None = None
    try:
        model = GaussianMixture(
            n_components=n_components,
            covariance_type="full",
            n_init=30,
            random_state=random_state,
        )
        raw_labels = model.fit_predict(scaled)
        probabilities = model.predict_proba(scaled)
        bic = float(model.bic(scaled))
        aic = float(model.aic(scaled))
    except Exception as exc:
        backend = "kmeans_fallback"
        backend_notes.append(f"GaussianMixture failed and the backend fell back to KMeans: {exc}")
        model = KMeans(n_clusters=n_components, n_init=30, random_state=random_state)
        raw_labels = model.fit_predict(scaled)
        bic = None
        aic = None

    class_summaries = _build_class_summaries(
        panel=panel,
        raw_labels=raw_labels,
        n_components=n_components,
        reference_target_bpm=reference_target_bpm,
    )
    ordered_raw_labels = _ordered_raw_labels(class_summaries)
    label_map = {raw_label: f"class_{index}" for index, raw_label in enumerate(ordered_raw_labels, start=1)}

    classified = df.copy()
    classified["trajectory_raw_label"] = raw_labels
    classified["heart_rate_trajectory_class"] = [label_map[int(label)] for label in raw_labels]
    classified["trajectory_reference_group"] = "class_1"
    if probabilities is not None:
        classified["trajectory_assignment_confidence"] = probabilities.max(axis=1)
    else:
        classified["trajectory_assignment_confidence"] = 1.0

    summary_rows: list[dict[str, Any]] = []
    for raw_label in ordered_raw_labels:
        stats = class_summaries[int(raw_label)]
        row = {
            "heart_rate_trajectory_class": label_map[int(raw_label)],
            "trajectory_raw_label": int(raw_label),
            "n": int(stats["n"]),
            "proportion": round(float(stats["proportion"]), 6),
            "overall_mean_hr": round(float(stats["overall_mean_hr"]), 6),
            "start_hr": round(float(stats["start_hr"]), 6),
            "end_hr": round(float(stats["end_hr"]), 6),
            "delta_hr": round(float(stats["delta_hr"]), 6),
            "mean_within_patient_sd": round(float(stats["mean_within_patient_sd"]), 6),
            "pattern_summary": str(stats["pattern_summary"]),
            "reference_target_distance": round(float(stats["reference_target_distance"]), 6),
            "assignment_backend": backend,
            "is_reference_class": label_map[int(raw_label)] == "class_1",
        }
        for column, value in stats["mean_profile"].items():
            row[column] = round(float(value), 6)
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    metadata = {
        "backend": backend,
        "n_components": int(n_components),
        "reference_class": "class_1",
        "reference_target_bpm": float(reference_target_bpm),
        "feature_mode": "quadratic_growth_features",
        "feature_columns": list(feature_frame.columns),
        "class_label_map": {str(raw): label_map[int(raw)] for raw in ordered_raw_labels},
        "backend_notes": backend_notes,
        "aic": aic,
        "bic": bic,
    }
    return TrajectoryFitResult(
        classified_df=classified,
        summary_df=summary_df,
        metadata=metadata,
    )


def _build_quadratic_feature_frame(panel: pd.DataFrame) -> pd.DataFrame:
    hours = np.linspace(-1.0, 1.0, num=panel.shape[1])
    records: list[dict[str, float]] = []
    for row in panel.to_numpy(dtype=float):
        quadratic_coef, linear_coef, intercept = np.polyfit(hours, row, deg=2)
        records.append(
            {
                "overall_mean_hr": float(np.mean(row)),
                "start_hr": float(row[0]),
                "end_hr": float(row[-1]),
                "delta_hr": float(row[-1] - row[0]),
                "patient_sd": float(np.std(row, ddof=0)),
                "quadratic_coef": float(quadratic_coef),
                "linear_coef": float(linear_coef),
                "intercept": float(intercept),
            }
        )
    return pd.DataFrame.from_records(records, index=panel.index)


def _build_class_summaries(
    *,
    panel: pd.DataFrame,
    raw_labels: np.ndarray,
    n_components: int,
    reference_target_bpm: float,
) -> dict[int, dict[str, Any]]:
    summaries: dict[int, dict[str, Any]] = {}
    total_n = len(panel)
    for raw_label in range(n_components):
        subset = panel.loc[raw_labels == raw_label]
        if subset.empty:
            continue
        mean_profile = subset.mean(axis=0)
        overall_mean = float(mean_profile.mean())
        start_hr = float(mean_profile.iloc[0])
        end_hr = float(mean_profile.iloc[-1])
        delta_hr = end_hr - start_hr
        within_patient_sd = float(subset.std(axis=1).mean())
        summaries[int(raw_label)] = {
            "n": int(len(subset)),
            "proportion": float(len(subset) / total_n),
            "mean_profile": mean_profile,
            "overall_mean_hr": overall_mean,
            "start_hr": start_hr,
            "end_hr": end_hr,
            "delta_hr": delta_hr,
            "mean_within_patient_sd": within_patient_sd,
            "reference_target_distance": abs(overall_mean - reference_target_bpm),
            "pattern_summary": _summarize_pattern(overall_mean, delta_hr, within_patient_sd),
        }
    return summaries


def _ordered_raw_labels(class_summaries: dict[int, dict[str, Any]]) -> list[int]:
    if not class_summaries:
        return []
    reference_raw = min(
        class_summaries,
        key=lambda raw: (
            class_summaries[raw]["reference_target_distance"],
            abs(class_summaries[raw]["delta_hr"]),
            class_summaries[raw]["mean_within_patient_sd"],
            class_summaries[raw]["overall_mean_hr"],
        ),
    )
    remaining = sorted(
        [raw for raw in class_summaries if raw != reference_raw],
        key=lambda raw: (
            class_summaries[raw]["overall_mean_hr"],
            abs(class_summaries[raw]["delta_hr"]),
            class_summaries[raw]["mean_within_patient_sd"],
        ),
    )
    return [int(reference_raw), *[int(raw) for raw in remaining]]


def _summarize_pattern(overall_mean: float, delta_hr: float, within_patient_sd: float) -> str:
    slope = "stable"
    if delta_hr >= 6.0:
        slope = "increasing"
    elif delta_hr <= -6.0:
        slope = "decreasing"

    level = "mid"
    if overall_mean < 75.0:
        level = "low"
    elif overall_mean > 110.0:
        level = "high"

    variability = "steady"
    if within_patient_sd >= 10.0:
        variability = "fluctuating"
    elif within_patient_sd >= 6.0:
        variability = "moderately_variable"

    return f"{level}_{slope}_{variability}"

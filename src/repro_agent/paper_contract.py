from __future__ import annotations

from typing import Any


PAPER_TYG_QUARTILE_BOUNDS: tuple[float, float, float] = (8.56, 9.03, 9.56)
PAPER_TYG_QUARTILE_TARGET_COUNTS: dict[str, int] = {
    "Q1": 436,
    "Q2": 435,
    "Q3": 435,
    "Q4": 436,
}

PAPER_PRIMARY_METRIC_TARGETS: list[dict[str, float]] = [
    {"metric": "in_hospital_q4_m1_hr", "target": 1.63},
    {"metric": "icu_q4_m1_hr", "target": 1.79},
    {"metric": "icu_q4_m3_hr", "target": 3.40},
    {"metric": "rcs_inflection", "target": 8.9},
]

PAPER_BASELINE_TARGETS: dict[str, dict[str, Any]] = {
    "age": {"kind": "continuous_mean", "target": 63.90, "unit": "years"},
    "weight_kg": {"kind": "continuous_mean", "target": 86.23, "unit": "kg"},
    "height_cm": {"kind": "continuous_mean", "target": 169.90, "unit": "cm"},
    "gender_male_pct": {"kind": "categorical_pct", "target": 57.23, "unit": "%"},
    "white_blood_cell_count": {"kind": "continuous_mean", "target": 13.48, "unit": "10^9/L"},
    "albumin": {"kind": "continuous_mean", "target": 3.08, "unit": "g/L"},
    "blood_glucose": {"kind": "continuous_mean", "target": 151.31, "unit": "mg/dL"},
    "hba1c": {"kind": "continuous_mean", "target": 6.24, "unit": "%"},
    "triglycerides": {"kind": "continuous_mean", "target": 186.48, "unit": "mg/dL"},
    "type2_diabetes_pct": {"kind": "categorical_pct", "target": 26.81, "unit": "%"},
    "hypertension_pct": {"kind": "categorical_pct", "target": 43.74, "unit": "%"},
    "heart_failure_pct": {"kind": "categorical_pct", "target": 27.04, "unit": "%"},
    "sofa_score": {"kind": "continuous_mean", "target": 6.62, "unit": "score"},
    "apache_iii_score": {"kind": "continuous_mean", "target": 53.81, "unit": "score"},
    "saps_ii_score": {"kind": "continuous_mean", "target": 40.91, "unit": "score"},
}

PAPER_KM_TARGETS: dict[str, float] = {
    "hospital_logrank_p_value": 0.012,
    "icu_logrank_p_value": 0.023,
}

PAPER_FIGURE_TARGETS: dict[str, Any] = {
    "figure2": {
        "x_axis_unit": "days",
        "x_axis_max_days": 36,
        "risk_table_groups": ["Q1", "Q2", "Q3", "Q4"],
        "show_logrank_p_value": True,
    },
    "figure3": {
        "panels": [
            {"panel": "a", "endpoint": "hospital", "model": "cox_m1_rcs_unadjusted"},
            {"panel": "b", "endpoint": "hospital", "model": "cox_m3_rcs_adjusted"},
            {"panel": "c", "endpoint": "icu", "model": "cox_m1_rcs_unadjusted"},
            {"panel": "d", "endpoint": "icu", "model": "cox_m3_rcs_adjusted"},
        ],
        "p_value_source": "likelihood_ratio_test",
        "caption_note": (
            "The p-values presented in the figures were derived from a likelihood ratio test "
            "comparing the spline model to the null model."
        ),
        "expected_nonlinearity_note": "All P-values for nonlinearity were less than 0.001.",
    },
}

PAPER_RCS_TARGETS: dict[str, dict[str, float]] = {
    "hospital_unadjusted": {"overall_p_value": 0.628, "nonlinearity_p_value": 0.432},
    "hospital_adjusted": {"overall_p_value": 0.001, "nonlinearity_p_value": 0.001, "inflection": 8.9},
    "icu_unadjusted": {"overall_p_value": 0.342, "nonlinearity_p_value": 0.188},
    "icu_adjusted": {"overall_p_value": 0.001, "nonlinearity_p_value": 0.001, "inflection": 8.9},
}

PAPER_SUSPECT_METRICS: list[str] = [
    "in_hospital_q3_m3_hr",
    "in_hospital_q4_m3_hr",
]


def build_paper_alignment_contract() -> dict[str, Any]:
    return {
        "name": "mimic_iv_tyg_sepsis_alignment",
        "source_files": [
            "papers/MIMIC.md",
            "papers/table.md",
            "papers/si.docx",
        ],
        "cohort_targets": {
            "final_n": 1742,
            "in_hospital_mortality_n": 344,
            "icu_mortality_n": 257,
            "tyg_quartile_bounds": {
                "q1_max": PAPER_TYG_QUARTILE_BOUNDS[0],
                "q2_max": PAPER_TYG_QUARTILE_BOUNDS[1],
                "q3_max": PAPER_TYG_QUARTILE_BOUNDS[2],
            },
            "tyg_quartile_target_counts": PAPER_TYG_QUARTILE_TARGET_COUNTS,
        },
        "baseline_targets": PAPER_BASELINE_TARGETS,
        "metric_targets": PAPER_PRIMARY_METRIC_TARGETS,
        "km_targets": PAPER_KM_TARGETS,
        "figure_targets": PAPER_FIGURE_TARGETS,
        "rcs_targets": PAPER_RCS_TARGETS,
        "suspect_metrics": PAPER_SUSPECT_METRICS,
        "notes": [
            "Main truth source is papers/MIMIC.md, cross-checked with papers/table.md and papers/si.docx.",
            "In-hospital Model 3 quartile values that reverse direction are treated as suspect OCR/table transcription artifacts.",
        ],
    }


def assign_paper_tyg_quartile(tyg_index: float | None) -> str | None:
    if tyg_index is None:
        return None
    q1_max, q2_max, q3_max = PAPER_TYG_QUARTILE_BOUNDS
    if tyg_index <= q1_max:
        return "Q1"
    if tyg_index <= q2_max:
        return "Q2"
    if tyg_index <= q3_max:
        return "Q3"
    return "Q4"

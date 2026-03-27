from __future__ import annotations

from pathlib import Path
from typing import Any

from .materials import parse_structured_paper_targets


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
    "age": {"kind": "continuous_mean", "target": 63.90, "unit": "years", "source_file": "papers/table.md"},
    "weight_kg": {"kind": "continuous_mean", "target": 86.23, "unit": "kg", "source_file": "papers/table.md"},
    "height_cm": {"kind": "continuous_mean", "target": 169.90, "unit": "cm", "source_file": "papers/table.md"},
    "gender_male_pct": {"kind": "categorical_pct", "target": 57.23, "unit": "%", "source_file": "papers/table.md"},
    "white_blood_cell_count": {"kind": "continuous_mean", "target": 13.48, "unit": "10^9/L", "source_file": "papers/table.md"},
    "albumin": {"kind": "continuous_mean", "target": 3.08, "unit": "g/L", "source_file": "papers/table.md"},
    "blood_glucose": {"kind": "continuous_mean", "target": 151.31, "unit": "mg/dL", "source_file": "papers/table.md"},
    "hba1c": {"kind": "continuous_mean", "target": 6.24, "unit": "%", "source_file": "papers/table.md"},
    "triglycerides": {"kind": "continuous_mean", "target": 186.48, "unit": "mg/dL", "source_file": "papers/table.md"},
    "type2_diabetes_pct": {"kind": "categorical_pct", "target": 26.81, "unit": "%", "source_file": "papers/table.md"},
    "hypertension_pct": {"kind": "categorical_pct", "target": 43.74, "unit": "%", "source_file": "papers/table.md"},
    "heart_failure_pct": {"kind": "categorical_pct", "target": 27.04, "unit": "%", "source_file": "papers/table.md"},
    "sofa_score": {"kind": "continuous_mean", "target": 6.62, "unit": "score", "source_file": "papers/table.md"},
    "apache_iii_score": {"kind": "continuous_mean", "target": 53.81, "unit": "score", "source_file": "papers/table.md"},
    "saps_ii_score": {"kind": "continuous_mean", "target": 40.91, "unit": "score", "source_file": "papers/table.md"},
}

PAPER_KM_TARGETS: dict[str, float] = {
    "hospital_logrank_p_value": 0.012,
    "icu_logrank_p_value": 0.023,
}

PAPER_FIGURE_TARGETS: dict[str, Any] = {
    "figure2": {
        "panels": [
            {"panel": "a", "endpoint": "hospital", "title": "A. Cumulative survival during hospitalization"},
            {"panel": "b", "endpoint": "icu", "title": "B. Cumulative survival during ICU stay"},
        ],
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


def build_paper_alignment_contract(project_root: Path | None = None) -> dict[str, Any]:
    root = project_root or Path(__file__).resolve().parents[2]
    parsed_targets = parse_structured_paper_targets(root / "papers" / "MIMIC.md")

    contract = {
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
        "baseline_targets": dict(PAPER_BASELINE_TARGETS),
        "supplement_baseline_targets": {},
        "metric_targets": list(PAPER_PRIMARY_METRIC_TARGETS),
        "cox_table_targets": [],
        "km_targets": dict(PAPER_KM_TARGETS),
        "figure_targets": dict(PAPER_FIGURE_TARGETS),
        "rcs_targets": dict(PAPER_RCS_TARGETS),
        "suspect_metrics": list(PAPER_SUSPECT_METRICS),
        "parsed_target_counts": {},
        "notes": [
            "Main truth source is papers/MIMIC.md, cross-checked with papers/table.md and papers/si.docx.",
            "In-hospital Model 3 quartile values that reverse direction are treated as suspect OCR/table transcription artifacts.",
        ],
    }

    _merge_contract_targets(contract, parsed_targets)
    return contract


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


def _merge_contract_targets(contract: dict[str, Any], parsed_targets: dict[str, Any]) -> None:
    if not parsed_targets:
        return

    source_files = list(contract.get("source_files", []))
    for item in parsed_targets.get("source_files", []):
        if item not in source_files:
            source_files.append(item)
    contract["source_files"] = source_files

    cohort_targets = dict(contract.get("cohort_targets", {}))
    cohort_targets.update(dict(parsed_targets.get("cohort_targets", {})))
    contract["cohort_targets"] = cohort_targets

    baseline_targets = dict(contract.get("baseline_targets", {}))
    baseline_targets.update(dict(parsed_targets.get("baseline_targets", {})))
    contract["baseline_targets"] = baseline_targets

    contract["supplement_baseline_targets"] = dict(parsed_targets.get("supplement_baseline_targets", {}))
    contract["cox_table_targets"] = list(parsed_targets.get("cox_table_targets", []))

    km_targets = dict(contract.get("km_targets", {}))
    km_targets.update(dict(parsed_targets.get("km_targets", {})))
    contract["km_targets"] = km_targets

    rcs_targets = dict(contract.get("rcs_targets", {}))
    rcs_targets.update(dict(parsed_targets.get("rcs_targets", {})))
    contract["rcs_targets"] = rcs_targets

    metric_targets = _derive_primary_metric_targets(
        cox_table_targets=contract["cox_table_targets"],
        rcs_targets=contract["rcs_targets"],
        fallbacks=list(contract.get("metric_targets", [])),
    )
    contract["metric_targets"] = metric_targets
    contract["parsed_target_counts"] = dict(parsed_targets.get("parsed_target_counts", {}))

    notes = list(contract.get("notes", []))
    for note in parsed_targets.get("notes", []):
        if note not in notes:
            notes.append(note)
    if contract["supplement_baseline_targets"]:
        notes.append("Supplementary baseline targets are now part of structured verification.")
    if contract["cox_table_targets"]:
        notes.append("Structured Cox targets parsed from papers/table.md are included in verification.")
    contract["notes"] = notes


def _derive_primary_metric_targets(
    cox_table_targets: list[dict[str, Any]],
    rcs_targets: dict[str, dict[str, float]],
    fallbacks: list[dict[str, Any]],
) -> list[dict[str, float]]:
    fallback_map = {str(item.get("metric")): float(item.get("target")) for item in fallbacks if item.get("metric") and item.get("target") is not None}
    target_map = dict(fallback_map)

    desired = {
        "in_hospital_q4_m1_hr": ("hospital", "cox_m1_quartile_unadjusted", "Q4_vs_Q1"),
        "icu_q4_m1_hr": ("icu", "cox_m1_quartile_unadjusted", "Q4_vs_Q1"),
        "icu_q4_m3_hr": ("icu", "cox_m3_quartile_adjusted", "Q4_vs_Q1"),
    }
    for metric_name, key in desired.items():
        parsed = _lookup_cox_target(cox_table_targets, *key)
        if parsed is not None:
            target_map[metric_name] = parsed

    inflection = dict(rcs_targets.get("hospital_adjusted", {})).get("inflection")
    if inflection is None:
        inflection = dict(rcs_targets.get("icu_adjusted", {})).get("inflection")
    if inflection is not None:
        target_map["rcs_inflection"] = float(inflection)

    ordered_metrics = ("in_hospital_q4_m1_hr", "icu_q4_m1_hr", "icu_q4_m3_hr", "rcs_inflection")
    return [
        {"metric": metric_name, "target": float(target_map[metric_name])}
        for metric_name in ordered_metrics
        if metric_name in target_map
    ]


def _lookup_cox_target(
    cox_table_targets: list[dict[str, Any]],
    endpoint: str,
    model: str,
    term: str,
) -> float | None:
    for item in cox_table_targets:
        if (
            str(item.get("endpoint")) == endpoint
            and str(item.get("model")) == model
            and str(item.get("term")) == term
        ):
            value = item.get("hazard_ratio")
            if value is not None:
                return float(value)
    return None

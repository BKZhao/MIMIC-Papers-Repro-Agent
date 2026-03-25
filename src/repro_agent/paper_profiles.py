from __future__ import annotations

from dataclasses import dataclass, field

from .paper_contract import PAPER_TYG_QUARTILE_BOUNDS


@dataclass(frozen=True)
class ModelAdjustmentProfile:
    name: str
    covariates: tuple[str, ...] = ()
    description: str = ""


@dataclass(frozen=True)
class SubgroupProfile:
    column: str
    display_name: str
    kind: str = "categorical"
    levels: tuple[str, ...] = ()
    cut: float | None = None
    lower_label: str = ""
    upper_label: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "column": self.column,
            "display_name": self.display_name,
            "kind": self.kind,
            "levels": list(self.levels),
            "cut": self.cut,
            "lower_label": self.lower_label,
            "upper_label": self.upper_label,
        }


@dataclass(frozen=True)
class PaperExecutionProfile:
    key: str
    title: str
    description: str
    dataset_adapter: str = "mimic_iv"
    execution_backend: str = "deterministic_bridge"
    expected_final_n: int = 0
    predictor_column: str = ""
    predictor_quartile_column: str = ""
    quartile_bounds: tuple[float, float, float] | None = None
    event_column: str = ""
    duration_column: str = ""
    source_dataset_version: str = ""
    duration_unit: str = "hours"
    km_time_horizon: float | None = None
    baseline_continuous_columns: tuple[str, ...] = ()
    baseline_categorical_columns: tuple[str, ...] = ()
    model_adjustments: tuple[ModelAdjustmentProfile, ...] = ()
    subgroups: tuple[SubgroupProfile, ...] = ()
    notes: tuple[str, ...] = ()
    outputs: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "dataset_adapter": self.dataset_adapter,
            "execution_backend": self.execution_backend,
            "expected_final_n": self.expected_final_n,
            "predictor_column": self.predictor_column,
            "predictor_quartile_column": self.predictor_quartile_column,
            "quartile_bounds": list(self.quartile_bounds) if self.quartile_bounds else [],
            "event_column": self.event_column,
            "duration_column": self.duration_column,
            "source_dataset_version": self.source_dataset_version,
            "duration_unit": self.duration_unit,
            "km_time_horizon": self.km_time_horizon,
            "baseline_continuous_columns": list(self.baseline_continuous_columns),
            "baseline_categorical_columns": list(self.baseline_categorical_columns),
            "model_adjustments": [
                {
                    "name": item.name,
                    "covariates": list(item.covariates),
                    "description": item.description,
                }
                for item in self.model_adjustments
            ],
            "subgroups": [item.as_dict() for item in self.subgroups],
            "notes": list(self.notes),
            "outputs": list(self.outputs),
        }


MIMIC_TYG_SEPSIS_PROFILE = PaperExecutionProfile(
    key="mimic_tyg_sepsis",
    title="MIMIC-IV TyG sepsis mortality reproduction",
    description="Paper profile for the TyG sepsis mortality study already supported by the deterministic bridge.",
    expected_final_n=1742,
    predictor_column="tyg_index",
    predictor_quartile_column="tyg_quartile",
    quartile_bounds=PAPER_TYG_QUARTILE_BOUNDS,
    event_column="in_hospital_mortality",
    duration_column="hospital_survival_hours",
    source_dataset_version="MIMIC-IV v2.0",
    duration_unit="hours",
    notes=(
        "This profile is kept mainly for compatibility while the execution layer moves to profile-driven scripts.",
    ),
)


MIMIC_NLR_SEPSIS_ELDERLY_PROFILE = PaperExecutionProfile(
    key="mimic_nlr_sepsis_elderly",
    title="MIMIC-IV elderly sepsis NLR 28-day mortality reproduction",
    description=(
        "Paper profile for the neutrophil-to-lymphocyte ratio study on 28-day mortality in elderly sepsis "
        "patients from MIMIC-IV."
    ),
    expected_final_n=7522,
    predictor_column="nlr",
    predictor_quartile_column="nlr_quartile",
    quartile_bounds=(4.9, 8.8, 16.0),
    event_column="mortality_28d",
    duration_column="time_to_event_28d_days",
    source_dataset_version="MIMIC-IV v3.1",
    duration_unit="days",
    km_time_horizon=28.0,
    baseline_continuous_columns=(
        "age",
        "bmi",
        "sbp",
        "dbp",
        "resp_rate",
        "heart_rate",
        "temperature",
        "spo2",
        "white_blood_cell_count",
        "neutrophil_count",
        "lymphocyte_count",
        "hemoglobin",
        "platelet_count",
        "alanine_aminotransferase",
        "international_normalized_ratio",
        "creatinine",
        "urea_nitrogen",
        "lactate",
        "blood_ph",
        "blood_glucose",
        "sodium",
        "potassium",
        "calcium",
        "saps_ii_score",
        "sofa_score",
        "nlr",
    ),
    baseline_categorical_columns=(
        "gender",
        "race",
        "chf",
        "cvd",
        "copd",
        "mi",
        "liver_disease",
        "renal_disease",
        "diabetes",
        "mechanical_ventilation",
        "renal_replacement_therapy",
        "vasopressor_use",
    ),
    model_adjustments=(
        ModelAdjustmentProfile(
            name="model_1",
            covariates=("age", "gender"),
            description="Adjusted for age and sex.",
        ),
        ModelAdjustmentProfile(
            name="model_2",
            covariates=(
                "age",
                "gender",
                "race",
                "bmi",
                "sbp",
                "dbp",
                "resp_rate",
                "heart_rate",
                "temperature",
                "spo2",
                "hemoglobin",
                "platelet_count",
                "alanine_aminotransferase",
                "international_normalized_ratio",
                "creatinine",
                "urea_nitrogen",
                "lactate",
                "blood_ph",
                "blood_glucose",
                "sodium",
                "potassium",
                "calcium",
                "chf",
                "cvd",
                "copd",
                "mi",
                "liver_disease",
                "renal_disease",
                "diabetes",
            ),
            description="Adjusted for demographics, vitals, key labs, and comorbidities from the paper's Model 2.",
        ),
        ModelAdjustmentProfile(
            name="model_3",
            covariates=(
                "age",
                "gender",
                "race",
                "bmi",
                "sbp",
                "dbp",
                "resp_rate",
                "heart_rate",
                "temperature",
                "spo2",
                "hemoglobin",
                "platelet_count",
                "alanine_aminotransferase",
                "international_normalized_ratio",
                "creatinine",
                "urea_nitrogen",
                "lactate",
                "blood_ph",
                "blood_glucose",
                "sodium",
                "potassium",
                "calcium",
                "chf",
                "cvd",
                "copd",
                "mi",
                "liver_disease",
                "renal_disease",
                "diabetes",
                "saps_ii_score",
                "sofa_score",
                "mechanical_ventilation",
                "renal_replacement_therapy",
                "vasopressor_use",
            ),
            description="Fully adjusted model aligned to the paper's Model 3.",
        ),
    ),
    subgroups=(
        SubgroupProfile(
            column="age",
            display_name="Age",
            kind="cut",
            cut=75.0,
            lower_label="65-75",
            upper_label=">=75",
        ),
        SubgroupProfile(
            column="gender",
            display_name="Gender",
            kind="categorical",
            levels=("F", "M"),
        ),
        SubgroupProfile(
            column="bmi",
            display_name="BMI,kg/m2",
            kind="cut",
            cut=25.0,
            lower_label="<25",
            upper_label=">=25",
        ),
        SubgroupProfile(
            column="sofa_score",
            display_name="SOFA",
            kind="cut",
            cut=6.0,
            lower_label="<6",
            upper_label=">=6",
        ),
        SubgroupProfile(
            column="diabetes",
            display_name="Diabetes",
            kind="categorical",
            levels=("0", "1"),
        ),
    ),
    notes=(
        "The paper reports older sepsis patients only.",
        "NLR quartiles are anchored to the paper thresholds: <4.9, 4.9-8.8, 8.8-16.0, >16.0.",
        "Autoimmune exclusion is approximated with the derived rheumatic disease flag in charlson.",
        "The source paper used MIMIC-IV v3.1, while the local execution environment currently uses MIMIC-IV v2.2.",
    ),
    outputs=(
        "cohort_funnel",
        "analysis_dataset",
        "baseline_table",
        "cox_results_table",
        "km_figure",
        "subgroup_table",
        "rcs_figure",
        "roc_figure",
        "reproduction_report",
    ),
)


BUILTIN_PAPER_EXECUTION_PROFILES: tuple[PaperExecutionProfile, ...] = (
    MIMIC_TYG_SEPSIS_PROFILE,
    MIMIC_NLR_SEPSIS_ELDERLY_PROFILE,
)


def list_paper_execution_profiles() -> list[PaperExecutionProfile]:
    return list(BUILTIN_PAPER_EXECUTION_PROFILES)


def get_paper_execution_profile(key: str | None) -> PaperExecutionProfile | None:
    normalized = (key or "").strip().lower()
    if not normalized:
        return None
    for profile in BUILTIN_PAPER_EXECUTION_PROFILES:
        if profile.key == normalized:
            return profile
    return None

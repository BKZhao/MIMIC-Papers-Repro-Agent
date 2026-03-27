from __future__ import annotations

from dataclasses import dataclass, field

from .contract import PAPER_TYG_QUARTILE_BOUNDS


STROKE_TYG_QUARTILE_BOUNDS: tuple[float, float, float] = (8.27, 8.62, 8.98)


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
class EndpointProfile:
    key: str
    display_name: str
    event_column: str
    duration_column: str
    km_time_horizon: float | None = None
    duration_unit: str = "days"
    figure_panel: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "display_name": self.display_name,
            "event_column": self.event_column,
            "duration_column": self.duration_column,
            "km_time_horizon": self.km_time_horizon,
            "duration_unit": self.duration_unit,
            "figure_panel": self.figure_panel,
        }


@dataclass(frozen=True)
class PaperExecutionProfile:
    key: str
    title: str
    description: str
    dataset_adapter: str = "mimic_iv"
    execution_backend: str = "deterministic_bridge"
    analysis_family: str = "quartile_survival"
    expected_final_n: int = 0
    predictor_column: str = ""
    predictor_quartile_column: str = ""
    group_column: str = ""
    reference_group: str = ""
    quartile_bounds: tuple[float, float, float] | None = None
    event_column: str = ""
    duration_column: str = ""
    source_dataset_version: str = ""
    execution_year_window: str = ""
    duration_unit: str = "hours"
    km_time_horizon: float | None = None
    trajectory_class_count: int = 0
    trajectory_panel_columns: tuple[str, ...] = ()
    baseline_continuous_columns: tuple[str, ...] = ()
    baseline_categorical_columns: tuple[str, ...] = ()
    model_adjustments: tuple[ModelAdjustmentProfile, ...] = ()
    subgroups: tuple[SubgroupProfile, ...] = ()
    endpoint_profiles: tuple[EndpointProfile, ...] = ()
    notes: tuple[str, ...] = ()
    outputs: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "dataset_adapter": self.dataset_adapter,
            "execution_backend": self.execution_backend,
            "analysis_family": self.analysis_family,
            "expected_final_n": self.expected_final_n,
            "predictor_column": self.predictor_column,
            "predictor_quartile_column": self.predictor_quartile_column,
            "group_column": self.group_column,
            "reference_group": self.reference_group,
            "quartile_bounds": list(self.quartile_bounds) if self.quartile_bounds else [],
            "event_column": self.event_column,
            "duration_column": self.duration_column,
            "source_dataset_version": self.source_dataset_version,
            "execution_year_window": self.execution_year_window,
            "duration_unit": self.duration_unit,
            "km_time_horizon": self.km_time_horizon,
            "trajectory_class_count": self.trajectory_class_count,
            "trajectory_panel_columns": list(self.trajectory_panel_columns),
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
            "endpoint_profiles": [item.as_dict() for item in self.endpoint_profiles],
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
    outputs=(
        "cohort_funnel",
        "analysis_dataset",
        "baseline_table",
        "cox_results_table",
        "km_figure",
        "rcs_figure",
        "reproduction_report",
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
    execution_year_window="2008-2019",
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
        "reproduction_report",
    ),
)


MIMIC_HEART_RATE_TRAJECTORY_SEPSIS_PROFILE = PaperExecutionProfile(
    key="mimic_hr_trajectory_sepsis",
    title="MIMIC-IV sepsis heart-rate trajectory 30-day mortality reproduction",
    description=(
        "Experimental Python trajectory profile for the sepsis heart-rate trajectory paper. "
        "This route extracts the repeated 10-hour heart-rate panel, fits a deterministic local "
        "mixture backend, then runs KM and Cox analysis on the derived trajectory classes."
    ),
    execution_backend="trajectory_python_bridge",
    analysis_family="trajectory_survival",
    expected_final_n=5511,
    predictor_column="heart_rate_trajectory_class",
    group_column="heart_rate_trajectory_class",
    reference_group="class_1",
    event_column="mortality_30d",
    duration_column="time_to_event_30d_days",
    source_dataset_version="MIMIC-IV v2.0",
    execution_year_window="2008-2019",
    duration_unit="days",
    km_time_horizon=30.0,
    trajectory_class_count=6,
    trajectory_panel_columns=(
        "heart_rate_hour_1",
        "heart_rate_hour_2",
        "heart_rate_hour_3",
        "heart_rate_hour_4",
        "heart_rate_hour_5",
        "heart_rate_hour_6",
        "heart_rate_hour_7",
        "heart_rate_hour_8",
        "heart_rate_hour_9",
        "heart_rate_hour_10",
    ),
    baseline_continuous_columns=(
        "age",
        "bmi",
        "sofa_score",
        "apsiii",
        "gcs_score",
        "heart_rate_initial",
        "heart_rate_mean_10h",
        "temperature",
        "hemoglobin",
        "neutrophils_abs",
        "pt",
        "ptt",
        "lactate",
    ),
    baseline_categorical_columns=(
        "gender",
        "race",
        "marital_status",
        "peripheral_vascular_disease",
        "liver_disease",
        "mechanical_ventilation",
        "renal_replacement_therapy",
        "vasopressor_use",
        "beta_blocker_use",
    ),
    model_adjustments=(
        ModelAdjustmentProfile(
            name="model_1",
            covariates=(),
            description="Unadjusted Cox model.",
        ),
        ModelAdjustmentProfile(
            name="model_2",
            covariates=("age", "gender"),
            description="Adjusted for age and gender.",
        ),
        ModelAdjustmentProfile(
            name="model_3",
            covariates=(
                "marital_status",
                "race",
                "peripheral_vascular_disease",
                "liver_disease",
                "age",
                "charlson_score",
                "apsiii",
                "temperature",
                "hemoglobin",
                "neutrophils_abs",
                "pt",
                "ptt",
            ),
            description=(
                "Adjusted to the paper's reported Model 3 covariates: marital status, race, peripheral vascular "
                "disease, liver disease, age, Charlson score, APSIII, temperature, hemoglobin, neutrophils, PT, and PTT."
            ),
        ),
    ),
    notes=(
        "The source paper reports 5511 sepsis patients, six heart-rate trajectory classes, and 30-day all-cause mortality.",
        "The paper-required clustering method is LGMM; this repository uses a Python-only local mixture backend and must be interpreted as method-aligned rather than paper-identical.",
        "The paper explicitly states MIMIC-IV 2.0 with admissions between 2008 and 2019, so the execution cohort should be restricted to that year window for better alignment.",
        "The paper reports exclusion of patients aged <18 or >90 years, ICU stay <48h, absent consecutive 10-hour hourly heart-rate records, and multiple ICU stays.",
        "The paper used R-based missing-data handling, while this execution route stays in the local Python stack and may run against a newer local MIMIC-IV schema snapshot.",
    ),
    outputs=(
        "cohort_funnel",
        "analysis_dataset",
        "missingness_report",
        "trajectory_table",
        "baseline_table",
        "cox_results_table",
        "km_figure",
        "trajectory_figure",
        "reproduction_report",
    ),
)


MIMIC_TYG_STROKE_NONDIABETIC_PROFILE = PaperExecutionProfile(
    key="mimic_tyg_stroke_nondiabetic",
    title="MIMIC-IV non-diabetic ischemic stroke TyG mortality reproduction",
    description=(
        "Experimental paper-first executable profile for the non-diabetic ischemic stroke TyG paper. "
        "The route keeps LLM control over paper understanding and figure intent, then runs deterministic "
        "local SQL extraction and multi-endpoint survival analyses against MIMIC-IV."
    ),
    execution_backend="profile_survival_bridge",
    analysis_family="multi_endpoint_quartile_survival",
    expected_final_n=1073,
    predictor_column="tyg_index",
    predictor_quartile_column="tyg_quartile",
    quartile_bounds=STROKE_TYG_QUARTILE_BOUNDS,
    event_column="in_hospital_mortality",
    duration_column="time_to_in_hospital_event_days",
    source_dataset_version="MIMIC-IV v3.1",
    execution_year_window="2008-2022",
    duration_unit="days",
    baseline_continuous_columns=(
        "age",
        "gcs_score",
        "sofa_score",
        "saps_ii",
        "apsiii",
        "oasis",
        "sirs",
        "heart_rate",
        "sbp",
        "dbp",
        "mbp",
        "spo2",
        "temperature",
        "white_blood_cell_count",
        "platelet_count",
        "hemoglobin",
        "creatinine",
        "bun",
        "initial_glucose",
        "max_glucose",
        "triglycerides",
        "tyg_index",
    ),
    baseline_categorical_columns=(
        "gender",
        "race",
        "hypertension",
        "myocardial_infarction",
        "congestive_heart_failure",
        "chronic_pulmonary_disease",
        "peripheral_vascular_disease",
        "renal_disease",
        "iv_tpa",
        "mechanical_thrombectomy",
        "insulin_treatment",
    ),
    model_adjustments=(
        ModelAdjustmentProfile(
            name="model_1",
            covariates=("age", "gender", "race"),
            description="Adjusted for age, gender, and ethnicity.",
        ),
        ModelAdjustmentProfile(
            name="model_2",
            covariates=(
                "age",
                "gender",
                "race",
                "hypertension",
                "myocardial_infarction",
                "congestive_heart_failure",
                "renal_disease",
                "iv_tpa",
                "mechanical_thrombectomy",
            ),
            description=(
                "Adjusted for demographics plus hypertension, myocardial infarction, congestive heart failure, "
                "renal diseases, intravenous thrombolysis, and mechanical thrombectomy."
            ),
        ),
        ModelAdjustmentProfile(
            name="model_3",
            covariates=(
                "age",
                "gender",
                "race",
                "hypertension",
                "myocardial_infarction",
                "congestive_heart_failure",
                "renal_disease",
                "iv_tpa",
                "mechanical_thrombectomy",
                "gcs_score",
                "sofa_score",
                "saps_ii",
                "apsiii",
                "oasis",
                "sirs",
            ),
            description=(
                "Fully adjusted model aligned to the paper's three-stage Cox adjustment strategy."
            ),
        ),
    ),
    subgroups=(
        SubgroupProfile(
            column="age",
            display_name="Age",
            kind="cut",
            cut=65.0,
            lower_label="<65",
            upper_label=">=65",
        ),
        SubgroupProfile(
            column="gender",
            display_name="Gender",
            kind="categorical",
            levels=("F", "M"),
        ),
        SubgroupProfile(
            column="hypertension",
            display_name="Hypertension",
            kind="categorical",
            levels=("0", "1"),
        ),
        SubgroupProfile(
            column="sofa_score",
            display_name="SOFA",
            kind="cut",
            cut=4.0,
            lower_label="<4",
            upper_label=">=4",
        ),
        SubgroupProfile(
            column="insulin_treatment",
            display_name="Insulin Treatment",
            kind="categorical",
            levels=("0", "1"),
        ),
    ),
    endpoint_profiles=(
        EndpointProfile(
            key="icu",
            display_name="ICU mortality",
            event_column="icu_mortality",
            duration_column="time_to_icu_event_days",
            figure_panel="A",
        ),
        EndpointProfile(
            key="in_hospital",
            display_name="In-hospital mortality",
            event_column="in_hospital_mortality",
            duration_column="time_to_in_hospital_event_days",
            figure_panel="B",
        ),
        EndpointProfile(
            key="day_30",
            display_name="30-day mortality",
            event_column="mortality_30d",
            duration_column="time_to_event_30d_days",
            km_time_horizon=30.0,
            figure_panel="C",
        ),
        EndpointProfile(
            key="day_90",
            display_name="90-day mortality",
            event_column="mortality_90d",
            duration_column="time_to_event_90d_days",
            km_time_horizon=90.0,
            figure_panel="D",
        ),
        EndpointProfile(
            key="day_180",
            display_name="180-day mortality",
            event_column="mortality_180d",
            duration_column="time_to_event_180d_days",
            km_time_horizon=180.0,
            figure_panel="E",
        ),
        EndpointProfile(
            key="year_1",
            display_name="1-year mortality",
            event_column="mortality_1y",
            duration_column="time_to_event_1y_days",
            km_time_horizon=365.0,
            figure_panel="F",
        ),
    ),
    notes=(
        "The paper reports non-diabetic adult ischemic stroke ICU patients from MIMIC-IV v3.1 between 2008 and 2022.",
        "TyG quartiles are fixed to the paper thresholds: [7.12, 8.27), [8.27, 8.62), [8.62, 8.98), [8.98, 12.10].",
        "The executable route approximates fasting TyG availability with an admission-anchored first-day paired triglyceride-glucose lab draw because fasting state is not explicitly encoded in MIMIC-IV.",
        "Diabetes-history exclusion is approximated with diagnosis records up to the index admission plus pre-index hypoglycemic medication history so acute inpatient insulin orders do not over-exclude the cohort.",
        "The paper also reports MICE plus PSM sensitivity analyses; this profile currently prioritizes the main multivariable Cox, KM, RCS, and subgroup figures.",
    ),
    outputs=(
        "cohort_funnel",
        "cohort_flowchart_figure",
        "analysis_dataset",
        "missingness_report",
        "baseline_table",
        "cox_results_table",
        "km_figure",
        "rcs_figure",
        "subgroup_figure",
        "reproduction_report",
    ),
)


BUILTIN_PAPER_EXECUTION_PROFILES: tuple[PaperExecutionProfile, ...] = (
    MIMIC_TYG_SEPSIS_PROFILE,
    MIMIC_NLR_SEPSIS_ELDERLY_PROFILE,
    MIMIC_HEART_RATE_TRAJECTORY_SEPSIS_PROFILE,
    MIMIC_TYG_STROKE_NONDIABETIC_PROFILE,
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

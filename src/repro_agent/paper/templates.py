from __future__ import annotations

from dataclasses import dataclass

from ..contracts import TaskContract


@dataclass(frozen=True)
class StudyPatternTemplate:
    key: str
    title: str
    description: str
    required_model_families: tuple[str, ...]
    suggested_outputs: tuple[str, ...]
    notes: tuple[str, ...] = ()

    def matches(self, model_families: set[str]) -> bool:
        return all(family in model_families for family in self.required_model_families)

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "required_model_families": list(self.required_model_families),
            "suggested_outputs": list(self.suggested_outputs),
            "notes": list(self.notes),
        }


BASELINE_SUBGROUP_SPLINE = StudyPatternTemplate(
    key="baseline_subgroup_spline",
    title="Baseline + Subgroup + Spline Clinical Study",
    description=(
        "Clinical observational study with baseline comparison, Cox-style survival models, "
        "restricted cubic spline diagnostics, and subgroup analysis."
    ),
    required_model_families=("cox_regression", "restricted_cubic_spline", "subgroup_analysis"),
    suggested_outputs=(
        "cohort_funnel",
        "analysis_dataset",
        "baseline_table",
        "model_results_table",
        "km_figure",
        "rcs_figure",
        "subgroup_figure",
        "reproduction_report",
    ),
    notes=("Strongest match for the current TyG sepsis preset.",),
)

LONGITUDINAL_TRAJECTORY_SURVIVAL = StudyPatternTemplate(
    key="longitudinal_trajectory_survival",
    title="Longitudinal Trajectory Survival Study",
    description=(
        "Observational survival study that derives exposure classes from repeated measurements "
        "before running Kaplan-Meier and Cox analyses."
    ),
    required_model_families=("trajectory_mixture_model", "cox_regression", "kaplan_meier"),
    suggested_outputs=(
        "cohort_funnel",
        "analysis_dataset",
        "trajectory_table",
        "trajectory_figure",
        "baseline_table",
        "model_results_table",
        "km_figure",
        "reproduction_report",
    ),
    notes=(
        "Typical examples include repeated-vital-sign trajectory papers that require a dedicated longitudinal modeling backend.",
    ),
)

SURVIVAL_OBSERVATIONAL = StudyPatternTemplate(
    key="survival_observational",
    title="Survival Observational Study",
    description=(
        "Observational study centered on time-to-event outcomes with Kaplan-Meier, log-rank, and Cox regression."
    ),
    required_model_families=("cox_regression", "kaplan_meier"),
    suggested_outputs=(
        "cohort_funnel",
        "analysis_dataset",
        "baseline_table",
        "model_results_table",
        "km_figure",
        "reproduction_report",
    ),
)

RISK_FACTOR_REGRESSION = StudyPatternTemplate(
    key="risk_factor_regression",
    title="Risk Factor Regression Study",
    description=(
        "Clinical risk-factor study focused on logistic regression or Cox regression without full spline/subgroup diagnostics."
    ),
    required_model_families=("cox_regression",),
    suggested_outputs=(
        "cohort_funnel",
        "analysis_dataset",
        "baseline_table",
        "model_results_table",
        "reproduction_report",
    ),
)


BUILTIN_STUDY_TEMPLATES: tuple[StudyPatternTemplate, ...] = (
    LONGITUDINAL_TRAJECTORY_SURVIVAL,
    BASELINE_SUBGROUP_SPLINE,
    SURVIVAL_OBSERVATIONAL,
    RISK_FACTOR_REGRESSION,
)


def list_study_templates() -> list[StudyPatternTemplate]:
    return list(BUILTIN_STUDY_TEMPLATES)


def infer_study_template(contract: TaskContract) -> StudyPatternTemplate | None:
    families = {item.family for item in contract.models if item.family}
    if not families:
        return None
    for template in BUILTIN_STUDY_TEMPLATES:
        if template.matches(families):
            return template
    return None

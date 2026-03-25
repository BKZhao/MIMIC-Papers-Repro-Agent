from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WorkflowPhase:
    key: str
    title: str
    objective: str
    primary_skills: tuple[str, ...]
    required_inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    success_checks: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "objective": self.objective,
            "primary_skills": list(self.primary_skills),
            "required_inputs": list(self.required_inputs),
            "outputs": list(self.outputs),
            "success_checks": list(self.success_checks),
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class WorkflowContract:
    key: str
    title: str
    dataset_scope: str
    phases: tuple[WorkflowPhase, ...]
    notes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "dataset_scope": self.dataset_scope,
            "phase_count": len(self.phases),
            "phases": [phase.as_dict() for phase in self.phases],
            "notes": list(self.notes),
        }


MIMIC_PAPER_REPRODUCTION_WORKFLOW = WorkflowContract(
    key="mimic_paper_reproduction_v1",
    title="MIMIC Clinical Paper Reproduction Workflow",
    dataset_scope="MIMIC-focused retrospective clinical studies",
    phases=(
        WorkflowPhase(
            key="paper_extraction",
            title="Paper Extraction",
            objective=(
                "Read the paper from PDF or markdown, then extract the study methods, cohort definition, "
                "variables, models, tables, figures, and reported result targets."
            ),
            primary_skills=("paper_intake_and_contract",),
            required_inputs=("paper_path | paper_content", "instructions"),
            outputs=("task_contract", "paper evidence summary", "table/figure target summary"),
            success_checks=(
                "exposure, outcome, and model families are identified",
                "table and figure targets are captured as structured evidence when possible",
            ),
        ),
        WorkflowPhase(
            key="patient_screening",
            title="Patient Screening",
            objective=(
                "Translate the extracted cohort logic into MIMIC patient screening steps and produce a reusable cohort blueprint "
                "or executable cohort extraction."
            ),
            primary_skills=("mimic_cohort_execution",),
            required_inputs=("task_contract", "MIMIC PostgreSQL connectivity"),
            outputs=("cohort blueprint", "cohort funnel", "screening diagnostics"),
            success_checks=(
                "inclusion and exclusion criteria are mapped to MIMIC semantics",
                "screening counts or blocked compiler gaps are recorded",
            ),
        ),
        WorkflowPhase(
            key="dataset_and_modeling",
            title="Dataset And Modeling",
            objective=(
                "Build the analysis dataset, map exposure and outcome variables, fit the requested statistical models, "
                "and generate the table-level results needed for reproduction."
            ),
            primary_skills=("analysis_dataset_expansion", "survival_stats_execution"),
            required_inputs=("cohort", "task_contract"),
            outputs=("analysis dataset", "missingness report", "model tables", "model diagnostics"),
            success_checks=(
                "requested variables are mapped or flagged as unsupported",
                "requested model families are executed or explicitly marked planning-only",
            ),
        ),
        WorkflowPhase(
            key="figure_generation",
            title="Figure Generation",
            objective=(
                "Render reproduction figures from the computed model outputs rather than from the original paper images."
            ),
            primary_skills=("result_figure_generation",),
            required_inputs=("analysis dataset", "model outputs", "task_contract"),
            outputs=("KM / spline / subgroup / ROC figures", "figure metadata"),
            success_checks=(
                "figures are generated from reproduced data",
                "requested figure outputs and file paths are recorded",
            ),
        ),
        WorkflowPhase(
            key="result_comparison",
            title="Result Comparison",
            objective=(
                "Compare each reproduced step and output against the original paper and report exactly where deviations occur."
            ),
            primary_skills=("paper_alignment_verification",),
            required_inputs=("paper targets", "reproduced tables and figures", "task_contract"),
            outputs=("deviation report", "alignment summary", "reproduction report support notes"),
            success_checks=(
                "deviations are reported for cohort, tables, figures, and model results",
                "missing or uncertain evidence is marked explicitly instead of hidden",
            ),
            notes=(
                "The comparison step should explain whether the deviation originates from cohort logic, variable mapping, modeling, figure rendering, or missing paper evidence.",
            ),
        ),
    ),
    notes=(
        "This workflow is optimized for MIMIC-based retrospective clinical papers.",
        "PDF parsing, patient screening, modeling, figure generation, and deviation comparison must remain explicitly separated.",
    ),
)


def default_mimic_paper_workflow() -> WorkflowContract:
    return MIMIC_PAPER_REPRODUCTION_WORKFLOW

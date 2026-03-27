from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..config import PipelineConfig
from ..contracts import (
    CohortSpec,
    DatasetSpec,
    ExecutionMode,
    InteractionMode,
    ModelSpec,
    OutputSpec,
    TaskContract,
    VariableRole,
    VariableSpec,
)
from ..llm import LLMError, OpenAICompatibleClient
from ..registry.semantic import load_mimic_semantic_registry, resolve_semantic_variable
from .contract import build_paper_alignment_contract
from .materials import collect_paper_materials
from .profiles import get_paper_execution_profile
from .presets import detect_paper_preset, get_paper_preset
from .templates import infer_study_template


MODEL_KEYWORDS: dict[str, str] = {
    "cox": "cox_regression",
    "calibration": "calibration_curve",
    "decision curve": "decision_curve_analysis",
    "dca": "decision_curve_analysis",
    "fine-gray": "competing_risk",
    "forest plot": "subgroup_analysis",
    "heatmap": "heatmap_visualization",
    "histogram": "distribution_comparison",
    "kaplan": "kaplan_meier",
    "km": "kaplan_meier",
    "lasso": "lasso_feature_selection",
    "log-rank": "logrank_test",
    "logrank": "logrank_test",
    "logistic": "logistic_regression",
    "love plot": "propensity_score_matching",
    "mixed effects": "mixed_effects",
    "nomogram": "nomogram_prediction",
    "rcs": "restricted_cubic_spline",
    "roc": "roc_analysis",
    "shap": "shap_explainability",
    "spline": "restricted_cubic_spline",
    "subgroup": "subgroup_analysis",
    "interaction": "interaction_analysis",
    "anova": "anova",
    "baseline": "baseline_table",
    "boxplot": "distribution_comparison",
    "violin": "distribution_comparison",
    "lgmm": "trajectory_mixture_model",
    "latent growth mixture": "trajectory_mixture_model",
    "growth mixture": "trajectory_mixture_model",
    "trajectory": "trajectory_mixture_model",
    "xgboost": "machine_learning_prediction",
    "random forest": "machine_learning_prediction",
    "svm": "machine_learning_prediction",
}

OUTPUT_BY_MODEL: dict[str, list[str]] = {
    "baseline_table": ["baseline_table"],
    "bayesian_survival": ["posterior_survival_figure"],
    "calibration_curve": ["calibration_figure"],
    "competing_risk": ["cif_figure"],
    "cox_regression": ["model_results_table"],
    "decision_curve_analysis": ["decision_curve_figure"],
    "deep_survival_prediction": ["time_auc_figure"],
    "distribution_comparison": ["distribution_figure"],
    "heatmap_visualization": ["heatmap_figure"],
    "kaplan_meier": ["km_figure"],
    "lasso_feature_selection": ["lasso_trace_figure"],
    "logrank_test": ["km_figure"],
    "restricted_cubic_spline": ["rcs_figure"],
    "nomogram_prediction": ["nomogram_figure"],
    "propensity_score_matching": ["love_plot_figure"],
    "roc_analysis": ["roc_figure"],
    "shap_explainability": ["shap_figure"],
    "subgroup_analysis": ["subgroup_figure"],
    "interaction_analysis": ["subgroup_figure"],
    "logistic_regression": ["model_results_table"],
    "anova": ["baseline_table"],
    "trajectory_mixture_model": ["trajectory_table", "trajectory_figure"],
}

DEFAULT_OUTPUT_SPECS: dict[str, dict[str, str]] = {
    "cohort_funnel": {"kind": "cohort_funnel", "format": "json"},
    "cohort_flowchart_figure": {"kind": "cohort_flowchart_figure", "format": "png"},
    "analysis_dataset": {"kind": "analysis_dataset", "format": "csv"},
    "missingness_report": {"kind": "missingness_report", "format": "json"},
    "trajectory_table": {"kind": "trajectory_table", "format": "csv"},
    "trajectory_figure": {"kind": "trajectory_figure", "format": "png"},
    "baseline_table": {"kind": "baseline_table", "format": "csv"},
    "model_results_table": {"kind": "model_results_table", "format": "csv"},
    "cox_results_table": {"kind": "cox_results_table", "format": "csv"},
    "km_figure": {"kind": "km_figure", "format": "png"},
    "rcs_figure": {"kind": "rcs_figure", "format": "png"},
    "roc_figure": {"kind": "roc_figure", "format": "png"},
    "subgroup_figure": {"kind": "subgroup_figure", "format": "png"},
    "calibration_figure": {"kind": "calibration_figure", "format": "png"},
    "decision_curve_figure": {"kind": "decision_curve_figure", "format": "png"},
    "nomogram_figure": {"kind": "nomogram_figure", "format": "png"},
    "shap_figure": {"kind": "shap_figure", "format": "png"},
    "love_plot_figure": {"kind": "love_plot_figure", "format": "png"},
    "cif_figure": {"kind": "cif_figure", "format": "png"},
    "lasso_trace_figure": {"kind": "lasso_trace_figure", "format": "png"},
    "time_auc_figure": {"kind": "time_auc_figure", "format": "png"},
    "posterior_survival_figure": {"kind": "posterior_survival_figure", "format": "png"},
    "distribution_figure": {"kind": "distribution_figure", "format": "png"},
    "heatmap_figure": {"kind": "heatmap_figure", "format": "png"},
    "reproduction_report": {"kind": "reproduction_report", "format": "md"},
}

PAPER_FIGURE_OUTPUT_MAP: dict[str, str] = {
    "figure1": "cohort_flowchart_figure",
    "figure2": "km_figure",
    "figure3": "rcs_figure",
    "figure4": "subgroup_figure",
}

STRUCTURED_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "exposure_variables": ("自变量", "暴露变量", "暴露因素", "exposure", "exposures", "independent variable", "independent variables"),
    "outcome_variables": ("因变量", "结局变量", "结局", "outcome", "outcomes", "dependent variable", "dependent variables"),
    "control_variables": ("控制变量", "协变量", "校正变量", "covariates", "covariate", "controls", "control variables", "adjusted for"),
    "subgroup_variables": ("亚组变量", "亚组", "subgroup", "subgroups"),
    "time_variables": ("时间变量", "生存时间变量", "time variable", "time variables"),
    "models": ("模型", "分析模型", "models", "model"),
    "outputs": ("输出", "产物", "outputs", "output"),
    "cohort_logic": ("队列逻辑", "cohort logic"),
}

STRUCTURED_FIELD_LOOKUP: dict[str, str] = {
    alias.lower(): field
    for field, aliases in STRUCTURED_FIELD_ALIASES.items()
    for alias in aliases
}

STRUCTURED_FIELD_PATTERN = re.compile(
    r"(?P<label>"
    + "|".join(sorted((re.escape(alias) for alias in STRUCTURED_FIELD_LOOKUP), key=len, reverse=True))
    + r")\s*[:：]",
    flags=re.IGNORECASE,
)

MIMIC_IV_VERSION_PATTERN = re.compile(
    r"mimic[\s-]*iv(?:\s+database)?(?:\s*[-,(])?\s*(?:version\s*)?(?P<version>v?\d+\.\d+)",
    flags=re.IGNORECASE,
)
YEAR_WINDOW_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bbetween\s+(?P<start>20\d{2})\s+and\s+(?P<end>20\d{2})\b", flags=re.IGNORECASE),
    re.compile(r"\bfrom\s+(?P<start>20\d{2})\s+to\s+(?P<end>20\d{2})\b", flags=re.IGNORECASE),
    re.compile(r"\b(?P<start>20\d{2})\s*[–—-]\s*(?P<end>20\d{2})\b", flags=re.IGNORECASE),
)
YEAR_WINDOW_CONTEXT_KEYWORDS: tuple[str, ...] = (
    "mimic",
    "database",
    "data source",
    "study population",
    "patients were",
    "clinical data",
    "admission",
    "icu",
)


@dataclass
class TaskBuildResult:
    contract: TaskContract
    used_llm: bool
    llm_error: str = ""
    paper_materials: dict[str, str] | None = None
    paper_evidence: dict[str, Any] | None = None


def build_task_contract(
    *,
    project_root: Path,
    config: PipelineConfig,
    paper_path: str,
    instructions: str,
    session_id: str = "",
    use_llm: bool = True,
) -> TaskBuildResult:
    paper_abspath = (project_root / paper_path).resolve()
    materials = collect_paper_materials(paper_abspath)
    task_id = session_id or f"task-{uuid4().hex[:12]}"
    llm_error = ""
    paper_evidence: dict[str, Any] | None = None

    if use_llm:
        client = OpenAICompatibleClient(config.llm)
        if client.is_enabled():
            try:
                parser_route = config.agent_routes.get("paper_parser_agent")
                parser_model = parser_route.model if parser_route and parser_route.model else None
                paper_evidence, _ = client.complete_json(
                    _paper_evidence_messages(
                        paper_path=paper_path,
                        instructions=instructions,
                        materials=materials,
                        dataset=config.run.dataset,
                    ),
                    model=parser_model,
                )
                study_route = config.agent_routes.get("study_design_agent")
                study_model = study_route.model if study_route and study_route.model else parser_model
                payload, _ = client.complete_json(
                    _task_builder_messages(
                        paper_path=paper_path,
                        instructions=instructions,
                        materials=materials,
                        dataset=config.run.dataset,
                        paper_evidence=paper_evidence,
                    ),
                    model=study_model,
                )
                contract = TaskContract.from_dict(payload)
                contract.task_id = contract.task_id or task_id
                contract.title = contract.title or _infer_title_from_path(paper_abspath)
                contract.execution_mode = ExecutionMode.AGENTIC
                contract.interaction_mode = InteractionMode.CHAT
                contract.source_paper_path = paper_path
                contract.instructions = instructions
                _attach_paper_evidence(contract, paper_evidence)
                _seed_contract_runtime_context(
                    contract,
                    dataset_label=config.run.dataset,
                    instructions=instructions,
                    paper_materials=materials,
                )
                contract = normalize_task_contract(contract, config=config, project_root=project_root)
                return TaskBuildResult(
                    contract=contract,
                    used_llm=True,
                    paper_materials=materials,
                    paper_evidence=paper_evidence,
                )
            except LLMError as exc:
                llm_error = str(exc)

    heuristic_contract = _build_heuristic_task_contract(
        task_id=task_id,
        paper_path=paper_path,
        instructions=instructions,
        dataset_label=config.run.dataset,
        paper_materials=materials,
    )
    _seed_contract_runtime_context(
        heuristic_contract,
        dataset_label=config.run.dataset,
        instructions=instructions,
        paper_materials=materials,
    )
    if paper_evidence:
        _attach_paper_evidence(heuristic_contract, paper_evidence)
    heuristic_contract = normalize_task_contract(heuristic_contract, config=config, project_root=project_root)
    return TaskBuildResult(
        contract=heuristic_contract,
        used_llm=False,
        llm_error=llm_error,
        paper_materials=materials,
        paper_evidence=paper_evidence,
    )


def refresh_task_contract_context(
    contract: TaskContract,
    *,
    config: PipelineConfig,
    project_root: Path,
) -> TaskContract:
    materials: dict[str, str] = {}
    if contract.source_paper_path:
        paper_abspath = (project_root / contract.source_paper_path).resolve()
        if paper_abspath.exists():
            materials = collect_paper_materials(paper_abspath)
    _seed_contract_runtime_context(
        contract,
        dataset_label=(contract.dataset.version or contract.dataset.name or config.run.dataset),
        instructions=contract.instructions,
        paper_materials=materials,
    )
    return normalize_task_contract(contract, config=config, project_root=project_root)


def normalize_task_contract(
    contract: TaskContract,
    config: PipelineConfig,
    project_root: Path | None = None,
) -> TaskContract:
    if not contract.dataset.name or contract.dataset.name == "unknown":
        contract.dataset = DatasetSpec(
            name=config.run.dataset or "MIMIC-IV",
            adapter=config.dataset_adapters.default_adapter,
            source_type="postgres",
            connector_env_prefix="MIMIC_PG",
            version=config.run.dataset,
            schemas=["mimiciv_hosp", "mimiciv_icu", "mimiciv_derived"],
        )
    if not contract.dataset.adapter:
        contract.dataset.adapter = config.dataset_adapters.default_adapter
    contract.dataset.adapter = _normalize_dataset_adapter_name(
        contract.dataset.adapter,
        default_adapter=config.dataset_adapters.default_adapter,
    )
    if not contract.dataset.source_type:
        contract.dataset.source_type = "postgres"
    contract.dataset.source_type = _normalize_dataset_source_type(
        contract.dataset.source_type,
        adapter=contract.dataset.adapter,
    )
    if not contract.dataset.connector_env_prefix:
        contract.dataset.connector_env_prefix = "MIMIC_PG"
    if not contract.dataset.version:
        contract.dataset.version = config.run.dataset
    if not contract.dataset.schemas and contract.dataset.adapter in {"mimic", "mimic_iv", "mimic-iv"}:
        contract.dataset.schemas = ["mimiciv_hosp", "mimiciv_icu", "mimiciv_derived"]
    if not contract.cohort.population:
        contract.cohort.population = "critically ill ICU patients"

    _apply_paper_evidence_identity_preferences(contract)
    _normalize_model_specs(contract)
    _normalize_output_specs(contract)
    _dedupe_variables(contract)
    _dedupe_models(contract)
    _dedupe_outputs(contract)
    _apply_preset_metadata(contract, project_root=project_root)
    _apply_semantic_registry_mappings(contract, project_root=project_root)
    _ensure_default_models(contract)
    _apply_study_template_metadata(contract)
    _apply_auto_binary_profile_metadata(contract)
    _ensure_default_outputs(contract)
    _apply_paper_figure_targets_metadata(contract, project_root=project_root)
    _apply_paper_evidence_output_preferences(contract)
    _ensure_default_notes(contract)
    _dedupe_models(contract)
    _dedupe_outputs(contract)
    return contract


def find_missing_high_impact_fields(contract: TaskContract) -> list[str]:
    missing: list[str] = []
    if not [item for item in contract.variables if item.role == VariableRole.EXPOSURE]:
        missing.append("exposure_variables")
    if not [item for item in contract.variables if item.role == VariableRole.OUTCOME]:
        missing.append("outcome_variables")
    if not contract.models:
        missing.append("models")
    if not contract.outputs:
        missing.append("outputs")
    if not contract.cohort.inclusion_criteria and not contract.cohort.exclusion_criteria:
        missing.append("cohort_logic")
    return missing


def apply_follow_up_answers(contract: TaskContract, answers: dict[str, str]) -> TaskContract:
    if "exposure_variables" in answers:
        for variable in _split_variable_answer(answers["exposure_variables"]):
            contract.variables.append(VariableSpec(name=variable, role=VariableRole.EXPOSURE))
    if "outcome_variables" in answers:
        for variable in _split_variable_answer(answers["outcome_variables"]):
            contract.variables.append(VariableSpec(name=variable, role=VariableRole.OUTCOME))
    if "control_variables" in answers:
        for variable in _split_variable_answer(answers["control_variables"]):
            contract.variables.append(VariableSpec(name=variable, role=VariableRole.CONTROL, required=False))
    if "models" in answers:
        for raw_family in _split_variable_answer(answers["models"]):
            family = _normalize_model_family(raw_family) or _canonical_name(raw_family)
            if not family:
                continue
            contract.models.append(
                ModelSpec(
                    name=family,
                    family=family,
                    exposure_variables=[item.name for item in contract.variables if item.role == VariableRole.EXPOSURE],
                    outcome_variables=[item.name for item in contract.variables if item.role == VariableRole.OUTCOME],
                    control_variables=[item.name for item in contract.variables if item.role == VariableRole.CONTROL],
                    subgroup_variables=[item.name for item in contract.variables if item.role == VariableRole.SUBGROUP],
                    time_variable=_first_role_name(contract, VariableRole.TIME),
                )
            )
    if "outputs" in answers:
        for output_name in _split_variable_answer(answers["outputs"]):
            output_key = _normalize_output_name(output_name)
            if output_key in DEFAULT_OUTPUT_SPECS:
                spec = DEFAULT_OUTPUT_SPECS[output_key]
                contract.outputs.append(
                    OutputSpec(
                        name=output_key,
                        kind=spec["kind"],
                        fmt=spec["format"],
                    )
                )
    if "cohort_logic" in answers:
        logic = answers["cohort_logic"].strip()
        if logic:
            contract.cohort.inclusion_criteria.append(logic)
    return contract


def summarize_task_contract(contract: TaskContract) -> str:
    exposures = ", ".join(item.name for item in contract.variables if item.role == VariableRole.EXPOSURE) or "none"
    outcomes = ", ".join(item.name for item in contract.variables if item.role == VariableRole.OUTCOME) or "none"
    controls = ", ".join(item.name for item in contract.variables if item.role == VariableRole.CONTROL) or "none"
    models = ", ".join(item.family for item in contract.models) or "none"
    outputs = ", ".join(item.kind for item in contract.outputs) or "none"
    cohort_bits = []
    if contract.cohort.min_age is not None:
        cohort_bits.append(f"age>={contract.cohort.min_age}")
    if contract.cohort.min_icu_los_hours is not None:
        cohort_bits.append(f"ICU LOS>={contract.cohort.min_icu_los_hours}h")
    if contract.cohort.first_stay_only is True:
        cohort_bits.append("first ICU stay only")
    if contract.cohort.diagnosis_logic:
        cohort_bits.append(contract.cohort.diagnosis_logic)
    cohort_text = "; ".join(cohort_bits) or "not fully specified"
    extra_lines: list[str] = []
    preset_title = str(contract.meta.get("preset_title") or contract.meta.get("preset") or "").strip()
    if preset_title:
        extra_lines.append(f"Preset: {preset_title}")
    template_title = str(contract.meta.get("study_template_title") or contract.meta.get("study_template") or "").strip()
    if template_title:
        extra_lines.append(f"Study Template: {template_title}")
    extra_text = "".join(f"{line}\n" for line in extra_lines)
    return (
        f"Task: {contract.title}\n"
        f"Dataset: {contract.dataset.name} ({contract.dataset.adapter})\n"
        f"{extra_text}"
        f"Exposures: {exposures}\n"
        f"Outcomes: {outcomes}\n"
        f"Controls: {controls}\n"
        f"Models: {models}\n"
        f"Outputs: {outputs}\n"
        f"Cohort: {cohort_text}"
    )


def _build_heuristic_task_contract(
    *,
    task_id: str,
    paper_path: str,
    instructions: str,
    dataset_label: str,
    paper_materials: dict[str, str],
) -> TaskContract:
    text = "\n".join([instructions, *paper_materials.values()]).lower()
    structured_sections = _extract_structured_sections(instructions)
    title = _infer_title_from_path(Path(paper_path))
    dataset = DatasetSpec(
        name=dataset_label or "MIMIC-IV",
        adapter="mimic_iv",
        source_type="postgres",
        connector_env_prefix="MIMIC_PG",
        version=dataset_label or "unknown",
        schemas=["mimiciv_hosp", "mimiciv_icu", "mimiciv_derived"],
    )
    cohort = CohortSpec(
        population="critically ill ICU patients",
        diagnosis_logic="Sepsis-3" if "sepsis" in text else "",
        first_stay_only=("first icu" in text or "首次" in text or "first time" in text) or None,
        min_age=_extract_first_int_after_keywords(
            text,
            ["age >=", "年龄≥", "aged >=", "aged ≥", "aged over", "over 18", "older than"],
        ),
        min_icu_los_hours=_extract_first_hours(text, ["icu stay", "icu 住院", "icu los", "stay is less than", "icu stay is less than"]),
        max_admit_to_icu_hours=_extract_first_hours(text, ["within 24", "24h", "24 h"]),
        inclusion_criteria=_extract_bullets_after_keywords(
            instructions,
            ["纳入标准", "inclusion criteria", "participants"],
        ),
        exclusion_criteria=_extract_bullets_after_keywords(
            instructions,
            ["排除标准", "exclusion criteria"],
        ),
    )
    cohort_logic = structured_sections.get("cohort_logic", "")
    if cohort_logic and cohort_logic not in cohort.inclusion_criteria:
        cohort.inclusion_criteria.insert(0, cohort_logic)
    if not cohort.inclusion_criteria and "first icu" in text:
        cohort.inclusion_criteria.append("first ICU stay")
    if "glucose" in text:
        cohort.required_measurements.append("blood_glucose")
    if "triglyceride" in text or "甘油三酯" in text or "tg" in text:
        cohort.required_measurements.append("triglycerides")
    if _requires_trajectory_modeling_from_text(text):
        cohort.population = "sepsis ICU patients with repeated early heart-rate measurements"
        cohort.required_measurements.append("heart_rate")
        cohort.meta["trajectory_measurement_window_hours"] = 10
        cohort.meta["trajectory_measurement_interval_hours"] = 1
        if "hourly heart rate" not in cohort.screening_steps:
            cohort.screening_steps.append("extract repeated heart-rate measurements with 1-hour spacing during the first 10 ICU hours")
        if not cohort.inclusion_criteria:
            cohort.inclusion_criteria.extend(
                [
                    "sepsis patients admitted to the ICU",
                    "repeated hourly heart-rate measurements available during the first 10 hours after ICU admission",
                ]
            )

    variables = _infer_variables(
        instructions=instructions,
        combined_text=text,
        structured_sections=structured_sections,
    )
    models = _infer_models(
        instructions=instructions,
        combined_text=text,
        variables=variables,
        structured_sections=structured_sections,
    )
    outputs = _infer_outputs(
        instructions=instructions,
        models=models,
        structured_sections=structured_sections,
    )

    return TaskContract(
        task_id=task_id,
        title=title,
        execution_mode=ExecutionMode.AGENTIC,
        interaction_mode=InteractionMode.CHAT,
        source_paper_path=paper_path,
        instructions=instructions,
        dataset=dataset,
        cohort=cohort,
        variables=variables,
        models=models,
        outputs=outputs,
        notes=[],
        meta={},
    )


def _task_builder_messages(
    *,
    paper_path: str,
    instructions: str,
    materials: dict[str, str],
    dataset: str,
    paper_evidence: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    material_blob = "\n\n".join(f"[{name}]\n{content[:12000]}" for name, content in materials.items())
    evidence_blob = json.dumps(paper_evidence or {}, ensure_ascii=False, indent=2)
    schema_hint = """
Return a JSON object with keys:
task_id, title, source_paper_path, instructions, dataset, cohort, variables, models, outputs, notes, verification_targets.
dataset keys: name, adapter, source_type, connector_env_prefix, version, schemas, meta.
cohort keys: population, inclusion_criteria, exclusion_criteria, diagnosis_logic, screening_steps, first_stay_only, min_age, max_age, min_icu_los_hours, max_admit_to_icu_hours, required_measurements, meta.
variables: list of objects with keys name, role, label, description, dataset_field, source_name, transform, formula, unit, required, meta.
models: list of objects with keys name, family, exposure_variables, outcome_variables, control_variables, subgroup_variables, time_variable, description, options.
outputs: list of objects with keys name, kind, format, description, required, model_refs, options.
Allowed variable roles: exposure, outcome, control, subgroup, time, id, derived.
Prefer canonical model families when possible, such as cox_regression, kaplan_meier, logrank_test,
restricted_cubic_spline, subgroup_analysis, interaction_analysis, logistic_regression, and trajectory_mixture_model.
"""
    return [
        {
            "role": "system",
            "content": (
                "You are building a structured clinical paper reproduction task contract. "
                "Use the paper materials and user instructions. Prefer explicit paper facts. "
                "Treat the structured paper evidence as the primary semantic grounding when it is available. "
                "If something is unknown, leave it blank rather than inventing it. "
                + schema_hint
            ),
        },
        {
            "role": "user",
            "content": (
                f"Paper path: {paper_path}\n"
                f"Dataset default: {dataset}\n"
                f"User instructions:\n{instructions}\n\n"
                f"Structured paper evidence:\n{evidence_blob}\n\n"
                f"Paper materials:\n{material_blob}"
            ),
        },
    ]


def _paper_evidence_messages(
    *,
    paper_path: str,
    instructions: str,
    materials: dict[str, str],
    dataset: str,
) -> list[dict[str, str]]:
    material_blob = "\n\n".join(f"[{name}]\n{content[:12000]}" for name, content in materials.items())
    schema_hint = """
Return a JSON object with keys:
title, paper_target_dataset_version, execution_year_window, population, cohort_logic,
inclusion_criteria, exclusion_criteria, exposures, outcomes, time_variables,
model_families, method_details, requested_tables, requested_figures, result_targets,
variable_candidates, uncertainty_notes.
Use empty strings or empty arrays for unknown fields.
Prefer the paper's own wording and explicit claims over inference.
`requested_figures` and `requested_tables` should list the actual paper-reported artifacts,
for example Kaplan-Meier figure, subgroup forest plot, calibration curve, SHAP beeswarm,
heatmap, violin plot, baseline table, or Cox regression table. Preserve caption-style wording
when possible because downstream routing depends on the paper's stated figure semantics.
"""
    return [
        {
            "role": "system",
            "content": (
                "You are the paper-intake agent for a clinical paper reproduction system. "
                "Read the paper materials and extract structured semantic evidence that can drive downstream contract building. "
                "Do not invent unavailable details. "
                + schema_hint
            ),
        },
        {
            "role": "user",
            "content": (
                f"Paper path: {paper_path}\n"
                f"Execution environment default dataset: {dataset}\n"
                f"User instructions:\n{instructions}\n\n"
                f"Paper materials:\n{material_blob}"
            ),
        },
    ]


def _attach_paper_evidence(contract: TaskContract, paper_evidence: dict[str, Any] | None) -> None:
    if not paper_evidence:
        return
    contract.meta["paper_evidence"] = dict(paper_evidence)
    evidence_title = str(paper_evidence.get("title", "")).strip()
    if evidence_title:
        contract.title = evidence_title
    paper_target_dataset_version = str(paper_evidence.get("paper_target_dataset_version", "")).strip()
    if paper_target_dataset_version:
        contract.meta.setdefault("paper_target_dataset_version", paper_target_dataset_version)
    execution_year_window = str(paper_evidence.get("execution_year_window", "")).strip()
    if execution_year_window:
        contract.meta.setdefault("execution_year_window", execution_year_window)


def _apply_paper_evidence_identity_preferences(contract: TaskContract) -> None:
    paper_evidence = contract.meta.get("paper_evidence")
    if not isinstance(paper_evidence, dict) or not paper_evidence:
        return

    evidence_title = str(paper_evidence.get("title", "")).strip()
    if evidence_title:
        contract.title = evidence_title


def _infer_variables(
    instructions: str,
    combined_text: str,
    structured_sections: dict[str, str] | None = None,
) -> list[VariableSpec]:
    variables: list[VariableSpec] = []
    sections = structured_sections or _extract_structured_sections(instructions)
    role_sections = {
        "exposure_variables": VariableRole.EXPOSURE,
        "outcome_variables": VariableRole.OUTCOME,
        "control_variables": VariableRole.CONTROL,
        "subgroup_variables": VariableRole.SUBGROUP,
        "time_variables": VariableRole.TIME,
    }
    for field_name, role in role_sections.items():
        raw_value = sections.get(field_name, "")
        if not raw_value:
            continue
        for variable in _split_variable_answer(raw_value):
            variables.append(VariableSpec(name=variable, role=role, required=role != VariableRole.CONTROL))

    role_patterns = {
        VariableRole.EXPOSURE: [r"自变量[:：]\s*([^\n]+)", r"independent variables?[:：]?\s*([^\n]+)", r"exposure[:：]?\s*([^\n]+)"],
        VariableRole.OUTCOME: [r"因变量[:：]\s*([^\n]+)", r"dependent variables?[:：]?\s*([^\n]+)", r"outcomes?[:：]?\s*([^\n]+)"],
        VariableRole.CONTROL: [r"控制变量[:：]\s*([^\n]+)", r"covariates?[:：]?\s*([^\n]+)", r"adjust(?:ed)? for[:：]?\s*([^\n]+)"],
        VariableRole.SUBGROUP: [r"亚组变量[:：]\s*([^\n]+)", r"subgroups?[:：]?\s*([^\n]+)"],
        VariableRole.TIME: [r"时间变量[:：]\s*([^\n]+)", r"time variable[:：]?\s*([^\n]+)"],
    }
    for role, patterns in role_patterns.items():
        if any(item.role == role for item in variables):
            continue
        for pattern in patterns:
            match = re.search(pattern, instructions, flags=re.IGNORECASE)
            if not match:
                continue
            for variable in _split_variable_answer(_clean_structured_value(match.group(1))):
                variables.append(VariableSpec(name=variable, role=role, required=role != VariableRole.CONTROL))

    if "tyg" in combined_text and not any(item.name.lower() == "tyg_index" for item in variables):
        variables.append(VariableSpec(name="tyg_index", role=VariableRole.EXPOSURE))
    if _requires_trajectory_modeling_from_text(combined_text):
        if not any(item.name == "heart_rate_trajectory_class" for item in variables):
            variables.append(
                VariableSpec(
                    name="heart_rate_trajectory_class",
                    role=VariableRole.EXPOSURE,
                    description="Trajectory class derived from repeated hourly heart-rate measurements during the first 10 ICU hours.",
                )
            )
        if not any(item.name == "heart_rate_hourly_panel_10h" for item in variables):
            variables.append(
                VariableSpec(
                    name="heart_rate_hourly_panel_10h",
                    role=VariableRole.DERIVED,
                    description="Patient-by-time panel of hourly heart-rate values collected over the first 10 ICU hours.",
                    required=True,
                )
            )
    if (
        any(token in combined_text for token in ("30-day mortality", "30 day mortality", "30-day all-cause mortality"))
        and not any(item.name == "mortality_30d" for item in variables)
    ):
        variables.append(VariableSpec(name="mortality_30d", role=VariableRole.OUTCOME))
    if (
        any(token in combined_text for token in ("30-day mortality", "30 day mortality", "short-term mortality"))
        and not any(item.name == "time_to_event_30d_days" for item in variables)
    ):
        variables.append(VariableSpec(name="time_to_event_30d_days", role=VariableRole.TIME))
    if any(token in combined_text for token in ("90-day mortality", "90 day mortality")) and not any(
        item.name == "mortality_90d" for item in variables
    ):
        variables.append(VariableSpec(name="mortality_90d", role=VariableRole.OUTCOME))
    if any(token in combined_text for token in ("90-day mortality", "90 day mortality")) and not any(
        item.name == "time_to_event_90d_days" for item in variables
    ):
        variables.append(VariableSpec(name="time_to_event_90d_days", role=VariableRole.TIME))
    if any(token in combined_text for token in ("180-day mortality", "180 day mortality")) and not any(
        item.name == "mortality_180d" for item in variables
    ):
        variables.append(VariableSpec(name="mortality_180d", role=VariableRole.OUTCOME))
    if any(token in combined_text for token in ("180-day mortality", "180 day mortality")) and not any(
        item.name == "time_to_event_180d_days" for item in variables
    ):
        variables.append(VariableSpec(name="time_to_event_180d_days", role=VariableRole.TIME))
    if any(token in combined_text for token in ("1-year mortality", "1 year mortality", "one-year mortality", "one year mortality")) and not any(
        item.name == "mortality_1y" for item in variables
    ):
        variables.append(VariableSpec(name="mortality_1y", role=VariableRole.OUTCOME))
    if any(token in combined_text for token in ("1-year mortality", "1 year mortality", "one-year mortality", "one year mortality")) and not any(
        item.name == "time_to_event_1y_days" for item in variables
    ):
        variables.append(VariableSpec(name="time_to_event_1y_days", role=VariableRole.TIME))
    if "hospital mortality" in combined_text and not any(item.name == "in_hospital_mortality" for item in variables):
        variables.append(VariableSpec(name="in_hospital_mortality", role=VariableRole.OUTCOME))
    if "icu mortality" in combined_text and not any(item.name == "icu_mortality" for item in variables):
        variables.append(VariableSpec(name="icu_mortality", role=VariableRole.OUTCOME))
    if "in-hospital mortality" in combined_text and not any(item.name == "time_to_in_hospital_event_days" for item in variables):
        variables.append(VariableSpec(name="time_to_in_hospital_event_days", role=VariableRole.TIME))
    if "icu mortality" in combined_text and not any(item.name == "time_to_icu_event_days" for item in variables):
        variables.append(VariableSpec(name="time_to_icu_event_days", role=VariableRole.TIME))
    if "hospital survival" in combined_text and not any(item.name == "hospital_survival_hours" for item in variables):
        variables.append(VariableSpec(name="hospital_survival_hours", role=VariableRole.TIME))
    if "icu survival" in combined_text and not any(item.name == "icu_survival_hours" for item in variables):
        variables.append(VariableSpec(name="icu_survival_hours", role=VariableRole.TIME))
    if "stay_id" not in {item.name for item in variables}:
        variables.append(VariableSpec(name="stay_id", role=VariableRole.ID, required=True))
    return variables


def _infer_models(
    instructions: str,
    combined_text: str,
    variables: list[VariableSpec],
    structured_sections: dict[str, str] | None = None,
) -> list[ModelSpec]:
    families: list[str] = []
    explicit_families = _extract_explicit_models(instructions, structured_sections=structured_sections)
    if explicit_families:
        families = explicit_families
    else:
        seen: set[str] = set()
        for keyword, family in MODEL_KEYWORDS.items():
            if keyword in combined_text and family not in seen:
                seen.add(family)
                families.append(family)
    exposure_variables = [item.name for item in variables if item.role == VariableRole.EXPOSURE]
    outcome_variables = [item.name for item in variables if item.role == VariableRole.OUTCOME]
    control_variables = [item.name for item in variables if item.role == VariableRole.CONTROL]
    subgroup_variables = [item.name for item in variables if item.role == VariableRole.SUBGROUP]
    time_variable = next((item.name for item in variables if item.role == VariableRole.TIME), "")
    return [
        ModelSpec(
            name=family,
            family=family,
            exposure_variables=exposure_variables,
            outcome_variables=outcome_variables,
            control_variables=control_variables,
            subgroup_variables=subgroup_variables,
            time_variable=time_variable,
        )
        for family in families
    ]


def _infer_outputs(
    instructions: str,
    models: list[ModelSpec],
    structured_sections: dict[str, str] | None = None,
) -> list[OutputSpec]:
    outputs: list[str] = ["cohort_funnel", "analysis_dataset", "missingness_report", "reproduction_report"]
    outputs.extend(_extract_explicit_outputs(instructions, structured_sections=structured_sections))
    for model in models:
        outputs.extend(OUTPUT_BY_MODEL.get(model.family, []))
    if "table" in instructions.lower() or "表" in instructions:
        outputs.append("baseline_table")
    if "figure" in instructions.lower() or "图" in instructions:
        if any(model.family == "kaplan_meier" for model in models):
            outputs.append("km_figure")
        if any(model.family == "restricted_cubic_spline" for model in models):
            outputs.append("rcs_figure")
        if any(model.family in {"subgroup_analysis", "interaction_analysis"} for model in models):
            outputs.append("subgroup_figure")
    deduped = []
    seen: set[str] = set()
    for output_name in outputs:
        if output_name in seen or output_name not in DEFAULT_OUTPUT_SPECS:
            continue
        seen.add(output_name)
        spec = DEFAULT_OUTPUT_SPECS[output_name]
        deduped.append(
            OutputSpec(
                name=output_name,
                kind=spec["kind"],
                fmt=spec["format"],
                required=True,
            )
        )
    return deduped


def _ensure_default_models(contract: TaskContract) -> None:
    if contract.models:
        return
    exposures = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE]
    outcomes = [item.name for item in contract.variables if item.role == VariableRole.OUTCOME]
    if not exposures or not outcomes:
        return
    contract.models = [
        ModelSpec(
            name="baseline_table",
            family="baseline_table",
            exposure_variables=exposures,
            outcome_variables=outcomes,
            control_variables=[item.name for item in contract.variables if item.role == VariableRole.CONTROL],
            subgroup_variables=[item.name for item in contract.variables if item.role == VariableRole.SUBGROUP],
            time_variable=_first_role_name(contract, VariableRole.TIME),
        )
    ]


def _ensure_default_outputs(contract: TaskContract) -> None:
    if contract.outputs:
        return
    output_names = {"cohort_funnel", "analysis_dataset", "missingness_report", "reproduction_report"}
    for model in contract.models:
        output_names.update(OUTPUT_BY_MODEL.get(model.family, []))
    contract.outputs = [
        OutputSpec(name=name, kind=DEFAULT_OUTPUT_SPECS[name]["kind"], fmt=DEFAULT_OUTPUT_SPECS[name]["format"])
        for name in sorted(output_names)
        if name in DEFAULT_OUTPUT_SPECS
    ]


def _ensure_default_notes(contract: TaskContract) -> None:
    notes = set(contract.notes)
    preset = get_paper_preset(contract.meta.get("preset"))
    if preset is not None:
        notes.add(f"This task matches the built-in preset: {preset.title}.")
        notes.add(
            "Preset tasks can use the deterministic bridge while the generic agentic contract remains the source of truth."
        )
    template_title = str(contract.meta.get("study_template_title", "")).strip()
    if template_title:
        notes.add(f"Study template inferred: {template_title}.")
    mapped_count = int(contract.meta.get("semantic_mapped_variable_count", 0) or 0)
    if mapped_count > 0:
        notes.add(f"Semantic registry resolved {mapped_count} variables for {contract.dataset.adapter}.")
    if contract.meta.get("semantic_unmapped_variables"):
        notes.add("Some variables are still unmapped and may require follow-up clarification or a dataset-specific compiler.")
    if contract.meta.get("requires_longitudinal_trajectory_modeling"):
        notes.add("This paper requires longitudinal trajectory modeling and is routed through the experimental Python trajectory bridge.")
    paper_required_methods = list(contract.meta.get("paper_required_methods", []))
    if paper_required_methods:
        notes.add(f"Paper-required methods: {', '.join(paper_required_methods)}.")
    planned_backend = str(contract.meta.get("engine_supported_trajectory_backend", "")).strip()
    if planned_backend:
        notes.add(f"Engine backend for this method family: {planned_backend}.")
    method_gap = str(contract.meta.get("method_gap", "")).strip()
    if method_gap:
        notes.add(method_gap)
    target_dataset_version = str(contract.meta.get("paper_target_dataset_version", "")).strip()
    if target_dataset_version:
        notes.add(f"Paper original dataset version: {target_dataset_version}.")
    execution_environment_dataset_version = str(
        contract.meta.get("execution_environment_dataset_version", contract.meta.get("configured_dataset_version", contract.dataset.version))
    ).strip()
    if execution_environment_dataset_version:
        notes.add(f"Execution environment dataset version: {execution_environment_dataset_version}.")
    execution_year_window = str(contract.meta.get("execution_year_window", "")).strip()
    if execution_year_window:
        notes.add(f"Execution year window: {execution_year_window}.")
    if contract.meta.get("dataset_version_mismatch"):
        notes.add(
            "Dataset version mismatch: "
            + f"paper original dataset version is {target_dataset_version or 'unknown'} "
            + f"while the execution environment dataset version is {execution_environment_dataset_version or 'unknown'}."
        )
    notes.add("Task contract was generated from user instructions and available paper materials.")
    contract.notes = sorted(notes)


def _dedupe_variables(contract: TaskContract) -> None:
    deduped: dict[tuple[str, str], VariableSpec] = {}
    for variable in contract.variables:
        key = (_canonical_name(variable.name), variable.role.value)
        deduped[key] = variable
    contract.variables = list(deduped.values())


def _dedupe_models(contract: TaskContract) -> None:
    deduped: dict[tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...], str], ModelSpec] = {}
    for model in contract.models:
        key = (
            _canonical_name(model.family or model.name),
            tuple(sorted(_canonical_name(name) for name in model.exposure_variables)),
            tuple(sorted(_canonical_name(name) for name in model.outcome_variables)),
            tuple(sorted(_canonical_name(name) for name in model.control_variables)),
            tuple(sorted(_canonical_name(name) for name in model.subgroup_variables)),
            _canonical_name(model.time_variable),
        )
        deduped[key] = model
    contract.models = list(deduped.values())


def _dedupe_outputs(contract: TaskContract) -> None:
    deduped: dict[tuple[str, str, str], OutputSpec] = {}
    for output in contract.outputs:
        key = (
            _canonical_name(output.name or output.kind),
            _canonical_name(output.kind),
            output.fmt.strip().lower(),
        )
        deduped[key] = output
    contract.outputs = list(deduped.values())


def _normalize_model_specs(contract: TaskContract) -> None:
    for model in contract.models:
        normalized_family = _normalize_model_family(model.family or model.name)
        if normalized_family:
            model.family = normalized_family
        if not model.name:
            model.name = model.family


def _normalize_output_specs(contract: TaskContract) -> None:
    for output in contract.outputs:
        normalized_name = _normalize_output_name(output.name or output.kind)
        if normalized_name:
            output.name = normalized_name
            output.kind = DEFAULT_OUTPUT_SPECS.get(normalized_name, {}).get("kind", output.kind or normalized_name)
            output.fmt = DEFAULT_OUTPUT_SPECS.get(normalized_name, {}).get("format", output.fmt or "json")


def _seed_contract_runtime_context(
    contract: TaskContract,
    *,
    dataset_label: str,
    instructions: str,
    paper_materials: dict[str, str],
) -> None:
    haystack = "\n".join(
        [
            dataset_label or contract.dataset.name,
            instructions,
            *paper_materials.values(),
        ]
    )
    preset = detect_paper_preset(
        dataset_label=dataset_label or contract.dataset.name,
        instructions=instructions,
        materials=paper_materials,
    )
    existing_preset = get_paper_preset(contract.meta.get("preset"))
    if preset is None and existing_preset is not None:
        if not paper_materials or existing_preset.matches(haystack):
            preset = existing_preset
        else:
            _clear_preset_metadata(contract)
    if preset is not None:
        contract.meta["preset"] = preset.key
        contract.meta["preset_title"] = preset.title
        contract.meta["execution_backend"] = preset.execution_backend
        contract.meta["preset_description"] = preset.description
        contract.meta["preset_supported_domains"] = list(preset.supported_domains)
        if not contract.dataset.adapter or contract.dataset.adapter in {"unknown", "generic"}:
            contract.dataset.adapter = preset.dataset_adapter
    _annotate_paper_requirements(
        contract,
        dataset_label=dataset_label,
        instructions=instructions,
        paper_materials=paper_materials,
    )


def _clear_preset_metadata(contract: TaskContract) -> None:
    for key in (
        "preset",
        "preset_title",
        "preset_description",
        "preset_supported_domains",
        "execution_backend",
    ):
        contract.meta.pop(key, None)


def _annotate_paper_requirements(
    contract: TaskContract,
    *,
    dataset_label: str,
    instructions: str,
    paper_materials: dict[str, str],
) -> None:
    haystack = "\n".join([dataset_label, instructions, *paper_materials.values()]).lower()
    execution_environment_dataset_version = (dataset_label or contract.dataset.version or contract.dataset.name).strip()
    if execution_environment_dataset_version:
        contract.meta["configured_dataset_version"] = execution_environment_dataset_version
        contract.meta["execution_environment_dataset_version"] = execution_environment_dataset_version

    requires_trajectory_modeling = _requires_trajectory_modeling(contract, haystack)
    experimental_profile = _resolve_experimental_execution_profile_key(
        contract,
        haystack=haystack,
        requires_trajectory_modeling=requires_trajectory_modeling,
    )
    if experimental_profile:
        contract.meta["experimental_profile"] = experimental_profile
    else:
        contract.meta.pop("experimental_profile", None)
    profile = _resolve_paper_execution_profile(contract, requires_trajectory_modeling=requires_trajectory_modeling)
    paper_target_dataset_version = _infer_paper_target_dataset_version(
        instructions=instructions,
        paper_materials=paper_materials,
        profile_key=profile.key if profile is not None else "",
    )
    if paper_target_dataset_version:
        contract.meta["paper_target_dataset_version"] = paper_target_dataset_version

    execution_year_window = _infer_execution_year_window(
        instructions=instructions,
        paper_materials=paper_materials,
        profile_key=profile.key if profile is not None else "",
    )
    if execution_year_window:
        contract.meta["execution_year_window"] = execution_year_window

    target_version = str(contract.meta.get("paper_target_dataset_version", "")).strip().lower()
    configured_version = execution_environment_dataset_version.lower()
    contract.meta["dataset_version_mismatch"] = bool(target_version and configured_version and target_version != configured_version)

    if experimental_profile != "mimic_hr_trajectory_sepsis":
        contract.meta.pop("requires_longitudinal_trajectory_modeling", None)
        contract.meta.pop("trajectory_measurement_window_hours", None)
        contract.meta.pop("trajectory_measurement_interval_hours", None)
    if experimental_profile == "mimic_hr_trajectory_sepsis":
        contract.meta["requires_longitudinal_trajectory_modeling"] = True
        contract.meta["paper_required_methods"] = [
            "latent_growth_mixture_model",
            "kaplan_meier",
            "cox_regression",
        ]
        contract.meta["required_analysis_families"] = [
            "trajectory_mixture_model",
            "kaplan_meier",
            "cox_regression",
        ]
        contract.meta["execution_backend"] = "trajectory_python_bridge"
        contract.meta["engine_supported_trajectory_backend"] = "python_only_mixture_route_v1"
        contract.meta["fidelity"] = "method_aligned_not_paper_identical"
        contract.meta["method_gap"] = (
            "Paper-required method: LGMM. Engine-supported backend: Python-only trajectory mixture route v1. "
            "Fidelity: method-aligned, not paper-identical."
        )
        contract.meta["trajectory_measurement_window_hours"] = 10
        contract.meta["trajectory_measurement_interval_hours"] = 1
        _apply_experimental_profile_contract_defaults(contract, experimental_profile)
        return

    contract.meta.pop("engine_supported_trajectory_backend", None)
    if experimental_profile == "mimic_tyg_stroke_nondiabetic":
        contract.meta["paper_required_methods"] = [
            "cox_regression",
            "kaplan_meier",
            "restricted_cubic_spline",
            "subgroup_analysis",
            "multiple_imputation",
            "propensity_score_matching",
        ]
        contract.meta["required_analysis_families"] = [
            "baseline_table",
            "cox_regression",
            "kaplan_meier",
            "restricted_cubic_spline",
            "subgroup_analysis",
            "missingness_report",
        ]
        contract.meta["execution_backend"] = "profile_survival_bridge"
        contract.meta["fidelity"] = "paper_aligned_with_fasting_and_sensitivity_gaps"
        contract.meta["method_gap"] = (
            "Paper-required methods include fasting TyG measurement semantics plus MICE and PSM sensitivity analysis. "
            "The current execution route reproduces the main Cox/KM/RCS/subgroup path with first-day ICU lab approximations."
        )
        _apply_experimental_profile_contract_defaults(contract, experimental_profile)
        return

    if str(contract.meta.get("execution_backend", "")).strip() in {"trajectory_python_bridge", "profile_survival_bridge"}:
        contract.meta["execution_backend"] = "spec_only"
    contract.meta.pop("paper_required_methods", None)
    contract.meta.pop("required_analysis_families", None)
    contract.meta.pop("method_gap", None)
    contract.meta.pop("fidelity", None)


def _resolve_paper_execution_profile(
    contract: TaskContract,
    *,
    requires_trajectory_modeling: bool,
):
    profile_key = str(contract.meta.get("experimental_profile", "")).strip()
    if not profile_key:
        profile_key = str(contract.meta.get("preset", "")).strip()
    if not profile_key:
        profile_key = _resolve_experimental_execution_profile_key(
            contract,
            haystack="",
            requires_trajectory_modeling=requires_trajectory_modeling,
        )
    return get_paper_execution_profile(profile_key)


def _resolve_experimental_execution_profile_key(
    contract: TaskContract,
    *,
    haystack: str,
    requires_trajectory_modeling: bool,
) -> str:
    existing = str(contract.meta.get("experimental_profile", "")).strip()
    if existing:
        return existing
    if requires_trajectory_modeling:
        return "mimic_hr_trajectory_sepsis"
    title = str(contract.title or "").strip().lower()
    text = f"{title}\n{haystack}".strip()
    if _matches_stroke_tyg_profile(text):
        return "mimic_tyg_stroke_nondiabetic"
    return ""


def _matches_stroke_tyg_profile(text: str) -> bool:
    lowered = text.lower()
    has_tyg_signal = (
        "tyg" in lowered
        or "triglyceride-glucose" in lowered
        or "triglyceride glucose" in lowered
    )
    has_stroke_signal = "ischemic stroke" in lowered or "ischaemic stroke" in lowered
    has_nondiabetic_signal = "non-diabetic" in lowered or "nondiabetic" in lowered or "non diabetic" in lowered
    return has_tyg_signal and has_stroke_signal and has_nondiabetic_signal


def _apply_experimental_profile_contract_defaults(contract: TaskContract, experimental_profile: str) -> None:
    if experimental_profile == "mimic_tyg_stroke_nondiabetic":
        contract.cohort.population = contract.cohort.population or "non-diabetic adult ischemic stroke ICU patients"
        contract.cohort.first_stay_only = True
        contract.cohort.min_age = 18
        contract.cohort.min_icu_los_hours = 3
        for measurement in ("blood_glucose", "triglycerides"):
            if measurement not in contract.cohort.required_measurements:
                contract.cohort.required_measurements.append(measurement)
        for item in (
            "first ICU admission with primary ischemic stroke diagnosis",
            "adult patients aged 18 years or older",
        ):
            if item not in contract.cohort.inclusion_criteria:
                contract.cohort.inclusion_criteria.append(item)
        for item in (
            "ICU length of stay shorter than 3 hours",
            "missing first-day TyG measurements",
            "history of diabetes or hypoglycemic-agent use",
        ):
            if item not in contract.cohort.exclusion_criteria:
                contract.cohort.exclusion_criteria.append(item)

        existing_variables = {(item.name, item.role) for item in contract.variables}
        for name, role in (
            ("tyg_index", VariableRole.EXPOSURE),
            ("icu_mortality", VariableRole.OUTCOME),
            ("in_hospital_mortality", VariableRole.OUTCOME),
            ("mortality_30d", VariableRole.OUTCOME),
            ("mortality_90d", VariableRole.OUTCOME),
            ("mortality_180d", VariableRole.OUTCOME),
            ("mortality_1y", VariableRole.OUTCOME),
            ("time_to_icu_event_days", VariableRole.TIME),
            ("time_to_in_hospital_event_days", VariableRole.TIME),
            ("time_to_event_30d_days", VariableRole.TIME),
            ("time_to_event_90d_days", VariableRole.TIME),
            ("time_to_event_180d_days", VariableRole.TIME),
            ("time_to_event_1y_days", VariableRole.TIME),
            ("age", VariableRole.SUBGROUP),
            ("gender", VariableRole.SUBGROUP),
            ("hypertension", VariableRole.SUBGROUP),
            ("sofa_score", VariableRole.SUBGROUP),
            ("insulin_treatment", VariableRole.SUBGROUP),
        ):
            if (name, role) not in existing_variables:
                contract.variables.append(VariableSpec(name=name, role=role, required=role != VariableRole.SUBGROUP))

        required_outputs = {
            "cohort_funnel",
            "analysis_dataset",
            "baseline_table",
            "km_figure",
            "rcs_figure",
            "subgroup_figure",
            "reproduction_report",
        }
        existing_output_names = {item.name for item in contract.outputs}
        for output_name in required_outputs:
            if output_name in existing_output_names or output_name not in DEFAULT_OUTPUT_SPECS:
                continue
            spec = DEFAULT_OUTPUT_SPECS[output_name]
            contract.outputs.append(
                OutputSpec(
                    name=output_name,
                    kind=spec["kind"],
                    fmt=spec["format"],
                )
            )


def _infer_paper_target_dataset_version(
    *,
    instructions: str,
    paper_materials: dict[str, str],
    profile_key: str = "",
) -> str:
    for text in [*paper_materials.values(), instructions]:
        match = MIMIC_IV_VERSION_PATTERN.search(text)
        if not match:
            continue
        version = str(match.group("version") or "").strip().lower().removeprefix("v")
        if version:
            return f"MIMIC-IV v{version}"
    profile = get_paper_execution_profile(profile_key)
    if profile is not None and profile.source_dataset_version:
        return profile.source_dataset_version
    return ""


def _infer_execution_year_window(
    *,
    instructions: str,
    paper_materials: dict[str, str],
    profile_key: str = "",
) -> str:
    for text in [*paper_materials.values(), instructions]:
        candidate = _extract_contextual_year_window(text)
        if candidate:
            return candidate
    profile = get_paper_execution_profile(profile_key)
    if profile is not None and profile.execution_year_window:
        return profile.execution_year_window
    return ""


def _extract_contextual_year_window(text: str) -> str:
    normalized_lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not normalized_lines:
        return ""
    for index, line in enumerate(normalized_lines):
        lowered = line.lower()
        if any(keyword in lowered for keyword in YEAR_WINDOW_CONTEXT_KEYWORDS):
            for pattern in YEAR_WINDOW_PATTERNS:
                match = pattern.search(line)
                if match:
                    start = str(match.group("start") or "").strip()
                    end = str(match.group("end") or "").strip()
                    if start and end:
                        return f"{start}-{end}"
            if index + 1 < len(normalized_lines):
                next_line = normalized_lines[index + 1]
                for pattern in YEAR_WINDOW_PATTERNS:
                    match = pattern.search(next_line)
                    if not match:
                        continue
                    start = str(match.group("start") or "").strip()
                    end = str(match.group("end") or "").strip()
                    if start and end:
                        return f"{start}-{end}"
    return ""


def _requires_trajectory_modeling(contract: TaskContract, haystack: str) -> bool:
    if any((item.family or "") == "trajectory_mixture_model" for item in contract.models):
        return True
    return _requires_trajectory_modeling_from_text(haystack)


def _requires_trajectory_modeling_from_text(text: str) -> bool:
    return (
        ("lgmm" in text or "latent growth mixture" in text or "growth mixture" in text or "trajectory" in text)
        and ("heart rate" in text or "repeated" in text or "hourly" in text)
    )


def _apply_preset_metadata(contract: TaskContract, *, project_root: Path | None) -> None:
    preset = get_paper_preset(contract.meta.get("preset"))
    if preset is None:
        contract.meta.setdefault("execution_backend", "spec_only")
        return
    contract.meta.setdefault("preset_title", preset.title)
    contract.meta.setdefault("preset_description", preset.description)
    contract.meta.setdefault("execution_backend", preset.execution_backend)
    contract.meta.setdefault("preset_supported_domains", list(preset.supported_domains))
    if not contract.dataset.adapter or contract.dataset.adapter in {"unknown", "generic"}:
        contract.dataset.adapter = preset.dataset_adapter
    if not contract.verification_targets:
        contract.verification_targets = preset.verification_targets(project_root)


def _apply_paper_figure_targets_metadata(contract: TaskContract, *, project_root: Path | None) -> None:
    preset = get_paper_preset(contract.meta.get("preset"))
    if preset is None:
        return
    if preset.key != "mimic_tyg_sepsis":
        return

    alignment_contract = build_paper_alignment_contract(project_root=project_root)
    figure_targets = dict(alignment_contract.get("figure_targets", {}))
    if not figure_targets:
        return

    contract.meta["paper_figure_targets"] = figure_targets
    manifest = _build_paper_figure_manifest(figure_targets)
    if manifest:
        contract.meta["paper_figure_manifest"] = manifest
    _sync_contract_outputs_with_paper_figure_manifest(contract, manifest)


def _apply_paper_evidence_output_preferences(contract: TaskContract) -> None:
    paper_evidence = contract.meta.get("paper_evidence")
    if not isinstance(paper_evidence, dict) or not paper_evidence:
        return

    requested_figures = _requested_output_specs_from_paper_evidence_items(paper_evidence.get("requested_figures", []))
    requested_tables = _requested_output_specs_from_paper_evidence_items(paper_evidence.get("requested_tables", []))
    if not requested_figures and not requested_tables:
        return

    figure_manifest = [item for item in requested_figures if _is_figure_output_name(item["output_name"])]
    if figure_manifest:
        contract.meta["paper_evidence_figure_manifest"] = [
            {
                "source_label": item["source_label"],
                "output_name": item["output_name"],
            }
            for item in figure_manifest
        ]
        _sync_contract_outputs_with_paper_figure_manifest(contract, figure_manifest)

    requested_table_outputs = {
        item["output_name"]
        for item in requested_tables
        if not _is_figure_output_name(item["output_name"])
    }
    outputs_by_name = {str(output.name).strip(): output for output in contract.outputs if str(output.name).strip()}
    for item in requested_tables:
        output_name = str(item.get("output_name", "")).strip()
        if not output_name or output_name not in DEFAULT_OUTPUT_SPECS or _is_figure_output_name(output_name):
            continue
        output = outputs_by_name.get(output_name)
        if output is None:
            spec = DEFAULT_OUTPUT_SPECS[output_name]
            output = OutputSpec(name=output_name, kind=spec["kind"], fmt=spec["format"])
            contract.outputs.append(output)
            outputs_by_name[output_name] = output
        output.required = True
        output.options = dict(output.options or {})
        output.options["paper_evidence_source_label"] = str(item.get("source_label", "")).strip()
        if not output.description:
            output.description = f"Paper-evidence requested artifact: {output_name}"

    if requested_table_outputs:
        contract.meta["paper_evidence_requested_table_outputs"] = sorted(requested_table_outputs)


def _requested_output_specs_from_paper_evidence_items(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []

    specs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        figure_key = ""
        if isinstance(item, dict):
            source_label = str(
                item.get("title")
                or item.get("label")
                or item.get("caption")
                or item.get("figure_title")
                or item.get("name")
                or item.get("text")
                or ""
            ).strip()
            figure_key = str(item.get("figure_key") or item.get("id") or item.get("figure") or "").strip()
        else:
            source_label = str(item).strip()
        output_name = _map_paper_evidence_artifact_to_output_name(source_label)
        if not output_name:
            continue
        style_hints = _infer_paper_artifact_style_hints(source_label, output_name)
        key = (output_name, source_label)
        if key in seen:
            continue
        seen.add(key)
        specs.append(
            {
                "figure_key": figure_key,
                "output_name": output_name,
                "source_label": source_label,
                "style_hints": style_hints,
                "target": {"source_label": source_label, "style_hints": style_hints},
            }
        )
    return specs


def _map_paper_evidence_artifact_to_output_name(label: str) -> str:
    lowered = label.strip().lower()
    if not lowered:
        return ""
    if any(
        token in lowered
        for token in ("flowchart", "flow chart", "participant selection", "patient selection", "study flow")
    ):
        return "cohort_flowchart_figure"
    if any(token in lowered for token in ("kaplan", "kaplan-meier", "km curve", "survival curve", "log-rank")):
        return "km_figure"
    if any(token in lowered for token in ("restricted cubic spline", "rcs", "spline curve")):
        return "rcs_figure"
    if any(token in lowered for token in ("subgroup", "forest plot", "forest")):
        return "subgroup_figure"
    if any(token in lowered for token in ("roc", "auc curve")):
        return "roc_figure"
    if any(token in lowered for token in ("calibration", "calibration curve", "calibration plot")):
        return "calibration_figure"
    if any(token in lowered for token in ("decision curve", "net benefit", "dca")):
        return "decision_curve_figure"
    if "nomogram" in lowered:
        return "nomogram_figure"
    if "shap" in lowered or "beeswarm" in lowered:
        return "shap_figure"
    if "love plot" in lowered:
        return "love_plot_figure"
    if any(token in lowered for token in ("cumulative incidence", "fine-gray", "competing risk", "cif")):
        return "cif_figure"
    if any(token in lowered for token in ("lasso path", "lasso trace", "regularization path")):
        return "lasso_trace_figure"
    if "time-dependent auc" in lowered or "time dependent auc" in lowered:
        return "time_auc_figure"
    if any(token in lowered for token in ("posterior survival", "bayesian survival")):
        return "posterior_survival_figure"
    if any(
        token in lowered
        for token in ("boxplot", "box plot", "violin plot", "swarm plot", "strip plot", "histogram", "density plot")
    ):
        return "distribution_figure"
    if any(token in lowered for token in ("heatmap", "correlation matrix", "clustermap")):
        return "heatmap_figure"
    if "trajectory" in lowered:
        return "trajectory_figure"
    if any(token in lowered for token in ("baseline characteristics", "baseline table", "table 1")):
        return "baseline_table"
    if any(token in lowered for token in ("cox regression", "hazard ratio", "multivariable cox", "table 2")):
        return "cox_results_table"
    if any(token in lowered for token in ("logistic regression", "odds ratio", "multivariable model")):
        return "model_results_table"
    return ""


def _infer_paper_artifact_style_hints(label: str, output_name: str) -> list[str]:
    lowered = label.strip().lower()
    hints: list[str] = []

    if output_name == "cohort_flowchart_figure":
        hints.extend(["participant_flowchart", "sequential_exclusions", "count_boxes"])
    elif output_name == "km_figure":
        hints.extend(["survival_curve"])
        if "risk" in lowered:
            hints.append("number_at_risk")
        if "30-day" in lowered or "90-day" in lowered or "1-year" in lowered or "180-day" in lowered:
            hints.append("multi_panel_if_needed")
    elif output_name == "rcs_figure":
        hints.extend(["smooth_effect_curve", "confidence_band", "reference_line"])
    elif output_name == "subgroup_figure":
        hints.extend(["horizontal_ci_plot", "reference_line_at_1"])
    elif output_name == "roc_figure":
        hints.extend(["roc_curve", "diagonal_reference_line", "auc_annotation"])
    elif output_name == "calibration_figure":
        hints.extend(["reference_diagonal", "calibration_bins_or_smoother"])
    elif output_name == "decision_curve_figure":
        hints.extend(["net_benefit_threshold_curve", "treat_all_vs_none_reference"])
    elif output_name == "nomogram_figure":
        hints.extend(["point_scale_layout", "linear_predictor_to_probability"])
    elif output_name == "shap_figure":
        hints.extend(["beeswarm_or_bar_summary", "feature_rank_ordering"])
    elif output_name == "love_plot_figure":
        hints.extend(["love_plot", "pre_post_balance_diagnostics"])
    elif output_name == "cif_figure":
        hints.extend(["cumulative_incidence_curve", "competing_event_legend"])
    elif output_name == "time_auc_figure":
        hints.extend(["time_dependent_auc_curve", "model_comparison_legend"])
    elif output_name == "posterior_survival_figure":
        hints.extend(["posterior_band", "credible_interval"])
    elif output_name == "distribution_figure":
        hints.extend(["groupwise_color_palette"])
        if "violin" in lowered:
            hints.append("violin_plot")
        if "box" in lowered:
            hints.append("box_plot")
        if "strip" in lowered or "swarm" in lowered:
            hints.append("raw_points_overlay")
        if "paired" in lowered:
            hints.append("paired_or_unpaired_labels")
    elif output_name == "heatmap_figure":
        hints.extend(["annotated_matrix", "colorbar"])

    return list(dict.fromkeys(hints))


def _build_paper_figure_manifest(figure_targets: dict[str, Any]) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    for figure_key, target in figure_targets.items():
        output_name = PAPER_FIGURE_OUTPUT_MAP.get(str(figure_key).strip())
        if not output_name:
            continue
        manifest.append(
            {
                "figure_key": str(figure_key).strip(),
                "output_name": output_name,
                "target": dict(target) if isinstance(target, dict) else {},
            }
        )
    return manifest


def _sync_contract_outputs_with_paper_figure_manifest(
    contract: TaskContract,
    manifest: list[dict[str, Any]],
) -> None:
    if not manifest:
        return

    allowed_figure_outputs = {
        str(item.get("output_name", "")).strip()
        for item in manifest
        if str(item.get("output_name", "")).strip()
    }
    contract.outputs = [
        output
        for output in contract.outputs
        if not _is_figure_output_name(output.name or output.kind) or (output.name in allowed_figure_outputs)
    ]

    outputs_by_name = {str(output.name).strip(): output for output in contract.outputs if str(output.name).strip()}
    for item in manifest:
        output_name = str(item.get("output_name", "")).strip()
        if not output_name:
            continue
        spec = DEFAULT_OUTPUT_SPECS.get(output_name, {})
        output = outputs_by_name.get(output_name)
        if output is None:
            output = OutputSpec(
                name=output_name,
                kind=spec.get("kind", output_name),
                fmt=spec.get("format", "png"),
                required=True,
            )
            contract.outputs.append(output)
            outputs_by_name[output_name] = output
        options = dict(output.options)
        options.update(
            {
                "paper_driven": True,
                "paper_figure_key": str(item.get("figure_key", "")).strip(),
                "paper_source_label": str(item.get("source_label", "")).strip(),
                "paper_figure_target": dict(item.get("target", {})),
                "paper_style_hints": list(item.get("style_hints", [])),
            }
        )
        output.options = options
        if not output.description:
            label = str(options.get("paper_source_label", "")).strip() or str(options["paper_figure_key"]).strip()
            output.description = f"Paper-aligned figure target: {label}"


def _is_figure_output_name(value: str) -> bool:
    normalized = _canonical_name(value)
    return normalized.endswith("_figure")


def _apply_study_template_metadata(contract: TaskContract) -> None:
    template = infer_study_template(contract)
    if template is None:
        contract.meta.pop("study_template", None)
        contract.meta.pop("study_template_title", None)
        contract.meta.pop("study_template_suggested_outputs", None)
        return
    contract.meta["study_template"] = template.key
    contract.meta["study_template_title"] = template.title
    contract.meta["study_template_suggested_outputs"] = list(template.suggested_outputs)


def _apply_auto_binary_profile_metadata(contract: TaskContract) -> None:
    if contract.meta.get("preset"):
        return
    if contract.dataset.adapter not in {"mimic", "mimic_iv", "mimic-iv"}:
        return
    if not _looks_like_arf_nomogram_contract(contract):
        return

    contract.meta.setdefault("auto_binary_profile", "mimic_arf_nomogram_v1")
    contract.meta.setdefault("analysis_dataset_builder", "scripts/analysis/build_arf_nomogram_dataset.py")
    contract.meta.setdefault("execution_backend", "hybrid_binary_runner")
    contract.notes.append(
        "Auto profile enabled: mimic_arf_nomogram_v1 (LLM-derived ARF contract routed to hybrid binary execution with explicit method-gap reporting)."
    )


def _looks_like_arf_nomogram_contract(contract: TaskContract) -> bool:
    outcome_names = [item.name for item in contract.variables if item.role == VariableRole.OUTCOME]
    has_28d_mortality_outcome = any(
        ("mortality" in name.lower() or "death" in name.lower()) and ("28" in name.lower() or "28d" in name.lower())
        for name in outcome_names
    )
    if not has_28d_mortality_outcome:
        return False

    paper_evidence = contract.meta.get("paper_evidence")
    evidence_text = ""
    if isinstance(paper_evidence, dict):
        evidence_text = "\n".join(
            [
                str(paper_evidence.get("title", "")),
                str(paper_evidence.get("population", "")),
                str(paper_evidence.get("cohort_logic", "")),
                " ".join(str(item) for item in paper_evidence.get("outcomes", []) if str(item).strip()),
                " ".join(str(item) for item in paper_evidence.get("method_details", []) if str(item).strip()),
            ]
        )
    haystack = "\n".join(
        [
            contract.title,
            contract.instructions,
            contract.cohort.population,
            " ".join(contract.cohort.inclusion_criteria),
            evidence_text,
        ]
    ).lower()
    has_arf_signal = ("acute respiratory failure" in haystack) or bool(re.search(r"\barf\b", haystack))
    has_nomogram_or_cox_signal = any(
        model.family in {"cox_regression", "nomogram_prediction", "logistic_regression"}
        for model in contract.models
    ) or ("nomogram" in haystack and "cox" in haystack)
    return has_arf_signal and has_nomogram_or_cox_signal


def _apply_semantic_registry_mappings(contract: TaskContract, *, project_root: Path | None) -> None:
    if project_root is None:
        return
    if contract.dataset.adapter not in {"mimic", "mimic_iv", "mimic-iv"}:
        return
    try:
        registry = load_mimic_semantic_registry(project_root)
    except FileNotFoundError:
        return

    mapped_count = 0
    unmapped: list[str] = []
    for variable in contract.variables:
        semantic = resolve_semantic_variable(registry, variable.name)
        if semantic is None:
            if variable.role != VariableRole.ID:
                unmapped.append(variable.name)
            continue
        mapped_count += 1
        if not variable.label:
            variable.label = semantic.name
        if not variable.description and semantic.description:
            variable.description = semantic.description
        if not variable.dataset_field:
            variable.dataset_field = semantic.dataset_field
        if not variable.source_name:
            variable.source_name = semantic.source_name or semantic.name
        variable.meta.setdefault("semantic_name", semantic.name)
        variable.meta.setdefault("semantic_category", semantic.category)
        if semantic.aliases:
            variable.meta.setdefault("semantic_aliases", list(semantic.aliases))

    contract.meta["semantic_registry"] = {
        "dataset": registry.dataset,
        "version": registry.version,
        "source_path": registry.source_path,
    }
    contract.dataset.meta.setdefault("semantic_registry_path", registry.source_path)
    contract.meta["semantic_mapped_variable_count"] = mapped_count
    deduped_unmapped = sorted(dict.fromkeys(unmapped))
    if deduped_unmapped:
        contract.meta["semantic_unmapped_variables"] = deduped_unmapped
    else:
        contract.meta.pop("semantic_unmapped_variables", None)


def _extract_first_int_after_keywords(text: str, keywords: list[str]) -> int | None:
    for keyword in keywords:
        index = text.find(keyword)
        if index < 0:
            continue
        match = re.search(r"(\d{1,3})", text[index : index + 40])
        if match:
            return int(match.group(1))
    return None


def _extract_first_hours(text: str, keywords: list[str]) -> int | None:
    for keyword in keywords:
        index = text.find(keyword)
        if index < 0:
            continue
        snippet = text[max(0, index - 20) : index + 60]
        for pattern in (
            r"(\d{1,3})\s*(?:hours?|hrs?|小时|h)\b",
            r"(?:within|less than|more than|at least|under|over)\s*(\d{1,3})\b",
            r"(\d{1,3})\s*(?:小时内|小时以下|小时以上)",
        ):
            match = re.search(pattern, snippet)
            if match:
                return int(match.group(1))
    return None


def _extract_bullets_after_keywords(text: str, keywords: list[str]) -> list[str]:
    lowered = text.lower()
    for keyword in keywords:
        index = lowered.find(keyword.lower())
        if index < 0:
            continue
        tail = text[index : index + 800]
        lines = []
        for raw_line in tail.splitlines()[1:]:
            line = raw_line.strip(" -*\t")
            if not line:
                if lines:
                    break
                continue
            if len(lines) >= 8:
                break
            lines.append(line)
        if lines:
            return lines
    return []


def _extract_structured_sections(instructions: str) -> dict[str, str]:
    matches = list(STRUCTURED_FIELD_PATTERN.finditer(instructions))
    if not matches:
        return {}
    sections: dict[str, str] = {}
    for index, match in enumerate(matches):
        field_name = STRUCTURED_FIELD_LOOKUP.get(match.group("label").strip().lower())
        if not field_name:
            continue
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(instructions)
        cleaned = _clean_structured_value(instructions[start:end])
        if not cleaned:
            continue
        if field_name in sections:
            sections[field_name] = f"{sections[field_name]}; {cleaned}"
        else:
            sections[field_name] = cleaned
    return sections


def _clean_structured_value(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value).strip()
    cleaned = cleaned.strip(" \t\r\n:：;,，；")
    cleaned = re.sub(r"^[\-\*\u2022]+", "", cleaned).strip()
    cleaned = re.sub(r"[.。;；,，]+$", "", cleaned).strip()
    return cleaned


def _split_variable_answer(text: str) -> list[str]:
    parts = re.split(r"[,\n;/，；、]+", text)
    cleaned = [_clean_structured_value(part) for part in parts]
    return [part for part in cleaned if part]


def _first_role_name(contract: TaskContract, role: VariableRole) -> str:
    for variable in contract.variables:
        if variable.role == role:
            return variable.name
    return ""


def _infer_title_from_path(path: Path) -> str:
    stem = path.stem.replace("_", " ").replace("-", " ").strip()
    return stem or "Untitled task"


def _normalize_output_name(name: str) -> str:
    lowered = name.strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "km": "km_figure",
        "kaplan_meier": "km_figure",
        "baseline": "baseline_table",
        "cox_table": "cox_results_table",
        "cox_results": "cox_results_table",
        "cox_results_table": "cox_results_table",
        "model_table": "model_results_table",
        "rcs": "rcs_figure",
        "subgroup": "subgroup_figure",
        "roc": "roc_figure",
        "calibration": "calibration_figure",
        "decision_curve": "decision_curve_figure",
        "dca": "decision_curve_figure",
        "nomogram": "nomogram_figure",
        "shap": "shap_figure",
        "love_plot": "love_plot_figure",
        "cif": "cif_figure",
        "lasso_trace": "lasso_trace_figure",
        "time_auc": "time_auc_figure",
        "posterior_survival": "posterior_survival_figure",
        "distribution": "distribution_figure",
        "heatmap": "heatmap_figure",
        "report": "reproduction_report",
        "trajectory": "trajectory_figure",
        "trajectory_plot": "trajectory_figure",
        "trajectory_figure": "trajectory_figure",
        "trajectory_table": "trajectory_table",
        "trajectory_summary_table": "trajectory_table",
    }
    return aliases.get(lowered, lowered)


def _extract_explicit_models(
    instructions: str,
    structured_sections: dict[str, str] | None = None,
) -> list[str]:
    sections = structured_sections or _extract_structured_sections(instructions)
    families: list[str] = []
    seen: set[str] = set()
    candidates: list[str] = []
    if sections.get("models"):
        candidates.extend(_split_variable_answer(sections["models"]))
    else:
        patterns = [
            r"模型[:：]\s*([^\n]+)",
            r"models?[:：]?\s*([^\n]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, instructions, flags=re.IGNORECASE)
            if not match:
                continue
            candidates.extend(_split_variable_answer(_clean_structured_value(match.group(1))))
    for raw_name in candidates:
        family = _normalize_model_family(raw_name)
        if family and family not in seen:
            seen.add(family)
            families.append(family)
    return families


def _extract_explicit_outputs(
    instructions: str,
    structured_sections: dict[str, str] | None = None,
) -> list[str]:
    sections = structured_sections or _extract_structured_sections(instructions)
    candidates: list[str] = []
    if sections.get("outputs"):
        candidates.extend(_split_variable_answer(sections["outputs"]))
    else:
        patterns = [
            r"输出[:：]\s*([^\n]+)",
            r"outputs?[:：]?\s*([^\n]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, instructions, flags=re.IGNORECASE)
            if not match:
                continue
            candidates.extend(_split_variable_answer(_clean_structured_value(match.group(1))))
    return [_normalize_output_name(name) for name in candidates]


def _normalize_model_family(name: str) -> str:
    lowered = name.strip().lower().replace(" ", "_")
    aliases = {
        "cox": "cox_regression",
        "cox_regression": "cox_regression",
        "coxph": "cox_regression",
        "kaplan_meier": "kaplan_meier",
        "kaplan-meier": "kaplan_meier",
        "km": "kaplan_meier",
        "logrank": "logrank_test",
        "log-rank": "logrank_test",
        "log_rank": "logrank_test",
        "rcs": "restricted_cubic_spline",
        "restricted_cubic_spline": "restricted_cubic_spline",
        "spline": "restricted_cubic_spline",
        "logistic": "logistic_regression",
        "logistic_regression": "logistic_regression",
        "subgroup": "subgroup_analysis",
        "subgroup_analysis": "subgroup_analysis",
        "interaction": "interaction_analysis",
        "interaction_analysis": "interaction_analysis",
        "anova": "anova",
        "baseline": "baseline_table",
        "baseline_table": "baseline_table",
        "lgmm": "trajectory_mixture_model",
        "latent_growth_mixture_model": "trajectory_mixture_model",
        "latent_growth_mixture_modelling": "trajectory_mixture_model",
        "latent_growth_mixture_modeling": "trajectory_mixture_model",
        "growth_mixture_model": "trajectory_mixture_model",
        "trajectory": "trajectory_mixture_model",
        "trajectory_model": "trajectory_mixture_model",
        "trajectory_analysis": "trajectory_mixture_model",
        "trajectory_mixture_model": "trajectory_mixture_model",
    }
    return aliases.get(lowered, "")


def _canonical_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def _normalize_dataset_adapter_name(value: str, *, default_adapter: str) -> str:
    normalized = _canonical_name(value)
    if normalized in {"", "unknown", "generic"}:
        normalized_default = _canonical_name(default_adapter)
        return normalized_default or "mimic_iv"
    if normalized in {
        "mimic",
        "mimiciv",
        "mimic_iv",
        "mimic_4",
        "mimic-iv",
        "postgres",
        "postgresql",
        "sql",
        "database",
        "relational_database",
        "relational",
    }:
        return "mimic_iv"
    return normalized


def _normalize_dataset_source_type(value: str, *, adapter: str) -> str:
    normalized = _canonical_name(value)
    if adapter in {"mimic", "mimic_iv", "mimic-iv"}:
        if normalized in {"", "unknown", "relational_database", "database", "postgresql", "postgres"}:
            return "postgres"
    if normalized in {"", "unknown"}:
        return "postgres"
    return normalized

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from .config import PipelineConfig
from .contracts import (
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
from .llm import LLMError, OpenAICompatibleClient
from .paper_materials import collect_paper_materials
from .preset_registry import detect_paper_preset, get_paper_preset
from .semantic_registry import load_mimic_semantic_registry, resolve_semantic_variable
from .study_templates import infer_study_template


MODEL_KEYWORDS: dict[str, str] = {
    "cox": "cox_regression",
    "kaplan": "kaplan_meier",
    "km": "kaplan_meier",
    "log-rank": "logrank_test",
    "logrank": "logrank_test",
    "logistic": "logistic_regression",
    "rcs": "restricted_cubic_spline",
    "spline": "restricted_cubic_spline",
    "subgroup": "subgroup_analysis",
    "interaction": "interaction_analysis",
    "anova": "anova",
    "baseline": "baseline_table",
}

OUTPUT_BY_MODEL: dict[str, list[str]] = {
    "baseline_table": ["baseline_table"],
    "cox_regression": ["model_results_table"],
    "kaplan_meier": ["km_figure"],
    "logrank_test": ["km_figure"],
    "restricted_cubic_spline": ["rcs_figure"],
    "subgroup_analysis": ["subgroup_figure"],
    "interaction_analysis": ["subgroup_figure"],
    "logistic_regression": ["model_results_table"],
    "anova": ["baseline_table"],
}

DEFAULT_OUTPUT_SPECS: dict[str, dict[str, str]] = {
    "cohort_funnel": {"kind": "cohort_funnel", "format": "json"},
    "analysis_dataset": {"kind": "analysis_dataset", "format": "csv"},
    "missingness_report": {"kind": "missingness_report", "format": "json"},
    "baseline_table": {"kind": "baseline_table", "format": "csv"},
    "model_results_table": {"kind": "model_results_table", "format": "csv"},
    "km_figure": {"kind": "km_figure", "format": "png"},
    "rcs_figure": {"kind": "rcs_figure", "format": "png"},
    "subgroup_figure": {"kind": "subgroup_figure", "format": "csv"},
    "reproduction_report": {"kind": "reproduction_report", "format": "md"},
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


@dataclass
class TaskBuildResult:
    contract: TaskContract
    used_llm: bool
    llm_error: str = ""
    paper_materials: dict[str, str] | None = None


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

    if use_llm:
        client = OpenAICompatibleClient(config.llm)
        if client.is_enabled():
            try:
                payload, _ = client.complete_json(
                    _task_builder_messages(
                        paper_path=paper_path,
                        instructions=instructions,
                        materials=materials,
                        dataset=config.run.dataset,
                    )
                )
                contract = TaskContract.from_dict(payload)
                contract.task_id = contract.task_id or task_id
                contract.title = contract.title or _infer_title_from_path(paper_abspath)
                contract.execution_mode = ExecutionMode.AGENTIC
                contract.interaction_mode = InteractionMode.CHAT
                contract.source_paper_path = paper_path
                contract.instructions = instructions
                _seed_contract_runtime_context(
                    contract,
                    dataset_label=config.run.dataset,
                    instructions=instructions,
                    paper_materials=materials,
                )
                contract = normalize_task_contract(contract, config=config, project_root=project_root)
                return TaskBuildResult(contract=contract, used_llm=True, paper_materials=materials)
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
    heuristic_contract = normalize_task_contract(heuristic_contract, config=config, project_root=project_root)
    return TaskBuildResult(
        contract=heuristic_contract,
        used_llm=False,
        llm_error=llm_error,
        paper_materials=materials,
    )


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
    if not contract.dataset.source_type:
        contract.dataset.source_type = "postgres"
    if not contract.dataset.connector_env_prefix:
        contract.dataset.connector_env_prefix = "MIMIC_PG"
    if not contract.dataset.version:
        contract.dataset.version = config.run.dataset
    if not contract.dataset.schemas and contract.dataset.adapter in {"mimic", "mimic_iv", "mimic-iv"}:
        contract.dataset.schemas = ["mimiciv_hosp", "mimiciv_icu", "mimiciv_derived"]
    if not contract.cohort.population:
        contract.cohort.population = "critically ill ICU patients"

    _dedupe_variables(contract)
    _apply_preset_metadata(contract, project_root=project_root)
    _apply_semantic_registry_mappings(contract, project_root=project_root)
    _ensure_default_models(contract)
    _apply_study_template_metadata(contract)
    _ensure_default_outputs(contract)
    _ensure_default_notes(contract)
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
        for family in _split_variable_answer(answers["models"]):
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
        min_age=_extract_first_int_after_keywords(text, ["age >=", "年龄≥", "aged over", "over 18", "older than"]),
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
) -> list[dict[str, str]]:
    material_blob = "\n\n".join(f"[{name}]\n{content[:12000]}" for name, content in materials.items())
    schema_hint = """
Return a JSON object with keys:
task_id, title, source_paper_path, instructions, dataset, cohort, variables, models, outputs, notes, verification_targets.
dataset keys: name, adapter, source_type, connector_env_prefix, version, schemas, meta.
cohort keys: population, inclusion_criteria, exclusion_criteria, diagnosis_logic, screening_steps, first_stay_only, min_age, max_age, min_icu_los_hours, max_admit_to_icu_hours, required_measurements, meta.
variables: list of objects with keys name, role, label, description, dataset_field, source_name, transform, formula, unit, required, meta.
models: list of objects with keys name, family, exposure_variables, outcome_variables, control_variables, subgroup_variables, time_variable, description, options.
outputs: list of objects with keys name, kind, format, description, required, model_refs, options.
Allowed variable roles: exposure, outcome, control, subgroup, time, id, derived.
"""
    return [
        {
            "role": "system",
            "content": (
                "You are building a structured clinical paper reproduction task contract. "
                "Use the paper materials and user instructions. Prefer explicit paper facts. "
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
                f"Paper materials:\n{material_blob}"
            ),
        },
    ]


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
    if "hospital mortality" in combined_text and not any(item.name == "in_hospital_mortality" for item in variables):
        variables.append(VariableSpec(name="in_hospital_mortality", role=VariableRole.OUTCOME))
    if "icu mortality" in combined_text and not any(item.name == "icu_mortality" for item in variables):
        variables.append(VariableSpec(name="icu_mortality", role=VariableRole.OUTCOME))
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
    notes.add("Task contract was generated from user instructions and available paper materials.")
    contract.notes = sorted(notes)


def _dedupe_variables(contract: TaskContract) -> None:
    deduped: dict[tuple[str, str], VariableSpec] = {}
    for variable in contract.variables:
        key = (_canonical_name(variable.name), variable.role.value)
        deduped[key] = variable
    contract.variables = list(deduped.values())


def _seed_contract_runtime_context(
    contract: TaskContract,
    *,
    dataset_label: str,
    instructions: str,
    paper_materials: dict[str, str],
) -> None:
    preset = detect_paper_preset(
        dataset_label=dataset_label or contract.dataset.name,
        instructions=instructions,
        materials=paper_materials,
    )
    if preset is None:
        preset = get_paper_preset(contract.meta.get("preset"))
    if preset is None:
        return
    contract.meta["preset"] = preset.key
    contract.meta.setdefault("preset_title", preset.title)
    contract.meta.setdefault("execution_backend", preset.execution_backend)
    contract.meta.setdefault("preset_description", preset.description)
    contract.meta.setdefault("preset_supported_domains", list(preset.supported_domains))
    if not contract.dataset.adapter or contract.dataset.adapter in {"unknown", "generic"}:
        contract.dataset.adapter = preset.dataset_adapter


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
        "cox_table": "model_results_table",
        "model_table": "model_results_table",
        "rcs": "rcs_figure",
        "subgroup": "subgroup_figure",
        "report": "reproduction_report",
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
    }
    return aliases.get(lowered, "")


def _canonical_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized

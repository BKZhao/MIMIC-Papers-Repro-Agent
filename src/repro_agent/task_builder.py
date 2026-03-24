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
from .paper_contract import build_paper_alignment_contract
from .paper_materials import collect_paper_materials


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
                contract = normalize_task_contract(contract, config=config)
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
    heuristic_contract = normalize_task_contract(heuristic_contract, config=config)
    return TaskBuildResult(
        contract=heuristic_contract,
        used_llm=False,
        llm_error=llm_error,
        paper_materials=materials,
    )


def normalize_task_contract(contract: TaskContract, config: PipelineConfig) -> TaskContract:
    if not contract.dataset.name or contract.dataset.name == "unknown":
        contract.dataset = DatasetSpec(
            name=config.run.dataset or "MIMIC-IV",
            adapter=config.dataset_adapters.default_adapter,
            source_type="postgres",
            connector_env_prefix="MIMIC_PG",
            version=config.run.dataset,
            schemas=["mimiciv_hosp", "mimiciv_icu", "mimiciv_derived"],
        )
    if not contract.cohort.population:
        contract.cohort.population = "critically ill ICU patients"

    _dedupe_variables(contract)
    _apply_builtin_presets(contract)
    _ensure_default_models(contract)
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
    return (
        f"Task: {contract.title}\n"
        f"Dataset: {contract.dataset.name} ({contract.dataset.adapter})\n"
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
    if not cohort.inclusion_criteria and "first icu" in text:
        cohort.inclusion_criteria.append("first ICU stay")
    if "glucose" in text:
        cohort.required_measurements.append("blood_glucose")
    if "triglyceride" in text or "甘油三酯" in text or "tg" in text:
        cohort.required_measurements.append("triglycerides")

    variables = _infer_variables(instructions=instructions, combined_text=text)
    models = _infer_models(instructions=instructions, combined_text=text, variables=variables)
    outputs = _infer_outputs(instructions=instructions, models=models)
    meta = {}
    if "tyg" in text and "sepsis" in text and "mimic" in text:
        meta["preset"] = "mimic_tyg_sepsis"

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
        meta=meta,
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


def _infer_variables(instructions: str, combined_text: str) -> list[VariableSpec]:
    variables: list[VariableSpec] = []
    role_patterns = {
        VariableRole.EXPOSURE: [r"自变量[:：]\s*([^\n]+)", r"independent variables?[:：]?\s*([^\n]+)", r"exposure[:：]?\s*([^\n]+)"],
        VariableRole.OUTCOME: [r"因变量[:：]\s*([^\n]+)", r"dependent variables?[:：]?\s*([^\n]+)", r"outcomes?[:：]?\s*([^\n]+)"],
        VariableRole.CONTROL: [r"控制变量[:：]\s*([^\n]+)", r"covariates?[:：]?\s*([^\n]+)", r"adjust(?:ed)? for[:：]?\s*([^\n]+)"],
        VariableRole.SUBGROUP: [r"亚组变量[:：]\s*([^\n]+)", r"subgroups?[:：]?\s*([^\n]+)"],
        VariableRole.TIME: [r"时间变量[:：]\s*([^\n]+)", r"time variable[:：]?\s*([^\n]+)"],
    }
    for role, patterns in role_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, instructions, flags=re.IGNORECASE)
            if not match:
                continue
            for variable in _split_variable_answer(match.group(1)):
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


def _infer_models(instructions: str, combined_text: str, variables: list[VariableSpec]) -> list[ModelSpec]:
    families: list[str] = []
    explicit_families = _extract_explicit_models(instructions)
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


def _infer_outputs(instructions: str, models: list[ModelSpec]) -> list[OutputSpec]:
    outputs: list[str] = ["cohort_funnel", "analysis_dataset", "missingness_report", "reproduction_report"]
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
    if contract.meta.get("preset") == "mimic_tyg_sepsis":
        notes.add("This task matches the built-in MIMIC TyG sepsis preset.")
        notes.add("Preset tasks can use the deterministic bridge while the generic agentic contract remains the source of truth.")
    notes.add("Task contract was generated from user instructions and available paper materials.")
    contract.notes = sorted(notes)


def _dedupe_variables(contract: TaskContract) -> None:
    deduped: dict[tuple[str, str], VariableSpec] = {}
    for variable in contract.variables:
        key = (_canonical_name(variable.name), variable.role.value)
        deduped[key] = variable
    contract.variables = list(deduped.values())


def _apply_builtin_presets(contract: TaskContract) -> None:
    if contract.meta.get("preset") != "mimic_tyg_sepsis":
        return
    contract.meta.setdefault("execution_backend", "deterministic_bridge")
    if contract.verification_targets:
        return
    paper_contract = build_paper_alignment_contract()
    contract.verification_targets = [dict(item) for item in paper_contract.get("metric_targets", [])]


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


def _split_variable_answer(text: str) -> list[str]:
    parts = re.split(r"[,\n;/，；、]+", text)
    return [part.strip() for part in parts if part.strip()]


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


def _extract_explicit_models(instructions: str) -> list[str]:
    patterns = [
        r"模型[:：]\s*([^\n]+)",
        r"models?[:：]?\s*([^\n]+)",
    ]
    families: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        match = re.search(pattern, instructions, flags=re.IGNORECASE)
        if not match:
            continue
        for raw_name in _split_variable_answer(match.group(1)):
            family = _normalize_model_family(raw_name)
            if family and family not in seen:
                seen.add(family)
                families.append(family)
    return families


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

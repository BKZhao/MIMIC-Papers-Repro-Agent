from __future__ import annotations

from typing import Any

from ..contracts import TaskContract, VariableRole
from .builder import summarize_task_contract


def build_paper_spec_surface(
    contract: TaskContract,
    *,
    paper_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    paper_evidence = dict(paper_evidence or contract.meta.get("paper_evidence") or {})
    variable_roles = _group_variables_by_role(contract)
    paper_target_dataset_version = str(
        contract.meta.get("paper_target_dataset_version")
        or paper_evidence.get("paper_target_dataset_version")
        or contract.dataset.version
    ).strip()
    execution_environment_dataset_version = str(
        contract.meta.get("execution_environment_dataset_version")
        or contract.meta.get("configured_dataset_version")
        or contract.dataset.version
    ).strip()
    configured_dataset_version = str(
        contract.meta.get("configured_dataset_version")
        or execution_environment_dataset_version
        or contract.dataset.version
    ).strip()

    return {
        "surface_version": "1",
        "surface_kind": "paper_spec_surface",
        "task_id": contract.task_id,
        "title": contract.title,
        "source_paper_path": contract.source_paper_path,
        "task_summary": summarize_task_contract(contract),
        "dataset_semantics": {
            "paper_target_dataset_version": paper_target_dataset_version,
            "execution_environment_dataset_version": execution_environment_dataset_version,
            "configured_dataset_version": configured_dataset_version,
            "execution_year_window": str(contract.meta.get("execution_year_window", "")).strip(),
            "dataset_version_mismatch": bool(contract.meta.get("dataset_version_mismatch", False)),
        },
        "cohort_spec": {
            "population": contract.cohort.population,
            "inclusion_criteria": list(contract.cohort.inclusion_criteria),
            "exclusion_criteria": list(contract.cohort.exclusion_criteria),
            "diagnosis_logic": contract.cohort.diagnosis_logic,
            "screening_steps": list(contract.cohort.screening_steps),
            "first_stay_only": contract.cohort.first_stay_only,
            "min_age": contract.cohort.min_age,
            "max_age": contract.cohort.max_age,
            "min_icu_los_hours": contract.cohort.min_icu_los_hours,
            "required_measurements": list(contract.cohort.required_measurements),
        },
        "variable_roles": variable_roles,
        "model_spec": [
            {
                "name": item.name,
                "family": item.family,
                "exposure_variables": list(item.exposure_variables),
                "outcome_variables": list(item.outcome_variables),
                "control_variables": list(item.control_variables),
                "subgroup_variables": list(item.subgroup_variables),
                "time_variable": item.time_variable,
            }
            for item in contract.models
        ],
        "output_targets": [
            {
                "name": item.name,
                "kind": item.kind,
                "format": item.fmt,
                "required": item.required,
                "model_refs": list(item.model_refs),
                "description": item.description,
                "options": dict(item.options),
            }
            for item in contract.outputs
        ],
        "paper_required_methods": list(contract.meta.get("paper_required_methods", [])),
        "fidelity_markers": {
            "requires_longitudinal_trajectory_modeling": bool(
                contract.meta.get("requires_longitudinal_trajectory_modeling", False)
            ),
            "experimental_profile": str(contract.meta.get("experimental_profile", "")).strip(),
            "execution_backend_hint": str(contract.meta.get("execution_backend", "")).strip(),
        },
        "paper_evidence_digest": {
            "title": str(paper_evidence.get("title", "")).strip(),
            "paper_target_dataset_version": str(paper_evidence.get("paper_target_dataset_version", "")).strip(),
            "execution_year_window": str(paper_evidence.get("execution_year_window", "")).strip(),
            "exposure_variables": _as_text_list(paper_evidence.get("exposure_variables")),
            "outcome_variables": _as_text_list(paper_evidence.get("outcome_variables")),
            "models": _as_text_list(paper_evidence.get("models")),
            "outputs": _as_text_list(paper_evidence.get("outputs")),
            "requested_figures": _as_text_list(paper_evidence.get("requested_figures")),
            "requested_tables": _as_text_list(paper_evidence.get("requested_tables")),
        },
    }


def build_analysis_spec_surface(
    contract: TaskContract,
    *,
    decision: Any,
    analysis_family_route: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route = dict(analysis_family_route or getattr(decision, "analysis_family_route", {}) or {})
    deterministic_steps = [
        "cohort_extraction",
        "analysis_dataset_build",
        "statistics",
        "figure_generation",
    ]
    control_plane_steps = [
        "paper_parsing",
        "study_design_normalization",
        "route_planning",
        "verification_framing",
        "report_narration",
    ]
    fidelity_gaps: list[str] = []
    if str(getattr(decision, "execution_backend", "")).strip() == "trajectory_python_bridge":
        fidelity_gaps.extend(
            [
                "paper-identical LGMM is not implemented",
                "the current backend is method-aligned Python trajectory fitting",
            ]
        )
    if bool(getattr(decision, "dataset_version_mismatch", False)):
        fidelity_gaps.append("paper dataset version and execution environment version do not fully match")

    return {
        "surface_version": "1",
        "surface_kind": "analysis_spec_surface",
        "task_id": contract.task_id,
        "title": contract.title,
        "route_summary": {
            "mode": str(getattr(decision, "mode", "")).strip(),
            "status": str(getattr(decision, "status", "")).strip(),
            "execution_supported": bool(getattr(decision, "execution_supported", False)),
            "execution_backend": str(getattr(decision, "execution_backend", "")).strip(),
            "recommended_run_profile": str(getattr(decision, "recommended_run_profile", "")).strip(),
            "planning_only": bool(getattr(decision, "planning_only", False)),
            "deterministic_bridge": bool(getattr(decision, "deterministic_bridge", False)),
        },
        "dataset_contract": {
            "name": contract.dataset.name,
            "adapter": contract.dataset.adapter,
            "source_type": contract.dataset.source_type,
            "connector_env_prefix": contract.dataset.connector_env_prefix,
            "schemas": list(contract.dataset.schemas),
            "paper_target_dataset_version": str(getattr(decision, "paper_target_dataset_version", "")).strip(),
            "execution_environment_dataset_version": str(
                getattr(decision, "execution_environment_dataset_version", "")
            ).strip(),
            "execution_year_window": str(getattr(decision, "execution_year_window", "")).strip(),
        },
        "cohort_seed_contract": {
            "population": contract.cohort.population,
            "first_stay_only": contract.cohort.first_stay_only,
            "min_age": contract.cohort.min_age,
            "min_icu_los_hours": contract.cohort.min_icu_los_hours,
            "required_measurements": list(contract.cohort.required_measurements),
        },
        "variable_inventory": _group_variables_by_role(contract),
        "model_inventory": [
            {
                "name": item.name,
                "family": item.family,
                "exposure_variables": list(item.exposure_variables),
                "outcome_variables": list(item.outcome_variables),
                "control_variables": list(item.control_variables),
                "subgroup_variables": list(item.subgroup_variables),
                "time_variable": item.time_variable,
            }
            for item in contract.models
        ],
        "output_contract": [
            {
                "name": item.name,
                "kind": item.kind,
                "format": item.fmt,
                "required": item.required,
                "description": item.description,
                "options": dict(item.options),
            }
            for item in contract.outputs
        ],
        "analysis_family_route": route,
        "selected_agent_sequence": list(getattr(decision, "selected_agent_sequence", ()) or ()),
        "missing_high_impact_fields": list(getattr(decision, "missing_high_impact_fields", ()) or ()),
        "blocking_gaps": list(getattr(decision, "missing_capabilities", ()) or ()),
        "fidelity_gaps": fidelity_gaps,
        "control_plane_steps": control_plane_steps,
        "deterministic_steps": deterministic_steps,
        "rationale": str(getattr(decision, "rationale", "")).strip(),
        "next_actions": list(getattr(decision, "next_actions", ()) or ()),
    }


def _group_variables_by_role(contract: TaskContract) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {
        role.value: []
        for role in VariableRole
    }
    for item in contract.variables:
        grouped[item.role.value].append(
            {
                "name": item.name,
                "label": item.label,
                "dataset_field": item.dataset_field,
                "source_name": item.source_name,
                "required": item.required,
                "transform": item.transform,
            }
        )
    return grouped


def _as_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []

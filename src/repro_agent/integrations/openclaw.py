from __future__ import annotations

import copy
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..agentic.runner import AgentRunner
from ..config import PipelineConfig, load_pipeline_config
from ..contracts import SessionState, TaskContract
from ..dataset_adapters import get_dataset_adapter
from ..pipeline import PaperReproPipeline
from ..paper.presets import get_paper_preset, list_builtin_presets
from ..runtime import LocalRuntime
from ..registry.semantic import SemanticRegistry, load_mimic_semantic_registry
from ..registry.skill_contracts import SkillContractManifest, load_skill_contract_manifest
from ..registry.skills import list_core_clinical_analysis_families
from ..paper.templates import infer_study_template, list_study_templates
from ..paper.builder import (
    apply_follow_up_answers,
    build_task_contract,
    normalize_task_contract,
    refresh_task_contract_context,
)
from ..registry.codex_skill_bridge import (
    default_codex_skill_bridge_manifest_path,
    load_codex_skill_bridge_manifest,
)
from ..workflow_contract import default_mimic_paper_workflow


OPENCLAW_AGENT_NAME = "paper-repro-scientist"
OPENCLAW_SOUL_PATH = "openclaw/SOUL.MD"
OPENCLAW_AGENTS_PATH = "openclaw/AGENTS.md"
OPENCLAW_CODEX_SKILL_BRIDGE_PATH = "openclaw/skills/codex_skill_bridge.yaml"

OPENCLAW_SKILL_PATHS: dict[str, str] = {
    "paper_intake_and_contract": "openclaw/skills/paper_intake_and_contract/SKILL.md",
    "mimic_cohort_execution": "openclaw/skills/mimic_cohort_execution/SKILL.md",
    "analysis_dataset_expansion": "openclaw/skills/analysis_dataset_expansion/SKILL.md",
    "longitudinal_trajectory_execution": "openclaw/skills/longitudinal_trajectory_execution/SKILL.md",
    "survival_stats_execution": "openclaw/skills/survival_stats_execution/SKILL.md",
    "result_figure_generation": "openclaw/skills/result_figure_generation/SKILL.md",
    "paper_alignment_verification": "openclaw/skills/paper_alignment_verification/SKILL.md",
}

OPENCLAW_RUN_PROFILES: dict[str, dict[str, Any]] = {
    "plan_only": {
        "config_path": "configs/openclaw.agentic.yaml",
        "dry_run": True,
        "purpose": "Build a TaskContract and return planning metadata without forcing a real database execution.",
    },
    "agentic_repro": {
        "config_path": "configs/openclaw.agentic.yaml",
        "dry_run": False,
        "purpose": "Plan then execute through the single paper-repro-scientist agent using the TaskContract bridge.",
    },
    "preset_real_run": {
        "config_path": "configs/openclaw.mimic-real-run.yaml",
        "dry_run": False,
        "purpose": "Run a preset-backed MIMIC reproduction and always emit tables, figures, verification, and report artifacts.",
    },
}

OPENCLAW_REQUEST_ALLOWED_FIELDS: set[str] = {
    "paper_path",
    "paper_content",
    "instructions",
    "config_path",
    "session_id",
    "answers",
    "run_mode",
    "use_llm",
    "dry_run",
}

OPENCLAW_REQUEST_TEMPLATES: dict[str, dict[str, Any]] = {
    "plan_only": {
        "paper_path": "papers/s12890-025-04067-0.pdf",
        "instructions": "Read the paper, extract evidence, and produce a TaskContract only.",
        "session_id": "session-openclaw-plan-demo",
        "run_mode": "plan_only",
        "config_path": "configs/openclaw.agentic.yaml",
        "use_llm": True,
    },
    "agentic_repro": {
        "paper_path": "papers/s12890-025-04067-0.pdf",
        "instructions": "Read the paper, build TaskContract, and auto-run when execution becomes ready.",
        "session_id": "session-openclaw-agentic-demo",
        "run_mode": "agentic_repro",
        "config_path": "configs/openclaw.agentic.yaml",
        "use_llm": True,
        "dry_run": False,
    },
    "follow_up": {
        "session_id": "session-openclaw-agentic-demo",
        "answers": {
            "exposure_variables": "TyG index",
            "outcome_variables": "28-day mortality",
            "models": "Cox, Kaplan-Meier, RCS, subgroup analyses",
            "outputs": "Table 1, KM figure, spline figure, ROC summary",
        },
        "instructions": "Apply follow-up answers and continue contract completion.",
        "run_mode": "agentic_repro",
        "config_path": "configs/openclaw.agentic.yaml",
        "dry_run": False,
    },
}

OPENCLAW_AGENT_CONTRACT: dict[str, Any] = {
    "agent_name": OPENCLAW_AGENT_NAME,
    "single_entrypoint": True,
    "external_role": "OpenClaw-facing clinical paper reproduction scientist",
    "architecture_posture": "hybrid_llm_plus_deterministic",
    "accepted_request_fields": [
        "paper_path | paper_content",
        "instructions",
        "config_path",
        "session_id?",
        "answers?",
        "run_mode?",
        "use_llm?",
    ],
    "request_modes": {
        "plan_only": "Build and persist a TaskContract without claiming completed execution.",
        "agentic_repro": "Build a TaskContract and execute supported steps through the agent runner.",
        "preset_real_run": "Prefer deterministic preset execution for supported MIMIC papers and return real artifacts.",
    },
    "guaranteed_response_fields": [
        "session_id",
        "status",
        "execution_backend",
        "execution_supported",
        "missing_high_impact_fields",
        "agent_decision",
        "agent_reply",
        "llm_execution_plan",
        "analysis_family_route",
        "analysis_family_route_path",
        "paper_evidence",
        "paper_evidence_path",
        "paper_spec_surface",
        "paper_spec_surface_path",
        "analysis_spec_surface",
        "analysis_spec_surface_path",
        "task_build_mode",
    ],
    "primary_exchange_object": "TaskContract",
    "artifact_roots": [
        "shared/",
        "results/",
        "shared/sessions/<session_id>/",
    ],
    "success_definition": [
        "For preset-supported MIMIC papers: return real cohort, stats, figure, verification, and report artifacts.",
        "For supported experimental profiles such as the heart-rate trajectory paper: return real cohort, dataset, trajectory, stats, figure, and report artifacts while preserving fidelity-gap notes.",
        "For unsupported or incomplete papers: return a clear planning result with missing fields and blocked capabilities.",
    ],
}


def describe_openclaw_integration(project_root: Path) -> dict[str, Any]:
    registry = _try_load_semantic_registry(project_root)
    skill_manifest = _try_load_skill_contract_manifest(project_root)
    codex_skill_bridge = _try_load_codex_skill_bridge_manifest(project_root)
    workflow_contract = default_mimic_paper_workflow()
    skill_paths = dict(OPENCLAW_SKILL_PATHS)
    if skill_manifest is not None:
        skill_paths = {name: contract.path for name, contract in skill_manifest.skills.items()}
    codex_skill_bridge_path = str(default_codex_skill_bridge_manifest_path(project_root))
    if codex_skill_bridge is not None:
        codex_skill_bridge_path = str(codex_skill_bridge.get("source_path", codex_skill_bridge_path)).strip()
    return {
        "agent_name": OPENCLAW_AGENT_NAME,
        "soul_path": OPENCLAW_SOUL_PATH,
        "agents_path": OPENCLAW_AGENTS_PATH,
        "recommended_config": "configs/openclaw.agentic.yaml",
        "recommended_real_run_config": "configs/openclaw.mimic-real-run.yaml",
        "architecture_posture": "hybrid_llm_plus_deterministic",
        "mission": (
            "Turn paper materials and user instructions into a structured TaskContract, "
            "use LLM reasoning for paper understanding and ambiguity resolution, "
            "use deterministic tooling for SQL, statistics, and figures, "
            "and always distinguish real execution from planning-only output."
        ),
        "agent_contract": dict(OPENCLAW_AGENT_CONTRACT),
        "initial_scope": [
            "MIMIC-IV",
            "PostgreSQL",
            "clinical observational studies",
            "common survival and regression analyses",
        ],
        "default_workflow": [phase.title for phase in workflow_contract.phases],
        "workflow_contract": workflow_contract.as_dict(),
        "capability_summary": {
            "overall_readiness": "partial_real_execution",
            "current_positioning": "clinical_paper_reproduction_engine_v1",
            "already_supported": [
                "TaskContract-driven paper intake and planning",
                "Hybrid planning that uses LLM reasoning for paper semantics and deterministic tooling for execution",
                "PDF-first paper extraction with explicit paper-evidence artifacts",
                "Deterministic MIMIC-IV TyG sepsis reproduction",
                "Experimental heart-rate trajectory execution with the Python trajectory bridge",
                "Artifact-first cohort, analysis dataset, stats, verification, and report outputs",
                "Preset registry, study template inference, semantic variable mapping, and skill contracts",
                "Core clinical analysis-family registry covering native, llm-compiled, and planning-reference method routes",
            ],
            "not_yet_supported": [
                "Generic SQL compilation from arbitrary CohortSpec to executable MIMIC SQL",
                "Generic variable-to-field mapping for arbitrary MIMIC papers",
                "Broad true end-to-end execution for arbitrary non-preset contracts",
                "Full deterministic execution for every statistical family found in arbitrary MIMIC papers",
                "Paper-identical LGMM execution for longitudinal sepsis papers with repeated vital-sign measurements",
            ],
        },
        "decision_boundaries": [
            "Do not pretend to execute unsupported non-MIMIC datasets.",
            "When exposure, outcome, model, or cohort logic is missing, complete the contract before execution.",
            "Always distinguish deterministic reproduction output from planning blueprints.",
        ],
        "failure_modes": [
            "Missing SQL tables or schemas -> return missing dependency diagnostics.",
            "Non-supported contract -> return planning blueprint instead of claiming a completed reproduction.",
            "Suspicious paper targets or OCR conflicts -> surface them as suspect in verification output.",
        ],
        "interfaces": [
            {
                "name": "plan_task",
                "description": "Build and persist a structured TaskContract from paper materials and user instructions.",
                "inputs": ["paper_path | paper_content", "instructions", "config", "session_id?"],
                "outputs": [
                    "task_contract",
                    "paper_evidence",
                    "paper_evidence_path",
                    "paper_spec_surface",
                    "paper_spec_surface_path",
                    "analysis_spec_surface",
                    "analysis_spec_surface_path",
                    "missing_high_impact_fields",
                    "agent_decision",
                    "analysis_family_route",
                    "llm_execution_plan",
                    "follow_up_questions",
                    "recommended_run_profile",
                    "paper_target_dataset_version",
                    "execution_environment_dataset_version",
                    "execution_year_window",
                    "dataset_version_mismatch",
                    "session_id",
                    "execution_backend",
                ],
            },
            {
                "name": "run_task",
                "description": "Execute a planned session through the multi-agent runner.",
                "inputs": ["session_id", "config", "dry_run?"],
                "outputs": ["summary", "artifacts", "status"],
            },
            {
                "name": "continue_session",
                "description": (
                    "Apply follow-up answers to an existing session, refresh the TaskContract, "
                    "and optionally auto-run when the task becomes ready."
                ),
                "inputs": ["session_id", "answers", "instructions?", "config", "run_if_ready?", "dry_run?"],
                "outputs": [
                    "task_contract",
                    "missing_high_impact_fields",
                    "agent_decision",
                    "analysis_family_route",
                    "paper_spec_surface",
                    "analysis_spec_surface",
                    "llm_execution_plan",
                    "follow_up_questions",
                    "recommended_run_profile",
                    "agent_reply",
                    "paper_target_dataset_version",
                    "execution_environment_dataset_version",
                    "execution_year_window",
                    "dataset_version_mismatch",
                    "execution?",
                ],
            },
            {
                "name": "export_contract",
                "description": "Export a persisted TaskContract by session id or contract path.",
                "inputs": ["session_id | contract_path"],
                "outputs": ["task_contract"],
            },
            {
                "name": "run_preset_pipeline",
                "description": "Run the deterministic pipeline for a preset configuration.",
                "inputs": ["config_path", "dry_run?"],
                "outputs": ["summary"],
            },
            {
                "name": "extract_analysis_dataset",
                "description": "Build the profile-driven analysis dataset and missingness report.",
                "inputs": ["project_root", "profile", "sepsis_source", "output", "missingness_output"],
                "outputs": ["analysis_dataset.csv", "analysis_missingness.json"],
            },
            {
                "name": "handle_openclaw_request",
                "description": (
                    "Single-entrypoint adapter for external orchestrators. Accepts one request object and routes it "
                    "through plan/continue/run based on run_mode and current readiness."
                ),
                "inputs": [
                    "request.paper_path | request.paper_content",
                    "request.instructions?",
                    "request.session_id?",
                    "request.answers?",
                    "request.run_mode?",
                    "request.config_path?",
                    "request.use_llm?",
                    "request.dry_run?",
                ],
                "outputs": [
                    "session_id",
                    "status",
                    "execution_supported",
                    "missing_high_impact_fields",
                    "agent_decision",
                    "analysis_family_route",
                    "execution?",
                    "artifacts?",
                ],
            },
        ],
        "skill_paths": skill_paths,
        "skill_contract_manifest_path": skill_manifest.source_path if skill_manifest is not None else "",
        "skill_contract_manifest": skill_manifest.as_dict() if skill_manifest is not None else {},
        "skill_contracts": (
            {name: contract.as_dict() for name, contract in skill_manifest.skills.items()}
            if skill_manifest is not None
            else {}
        ),
        "bridge_artifacts": {
            "codex_skill_bridge": {
                "path": OPENCLAW_CODEX_SKILL_BRIDGE_PATH,
                "present": codex_skill_bridge is not None,
                "purpose": str(codex_skill_bridge.get("purpose", "")).strip() if codex_skill_bridge else "",
                "project_skill_root": (
                    str(codex_skill_bridge.get("repo_skill_root", ".codex/skills")).strip()
                    if codex_skill_bridge
                    else ".codex/skills"
                ),
                "project_skill_count": (
                    int(codex_skill_bridge.get("project_skill_count", 0))
                    if codex_skill_bridge
                    else 0
                ),
                "category_group_count": (
                    int(codex_skill_bridge.get("category_group_count", 0))
                    if codex_skill_bridge
                    else 0
                ),
                "stage_bridge_count": (
                    int(codex_skill_bridge.get("stage_bridge_count", 0))
                    if codex_skill_bridge
                    else 0
                ),
            }
        },
        "codex_skill_bridge_path": codex_skill_bridge_path,
        "codex_skill_bridge": codex_skill_bridge if codex_skill_bridge is not None else {},
        "clinical_analysis_registry": {
            family.key: family.as_dict()
            for family in list_core_clinical_analysis_families()
        },
        "openclaw_request_templates": {
            name: get_openclaw_request_template(name)
            for name in sorted(OPENCLAW_REQUEST_TEMPLATES)
        },
        "run_profiles": dict(OPENCLAW_RUN_PROFILES),
        "supported_presets": [preset.as_dict() for preset in list_builtin_presets()],
        "supported_study_templates": [template.as_dict() for template in list_study_templates()],
        "semantic_registry": registry.as_dict() if registry is not None else {},
        "recommended_model_lanes": {
            "paper_parser_agent": "strong_reasoning_or_long_context",
            "study_design_agent": "strong_reasoning",
            "verify_agent": "mid_to_high_verification",
            "report_agent": "mid_to_high_reporting",
            "cohort_agent": "deterministic_local_sql_python",
            "feature_agent": "deterministic_local_sql_python",
            "stats_agent": "deterministic_local_sql_python",
            "figure_agent": "deterministic_local_plotting",
        },
        "artifact_roots": {
            "shared": "shared/",
            "results": "results/",
            "sessions": "shared/sessions/<session_id>/",
        },
        "openclaw_handoff": {
            "call_pattern": [
                "OpenClaw sends paper_path or paper_content plus instructions.",
                "Call plan_task first.",
                "If follow_up_questions are returned, call continue_session with structured answers.",
                "If execution_supported is true and required fields are complete, call run_task.",
                "Read final figures, tables, and report from returned artifacts.",
            ],
            "do_not_do": [
                "Do not let OpenClaw infer cohort SQL itself.",
                "Do not let OpenClaw invent a second task schema outside TaskContract.",
                "Do not claim unsupported papers are fully reproduced.",
            ],
        },
    }


def plan_task(
    *,
    project_root: Path,
    config_path: Path,
    paper_path: str = "",
    paper_content: str = "",
    instructions: str = "",
    session_id: str = "",
    use_llm: bool = True,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    _load_project_env(project_root)
    config = load_pipeline_config(config_path.resolve())
    actual_paper_path = _materialize_paper_input(
        project_root=project_root,
        paper_path=paper_path,
        paper_content=paper_content,
        session_id=session_id,
    )
    task_result = build_task_contract(
        project_root=project_root,
        config=config,
        paper_path=actual_paper_path,
        instructions=instructions,
        session_id=session_id,
        use_llm=use_llm,
    )
    contract = normalize_task_contract(task_result.contract, config=config, project_root=project_root)
    runner = AgentRunner(project_root=project_root, config=config)
    session = runner.create_session(
        contract,
        paper_path=actual_paper_path,
        instructions=instructions,
        session_id=session_id,
    )
    _persist_task_build_artifacts(runner=runner, session=session, task_result=task_result)
    decision = runner.prepare_agent_decision(contract, session)
    _append_session_message(
        runtime=runner.runtime,
        session=session,
        role="user",
        content=_format_request_message(actual_paper_path, instructions),
        kind="task_request",
        meta={"paper_path": actual_paper_path},
    )
    _append_session_message(
        runtime=runner.runtime,
        session=session,
        role="assistant",
        content=_read_agent_reply(project_root, session),
        kind="agent_reply",
        meta={"mode": decision.mode, "recommended_run_profile": decision.recommended_run_profile},
    )
    return _build_agent_payload(
        project_root=project_root,
        contract=contract,
        session=session,
        decision=decision,
        used_llm=task_result.used_llm,
        llm_error=task_result.llm_error,
    )


def continue_session(
    *,
    project_root: Path,
    config_path: Path,
    session_id: str,
    answers: dict[str, str] | None = None,
    instructions: str = "",
    run_if_ready: bool = False,
    dry_run: bool | None = None,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    _load_project_env(project_root)
    config = load_pipeline_config(config_path.resolve())
    runtime = LocalRuntime(project_root=project_root)
    session = runtime.read_session_state(session_id)
    contract = TaskContract.from_dict(_read_json(project_root, session.task_contract_path))

    cleaned_answers = {
        str(key).strip(): str(value).strip()
        for key, value in (answers or {}).items()
        if str(key).strip() and str(value).strip()
    }
    if instructions.strip():
        contract.instructions = _merge_instruction_text(contract.instructions, instructions.strip())
        session.instructions = contract.instructions
        _append_session_message(
            runtime=runtime,
            session=session,
            role="user",
            content=instructions.strip(),
            kind="follow_up_instruction",
        )
    if cleaned_answers:
        contract = apply_follow_up_answers(contract, cleaned_answers)
        answers_text = _format_follow_up_answers(cleaned_answers)
        contract.instructions = _merge_instruction_text(contract.instructions, answers_text)
        session.instructions = contract.instructions
        _append_session_message(
            runtime=runtime,
            session=session,
            role="user",
            content=answers_text,
            kind="follow_up_answers",
            meta={"answers": cleaned_answers},
        )

    contract = refresh_task_contract_context(contract, config=config, project_root=project_root)
    runtime.write_task_contract(session.task_contract_path, contract)

    runner = AgentRunner(project_root=project_root, config=config)
    decision = runner.prepare_agent_decision(contract, session)
    _append_session_message(
        runtime=runtime,
        session=session,
        role="assistant",
        content=_read_agent_reply(project_root, session),
        kind="agent_reply",
        meta={"mode": decision.mode, "recommended_run_profile": decision.recommended_run_profile},
    )

    payload = _build_agent_payload(
        project_root=project_root,
        contract=contract,
        session=session,
        decision=decision,
        used_llm=False,
        llm_error="",
    )
    if run_if_ready and not decision.missing_high_impact_fields:
        execution = runner.run_task(contract, session=session, dry_run=dry_run)
        payload["execution"] = execution.as_dict()
        payload["status"] = execution.summary.status.value
        latest_session = runtime.read_session_state(session_id)
        payload["artifacts"] = [artifact.as_dict() for artifact in latest_session.artifact_records]
    return payload


def run_task(
    *,
    project_root: Path,
    config_path: Path,
    session_id: str,
    dry_run: bool | None = None,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    _load_project_env(project_root)
    config = load_pipeline_config(config_path.resolve())
    runtime = LocalRuntime(project_root=project_root)
    session = runtime.read_session_state(session_id)
    contract = TaskContract.from_dict(_read_json(project_root, session.task_contract_path))
    runner = AgentRunner(project_root=project_root, config=config)
    execution = runner.run_task(contract, session=session, dry_run=dry_run)
    latest_session = runtime.read_session_state(session_id)
    return {
        "session_id": execution.session_id,
        "task_contract_path": execution.task_contract_path,
        "session_state_path": execution.session_state_path,
        "summary": execution.summary.as_dict(),
        "status": execution.summary.status.value,
        "artifacts": [artifact.as_dict() for artifact in latest_session.artifact_records],
    }


def handle_openclaw_request(
    *,
    project_root: Path,
    request: dict[str, Any],
) -> dict[str, Any]:
    """
    Single-entrypoint adapter for external orchestrators such as OpenClaw.

    Behavior:
    1) Plan when paper input is provided.
    2) Continue when answers/instructions are provided for an existing session.
    3) Optionally run when run_mode requests execution and the task is ready.
    """
    payload = request or {}
    if not isinstance(payload, dict):
        raise ValueError("OpenClaw request must be a JSON object")

    unknown_fields = sorted(set(payload) - OPENCLAW_REQUEST_ALLOWED_FIELDS)
    request_warnings = []
    if unknown_fields:
        request_warnings.append(
            "Unknown request fields were ignored: " + ", ".join(unknown_fields)
        )

    run_mode = str(payload.get("run_mode", "plan_only")).strip() or "plan_only"
    if run_mode not in OPENCLAW_RUN_PROFILES:
        raise ValueError(
            "Invalid run_mode: "
            + run_mode
            + ". Expected one of: "
            + ", ".join(sorted(OPENCLAW_RUN_PROFILES))
        )

    project_root = project_root.resolve()
    _load_project_env(project_root)

    config_path = _resolve_openclaw_request_config_path(project_root=project_root, request=payload, run_mode=run_mode)
    session_id = str(payload.get("session_id", "")).strip()
    paper_path = str(payload.get("paper_path", "")).strip()
    paper_content = str(payload.get("paper_content", "")).strip()
    instructions = str(payload.get("instructions", "")).strip()
    use_llm = _coerce_bool(payload.get("use_llm"), default=True, strict=True, field_name="use_llm")
    dry_run = _coerce_optional_bool(payload.get("dry_run"), strict=True, field_name="dry_run")
    answers = _coerce_answers(payload.get("answers"))

    has_paper_input = bool(paper_path or paper_content)
    has_follow_up_inputs = bool(answers) or bool(instructions)
    response: dict[str, Any] | None = None

    if has_paper_input:
        response = plan_task(
            project_root=project_root,
            config_path=config_path,
            paper_path=paper_path,
            paper_content=paper_content,
            instructions=instructions,
            session_id=session_id,
            use_llm=use_llm,
        )
        session_id = str(response.get("session_id", session_id)).strip()

    if has_follow_up_inputs:
        if not session_id:
            raise ValueError("session_id is required for follow-up answers/instructions when no paper input is provided")
        follow_up_instructions = "" if has_paper_input else instructions
        response = continue_session(
            project_root=project_root,
            config_path=config_path,
            session_id=session_id,
            answers=answers,
            instructions=follow_up_instructions,
            run_if_ready=False,
            dry_run=dry_run,
        )
    elif response is None:
        if not session_id:
            raise ValueError("Either paper_path/paper_content or session_id must be provided")
        # Refresh and return the current session payload without mutating it.
        response = continue_session(
            project_root=project_root,
            config_path=config_path,
            session_id=session_id,
            answers={},
            instructions="",
            run_if_ready=False,
            dry_run=dry_run,
        )

    if run_mode == "plan_only":
        response["run_profile_used"] = "plan_only"
        return _with_request_warnings(response, request_warnings)

    ready_for_execution = bool(response.get("execution_supported")) and not bool(
        response.get("missing_high_impact_fields", [])
    )
    if not ready_for_execution:
        response["run_profile_used"] = run_mode
        response["execution"] = {
            "status": "skipped",
            "reason": "task_not_ready",
            "execution_supported": bool(response.get("execution_supported", False)),
            "missing_high_impact_fields": list(response.get("missing_high_impact_fields", [])),
        }
        return _with_request_warnings(response, request_warnings)

    run_response = run_task(
        project_root=project_root,
        config_path=config_path,
        session_id=str(response.get("session_id", session_id)).strip(),
        dry_run=dry_run,
    )
    response["run_profile_used"] = run_mode
    response["execution"] = run_response
    response["status"] = str(run_response.get("status", response.get("status", "unknown")))
    response["artifacts"] = list(run_response.get("artifacts", []))
    return _with_request_warnings(response, request_warnings)


def get_openclaw_request_template(mode: str = "agentic_repro") -> dict[str, Any]:
    normalized = str(mode).strip().lower() or "agentic_repro"
    if normalized not in OPENCLAW_REQUEST_TEMPLATES:
        raise ValueError(
            "Invalid template mode: "
            + normalized
            + ". Expected one of: "
            + ", ".join(sorted(OPENCLAW_REQUEST_TEMPLATES))
        )
    return copy.deepcopy(OPENCLAW_REQUEST_TEMPLATES[normalized])


def export_contract(
    *,
    project_root: Path,
    session_id: str = "",
    contract_path: str = "",
) -> dict[str, Any]:
    project_root = project_root.resolve()
    if session_id:
        runtime = LocalRuntime(project_root=project_root)
        session = runtime.read_session_state(session_id)
        return _read_json(project_root, session.task_contract_path)
    if contract_path:
        return _read_json(project_root, contract_path)
    raise ValueError("Either session_id or contract_path is required")


def run_preset_pipeline(
    *,
    project_root: Path,
    config_path: Path,
    dry_run: bool | None = None,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    _load_project_env(project_root)
    config = load_pipeline_config(config_path.resolve())
    pipeline = PaperReproPipeline(project_root=project_root, config=config)
    summary = pipeline.run(dry_run=dry_run)
    return summary.as_dict()


def extract_analysis_dataset(
    *,
    project_root: Path,
    profile: str = "mimic_tyg_sepsis",
    sepsis_source: str = "auto",
    output: str = "shared/analysis_dataset.csv",
    missingness_output: str = "shared/analysis_missingness.json",
) -> dict[str, Any]:
    project_root = project_root.resolve()
    _load_project_env(project_root)
    script_path = project_root / "scripts" / "profiles" / "build_profile_analysis_dataset.py"
    cmd = [
        "python3",
        str(script_path),
        "--project-root",
        str(project_root),
        "--profile",
        profile,
        "--output",
        output,
        "--missingness-output",
        missingness_output,
        "--sepsis-source",
        sepsis_source,
    ]
    completed = subprocess.run(cmd, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "analysis dataset extraction failed")
    return {
        "status": "success",
        "profile": profile,
        "analysis_dataset_path": output,
        "missingness_output_path": missingness_output,
        "stdout": completed.stdout.strip(),
    }


def _resolve_openclaw_request_config_path(*, project_root: Path, request: dict[str, Any], run_mode: str) -> Path:
    requested = str(request.get("config_path", "")).strip()
    if requested:
        path = Path(requested)
    else:
        profile = OPENCLAW_RUN_PROFILES.get(run_mode, OPENCLAW_RUN_PROFILES["plan_only"])
        path = Path(str(profile.get("config_path", "configs/openclaw.agentic.yaml")))
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def _coerce_answers(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    cleaned: dict[str, str] = {}
    for key, value in raw.items():
        key_text = str(key).strip()
        value_text = str(value).strip()
        if key_text and value_text:
            cleaned[key_text] = value_text
    return cleaned


def _coerce_bool(
    value: Any,
    *,
    default: bool,
    strict: bool = False,
    field_name: str = "value",
) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    if strict:
        raise ValueError(
            f"Invalid boolean value for {field_name}: {value!r}. "
            "Use one of: true/false, yes/no, on/off, 1/0."
        )
    return default


def _coerce_optional_bool(
    value: Any,
    *,
    strict: bool = False,
    field_name: str = "value",
) -> bool | None:
    if value is None:
        return None
    return _coerce_bool(value, default=False, strict=strict, field_name=field_name)


def _with_request_warnings(payload: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    if warnings:
        payload["request_warnings"] = list(warnings)
    return payload


def _load_project_env(project_root: Path) -> None:
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def _materialize_paper_input(
    *,
    project_root: Path,
    paper_path: str,
    paper_content: str,
    session_id: str,
) -> str:
    if paper_path:
        return str(paper_path)
    if not paper_content.strip():
        raise ValueError("Either paper_path or paper_content must be provided")
    runtime = LocalRuntime(project_root=project_root)
    runtime.ensure_layout()
    actual_session_id = session_id or "draft-openclaw"
    rel_path = f"shared/sessions/{actual_session_id}/source_paper.md"
    runtime.write_text(rel_path, paper_content)
    return rel_path


def _read_json(project_root: Path, rel_or_abs_path: str) -> dict[str, Any]:
    path = Path(rel_or_abs_path)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return json.loads(path.read_text(encoding="utf-8"))


def _append_session_message(
    *,
    runtime: LocalRuntime,
    session: SessionState,
    role: str,
    content: str,
    kind: str,
    meta: dict[str, Any] | None = None,
) -> None:
    text = content.strip()
    if not text:
        return
    session.messages.append(
        {
            "timestamp_utc": datetime.now(tz=UTC).isoformat(),
            "role": role,
            "kind": kind,
            "content": text,
            "meta": meta or {},
        }
    )
    runtime.write_session_state(session)


def _build_agent_payload(
    *,
    project_root: Path,
    contract: TaskContract,
    session: SessionState,
    decision: Any,
    used_llm: bool,
    llm_error: str,
) -> dict[str, Any]:
    support = get_dataset_adapter(contract.dataset.adapter).describe_contract(contract)
    preset = get_paper_preset(contract.meta.get("preset"))
    template = infer_study_template(contract)
    agent_reply_path = str(session.meta.get("agent_reply_path", ""))
    llm_execution_plan_path = str(session.meta.get("llm_execution_plan_path", ""))
    paper_evidence_path = str(session.meta.get("paper_evidence_path", ""))
    paper_spec_surface_path = str(session.meta.get("paper_spec_surface_path", ""))
    analysis_family_route_path = str(session.meta.get("analysis_family_route_path", ""))
    analysis_spec_surface_path = str(session.meta.get("analysis_spec_surface_path", ""))
    llm_execution_plan = _read_json(project_root, llm_execution_plan_path) if llm_execution_plan_path else None
    paper_evidence = _read_json(project_root, paper_evidence_path) if paper_evidence_path else contract.meta.get("paper_evidence")
    paper_spec_surface = _read_json(project_root, paper_spec_surface_path) if paper_spec_surface_path else {}
    analysis_family_route = (
        _read_json(project_root, analysis_family_route_path)
        if analysis_family_route_path
        else decision.as_dict().get("analysis_family_route", {})
    )
    analysis_spec_surface = _read_json(project_root, analysis_spec_surface_path) if analysis_spec_surface_path else {}
    return {
        "task_contract": contract.as_dict(),
        "missing_high_impact_fields": list(decision.missing_high_impact_fields),
        "agent_decision": decision.as_dict(),
        "follow_up_questions": [item.as_dict() for item in decision.follow_up_questions],
        "recommended_run_profile": decision.recommended_run_profile,
        "selected_agent_sequence": list(decision.selected_agent_sequence),
        "session_id": session.session_id,
        "task_contract_path": session.task_contract_path,
        "used_llm": used_llm,
        "llm_error": llm_error,
        "execution_backend": support.execution_backend,
        "execution_supported": support.execution_supported,
        "paper_required_methods": list(decision.paper_required_methods),
        "unsupported_required_capabilities": list(decision.missing_capabilities),
        "paper_target_dataset_version": decision.paper_target_dataset_version,
        "execution_environment_dataset_version": decision.execution_environment_dataset_version,
        "configured_dataset_version": decision.configured_dataset_version,
        "execution_year_window": decision.execution_year_window,
        "dataset_version_mismatch": decision.dataset_version_mismatch,
        "preset": preset.as_dict() if preset is not None else None,
        "study_template": template.as_dict() if template is not None else None,
        "agent_reply": _read_agent_reply(project_root, session),
        "agent_reply_path": agent_reply_path,
        "analysis_family_route": analysis_family_route,
        "analysis_family_route_path": analysis_family_route_path,
        "paper_evidence": paper_evidence,
        "paper_evidence_path": paper_evidence_path,
        "paper_spec_surface": paper_spec_surface,
        "paper_spec_surface_path": paper_spec_surface_path,
        "analysis_spec_surface": analysis_spec_surface,
        "analysis_spec_surface_path": analysis_spec_surface_path,
        "llm_execution_plan": llm_execution_plan,
        "llm_execution_plan_path": llm_execution_plan_path,
        "llm_execution_plan_error": str(session.meta.get("llm_execution_plan_error", "")),
        "task_build_mode": str(session.meta.get("task_build_mode", "")),
        "conversation_turns": len(session.messages),
        "status": decision.status,
    }


def _persist_task_build_artifacts(
    *,
    runner: AgentRunner,
    session: SessionState,
    task_result: Any,
) -> None:
    build_mode = "deterministic_only"
    if bool(getattr(task_result, "used_llm", False)):
        build_mode = "hybrid_llm_assisted"
    elif str(getattr(task_result, "llm_error", "")).strip():
        build_mode = "deterministic_fallback_after_llm_error"
    session.meta["task_build_mode"] = build_mode

    paper_evidence = getattr(task_result, "paper_evidence", None)
    if isinstance(paper_evidence, dict) and paper_evidence:
        rel_path = f"shared/sessions/{session.session_id}/paper_evidence.json"
        runner.runtime.write_json(rel_path, paper_evidence)
        session.meta["paper_evidence_path"] = rel_path
        runner._record_system_artifact(
            session,
            rel_path,
            producer="paper_intake_and_contract",
            meta={"kind": "paper_evidence", "task_build_mode": build_mode},
        )

    runner.runtime.write_session_state(session)


def _format_request_message(paper_path: str, instructions: str) -> str:
    lines = [f"Paper path: {paper_path}"]
    if instructions.strip():
        lines.extend(["", "Instructions:", instructions.strip()])
    return "\n".join(lines)


def _format_follow_up_answers(answers: dict[str, str]) -> str:
    lines = ["Follow-up answers:"]
    for field, value in answers.items():
        lines.append(f"- {field}: {value}")
    return "\n".join(lines)


def _merge_instruction_text(existing: str, new_text: str) -> str:
    existing_text = existing.strip()
    appended = new_text.strip()
    if not existing_text:
        return appended
    if not appended:
        return existing_text
    return f"{existing_text}\n\n{appended}"


def _read_agent_reply(project_root: Path, session: SessionState) -> str:
    rel_path = str(session.meta.get("agent_reply_path", "")).strip()
    if not rel_path:
        return ""
    path = (project_root / rel_path).resolve()
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _try_load_semantic_registry(project_root: Path) -> SemanticRegistry | None:
    try:
        return load_mimic_semantic_registry(project_root)
    except FileNotFoundError:
        return None


def _try_load_skill_contract_manifest(project_root: Path) -> SkillContractManifest | None:
    try:
        return load_skill_contract_manifest(project_root)
    except FileNotFoundError:
        return None


def _try_load_codex_skill_bridge_manifest(project_root: Path) -> dict[str, Any] | None:
    try:
        return load_codex_skill_bridge_manifest(project_root)
    except FileNotFoundError:
        return None

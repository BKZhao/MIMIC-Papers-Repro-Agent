from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from .agent_runner import AgentRunner
from .config import PipelineConfig, load_pipeline_config
from .contracts import SessionState, TaskContract
from .dataset_adapters import get_dataset_adapter
from .pipeline import PaperReproPipeline
from .preset_registry import get_paper_preset, list_builtin_presets
from .runtime import LocalRuntime
from .semantic_registry import SemanticRegistry, load_mimic_semantic_registry
from .skill_contracts import SkillContractManifest, load_skill_contract_manifest
from .study_templates import infer_study_template, list_study_templates
from .task_builder import build_task_contract, find_missing_high_impact_fields, normalize_task_contract
from .workflow_contract import default_mimic_paper_workflow


OPENCLAW_AGENT_NAME = "paper-repro-scientist"

OPENCLAW_SKILL_PATHS: dict[str, str] = {
    "paper_intake_and_contract": "openclaw/skills/paper_intake_and_contract/SKILL.md",
    "mimic_cohort_execution": "openclaw/skills/mimic_cohort_execution/SKILL.md",
    "analysis_dataset_expansion": "openclaw/skills/analysis_dataset_expansion/SKILL.md",
    "survival_stats_execution": "openclaw/skills/survival_stats_execution/SKILL.md",
    "result_figure_generation": "openclaw/skills/result_figure_generation/SKILL.md",
    "paper_alignment_verification": "openclaw/skills/paper_alignment_verification/SKILL.md",
    "git_update": "openclaw/skills/git_update/SKILL.md",
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

OPENCLAW_AGENT_CONTRACT: dict[str, Any] = {
    "agent_name": OPENCLAW_AGENT_NAME,
    "single_entrypoint": True,
    "external_role": "Lobster-facing clinical paper reproduction scientist",
    "accepted_request_fields": [
        "paper_path | paper_content",
        "instructions",
        "config_path",
        "session_id?",
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
    ],
    "primary_exchange_object": "TaskContract",
    "artifact_roots": [
        "shared/",
        "results/",
        "shared/sessions/<session_id>/",
    ],
    "success_definition": [
        "For preset-supported MIMIC papers: return real cohort, stats, figure, verification, and report artifacts.",
        "For unsupported or incomplete papers: return a clear planning result with missing fields and blocked capabilities.",
    ],
}


def describe_openclaw_integration(project_root: Path) -> dict[str, Any]:
    registry = _try_load_semantic_registry(project_root)
    skill_manifest = _try_load_skill_contract_manifest(project_root)
    workflow_contract = default_mimic_paper_workflow()
    skill_paths = dict(OPENCLAW_SKILL_PATHS)
    if skill_manifest is not None:
        skill_paths = {name: contract.path for name, contract in skill_manifest.skills.items()}
    return {
        "agent_name": OPENCLAW_AGENT_NAME,
        "soul_path": "openclaw/SOUL.MD",
        "recommended_config": "configs/openclaw.agentic.yaml",
        "recommended_real_run_config": "configs/openclaw.mimic-real-run.yaml",
        "mission": (
            "Turn paper materials and user instructions into a structured TaskContract, "
            "execute supported clinical reproduction workflows, and always distinguish real execution from planning-only output."
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
                "PDF-first paper extraction with project-owned fallback parsing",
                "Deterministic MIMIC-IV TyG sepsis reproduction",
                "Artifact-first cohort, analysis dataset, stats, verification, and report outputs",
                "Preset registry, study template inference, semantic variable mapping, and skill contracts",
            ],
            "not_yet_supported": [
                "Generic SQL compilation from arbitrary CohortSpec to executable MIMIC SQL",
                "Generic variable-to-field mapping for arbitrary MIMIC papers",
                "True end-to-end execution for non-preset contracts",
                "Full deterministic execution for every statistical family found in arbitrary MIMIC papers",
            ],
        },
        "decision_boundaries": [
            "Do not pretend to execute unsupported non-MIMIC datasets.",
            "When exposure, outcome, model, or cohort logic is missing, complete the contract before execution.",
            "Always distinguish deterministic reproduction output from planning blueprints.",
        ],
        "failure_modes": [
            "Missing SQL tables or schemas -> return missing dependency diagnostics.",
            "Non-preset contract -> return planning blueprint instead of claiming a completed reproduction.",
            "Suspicious paper targets or OCR conflicts -> surface them as suspect in verification output.",
        ],
        "interfaces": [
            {
                "name": "plan_task",
                "description": "Build and persist a structured TaskContract from paper materials and user instructions.",
                "inputs": ["paper_path | paper_content", "instructions", "config", "session_id?"],
                "outputs": ["task_contract", "missing_high_impact_fields", "session_id", "execution_backend"],
            },
            {
                "name": "run_task",
                "description": "Execute a planned session through the multi-agent runner.",
                "inputs": ["session_id", "config", "dry_run?"],
                "outputs": ["summary", "artifacts", "status"],
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
        ],
        "skill_paths": skill_paths,
        "skill_contract_manifest_path": skill_manifest.source_path if skill_manifest is not None else "",
        "skill_contract_manifest": skill_manifest.as_dict() if skill_manifest is not None else {},
        "skill_contracts": (
            {name: contract.as_dict() for name, contract in skill_manifest.skills.items()}
            if skill_manifest is not None
            else {}
        ),
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
        "lobster_handoff": {
            "call_pattern": [
                "Lobster sends paper_path or paper_content plus instructions.",
                "Call plan_task first.",
                "If execution_supported is true and required fields are complete, call run_task.",
                "Read final figures, tables, and report from returned artifacts.",
            ],
            "do_not_do": [
                "Do not let Lobster infer cohort SQL itself.",
                "Do not let Lobster invent a second task schema outside TaskContract.",
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
    support = get_dataset_adapter(contract.dataset.adapter).describe_contract(contract)
    preset = get_paper_preset(contract.meta.get("preset"))
    template = infer_study_template(contract)
    return {
        "task_contract": contract.as_dict(),
        "missing_high_impact_fields": find_missing_high_impact_fields(contract),
        "session_id": session.session_id,
        "task_contract_path": session.task_contract_path,
        "used_llm": task_result.used_llm,
        "llm_error": task_result.llm_error,
        "execution_backend": support.execution_backend,
        "execution_supported": support.execution_supported,
        "preset": preset.as_dict() if preset is not None else None,
        "study_template": template.as_dict() if template is not None else None,
    }


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

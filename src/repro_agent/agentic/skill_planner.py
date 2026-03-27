from __future__ import annotations

import json
from typing import Any

from ..config import PipelineConfig
from ..contracts import TaskContract
from ..dataset_adapters import AdapterSupport
from ..llm import LLMError, OpenAICompatibleClient
from ..paper.builder import summarize_task_contract


def build_llm_execution_plan(
    *,
    contract: TaskContract,
    support: AdapterSupport,
    config: PipelineConfig,
    recommended_run_profile: str,
    selected_agent_sequence: tuple[str, ...],
    skill_routes: dict[str, list[str]],
) -> dict[str, Any]:
    client = OpenAICompatibleClient(config.llm)
    if not client.is_enabled():
        raise LLMError(f"LLM provider {config.llm.provider} is not configured via {config.llm.api_key_env}")
    route = config.agent_routes.get("study_design_agent")
    route_model = route.model if route and route.model else None

    payload, _ = client.complete_json(
        _planner_messages(
            contract=contract,
            support=support,
            config=config,
            recommended_run_profile=recommended_run_profile,
            selected_agent_sequence=selected_agent_sequence,
            skill_routes=skill_routes,
        ),
        model=route_model,
        temperature=0.0,
        max_tokens=min(config.llm.max_tokens, 2500),
    )
    return _normalize_plan_payload(
        payload,
        contract=contract,
        support=support,
        recommended_run_profile=recommended_run_profile,
        selected_agent_sequence=selected_agent_sequence,
        skill_routes=skill_routes,
    )


def render_llm_execution_plan_markdown(plan: dict[str, Any], *, title: str = "") -> str:
    lines = [
        f"# LLM Execution Plan{f': {title}' if title else ''}",
        "",
        f"- Planner mode: {plan.get('planner_mode', 'unknown')}",
        f"- Execution posture: {plan.get('execution_posture', 'unknown')}",
        f"- Recommended run profile: {plan.get('recommended_run_profile', 'unknown')}",
        "",
        "## Route Summary",
        str(plan.get("route_summary", "")),
        "",
        "## Skill Sequence",
    ]
    for item in plan.get("skill_sequence", []):
        if not isinstance(item, dict):
            continue
        skills = ", ".join(str(skill) for skill in item.get("skills", []) if str(skill).strip()) or "none"
        lines.append(
            f"- {item.get('agent_name', 'unknown')}: skills={skills}; execution={item.get('execution_mode', 'unknown')}."
        )
        if str(item.get("reason", "")).strip():
            lines.append(f"  reason: {item.get('reason')}")
    tool_plan = plan.get("tool_plan", [])
    if tool_plan:
        lines.extend(["", "## Tool Plan"])
        for item in tool_plan:
            if not isinstance(item, dict):
                continue
            executor = str(item.get("executor", "")).strip() or "unspecified"
            outputs = ", ".join(str(value) for value in item.get("expected_outputs", []) if str(value).strip()) or "none"
            lines.append(f"- {item.get('step', 'step')}: executor={executor}; outputs={outputs}")
            if str(item.get("reason", "")).strip():
                lines.append(f"  reason: {item.get('reason')}")
    blocked = [str(item).strip() for item in plan.get("blocked_capabilities", []) if str(item).strip()]
    if blocked:
        lines.extend(["", "## Blocked Capabilities", *[f"- {item}" for item in blocked]])
    validation = [str(item).strip() for item in plan.get("validation_checkpoints", []) if str(item).strip()]
    if validation:
        lines.extend(["", "## Validation Checkpoints", *[f"- {item}" for item in validation]])
    notes = [str(item).strip() for item in plan.get("notes", []) if str(item).strip()]
    if notes:
        lines.extend(["", "## Notes", *[f"- {item}" for item in notes]])
    return "\n".join(lines) + "\n"


def _planner_messages(
    *,
    contract: TaskContract,
    support: AdapterSupport,
    config: PipelineConfig,
    recommended_run_profile: str,
    selected_agent_sequence: tuple[str, ...],
    skill_routes: dict[str, list[str]],
) -> list[dict[str, str]]:
    schema_hint = """
Return a JSON object with keys:
planner_mode, execution_posture, route_summary, recommended_run_profile, skill_sequence, tool_plan,
blocked_capabilities, validation_checkpoints, notes.

skill_sequence: list of objects with keys:
agent_name, skills, execution_mode, reason.

tool_plan: list of objects with keys:
step, executor, inputs, expected_outputs, blocking, reason.

Rules:
- The LLM should orchestrate and explain, not fabricate execution.
- Use deterministic tools for SQL extraction, dataset building, statistics, and figure generation.
- Use LLM-driven reasoning for paper interpretation, contract refinement, route decisions, verification framing, and reporting.
- If the task is blocked, say exactly why and what tool/skill chain is missing.
- Do not claim paper-identical reproduction when the backend is only method-aligned.
"""
    task_summary = summarize_task_contract(contract)
    return [
        {
            "role": "system",
            "content": (
                "You are the orchestration brain for a clinical paper reproduction agent. "
                "You control skill selection and execution routing across deterministic tools and LLM reasoning. "
                + schema_hint
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "task_summary": task_summary,
                    "dataset": contract.dataset.as_dict(),
                    "cohort": contract.cohort.as_dict(),
                    "models": [item.as_dict() for item in contract.models],
                    "outputs": [item.as_dict() for item in contract.outputs],
                    "notes": list(contract.notes),
                    "paper_required_methods": list(contract.meta.get("paper_required_methods", [])),
                    "requires_longitudinal_trajectory_modeling": bool(
                        contract.meta.get("requires_longitudinal_trajectory_modeling")
                    ),
                    "paper_target_dataset_version": str(contract.meta.get("paper_target_dataset_version", "")),
                    "execution_environment_dataset_version": str(
                        contract.meta.get(
                            "execution_environment_dataset_version",
                            contract.meta.get("configured_dataset_version", contract.dataset.version),
                        )
                    ),
                    "configured_dataset_version": str(
                        contract.meta.get("configured_dataset_version", contract.dataset.version)
                    ),
                    "execution_year_window": str(contract.meta.get("execution_year_window", "")),
                    "support": support.as_dict(),
                    "recommended_run_profile": recommended_run_profile,
                    "selected_agent_sequence": list(selected_agent_sequence),
                    "skill_routes": skill_routes,
                    "llm_provider": config.llm.provider,
                    "llm_model": config.llm.default_model,
                },
                ensure_ascii=False,
                indent=2,
            ),
        },
    ]


def _normalize_plan_payload(
    payload: dict[str, Any],
    *,
    contract: TaskContract,
    support: AdapterSupport,
    recommended_run_profile: str,
    selected_agent_sequence: tuple[str, ...],
    skill_routes: dict[str, list[str]],
) -> dict[str, Any]:
    planner_mode = str(payload.get("planner_mode", "")).strip() or "llm_orchestrated"
    execution_posture = str(payload.get("execution_posture", "")).strip() or (
        "planning_only" if not support.execution_supported else "hybrid_deterministic"
    )
    route_summary = str(payload.get("route_summary", "")).strip()
    if not route_summary:
        route_summary = (
            "LLM controls paper interpretation and skill routing, while SQL extraction and statistics stay in "
            "deterministic local tools."
        )

    raw_skill_sequence = payload.get("skill_sequence", [])
    skill_sequence: list[dict[str, Any]] = []
    for agent_name in selected_agent_sequence:
        route_skills = list(skill_routes.get(agent_name, []))
        matched = next(
            (
                item
                for item in raw_skill_sequence
                if isinstance(item, dict) and str(item.get("agent_name", "")).strip() == agent_name
            ),
            None,
        )
        execution_mode = "deterministic_tooling"
        if agent_name in {"paper_parser_agent", "study_design_agent", "verify_agent", "report_agent"}:
            execution_mode = "llm_reasoning_plus_artifacts"
        entry = {
            "agent_name": agent_name,
            "skills": route_skills,
            "execution_mode": execution_mode,
            "reason": "",
        }
        if isinstance(matched, dict):
            entry["skills"] = [
                str(skill).strip()
                for skill in matched.get("skills", route_skills)
                if str(skill).strip()
            ] or route_skills
            entry["execution_mode"] = str(matched.get("execution_mode", entry["execution_mode"])).strip() or entry[
                "execution_mode"
            ]
            entry["reason"] = str(matched.get("reason", "")).strip()
        skill_sequence.append(entry)

    raw_tool_plan = payload.get("tool_plan", [])
    tool_plan: list[dict[str, Any]] = []
    for item in raw_tool_plan:
        if not isinstance(item, dict):
            continue
        tool_plan.append(
            {
                "step": str(item.get("step", "")).strip(),
                "executor": str(item.get("executor", "")).strip(),
                "inputs": [str(value).strip() for value in item.get("inputs", []) if str(value).strip()],
                "expected_outputs": [
                    str(value).strip() for value in item.get("expected_outputs", []) if str(value).strip()
                ],
                "blocking": bool(item.get("blocking", False)),
                "reason": str(item.get("reason", "")).strip(),
            }
        )

    if not tool_plan and contract.meta.get("requires_longitudinal_trajectory_modeling"):
        tool_plan = [
            {
                "step": "cohort_extraction",
                "executor": "scripts/profiles/build_profile_cohort.py --profile mimic_hr_trajectory_sepsis",
                "inputs": ["paper-derived cohort logic", "MIMIC PostgreSQL"],
                "expected_outputs": ["cohort.csv", "cohort_funnel.json"],
                "blocking": True,
                "reason": "The trajectory paper needs a cohort with complete hourly heart-rate panel coverage.",
            },
            {
                "step": "analysis_dataset_expansion",
                "executor": "scripts/profiles/build_profile_analysis_dataset.py --profile mimic_hr_trajectory_sepsis",
                "inputs": ["trajectory cohort", "first-day covariates"],
                "expected_outputs": ["analysis_dataset.csv", "analysis_missingness.json"],
                "blocking": True,
                "reason": "The downstream trajectory model and Cox workflow need a model-ready wide table.",
            },
            {
                "step": "trajectory_and_survival_stats",
                "executor": "scripts/profiles/run_profile_stats.py --profile mimic_hr_trajectory_sepsis",
                "inputs": ["analysis dataset with 10-hour heart-rate panel"],
                "expected_outputs": ["trajectory table", "trajectory figure", "KM figure", "Cox table", "report"],
                "blocking": True,
                "reason": "Trajectory class derivation must happen before KM and Cox can be reproduced.",
            },
        ]
    if not tool_plan and str(contract.meta.get("experimental_profile", "")).strip() == "mimic_tyg_stroke_nondiabetic":
        tool_plan = [
            {
                "step": "cohort_extraction",
                "executor": "scripts/profiles/build_profile_cohort.py --profile mimic_tyg_stroke_nondiabetic",
                "inputs": ["paper-derived ischemic stroke cohort logic", "MIMIC PostgreSQL"],
                "expected_outputs": ["cohort.csv", "cohort_funnel.json", "cohort_alignment.json"],
                "blocking": True,
                "reason": "The stroke TyG paper needs a non-diabetic ischemic stroke ICU cohort before any analysis can run.",
            },
            {
                "step": "analysis_dataset_expansion",
                "executor": "scripts/profiles/build_profile_analysis_dataset.py --profile mimic_tyg_stroke_nondiabetic",
                "inputs": ["stroke cohort", "first-day ICU covariates", "TyG day-1 labs"],
                "expected_outputs": ["analysis_dataset.csv", "analysis_missingness.json"],
                "blocking": True,
                "reason": "The downstream Cox, Kaplan-Meier, RCS, and subgroup figures need a model-ready wide table with all six mortality endpoints.",
            },
            {
                "step": "multi_endpoint_survival_stats",
                "executor": "scripts/profiles/run_profile_stats.py --profile mimic_tyg_stroke_nondiabetic",
                "inputs": ["analysis dataset with TyG quartiles and six mortality endpoints"],
                "expected_outputs": ["baseline table", "Cox table", "KM figure", "RCS figure", "subgroup figure", "report"],
                "blocking": True,
                "reason": "The paper's main contribution is a six-endpoint survival analysis package driven by TyG quartiles.",
            },
        ]

    blocked_capabilities = [
        str(item).strip()
        for item in payload.get("blocked_capabilities", support.missing_capabilities)
        if str(item).strip()
    ]
    validation_checkpoints = _normalize_validation_checkpoints(
        payload.get(
            "validation_checkpoints",
            [
                "Check whether cohort size and exclusion funnel match the paper closely.",
                "Check whether the trajectory class count and shape summaries resemble the paper.",
                "Check whether the Cox HR ordering and KM separation are directionally aligned with the paper.",
            ],
        )
    )
    notes = [str(item).strip() for item in payload.get("notes", []) if str(item).strip()]
    if contract.meta.get("requires_longitudinal_trajectory_modeling"):
        notes.append(
            "This is an LLM-orchestrated route over deterministic SQL/stats tools, not unconstrained free-form LLM execution."
        )
    if str(contract.meta.get("experimental_profile", "")).strip() == "mimic_tyg_stroke_nondiabetic":
        notes.append(
            "The stroke TyG profile is executable for the main survival figures, but fasting-lab semantics and MICE/PSM sensitivity steps remain fidelity gaps."
        )
    return {
        "planner_mode": planner_mode,
        "execution_posture": execution_posture,
        "route_summary": route_summary,
        "recommended_run_profile": recommended_run_profile,
        "skill_sequence": skill_sequence,
        "tool_plan": tool_plan,
        "blocked_capabilities": blocked_capabilities,
        "validation_checkpoints": validation_checkpoints,
        "notes": notes,
    }


def _normalize_validation_checkpoints(value: Any) -> list[str]:
    if not isinstance(value, list):
        value = [value]
    normalized: list[str] = []
    for item in value:
        if isinstance(item, dict):
            step = str(item.get("step", "")).strip()
            checkpoint = str(item.get("checkpoint", "")).strip()
            description = str(item.get("description", "")).strip()
            text_parts = [part for part in (step, checkpoint, description) if part]
            text = " | ".join(text_parts)
            if text:
                normalized.append(text)
            continue
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..analysis.router import resolve_clinical_analysis_route
from ..contracts import TaskContract
from ..dataset_adapters import AdapterSupport
from ..paper.builder import find_missing_high_impact_fields


AGENT_STEP_PURPOSES: dict[str, str] = {
    "paper_parser_agent": "Collect the paper materials and prepare a structured paper evidence manifest.",
    "study_design_agent": "Judge contract completeness, execution support, and the current study route.",
    "cohort_agent": "Prepare the cohort plan or cohort extraction blueprint for the current dataset adapter.",
    "feature_agent": "Prepare the feature mapping and analysis-dataset blueprint.",
    "stats_agent": "Run deterministic statistics when supported, or emit the model plan when execution is not wired.",
    "figure_agent": "Render figures from reproduced data or prepare a figure-generation plan.",
    "verify_agent": "Compare reproduced artifacts with paper targets or prepare verification diagnostics.",
    "report_agent": "Write a human-readable execution summary and next-step report.",
}


FOLLOW_UP_QUESTION_TEXT: dict[str, str] = {
    "exposure_variables": "Which exposure or primary independent variable from the paper should drive the analysis?",
    "outcome_variables": "Which outcome or endpoint should the reproduction target?",
    "models": "Which model families from the paper do you want to reproduce first?",
    "outputs": "Which tables or figures do you want as required outputs?",
    "cohort_logic": "What cohort inclusion and exclusion logic from the paper must be enforced?",
}


@dataclass(frozen=True)
class FollowUpQuestion:
    field: str
    question: str
    rationale: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "question": self.question,
            "rationale": self.rationale,
        }


@dataclass(frozen=True)
class AgentPlanStep:
    agent_name: str
    enabled: bool
    purpose: str
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "enabled": self.enabled,
            "purpose": self.purpose,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class AgentDecision:
    mode: str
    status: str
    rationale: str
    execution_supported: bool
    execution_backend: str
    recommended_run_profile: str
    missing_high_impact_fields: tuple[str, ...] = ()
    missing_capabilities: tuple[str, ...] = ()
    support_notes: tuple[str, ...] = ()
    paper_required_methods: tuple[str, ...] = ()
    paper_target_dataset_version: str = ""
    execution_environment_dataset_version: str = ""
    configured_dataset_version: str = ""
    execution_year_window: str = ""
    dataset_version_mismatch: bool = False
    follow_up_questions: tuple[FollowUpQuestion, ...] = ()
    next_actions: tuple[str, ...] = ()
    selected_agent_sequence: tuple[str, ...] = ()
    step_plan: tuple[AgentPlanStep, ...] = ()
    analysis_family_route: dict[str, Any] = field(default_factory=dict)
    planning_only: bool = False
    deterministic_bridge: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "status": self.status,
            "rationale": self.rationale,
            "execution_supported": self.execution_supported,
            "execution_backend": self.execution_backend,
            "recommended_run_profile": self.recommended_run_profile,
            "missing_high_impact_fields": list(self.missing_high_impact_fields),
            "missing_capabilities": list(self.missing_capabilities),
            "support_notes": list(self.support_notes),
            "paper_required_methods": list(self.paper_required_methods),
            "paper_target_dataset_version": self.paper_target_dataset_version,
            "execution_environment_dataset_version": self.execution_environment_dataset_version,
            "configured_dataset_version": self.configured_dataset_version,
            "execution_year_window": self.execution_year_window,
            "dataset_version_mismatch": self.dataset_version_mismatch,
            "follow_up_questions": [item.as_dict() for item in self.follow_up_questions],
            "next_actions": list(self.next_actions),
            "selected_agent_sequence": list(self.selected_agent_sequence),
            "step_plan": [item.as_dict() for item in self.step_plan],
            "analysis_family_route": dict(self.analysis_family_route),
            "planning_only": self.planning_only,
            "deterministic_bridge": self.deterministic_bridge,
        }


def build_agent_decision(contract: TaskContract, support: AdapterSupport) -> AgentDecision:
    analysis_route = resolve_clinical_analysis_route(contract)
    missing_fields = tuple(find_missing_high_impact_fields(contract))
    actionable_missing_fields = missing_fields
    preset_key = str(contract.meta.get("preset", "")).strip()
    missing_capabilities = tuple(dict.fromkeys(item for item in support.missing_capabilities if item))
    support_notes = tuple(dict.fromkeys(item for item in support.notes if item))
    paper_required_methods = tuple(str(item) for item in contract.meta.get("paper_required_methods", []) if str(item).strip())
    paper_target_dataset_version = str(contract.meta.get("paper_target_dataset_version", support.paper_target_dataset_version)).strip()
    execution_environment_dataset_version = str(
        contract.meta.get(
            "execution_environment_dataset_version",
            support.execution_environment_dataset_version or support.configured_dataset_version,
        )
    ).strip()
    configured_dataset_version = execution_environment_dataset_version
    execution_year_window = str(contract.meta.get("execution_year_window", support.execution_year_window)).strip()
    dataset_version_mismatch = bool(contract.meta.get("dataset_version_mismatch", support.dataset_version_mismatch))
    requires_trajectory_modeling = bool(contract.meta.get("requires_longitudinal_trajectory_modeling"))

    if support.execution_supported and support.execution_backend == "deterministic_bridge" and preset_key:
        mode = "deterministic_preset_run"
        status = "ready"
        recommended_run_profile = "preset_real_run"
        actionable_missing_fields = ()
        rationale = (
            "The task matches a supported preset and the dataset adapter reports deterministic execution support, "
            "so the agent should route into the preset-backed real execution path."
        )
        if missing_fields:
            rationale += " Missing contract fields are tolerated here because the preset supplies the executable default logic."
        next_actions = (
            "Run the preset-backed deterministic bridge.",
            "Collect the produced tables, figures, verification artifacts, and report.",
            "Summarize where the reproduced outputs align with or diverge from the paper.",
        )
    elif missing_fields:
        mode = "needs_contract_completion"
        status = "blocked"
        recommended_run_profile = "plan_only"
        rationale = (
            "The contract is still missing high-impact study fields, so the next best agent behavior is to stop "
            "guessing, ask targeted follow-up questions, and complete the TaskContract before execution."
        )
        next_actions = (
            "Ask follow-up questions for the missing high-impact fields.",
            "Normalize the updated TaskContract after the user answers.",
            "Re-evaluate whether the task can run through a deterministic preset, a supported experimental route, or a planning-only path.",
        )
    elif support.execution_supported:
        mode = "agentic_execution"
        status = "ready"
        recommended_run_profile = "agentic_repro"
        if requires_trajectory_modeling:
            rationale = (
                "The task is executable through the experimental trajectory bridge. The agent can run the heart-rate "
                "cohort and staged analysis-dataset build, derive trajectory classes with the local Python backend, "
                "then execute KM and Cox analysis while preserving the explicit LGMM-vs-backend fidelity gap."
            )
            next_actions = (
                "Run the experimental trajectory execution path.",
                "Persist cohort, analysis dataset, trajectory, KM, Cox, and report artifacts under the standard shared/results layout.",
                "Summarize the cohort gap, source-strategy gap, and method gap against the paper.",
            )
        else:
            rationale = (
                "The task is executable through the current dataset adapter but does not require the preset-backed "
                "deterministic bridge, so the agent can proceed through the internal execution steps."
            )
            next_actions = (
                "Run the agentic execution path.",
                "Persist all execution artifacts under the standard shared/results layout.",
                "Compare outputs against the paper and summarize any deviations.",
            )
    elif support.planning_supported:
        mode = "planning_only"
        status = "planning_ready"
        recommended_run_profile = "plan_only"
        if requires_trajectory_modeling:
            rationale = (
                "The contract is complete enough for planning, but this paper requires longitudinal trajectory modeling "
                "and does not currently match a wired experimental execution profile. The repository can already execute "
                "the heart-rate trajectory sepsis profile, but other longitudinal trajectory papers still need paper-specific "
                "dataset blocks and a trajectory-to-survival bridge before they can be treated as runnable."
            )
            next_actions = (
                "Map this paper onto an existing supported trajectory profile if one truly matches the cohort and repeated-measure design.",
                "If no profile matches, add the paper-specific repeated-measure extraction blocks and dataset contract.",
                "Wire the resulting trajectory class labels into downstream KM and Cox analysis, then keep the fidelity gap explicit.",
                "Keep this task labeled as planning-only until that paper-specific trajectory bridge exists.",
            )
        else:
            rationale = (
                "The contract is complete enough for planning, but the dataset adapter does not yet support true "
                "execution for this paper. The agent should continue by producing blueprints and explicit next steps "
                "instead of pretending the paper has been reproduced."
            )
            next_actions = (
                "Produce cohort, feature, model, figure, and verification blueprints.",
                "Explain which compiler or mapping capabilities are still missing.",
                "Keep the result labeled as planning-only rather than completed execution.",
            )
        if dataset_version_mismatch:
            rationale += (
                " There is also a dataset-version mismatch: "
                + f"the paper original dataset version is {paper_target_dataset_version or 'unknown'}, "
                + f"while the execution environment dataset version is {execution_environment_dataset_version or 'unknown'}."
            )
    else:
        mode = "unsupported"
        status = "blocked"
        recommended_run_profile = "plan_only"
        rationale = (
            "The current dataset adapter cannot plan or execute this contract, so the agent should stop and return "
            "a clear unsupported-task diagnostic."
        )
        next_actions = (
            "Return the unsupported-task diagnostic.",
            "Explain the missing dataset adapter capability.",
        )

    follow_up_questions = tuple(_build_follow_up_questions(actionable_missing_fields))
    selected_agent_sequence = _select_agent_sequence(contract=contract, mode=mode)
    step_plan = tuple(
        AgentPlanStep(
            agent_name=agent_name,
            enabled=agent_name in selected_agent_sequence,
            purpose=AGENT_STEP_PURPOSES.get(agent_name, ""),
            reason=_step_reason(agent_name=agent_name, mode=mode, selected_agent_sequence=selected_agent_sequence),
        )
        for agent_name in AGENT_STEP_PURPOSES
    )
    return AgentDecision(
        mode=mode,
        status=status,
        rationale=rationale,
        execution_supported=support.execution_supported,
        execution_backend=support.execution_backend,
        recommended_run_profile=recommended_run_profile,
        missing_high_impact_fields=actionable_missing_fields,
        missing_capabilities=missing_capabilities,
        support_notes=support_notes,
        paper_required_methods=paper_required_methods,
        paper_target_dataset_version=paper_target_dataset_version,
        execution_environment_dataset_version=execution_environment_dataset_version,
        configured_dataset_version=configured_dataset_version,
        execution_year_window=execution_year_window,
        dataset_version_mismatch=dataset_version_mismatch,
        follow_up_questions=follow_up_questions,
        next_actions=next_actions,
        selected_agent_sequence=selected_agent_sequence,
        step_plan=step_plan,
        analysis_family_route=analysis_route.as_dict(),
        planning_only=mode == "planning_only",
        deterministic_bridge=mode == "deterministic_preset_run",
    )


def render_agent_decision_markdown(decision: AgentDecision, *, title: str = "") -> str:
    lines = [
        f"# Agent Execution Plan{f': {title}' if title else ''}",
        "",
        "## Decision",
        f"- Mode: {decision.mode}",
        f"- Status: {decision.status}",
        f"- Execution supported: {decision.execution_supported}",
        f"- Execution backend: {decision.execution_backend}",
        f"- Recommended run profile: {decision.recommended_run_profile}",
        "",
        "## Rationale",
        decision.rationale,
        "",
        "## Selected Agent Sequence",
        *[f"- {item}" for item in decision.selected_agent_sequence],
    ]
    if decision.missing_high_impact_fields:
        lines.extend(
            [
                "",
                "## Missing High-Impact Fields",
                *[f"- {item}" for item in decision.missing_high_impact_fields],
            ]
        )
    if decision.paper_required_methods:
        lines.extend(["", "## Paper-Required Methods", *[f"- {item}" for item in decision.paper_required_methods]])
    route = decision.analysis_family_route
    if route:
        requested = [str(item) for item in route.get("requested_families", []) if str(item).strip()]
        native = [str(item) for item in route.get("native_supported_families", []) if str(item).strip()]
        hybrid = [str(item) for item in route.get("llm_compiled_families", []) if str(item).strip()]
        planning = [str(item) for item in route.get("planning_reference_families", []) if str(item).strip()]
        libraries = [str(item) for item in route.get("preferred_libraries", []) if str(item).strip()]
        if requested:
            lines.extend(["", "## Analysis Families", *[f"- Requested: {', '.join(requested)}"]])
            if native:
                lines.append(f"- Native supported: {', '.join(native)}")
            if hybrid:
                lines.append(f"- LLM-compiled then execute: {', '.join(hybrid)}")
            if planning:
                lines.append(f"- Planning reference only: {', '.join(planning)}")
            if libraries:
                lines.append(f"- Preferred libraries: {', '.join(libraries)}")
    if decision.missing_capabilities:
        lines.extend(["", "## Missing Capabilities", *[f"- {item}" for item in decision.missing_capabilities]])
    if (
        decision.paper_target_dataset_version
        or decision.execution_environment_dataset_version
        or decision.execution_year_window
    ):
        lines.extend(
            [
                "",
                "## Dataset Semantics",
                f"- Paper original dataset version: {decision.paper_target_dataset_version or 'unknown'}",
                f"- Execution environment dataset version: {decision.execution_environment_dataset_version or 'unknown'}",
                f"- Execution year window: {decision.execution_year_window or 'unknown'}",
                f"- Version mismatch: {decision.dataset_version_mismatch}",
            ]
        )
    if decision.support_notes:
        lines.extend(["", "## Support Notes", *[f"- {item}" for item in decision.support_notes]])
    if decision.follow_up_questions:
        lines.extend(["", "## Follow-Up Questions"])
        for item in decision.follow_up_questions:
            lines.append(f"- [{item.field}] {item.question}")
    if decision.next_actions:
        lines.extend(["", "## Next Actions", *[f"- {item}" for item in decision.next_actions]])
    lines.extend(["", "## Step Plan"])
    for item in decision.step_plan:
        enabled = "enabled" if item.enabled else "skipped"
        lines.append(f"- {item.agent_name}: {enabled} — {item.reason}")
    return "\n".join(lines) + "\n"


def render_agent_reply_markdown(decision: AgentDecision, *, title: str = "") -> str:
    lines = [
        f"# Agent Reply{f': {title}' if title else ''}",
        "",
        f"I reviewed the paper task and selected the `{decision.mode}` route.",
        "",
        f"- Status: {decision.status}",
        f"- Recommended run profile: {decision.recommended_run_profile}",
        f"- Execution backend: {decision.execution_backend}",
    ]
    if (
        decision.paper_target_dataset_version
        or decision.execution_environment_dataset_version
        or decision.execution_year_window
    ):
        lines.extend(
            [
                f"- Paper original dataset version: {decision.paper_target_dataset_version or 'unknown'}",
                f"- Execution environment dataset version: {decision.execution_environment_dataset_version or 'unknown'}",
                f"- Execution year window: {decision.execution_year_window or 'unknown'}",
                f"- Version mismatch: {decision.dataset_version_mismatch}",
            ]
        )
    if decision.paper_required_methods:
        lines.extend(["", "## Paper-Required Methods", *[f"- {item}" for item in decision.paper_required_methods]])
    if decision.missing_capabilities:
        lines.extend(["", "## Missing Capabilities", *[f"- {item}" for item in decision.missing_capabilities]])
    if decision.follow_up_questions:
        lines.extend(
            [
                "",
                "## What I Still Need",
                "Please clarify the following before I treat this as a real reproduction run:",
            ]
        )
        for item in decision.follow_up_questions:
            lines.append(f"- [{item.field}] {item.question}")
    else:
        lines.extend(
            [
                "",
                "## Next Actions",
                *[f"- {item}" for item in decision.next_actions],
            ]
        )
    if decision.support_notes:
        lines.extend(["", "## Support Notes", *[f"- {item}" for item in decision.support_notes]])
    if decision.selected_agent_sequence:
        lines.extend(
            [
                "",
                "## Selected Agents",
                *[f"- {item}" for item in decision.selected_agent_sequence],
            ]
        )
    return "\n".join(lines) + "\n"


def _build_follow_up_questions(missing_fields: tuple[str, ...]) -> list[FollowUpQuestion]:
    questions: list[FollowUpQuestion] = []
    for field in missing_fields:
        question = FOLLOW_UP_QUESTION_TEXT.get(field, f"Please clarify the missing field: {field}.")
        questions.append(
            FollowUpQuestion(
                field=field,
                question=question,
                rationale="This field is required before the task can be treated as a real reproduction run.",
            )
        )
    return questions


def _select_agent_sequence(*, contract: TaskContract, mode: str) -> tuple[str, ...]:
    wants_figures = any(item.kind.endswith("figure") for item in contract.outputs)
    sequence: list[str] = ["paper_parser_agent", "study_design_agent"]

    if mode == "needs_contract_completion":
        sequence.append("report_agent")
    else:
        sequence.extend(["cohort_agent", "feature_agent", "stats_agent"])
        if wants_figures or mode in {"deterministic_preset_run", "agentic_execution", "planning_only"}:
            sequence.append("figure_agent")
        sequence.extend(["verify_agent", "report_agent"])
    return tuple(sequence)


def _step_reason(*, agent_name: str, mode: str, selected_agent_sequence: tuple[str, ...]) -> str:
    if agent_name not in selected_agent_sequence:
        return "Not required for the current agent decision."
    if mode == "needs_contract_completion":
        mapping = {
            "paper_parser_agent": "Needed to keep the source paper evidence visible while the contract is being completed.",
            "study_design_agent": "Needed to identify the missing high-impact fields and block unsafe execution.",
            "report_agent": "Needed to summarize the blocked state and the next required follow-up actions.",
        }
        return mapping.get(agent_name, "Enabled for contract-completion mode.")
    if mode == "planning_only":
        return "Enabled to produce a planning artifact without claiming completed execution."
    if mode == "deterministic_preset_run":
        return "Enabled because the task can run through the preset-backed deterministic execution path."
    if mode == "agentic_execution":
        return "Enabled because the task is executable through the current agentic path."
    return "Enabled for the current agent decision."

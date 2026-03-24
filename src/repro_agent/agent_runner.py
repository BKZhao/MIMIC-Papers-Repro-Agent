from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .config import PipelineConfig
from .contracts import (
    AgentRun,
    ArtifactRecord,
    ExecutionMode,
    RunSummary,
    SessionState,
    StepResult,
    StepStatus,
    TaskContract,
)
from .dataset_adapters import get_dataset_adapter
from .paper_contract import build_paper_alignment_contract
from .paper_materials import collect_paper_materials
from .pipeline import PaperReproPipeline
from .runtime import LocalRuntime
from .skills_registry import build_skill_registry, resolve_agent_skills
from .task_builder import find_missing_high_impact_fields, summarize_task_contract


AGENT_SEQUENCE: tuple[str, ...] = (
    "paper_parser_agent",
    "study_design_agent",
    "cohort_agent",
    "feature_agent",
    "stats_agent",
    "figure_agent",
    "verify_agent",
    "report_agent",
    "git_update_agent",
)


@dataclass(frozen=True)
class TaskExecutionResult:
    session_id: str
    task_contract_path: str
    session_state_path: str
    summary: RunSummary

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "task_contract_path": self.task_contract_path,
            "session_state_path": self.session_state_path,
            "summary": self.summary.as_dict(),
        }


class AgentRunner:
    def __init__(self, project_root: Path, config: PipelineConfig):
        self.project_root = project_root
        self.config = config
        self.runtime = LocalRuntime(project_root=project_root)
        self.skill_registry = build_skill_registry(config)
        self._bridge_step_results: dict[str, StepResult] = {}

    def create_session(
        self,
        contract: TaskContract,
        *,
        paper_path: str = "",
        instructions: str = "",
        session_id: str = "",
    ) -> SessionState:
        self.runtime.ensure_layout()
        actual_session_id = session_id or self.config.run.session_id or f"session-{uuid4().hex[:12]}"
        task_contract_rel = f"shared/sessions/{actual_session_id}/task_contract.json"
        session = SessionState(
            session_id=actual_session_id,
            task_id=contract.task_id,
            paper_path=paper_path or contract.source_paper_path,
            instructions=instructions or contract.instructions,
            status="planned",
            task_contract_path=task_contract_rel,
            meta={
                "dataset_adapter": contract.dataset.adapter,
                "execution_mode": contract.execution_mode.value,
                "interaction_mode": contract.interaction_mode.value,
            },
        )
        self.runtime.write_task_contract(task_contract_rel, contract)
        self.runtime.write_session_state(session)
        return session

    def run_task(
        self,
        contract: TaskContract,
        *,
        session: SessionState | None = None,
        dry_run: bool | None = None,
    ) -> TaskExecutionResult:
        effective_dry_run = self.config.run.dry_run if dry_run is None else dry_run
        self.runtime.ensure_layout()
        session_state = session or self.create_session(
            contract,
            paper_path=contract.source_paper_path,
            instructions=contract.instructions,
        )
        session_state.status = "running"

        task_contract_rel = self._persist_shared_task_contract(contract)
        run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        step_results: list[StepResult] = []
        self._bridge_step_results = {}

        for agent_name in AGENT_SEQUENCE:
            if not self._is_enabled(agent_name):
                step_result = StepResult(step=agent_name, status=StepStatus.SKIPPED, message="Agent disabled by config")
            else:
                handler = getattr(self, f"_run_{agent_name}")
                step_result = handler(contract=contract, session=session_state, dry_run=effective_dry_run)
            step_results.append(step_result)
            self._record_agent_step(session_state, agent_name, step_result)
            if step_result.status in {StepStatus.BLOCKED, StepStatus.FAILED} and self.config.verification_policy.stop_on_fail:
                session_state.status = step_result.status.value
                self.runtime.write_session_state(session_state)
                return TaskExecutionResult(
                    session_id=session_state.session_id,
                    task_contract_path=task_contract_rel,
                    session_state_path=f"shared/sessions/{session_state.session_id}/session_state.json",
                    summary=RunSummary(run_id=run_id, status=step_result.status, step_results=step_results),
                )

        final_status = StepStatus.SUCCESS
        for item in step_results:
            if item.status == StepStatus.FAILED:
                final_status = StepStatus.FAILED
                break
            if item.status == StepStatus.BLOCKED:
                final_status = StepStatus.BLOCKED
                break
        session_state.status = final_status.value
        self.runtime.write_session_state(session_state)
        return TaskExecutionResult(
            session_id=session_state.session_id,
            task_contract_path=task_contract_rel,
            session_state_path=f"shared/sessions/{session_state.session_id}/session_state.json",
            summary=RunSummary(run_id=run_id, status=final_status, step_results=step_results),
        )

    def _run_paper_parser_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        paper_path = (self.project_root / contract.source_paper_path).resolve() if contract.source_paper_path else None
        materials = collect_paper_materials(paper_path) if paper_path and paper_path.exists() else {}
        manifest = {
            "source_paper_path": contract.source_paper_path,
            "material_names": sorted(materials.keys()),
            "material_char_counts": {name: len(text) for name, text in materials.items()},
            "contract_summary": summarize_task_contract(contract),
        }
        manifest_rel = f"shared/sessions/{session.session_id}/paper_materials_manifest.json"
        summary_rel = f"shared/sessions/{session.session_id}/task_summary.txt"
        outputs = [
            self.runtime.write_json(manifest_rel, manifest),
            self.runtime.write_text(summary_rel, summarize_task_contract(contract) + "\n"),
        ]
        return StepResult(
            step="paper_parser_agent",
            status=StepStatus.SUCCESS,
            message="Task contract and paper materials manifest prepared",
            outputs=outputs,
            meta={"material_count": len(materials), "dry_run": dry_run},
        )

    def _run_study_design_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        missing_fields = find_missing_high_impact_fields(contract)
        adapter = get_dataset_adapter(contract.dataset.adapter)
        support = adapter.describe_contract(contract)
        study_payload = {
            "task_id": contract.task_id,
            "title": contract.title,
            "summary": summarize_task_contract(contract),
            "missing_high_impact_fields": missing_fields,
            "adapter_support": support.as_dict(),
            "verification_target_count": len(contract.verification_targets),
            "requested_outputs": [item.kind for item in contract.outputs],
        }
        json_rel = f"shared/sessions/{session.session_id}/study_design.json"
        md_rel = f"shared/sessions/{session.session_id}/study_design.md"
        outputs = [
            self.runtime.write_json(json_rel, study_payload),
            self.runtime.write_text(md_rel, _study_design_markdown(study_payload)),
        ]
        if missing_fields:
            return StepResult(
                step="study_design_agent",
                status=StepStatus.BLOCKED,
                message="Task contract is missing high-impact fields",
                outputs=outputs,
                meta={"missing_high_impact_fields": missing_fields},
            )
        return StepResult(
            step="study_design_agent",
            status=StepStatus.SUCCESS,
            message="Study design contract reviewed",
            outputs=outputs,
            meta={"dry_run": dry_run, "execution_backend": support.execution_backend},
        )

    def _run_cohort_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        adapter = get_dataset_adapter(contract.dataset.adapter)
        support = adapter.describe_contract(contract)
        blueprint_rel = f"shared/sessions/{session.session_id}/cohort_blueprint.json"
        payload = {
            "adapter_support": support.as_dict(),
            "cohort_blueprint": adapter.compile_cohort_blueprint(contract),
        }
        output = self.runtime.write_json(blueprint_rel, payload)
        return StepResult(
            step="cohort_agent",
            status=StepStatus.SUCCESS,
            message="Cohort blueprint prepared",
            outputs=[output],
            meta={"dry_run": dry_run, "execution_backend": support.execution_backend},
        )

    def _run_feature_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        adapter = get_dataset_adapter(contract.dataset.adapter)
        blueprint_rel = f"shared/sessions/{session.session_id}/feature_blueprint.json"
        payload = adapter.compile_feature_blueprint(contract)
        output = self.runtime.write_json(blueprint_rel, payload)
        return StepResult(
            step="feature_agent",
            status=StepStatus.SUCCESS,
            message="Feature blueprint prepared",
            outputs=[output],
            meta={"dry_run": dry_run, "variable_count": len(contract.variables)},
        )

    def _run_stats_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        adapter = get_dataset_adapter(contract.dataset.adapter)
        support = adapter.describe_contract(contract)
        model_blueprint_rel = f"shared/sessions/{session.session_id}/model_blueprint.json"
        model_blueprint_output = self.runtime.write_json(model_blueprint_rel, adapter.compile_model_blueprint(contract))

        if support.execution_backend != "deterministic_bridge":
            status = StepStatus.SUCCESS if dry_run else StepStatus.BLOCKED
            message = (
                "Model blueprint prepared; generic execution backend is not wired for this contract yet"
                if not dry_run
                else "Model blueprint prepared in planning mode"
            )
            return StepResult(
                step="stats_agent",
                status=status,
                message=message,
                outputs=[model_blueprint_output],
                meta={"adapter_support": support.as_dict()},
            )

        bridge_config = _build_bridge_config(self.config, contract)
        pipeline = PaperReproPipeline(project_root=self.project_root, config=bridge_config)
        bridge_summary = pipeline.run(dry_run=dry_run)
        self._bridge_step_results = {item.step: item for item in bridge_summary.step_results}
        bridge_step = self._bridge_step_results.get("stats_agent")
        bridge_rel = f"shared/sessions/{session.session_id}/deterministic_bridge_summary.json"
        bridge_output = self.runtime.write_json(bridge_rel, bridge_summary.as_dict())

        if bridge_step is None:
            return StepResult(
                step="stats_agent",
                status=StepStatus.FAILED,
                message="Deterministic bridge did not return a stats step",
                outputs=[model_blueprint_output, bridge_output],
                meta={"bridge_status": bridge_summary.status.value},
            )

        outputs = list(dict.fromkeys([model_blueprint_output, bridge_output, *bridge_step.outputs]))
        return StepResult(
            step="stats_agent",
            status=bridge_step.status,
            message=f"Stats executed via deterministic bridge: {bridge_step.message}",
            outputs=outputs,
            meta={"bridge_run_status": bridge_summary.status.value},
        )

    def _run_figure_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        if self._bridge_step_results:
            figure_outputs = [
                rel_path
                for rel_path in (
                    "results/km_hospital_tyg.png",
                    "results/km_icu_tyg.png",
                    "results/rcs_tyg_mortality.png",
                )
                if (self.project_root / rel_path).exists()
            ]
            if figure_outputs:
                return StepResult(
                    step="figure_agent",
                    status=StepStatus.SUCCESS,
                    message="Figure artifacts detected from deterministic bridge",
                    outputs=figure_outputs,
                    meta={"figure_count": len(figure_outputs)},
                )

        figure_plan_rel = f"shared/sessions/{session.session_id}/figure_plan.json"
        figure_outputs = [item.kind for item in contract.outputs if item.kind.endswith("figure")]
        payload = {
            "requested_figure_outputs": figure_outputs,
            "dry_run": dry_run,
        }
        output = self.runtime.write_json(figure_plan_rel, payload)
        return StepResult(
            step="figure_agent",
            status=StepStatus.SUCCESS,
            message="Figure plan prepared",
            outputs=[output],
            meta={"requested_figure_outputs": figure_outputs},
        )

    def _run_verify_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        if self._bridge_step_results and "verify_agent" in self._bridge_step_results:
            bridge_step = self._bridge_step_results["verify_agent"]
            outputs = list(bridge_step.outputs)
            return StepResult(
                step="verify_agent",
                status=bridge_step.status,
                message=f"Verification reused deterministic bridge results: {bridge_step.message}",
                outputs=outputs,
                meta={"bridge": True},
            )

        verify_rel = f"shared/sessions/{session.session_id}/verify_plan.json"
        payload = {
            "verification_target_count": len(contract.verification_targets),
            "verification_targets": list(contract.verification_targets),
            "dry_run": dry_run,
        }
        output = self.runtime.write_json(verify_rel, payload)
        return StepResult(
            step="verify_agent",
            status=StepStatus.SUCCESS,
            message="Verification plan prepared",
            outputs=[output],
            meta={"bridge": False},
        )

    def _run_report_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        if self._bridge_step_results and "report_agent" in self._bridge_step_results:
            bridge_step = self._bridge_step_results["report_agent"]
            outputs = list(bridge_step.outputs)
            return StepResult(
                step="report_agent",
                status=bridge_step.status,
                message=f"Report reused deterministic bridge results: {bridge_step.message}",
                outputs=outputs,
                meta={"bridge": True},
            )

        report_rel = f"shared/sessions/{session.session_id}/task_execution_report.md"
        lines = [
            f"# Task Execution Report: {contract.title}",
            "",
            f"- Session ID: {session.session_id}",
            f"- Dry run: {dry_run}",
            f"- Dataset: {contract.dataset.name} ({contract.dataset.adapter})",
            "",
            "## Summary",
            summarize_task_contract(contract),
            "",
            "## Notes",
            *[f"- {note}" for note in contract.notes],
        ]
        output = self.runtime.write_text(report_rel, "\n".join(lines) + "\n")
        return StepResult(
            step="report_agent",
            status=StepStatus.SUCCESS,
            message="Task execution report prepared",
            outputs=[output],
            meta={"bridge": False},
        )

    def _run_git_update_agent(self, *, contract: TaskContract, session: SessionState, dry_run: bool) -> StepResult:
        plan_rel = f"shared/sessions/{session.session_id}/git_update_plan.json"
        payload = {
            "enabled": self._is_enabled("git_update_agent"),
            "dry_run": dry_run,
            "notes": [
                "Git update is modeled as a dedicated agent step.",
                "The concrete push/pull implementation remains delegated to the git-github-update skill.",
            ],
            "requested_by_task": bool(contract.meta.get("git_update")),
        }
        output = self.runtime.write_json(plan_rel, payload)
        return StepResult(
            step="git_update_agent",
            status=StepStatus.SUCCESS,
            message="Git update plan prepared",
            outputs=[output],
            meta={"requested_by_task": bool(contract.meta.get("git_update"))},
        )

    def _persist_shared_task_contract(self, contract: TaskContract) -> str:
        rel_path = self.config.run.task_contract_path or "shared/task_contract.json"
        return self.runtime.write_task_contract(rel_path, contract)

    def _is_enabled(self, agent_name: str) -> bool:
        if agent_name == "git_update_agent" and agent_name not in self.config.agents:
            return False
        raw = self.config.agents.get(agent_name)
        if isinstance(raw, dict):
            return bool(raw.get("enabled", True))
        return True

    def _record_agent_step(self, session: SessionState, agent_name: str, step_result: StepResult) -> None:
        provider, model, skills = self._resolve_agent_route(agent_name)
        agent_run = AgentRun(
            agent_name=agent_name,
            status=step_result.status,
            message=step_result.message,
            provider=provider,
            model=model,
            selected_skills=skills,
            inputs=[session.task_contract_path],
            outputs=list(step_result.outputs),
            meta=dict(step_result.meta),
        )
        session.agent_runs.append(agent_run)
        self.runtime.append_agent_run(agent_run)
        for output in step_result.outputs:
            artifact = ArtifactRecord(
                name=Path(output).name,
                rel_path=output,
                artifact_type=_guess_artifact_type(output),
                producer=agent_name,
                required=True,
                meta={"status": step_result.status.value},
            )
            session.artifact_records.append(artifact)
            self.runtime.record_artifact(artifact)
        self.runtime.write_session_state(session)

    def _resolve_agent_route(self, agent_name: str) -> tuple[str, str, list[str]]:
        route = self.config.agent_routes.get(agent_name)
        provider = self.config.llm.provider
        model = self.config.llm.default_model
        if route:
            provider = route.provider or provider
            model = route.model or model
        skills = [name for name in resolve_agent_skills(self.config, agent_name) if name in self.skill_registry]
        return provider, model, skills


def _guess_artifact_type(rel_path: str) -> str:
    suffix = Path(rel_path).suffix.lower()
    mapping = {
        ".json": "json",
        ".csv": "csv",
        ".md": "markdown",
        ".txt": "text",
        ".png": "figure",
    }
    return mapping.get(suffix, "artifact")


def _study_design_markdown(payload: dict[str, Any]) -> str:
    support = payload.get("adapter_support", {})
    lines = [
        f"# Study Design: {payload.get('title', 'Untitled task')}",
        "",
        "## Contract Summary",
        str(payload.get("summary", "")),
        "",
        "## Adapter Support",
        f"- Adapter: {support.get('adapter_name', 'unknown')}",
        f"- Planning supported: {support.get('planning_supported', False)}",
        f"- Execution supported: {support.get('execution_supported', False)}",
        f"- Execution backend: {support.get('execution_backend', 'unknown')}",
    ]
    missing = list(payload.get("missing_high_impact_fields", []))
    if missing:
        lines.extend(
            [
                "",
                "## Missing High-Impact Fields",
                *[f"- {item}" for item in missing],
            ]
        )
    notes = list(support.get("notes", []))
    if notes:
        lines.extend(
            [
                "",
                "## Notes",
                *[f"- {item}" for item in notes],
            ]
        )
    missing_capabilities = list(support.get("missing_capabilities", []))
    if missing_capabilities:
        lines.extend(
            [
                "",
                "## Missing Capabilities",
                *[f"- {item}" for item in missing_capabilities],
            ]
        )
    return "\n".join(lines) + "\n"


def _build_bridge_config(config: PipelineConfig, contract: TaskContract) -> PipelineConfig:
    bridge_config = copy.deepcopy(config)
    bridge_config.run.execution_mode = ExecutionMode.DETERMINISTIC
    if contract.source_paper_path:
        bridge_config.run.paper_path = contract.source_paper_path
    if contract.verification_targets:
        bridge_config.targets = [dict(item) for item in contract.verification_targets]

    if contract.meta.get("preset") == "mimic_tyg_sepsis":
        paper_contract = build_paper_alignment_contract()
        if not bridge_config.targets:
            bridge_config.targets = [dict(item) for item in paper_contract.get("metric_targets", [])]
        expected = int(dict(paper_contract.get("cohort_targets", {})).get("final_n", 0))
        if bridge_config.quality_gates.expected_cohort_size <= 0:
            bridge_config.quality_gates.expected_cohort_size = expected
        if bridge_config.run.doi == "unknown":
            bridge_config.run.doi = "10.1038/s41598-024-75050-8"

    return bridge_config

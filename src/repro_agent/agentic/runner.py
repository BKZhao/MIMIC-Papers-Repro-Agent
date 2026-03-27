from __future__ import annotations

import copy
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .decision import (
    AgentDecision,
    build_agent_decision,
    render_agent_decision_markdown,
    render_agent_reply_markdown,
)
from .skill_planner import build_llm_execution_plan, render_llm_execution_plan_markdown
from ..config import PipelineConfig
from ..contracts import (
    AgentRun,
    ArtifactRecord,
    ExecutionMode,
    InteractionMode,
    RunSummary,
    SessionState,
    StepResult,
    StepStatus,
    TaskContract,
)
from ..dataset_adapters import get_dataset_adapter
from ..llm import LLMError
from ..analysis.binary_outcome import run_binary_outcome_analysis_workflow
from ..analysis.scaffolds import build_hybrid_scaffold_bundle
from ..paper.materials import collect_paper_materials
from ..paper.presets import get_paper_preset
from ..paper.spec_surfaces import build_analysis_spec_surface, build_paper_spec_surface
from ..paper.builder import find_missing_high_impact_fields, summarize_task_contract
from ..pipeline import PaperReproPipeline
from ..registry.skills import build_skill_registry, resolve_agent_skills
from ..runtime import LocalRuntime


AGENT_SEQUENCE: tuple[str, ...] = (
    "paper_parser_agent",
    "study_design_agent",
    "cohort_agent",
    "feature_agent",
    "stats_agent",
    "figure_agent",
    "verify_agent",
    "report_agent",
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
        self._agent_decision: AgentDecision | None = None

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
        decision = self.prepare_agent_decision(contract, session_state)
        run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        step_results: list[StepResult] = []
        self._bridge_step_results = {}

        for agent_name in decision.selected_agent_sequence or AGENT_SEQUENCE:
            if not self._is_enabled(agent_name):
                step_result = StepResult(step=agent_name, status=StepStatus.SKIPPED, message="Agent disabled by config")
            else:
                handler = getattr(self, f"_run_{agent_name}")
                step_result = handler(contract=contract, session=session_state, dry_run=effective_dry_run)
            step_results.append(step_result)
            self._record_agent_step(session_state, agent_name, step_result)
            if (
                step_result.status in {StepStatus.BLOCKED, StepStatus.FAILED}
                and self.config.verification_policy.stop_on_fail
                and not self._should_continue_after_block(agent_name, step_result)
            ):
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

    def build_agent_decision(self, contract: TaskContract) -> AgentDecision:
        adapter = get_dataset_adapter(contract.dataset.adapter)
        support = adapter.describe_contract(contract)
        decision = build_agent_decision(contract, support)
        self._agent_decision = decision
        return decision

    def prepare_agent_decision(self, contract: TaskContract, session: SessionState) -> AgentDecision:
        decision = self.build_agent_decision(contract)
        json_rel = f"shared/sessions/{session.session_id}/agent_decision.json"
        route_rel = f"shared/sessions/{session.session_id}/analysis_family_route.json"
        paper_surface_rel = f"shared/sessions/{session.session_id}/paper_spec_surface.json"
        analysis_surface_rel = f"shared/sessions/{session.session_id}/analysis_spec_surface.json"
        md_rel = f"shared/sessions/{session.session_id}/agent_execution_plan.md"
        reply_rel = f"shared/sessions/{session.session_id}/agent_reply.md"
        self.runtime.write_json(
            paper_surface_rel,
            build_paper_spec_surface(contract, paper_evidence=contract.meta.get("paper_evidence")),
        )
        self.runtime.write_json(json_rel, decision.as_dict())
        self.runtime.write_json(route_rel, decision.analysis_family_route)
        self.runtime.write_json(
            analysis_surface_rel,
            build_analysis_spec_surface(contract, decision=decision, analysis_family_route=decision.analysis_family_route),
        )
        self.runtime.write_text(md_rel, render_agent_decision_markdown(decision, title=contract.title))
        self.runtime.write_text(reply_rel, render_agent_reply_markdown(decision, title=contract.title))
        session.meta["agent_decision"] = decision.as_dict()
        session.meta["paper_spec_surface_path"] = paper_surface_rel
        session.meta["analysis_family_route_path"] = route_rel
        session.meta["analysis_spec_surface_path"] = analysis_surface_rel
        session.meta["recommended_run_profile"] = decision.recommended_run_profile
        session.meta["selected_agent_sequence"] = list(decision.selected_agent_sequence)
        session.meta["planning_only"] = decision.planning_only
        session.meta["agent_reply_path"] = reply_rel
        self._record_system_artifact(
            session,
            paper_surface_rel,
            producer="paper_intake_and_contract",
            meta={"kind": "paper_spec_surface"},
        )
        self._record_system_artifact(session, json_rel, producer="agent_orchestrator", meta={"kind": "agent_decision"})
        self._record_system_artifact(
            session,
            route_rel,
            producer="agent_orchestrator",
            meta={"kind": "analysis_family_route"},
        )
        self._record_system_artifact(
            session,
            analysis_surface_rel,
            producer="study_design_agent",
            meta={"kind": "analysis_spec_surface"},
        )
        self._record_system_artifact(
            session,
            md_rel,
            producer="agent_orchestrator",
            meta={"kind": "agent_execution_plan"},
        )
        self._record_system_artifact(session, reply_rel, producer="agent_orchestrator", meta={"kind": "agent_reply"})
        self._maybe_write_llm_execution_plan(contract=contract, session=session, decision=decision)
        self.runtime.write_session_state(session)
        return decision

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
            "preset": {
                "key": contract.meta.get("preset", ""),
                "title": contract.meta.get("preset_title", ""),
                "execution_backend": contract.meta.get("execution_backend", support.execution_backend),
            },
            "study_template": {
                "key": contract.meta.get("study_template", ""),
                "title": contract.meta.get("study_template_title", ""),
            },
            "semantic_mapping_summary": {
                "mapped_variable_count": contract.meta.get("semantic_mapped_variable_count", 0),
                "unmapped_variables": list(contract.meta.get("semantic_unmapped_variables", [])),
            },
            "paper_required_methods": list(contract.meta.get("paper_required_methods", [])),
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

        if support.execution_backend == "trajectory_python_bridge":
            if dry_run:
                return StepResult(
                    step="stats_agent",
                    status=StepStatus.SUCCESS,
                    message="Model blueprint prepared for experimental trajectory execution",
                    outputs=[model_blueprint_output],
                    meta={"adapter_support": support.as_dict(), "experimental": True},
                )
            try:
                trajectory_run = self._run_trajectory_profile_execution(contract)
            except Exception as exc:
                return StepResult(
                    step="stats_agent",
                    status=StepStatus.FAILED,
                    message="Experimental trajectory execution failed",
                    outputs=[model_blueprint_output],
                    meta={"error": str(exc), "adapter_support": support.as_dict()},
                )
            outputs = list(dict.fromkeys([model_blueprint_output, *trajectory_run["outputs"]]))
            return StepResult(
                step="stats_agent",
                status=StepStatus.SUCCESS,
                message="Stats executed via experimental trajectory bridge",
                outputs=outputs,
                meta={"experimental": True, "profile": trajectory_run["profile"], "row_count": trajectory_run["row_count"]},
            )

        if support.execution_backend == "profile_survival_bridge":
            if dry_run:
                return StepResult(
                    step="stats_agent",
                    status=StepStatus.SUCCESS,
                    message="Model blueprint prepared for experimental profile survival execution",
                    outputs=[model_blueprint_output],
                    meta={"adapter_support": support.as_dict(), "experimental": True},
                )
            try:
                profile_run = self._run_profile_survival_execution(contract)
            except Exception as exc:
                return StepResult(
                    step="stats_agent",
                    status=StepStatus.FAILED,
                    message="Experimental profile survival execution failed",
                    outputs=[model_blueprint_output],
                    meta={"error": str(exc), "adapter_support": support.as_dict()},
                )
            outputs = list(dict.fromkeys([model_blueprint_output, *profile_run["outputs"]]))
            return StepResult(
                step="stats_agent",
                status=StepStatus.SUCCESS,
                message="Stats executed via experimental profile survival bridge",
                outputs=outputs,
                meta={"experimental": True, "profile": profile_run["profile"], "row_count": profile_run["row_count"]},
            )

        if support.execution_backend == "hybrid_binary_runner":
            if dry_run:
                scaffold_outputs = self._write_hybrid_scaffold_artifacts(contract=contract, session=session)
                return StepResult(
                    step="stats_agent",
                    status=StepStatus.SUCCESS,
                    message="Model blueprint prepared for hybrid binary-outcome execution",
                    outputs=list(dict.fromkeys([model_blueprint_output, *scaffold_outputs])),
                    meta={"adapter_support": support.as_dict(), "experimental": True},
                )
            try:
                binary_run = self._run_hybrid_binary_execution(contract=contract, session=session)
            except Exception as exc:
                return StepResult(
                    step="stats_agent",
                    status=StepStatus.FAILED,
                    message="Hybrid binary-outcome execution failed",
                    outputs=[model_blueprint_output],
                    meta={"error": str(exc), "adapter_support": support.as_dict()},
                )
            scaffold_outputs = self._write_hybrid_scaffold_artifacts(contract=contract, session=session)
            outputs = list(dict.fromkeys([model_blueprint_output, *binary_run["outputs"], *scaffold_outputs]))
            return StepResult(
                step="stats_agent",
                status=StepStatus.SUCCESS,
                message="Stats executed via hybrid binary-outcome runner",
                outputs=outputs,
                meta={
                    "experimental": True,
                    "analysis_dataset_rel": binary_run["analysis_dataset_rel"],
                    "row_count": binary_run["row_count"],
                },
            )

        if support.execution_backend != "deterministic_bridge":
            scaffold_outputs = self._write_hybrid_scaffold_artifacts(contract=contract, session=session)
            outputs = list(dict.fromkeys([model_blueprint_output, *scaffold_outputs]))
            planning_only = self._agent_decision.planning_only if self._agent_decision is not None else False
            status = StepStatus.SUCCESS if dry_run or planning_only else StepStatus.BLOCKED
            message = "Model blueprint prepared in planning-only mode; execution is not wired for this contract yet"
            if dry_run:
                message = "Model blueprint prepared in planning mode"
            elif not planning_only:
                message = "Model blueprint prepared; generic execution backend is not wired for this contract yet"
            if scaffold_outputs:
                message += " Hybrid scaffold artifacts were generated for LLM-compiled analysis families."
            return StepResult(
                step="stats_agent",
                status=status,
                message=message,
                outputs=outputs,
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
        if self._bridge_step_results and "figure_agent" in self._bridge_step_results:
            bridge_step = self._bridge_step_results["figure_agent"]
            return StepResult(
                step="figure_agent",
                status=bridge_step.status,
                message=f"Figure artifacts reused execution results: {bridge_step.message}",
                outputs=list(bridge_step.outputs),
                meta={"bridge": True},
            )
        if self._bridge_step_results:
            stats_bridge_step = self._bridge_step_results.get("stats_agent")
            if stats_bridge_step is not None:
                figure_outputs = [
                    rel_path
                    for rel_path in stats_bridge_step.outputs
                    if rel_path.startswith("results/") and rel_path.endswith(".png")
                ]
                if figure_outputs:
                    return StepResult(
                        step="figure_agent",
                        status=StepStatus.SUCCESS,
                        message="Figure artifacts collected from stats execution outputs",
                        outputs=figure_outputs,
                        meta={"figure_count": len(figure_outputs), "bridge": True},
                    )
            figure_outputs = [
                rel_path
                for rel_path in (
                    "results/km_tyg_figure2.png",
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
                message=f"Verification reused execution results: {bridge_step.message}",
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
                message=f"Report reused execution results: {bridge_step.message}",
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
        ]
        if self._agent_decision is not None:
            lines.extend(
                [
                    "## Agent Decision",
                    f"- Mode: {self._agent_decision.mode}",
                    f"- Status: {self._agent_decision.status}",
                    f"- Recommended run profile: {self._agent_decision.recommended_run_profile}",
                    f"- Execution backend: {self._agent_decision.execution_backend}",
                    "",
                    "## Next Actions",
                    *[f"- {item}" for item in self._agent_decision.next_actions],
                    "",
                ]
            )
            if self._agent_decision.follow_up_questions:
                lines.extend(["## Follow-Up Questions", *[
                    f"- [{item.field}] {item.question}" for item in self._agent_decision.follow_up_questions
                ], ""])
            if self._agent_decision.paper_required_methods:
                lines.extend(
                    [
                        "## Paper-Required Methods",
                        *[f"- {item}" for item in self._agent_decision.paper_required_methods],
                        "",
                    ]
                )
            if self._agent_decision.missing_capabilities:
                lines.extend(
                    [
                        "## Missing Capabilities",
                        *[f"- {item}" for item in self._agent_decision.missing_capabilities],
                        "",
                    ]
                )
            if (
                self._agent_decision.paper_target_dataset_version
                or self._agent_decision.execution_environment_dataset_version
                or self._agent_decision.execution_year_window
            ):
                lines.extend(
                    [
                        "## Dataset Semantics",
                        f"- Paper original dataset version: {self._agent_decision.paper_target_dataset_version or 'unknown'}",
                        f"- Execution environment dataset version: {self._agent_decision.execution_environment_dataset_version or 'unknown'}",
                        f"- Execution year window: {self._agent_decision.execution_year_window or 'unknown'}",
                        f"- Version mismatch: {self._agent_decision.dataset_version_mismatch}",
                        "",
                    ]
                )
            if self._agent_decision.support_notes:
                lines.extend(["## Support Notes", *[f"- {item}" for item in self._agent_decision.support_notes], ""])
        lines.extend(["## Notes", *[f"- {note}" for note in contract.notes]])
        output = self.runtime.write_text(report_rel, "\n".join(lines) + "\n")
        return StepResult(
            step="report_agent",
            status=StepStatus.SUCCESS,
            message="Task execution report prepared",
            outputs=[output],
            meta={"bridge": False},
        )

    def _run_trajectory_profile_execution(self, contract: TaskContract) -> dict[str, Any]:
        profile_key = str(contract.meta.get("experimental_profile", "")).strip() or "mimic_hr_trajectory_sepsis"
        run_prefix = f"runs/{profile_key}"
        cohort_rel = f"shared/{run_prefix}/cohort.csv"
        funnel_rel = f"shared/{run_prefix}/cohort_funnel.json"
        alignment_rel = f"shared/{run_prefix}/cohort_alignment.json"
        analysis_rel = f"shared/{run_prefix}/analysis_dataset.csv"
        missingness_rel = f"shared/{run_prefix}/analysis_missingness.json"

        commands = [
            [
                "python3",
                str(self.project_root / "scripts" / "profiles" / "build_profile_cohort.py"),
                "--project-root",
                str(self.project_root),
                "--profile",
                profile_key,
                "--output",
                cohort_rel,
                "--funnel-output",
                funnel_rel,
                "--alignment-output",
                alignment_rel,
                "--sepsis-source",
                "derived",
                "--execution-environment-version",
                str(
                    contract.meta.get(
                        "execution_environment_dataset_version",
                        contract.meta.get("configured_dataset_version", contract.dataset.version),
                    )
                    or ""
                ),
                "--execution-year-window",
                str(contract.meta.get("execution_year_window", "") or ""),
            ],
            [
                "python3",
                str(self.project_root / "scripts" / "profiles" / "build_profile_analysis_dataset.py"),
                "--project-root",
                str(self.project_root),
                "--profile",
                profile_key,
                "--output",
                analysis_rel,
                "--missingness-output",
                missingness_rel,
                "--sepsis-source",
                "derived",
            ],
            [
                "python3",
                str(self.project_root / "scripts" / "profiles" / "run_profile_stats.py"),
                "--project-root",
                str(self.project_root),
                "--profile",
                profile_key,
                "--analysis-dataset",
                analysis_rel,
                "--missingness",
                missingness_rel,
                "--artifact-subdir",
                run_prefix,
                "--execution-environment-version",
                str(
                    contract.meta.get(
                        "execution_environment_dataset_version",
                        contract.meta.get("configured_dataset_version", contract.dataset.version),
                    )
                    or ""
                ),
                "--execution-year-window",
                str(contract.meta.get("execution_year_window", "") or ""),
            ],
        ]

        last_payload: dict[str, Any] = {}
        for cmd in commands:
            completed = subprocess.run(cmd, text=True, capture_output=True)
            if completed.returncode != 0:
                raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f"command failed: {' '.join(cmd)}")
            last_payload = _json_payload_from_stdout(completed.stdout)

        outputs = list(
            dict.fromkeys(
                [
                    cohort_rel,
                    funnel_rel,
                    alignment_rel,
                    analysis_rel,
                    missingness_rel,
                    *last_payload.get("outputs", _default_trajectory_outputs(profile_key)),
                ]
            )
        )
        figure_outputs = [rel_path for rel_path in outputs if rel_path.startswith("results/") and rel_path.endswith(".png")]
        report_outputs = [rel_path for rel_path in outputs if rel_path.endswith("_reproduction_report.md")]
        verify_outputs = [
            rel_path
            for rel_path in outputs
            if rel_path.endswith("cohort_alignment.json")
            or rel_path.endswith("analysis_missingness.json")
            or rel_path.endswith("_stats_summary.json")
            or rel_path.endswith("_reproduction_report.md")
        ]

        self._bridge_step_results["figure_agent"] = StepResult(
            step="figure_agent",
            status=StepStatus.SUCCESS,
            message="Trajectory figures detected from experimental execution",
            outputs=figure_outputs,
            meta={"experimental": True},
        )
        self._bridge_step_results["verify_agent"] = StepResult(
            step="verify_agent",
            status=StepStatus.SUCCESS,
            message="Trajectory alignment diagnostics collected from experimental execution",
            outputs=verify_outputs,
            meta={"experimental": True},
        )
        self._bridge_step_results["report_agent"] = StepResult(
            step="report_agent",
            status=StepStatus.SUCCESS,
            message="Trajectory reproduction report detected from experimental execution",
            outputs=report_outputs or verify_outputs,
            meta={"experimental": True},
        )
        return {
            "profile": profile_key,
            "row_count": int(last_payload.get("row_count", 0) or 0),
            "outputs": outputs,
            "metrics": dict(last_payload.get("metrics", {})),
        }

    def _run_profile_survival_execution(self, contract: TaskContract) -> dict[str, Any]:
        profile_key = str(contract.meta.get("experimental_profile", "")).strip()
        if not profile_key:
            raise ValueError("experimental_profile is required for profile survival execution")

        run_prefix = f"runs/{profile_key}"
        cohort_rel = f"shared/{run_prefix}/cohort.csv"
        funnel_rel = f"shared/{run_prefix}/cohort_funnel.json"
        alignment_rel = f"shared/{run_prefix}/cohort_alignment.json"
        analysis_rel = f"shared/{run_prefix}/analysis_dataset.csv"
        missingness_rel = f"shared/{run_prefix}/analysis_missingness.json"

        commands = [
            [
                "python3",
                str(self.project_root / "scripts" / "profiles" / "build_profile_cohort.py"),
                "--project-root",
                str(self.project_root),
                "--profile",
                profile_key,
                "--output",
                cohort_rel,
                "--funnel-output",
                funnel_rel,
                "--alignment-output",
                alignment_rel,
                "--sepsis-source",
                "auto",
                "--execution-environment-version",
                str(
                    contract.meta.get(
                        "execution_environment_dataset_version",
                        contract.meta.get("configured_dataset_version", contract.dataset.version),
                    )
                    or ""
                ),
                "--execution-year-window",
                str(contract.meta.get("execution_year_window", "") or ""),
            ],
            [
                "python3",
                str(self.project_root / "scripts" / "profiles" / "build_profile_analysis_dataset.py"),
                "--project-root",
                str(self.project_root),
                "--profile",
                profile_key,
                "--output",
                analysis_rel,
                "--missingness-output",
                missingness_rel,
                "--sepsis-source",
                "auto",
            ],
            [
                "python3",
                str(self.project_root / "scripts" / "profiles" / "run_profile_stats.py"),
                "--project-root",
                str(self.project_root),
                "--profile",
                profile_key,
                "--analysis-dataset",
                analysis_rel,
                "--missingness",
                missingness_rel,
                "--artifact-subdir",
                run_prefix,
                "--execution-environment-version",
                str(
                    contract.meta.get(
                        "execution_environment_dataset_version",
                        contract.meta.get("configured_dataset_version", contract.dataset.version),
                    )
                    or ""
                ),
                "--execution-year-window",
                str(contract.meta.get("execution_year_window", "") or ""),
            ],
        ]

        last_payload: dict[str, Any] = {}
        for cmd in commands:
            completed = subprocess.run(cmd, text=True, capture_output=True)
            if completed.returncode != 0:
                raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or f"command failed: {' '.join(cmd)}")
            last_payload = _json_payload_from_stdout(completed.stdout)

        outputs = list(
            dict.fromkeys(
                [
                    cohort_rel,
                    funnel_rel,
                    alignment_rel,
                    analysis_rel,
                    missingness_rel,
                    *last_payload.get("outputs", []),
                ]
            )
        )
        figure_outputs = [rel_path for rel_path in outputs if rel_path.startswith("results/") and rel_path.endswith(".png")]
        report_outputs = [rel_path for rel_path in outputs if rel_path.endswith("_reproduction_report.md")]
        verify_outputs = [
            rel_path
            for rel_path in outputs
            if rel_path.endswith("cohort_alignment.json")
            or rel_path.endswith("analysis_missingness.json")
            or rel_path.endswith("_stats_summary.json")
            or rel_path.endswith("_reproduction_report.md")
        ]

        self._bridge_step_results["figure_agent"] = StepResult(
            step="figure_agent",
            status=StepStatus.SUCCESS,
            message="Profile survival figures detected from experimental execution",
            outputs=figure_outputs,
            meta={"experimental": True},
        )
        self._bridge_step_results["verify_agent"] = StepResult(
            step="verify_agent",
            status=StepStatus.SUCCESS,
            message="Profile survival diagnostics collected from experimental execution",
            outputs=verify_outputs,
            meta={"experimental": True},
        )
        self._bridge_step_results["report_agent"] = StepResult(
            step="report_agent",
            status=StepStatus.SUCCESS,
            message="Profile survival reproduction report detected from experimental execution",
            outputs=report_outputs or verify_outputs,
            meta={"experimental": True},
        )
        return {
            "profile": profile_key,
            "row_count": int(last_payload.get("row_count", 0) or 0),
            "outputs": outputs,
            "metrics": dict(last_payload.get("metrics", {})),
        }

    def _run_hybrid_binary_execution(self, *, contract: TaskContract, session: SessionState) -> dict[str, Any]:
        analysis_dataset_rel, missingness_rel, bootstrap_outputs = self._ensure_hybrid_binary_inputs(
            contract=contract,
            session=session,
        )
        artifact_subdir = f"sessions/{session.session_id}/binary_outcome"
        result = run_binary_outcome_analysis_workflow(
            project_root=self.project_root,
            contract=contract,
            analysis_dataset_rel=analysis_dataset_rel,
            artifact_subdir=artifact_subdir,
            missingness_rel=missingness_rel,
        )

        figure_outputs = [rel_path for rel_path in result.outputs if rel_path.startswith("results/") and rel_path.endswith(".png")]
        verify_outputs = [
            rel_path
            for rel_path in result.outputs
            if rel_path.endswith("stats_summary.json")
            or rel_path.endswith("roc_summary.json")
            or rel_path.endswith("reproduction_report.md")
        ]
        report_outputs = [rel_path for rel_path in result.outputs if rel_path.endswith("reproduction_report.md")]

        self._bridge_step_results["figure_agent"] = StepResult(
            step="figure_agent",
            status=StepStatus.SUCCESS,
            message="Binary-outcome figures detected from hybrid execution",
            outputs=figure_outputs,
            meta={"experimental": True},
        )
        self._bridge_step_results["verify_agent"] = StepResult(
            step="verify_agent",
            status=StepStatus.SUCCESS,
            message="Binary-outcome diagnostics collected from hybrid execution",
            outputs=verify_outputs,
            meta={"experimental": True},
        )
        self._bridge_step_results["report_agent"] = StepResult(
            step="report_agent",
            status=StepStatus.SUCCESS,
            message="Binary-outcome reproduction report detected from hybrid execution",
            outputs=report_outputs or verify_outputs,
            meta={"experimental": True},
        )
        return {
            "analysis_dataset_rel": result.analysis_dataset_rel,
            "row_count": result.row_count,
            "outputs": list(dict.fromkeys([*bootstrap_outputs, *result.outputs])),
            "metrics": dict(result.metrics),
        }

    def _ensure_hybrid_binary_inputs(
        self,
        *,
        contract: TaskContract,
        session: SessionState,
    ) -> tuple[str, str, list[str]]:
        analysis_dataset_rel = str(contract.meta.get("analysis_dataset_rel", "")).strip()
        missingness_rel = str(contract.meta.get("missingness_rel", "")).strip()
        output_paths: list[str] = []

        if analysis_dataset_rel:
            analysis_dataset_path = (self.project_root / analysis_dataset_rel).resolve()
            if analysis_dataset_path.exists():
                return analysis_dataset_rel, missingness_rel, output_paths

        auto_profile = str(contract.meta.get("auto_binary_profile", "")).strip().lower()
        if auto_profile != "mimic_arf_nomogram_v1":
            if analysis_dataset_rel:
                raise FileNotFoundError(f"Contract meta.analysis_dataset_rel points to a missing file: {analysis_dataset_rel}")
            raise ValueError("Contract meta.analysis_dataset_rel is required for hybrid binary-outcome execution")

        analysis_dataset_rel = f"shared/sessions/{session.session_id}/arf_nomogram/analysis_dataset.csv"
        missingness_rel = f"shared/sessions/{session.session_id}/arf_nomogram/analysis_missingness.json"
        funnel_rel = f"shared/sessions/{session.session_id}/arf_nomogram/cohort_funnel.json"
        script_rel = str(contract.meta.get("analysis_dataset_builder", "scripts/analysis/build_arf_nomogram_dataset.py")).strip()
        script_path = (self.project_root / script_rel).resolve()
        if not script_path.exists():
            raise FileNotFoundError(f"Auto binary dataset builder script is missing: {script_rel}")

        command = [
            sys.executable,
            str(script_path),
            "--project-root",
            str(self.project_root),
            "--output",
            analysis_dataset_rel,
            "--missingness-output",
            missingness_rel,
            "--funnel-output",
            funnel_rel,
        ]
        completed = subprocess.run(command, text=True, capture_output=True)
        if completed.returncode != 0:
            stderr = completed.stderr.strip()
            stdout = completed.stdout.strip()
            detail = stderr or stdout or "auto dataset builder failed"
            raise RuntimeError(detail)

        payload = _json_payload_from_stdout(completed.stdout)
        if payload.get("analysis_dataset"):
            analysis_dataset_rel = str(payload["analysis_dataset"]).strip() or analysis_dataset_rel
        if payload.get("missingness_output"):
            missingness_rel = str(payload["missingness_output"]).strip() or missingness_rel
        if payload.get("funnel_output"):
            funnel_rel = str(payload["funnel_output"]).strip() or funnel_rel

        contract.meta["analysis_dataset_rel"] = analysis_dataset_rel
        contract.meta["missingness_rel"] = missingness_rel
        contract.meta["cohort_funnel_rel"] = funnel_rel
        self.runtime.write_task_contract(session.task_contract_path, contract)

        output_paths = [path for path in (analysis_dataset_rel, missingness_rel, funnel_rel) if path]
        return analysis_dataset_rel, missingness_rel, output_paths

    def _persist_shared_task_contract(self, contract: TaskContract) -> str:
        rel_path = self.config.run.task_contract_path or "shared/task_contract.json"
        return self.runtime.write_task_contract(rel_path, contract)

    def _is_enabled(self, agent_name: str) -> bool:
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
            existing = next((item for item in session.artifact_records if item.rel_path == output), None)
            if existing is not None:
                existing.producer = agent_name
                existing.meta.update({"status": step_result.status.value})
                continue
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

    def _should_continue_after_block(self, agent_name: str, step_result: StepResult) -> bool:
        if step_result.status != StepStatus.BLOCKED or self._agent_decision is None:
            return False
        return self._agent_decision.mode == "needs_contract_completion" and agent_name == "study_design_agent"

    def _record_system_artifact(
        self,
        session: SessionState,
        rel_path: str,
        *,
        producer: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if any(item.rel_path == rel_path for item in session.artifact_records):
            return
        artifact = ArtifactRecord(
            name=Path(rel_path).name,
            rel_path=rel_path,
            artifact_type=_guess_artifact_type(rel_path),
            producer=producer,
            required=True,
            meta=meta or {},
        )
        session.artifact_records.append(artifact)
        self.runtime.record_artifact(artifact)

    def _write_hybrid_scaffold_artifacts(self, *, contract: TaskContract, session: SessionState) -> list[str]:
        bundle = build_hybrid_scaffold_bundle(contract)
        needs_scaffold = bool(
            bundle.analysis_spec.get("llm_compiled_families")
            or bundle.analysis_spec.get("planning_reference_families")
        )
        if not needs_scaffold:
            return []

        analysis_rel = f"shared/sessions/{session.session_id}/analysis_spec.json"
        figure_rel = f"shared/sessions/{session.session_id}/figure_spec.json"
        executor_rel = f"shared/sessions/{session.session_id}/executor_scaffold.py"
        return [
            self.runtime.write_json(analysis_rel, bundle.analysis_spec),
            self.runtime.write_json(figure_rel, bundle.figure_spec),
            self.runtime.write_text(executor_rel, bundle.executor_scaffold),
        ]

    def _maybe_write_llm_execution_plan(
        self,
        *,
        contract: TaskContract,
        session: SessionState,
        decision: AgentDecision,
    ) -> None:
        skill_routes = {
            agent_name: [name for name in resolve_agent_skills(self.config, agent_name) if name in self.skill_registry]
            for agent_name in decision.selected_agent_sequence
        }
        support = get_dataset_adapter(contract.dataset.adapter).describe_contract(contract)
        try:
            payload = build_llm_execution_plan(
                contract=contract,
                support=support,
                config=self.config,
                recommended_run_profile=decision.recommended_run_profile,
                selected_agent_sequence=decision.selected_agent_sequence,
                skill_routes=skill_routes,
            )
        except LLMError as exc:
            session.meta["llm_execution_plan_error"] = str(exc)
            return

        json_rel = f"shared/sessions/{session.session_id}/llm_execution_plan.json"
        md_rel = f"shared/sessions/{session.session_id}/llm_execution_plan.md"
        self.runtime.write_json(json_rel, payload)
        self.runtime.write_text(md_rel, render_llm_execution_plan_markdown(payload, title=contract.title))
        session.meta["llm_execution_plan_path"] = json_rel
        session.meta["llm_execution_plan_markdown_path"] = md_rel
        session.meta["llm_execution_plan"] = payload
        self._record_system_artifact(
            session,
            json_rel,
            producer="agent_orchestrator",
            meta={"kind": "llm_execution_plan"},
        )
        self._record_system_artifact(
            session,
            md_rel,
            producer="agent_orchestrator",
            meta={"kind": "llm_execution_plan_markdown"},
        )


def _guess_artifact_type(rel_path: str) -> str:
    suffix = Path(rel_path).suffix.lower()
    mapping = {
        ".json": "json",
        ".csv": "csv",
        ".md": "markdown",
        ".py": "code",
        ".txt": "text",
        ".png": "figure",
    }
    return mapping.get(suffix, "artifact")


def _study_design_markdown(payload: dict[str, Any]) -> str:
    support = payload.get("adapter_support", {})
    preset = payload.get("preset", {}) if isinstance(payload.get("preset"), dict) else {}
    template = payload.get("study_template", {}) if isinstance(payload.get("study_template"), dict) else {}
    semantic_summary = (
        payload.get("semantic_mapping_summary", {})
        if isinstance(payload.get("semantic_mapping_summary"), dict)
        else {}
    )
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
    if preset.get("key"):
        lines.extend(
            [
                "",
                "## Preset",
                f"- Key: {preset.get('key', '')}",
                f"- Title: {preset.get('title', '')}",
                f"- Execution backend: {preset.get('execution_backend', '')}",
            ]
        )
    if template.get("key"):
        lines.extend(
            [
                "",
                "## Study Template",
                f"- Key: {template.get('key', '')}",
                f"- Title: {template.get('title', '')}",
            ]
        )
    mapped_count = int(semantic_summary.get("mapped_variable_count", 0) or 0)
    if mapped_count > 0 or semantic_summary.get("unmapped_variables"):
        lines.extend(
            [
                "",
                "## Semantic Mapping",
                f"- Mapped variables: {mapped_count}",
            ]
        )
        for item in semantic_summary.get("unmapped_variables", []):
            lines.append(f"- Unmapped: {item}")
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
    if (
        support.get("paper_target_dataset_version")
        or support.get("execution_environment_dataset_version")
        or support.get("execution_year_window")
    ):
        lines.extend(
            [
                "",
                "## Dataset Semantics",
                f"- Paper original dataset version: {support.get('paper_target_dataset_version', 'unknown') or 'unknown'}",
                f"- Execution environment dataset version: {support.get('execution_environment_dataset_version', support.get('configured_dataset_version', 'unknown')) or 'unknown'}",
                f"- Execution year window: {support.get('execution_year_window', 'unknown') or 'unknown'}",
                f"- Version mismatch: {support.get('dataset_version_mismatch', False)}",
            ]
        )
    paper_required_methods = payload.get("paper_required_methods", [])
    if isinstance(paper_required_methods, list) and paper_required_methods:
        lines.extend(["", "## Paper-Required Methods", *[f"- {item}" for item in paper_required_methods]])
    return "\n".join(lines) + "\n"


def _build_bridge_config(config: PipelineConfig, contract: TaskContract) -> PipelineConfig:
    bridge_config = copy.deepcopy(config)
    bridge_config.run.execution_mode = ExecutionMode.DETERMINISTIC
    bridge_config.run.interaction_mode = InteractionMode.BATCH
    if contract.dataset.name:
        bridge_config.run.dataset = contract.dataset.name
    if contract.source_paper_path:
        bridge_config.run.paper_path = contract.source_paper_path
    if contract.verification_targets:
        bridge_config.targets = [dict(item) for item in contract.verification_targets]

    preset = get_paper_preset(contract.meta.get("preset"))
    if preset is not None:
        if not bridge_config.targets:
            bridge_config.targets = preset.verification_targets()
        if bridge_config.quality_gates.expected_cohort_size <= 0 and preset.default_expected_cohort_size > 0:
            bridge_config.quality_gates.expected_cohort_size = preset.default_expected_cohort_size
        if bridge_config.run.doi == "unknown":
            bridge_config.run.doi = preset.doi
        bridge_config.run.name = preset.key

    return bridge_config


def _json_payload_from_stdout(text: str) -> dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        start = payload.find("{")
        end = payload.rfind("}")
        if start < 0 or end <= start:
            return {}
        try:
            loaded = json.loads(payload[start : end + 1])
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}


def _default_trajectory_outputs(profile_key: str) -> list[str]:
    run_prefix = f"runs/{profile_key}"
    return [
        f"shared/{run_prefix}/cohort.csv",
        f"shared/{run_prefix}/cohort_funnel.json",
        f"shared/{run_prefix}/cohort_alignment.json",
        f"shared/{run_prefix}/analysis_dataset.csv",
        f"shared/{run_prefix}/analysis_missingness.json",
        f"shared/{run_prefix}/{profile_key}_trajectory_assignments.csv",
        f"shared/{run_prefix}/{profile_key}_trajectory_table.csv",
        f"shared/{run_prefix}/{profile_key}_trajectory_table.md",
        f"shared/{run_prefix}/{profile_key}_trajectory_backend_summary.json",
        f"shared/{run_prefix}/{profile_key}_baseline_table.csv",
        f"shared/{run_prefix}/{profile_key}_baseline_table.md",
        f"shared/{run_prefix}/{profile_key}_cox_models.csv",
        f"shared/{run_prefix}/{profile_key}_cox_models.md",
        f"shared/{run_prefix}/{profile_key}_km_summary.json",
        f"shared/{run_prefix}/{profile_key}_stats_summary.json",
        f"shared/{run_prefix}/{profile_key}_reproduction_report.md",
        f"results/{run_prefix}/{profile_key}_trajectory.png",
        f"results/{run_prefix}/{profile_key}_km.png",
    ]

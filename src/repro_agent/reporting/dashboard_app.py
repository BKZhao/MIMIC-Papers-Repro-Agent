from __future__ import annotations

import json
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

try:
    from .job_runtime import (
        JOB_STATUS_COMPLETED,
        JOB_STATUS_FAILED,
        JOB_STATUS_QUEUED,
        JOB_STATUS_RUNNING,
        JOB_STATUS_WAITING_USER_INPUT,
        create_job,
        get_job,
        get_worker_state,
        list_jobs,
        resume_job_with_answers,
        run_next_queued_job_async,
    )
except ImportError:  # pragma: no cover - script execution fallback
    src_root = Path(__file__).resolve().parents[2]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)
    from repro_agent.reporting.job_runtime import (
        JOB_STATUS_COMPLETED,
        JOB_STATUS_FAILED,
        JOB_STATUS_QUEUED,
        JOB_STATUS_RUNNING,
        JOB_STATUS_WAITING_USER_INPUT,
        create_job,
        get_job,
        get_worker_state,
        list_jobs,
        resume_job_with_answers,
        run_next_queued_job_async,
    )

_RUN_MODE_META: dict[str, dict[str, str]] = {
    "agentic_repro": {
        "label": "完整复现（推荐）",
        "desc": "自动串联 plan/continue/run，适合正式跑论文并拿完整状态与报告。",
    },
    "plan_only": {
        "label": "仅规划",
        "desc": "只做论文解析和任务规划，不执行 cohort/stats。",
    },
    "preset_real_run": {
        "label": "预置快速执行",
        "desc": "优先走 preset/profile 路线，适合已有预置的任务快速验证。",
    },
}

_JOB_STATUS_META: dict[str, str] = {
    JOB_STATUS_QUEUED: "排队中",
    JOB_STATUS_RUNNING: "运行中",
    JOB_STATUS_WAITING_USER_INPUT: "等待补答",
    JOB_STATUS_COMPLETED: "已完成",
    JOB_STATUS_FAILED: "失败",
}


@dataclass
class SessionSnapshot:
    session_id: str
    session_dir: Path
    session_state: dict[str, Any]
    verdict: dict[str, Any]
    iteration_log: dict[str, Any]
    token_summary: dict[str, Any]
    workflow_stage_report_path: Path | None
    token_summary_path: Path | None
    task_contract_path: Path | None

    @property
    def status(self) -> str:
        return str(self.session_state.get("status", "")).strip() or "unknown"

    @property
    def paper_path(self) -> str:
        return str(self.session_state.get("paper_path", "")).strip()

    @property
    def execution_route(self) -> str:
        meta = self.session_state.get("meta", {})
        if isinstance(meta, dict):
            return str(meta.get("execution_route", "")).strip()
        return ""

    @property
    def route_reason(self) -> str:
        meta = self.session_state.get("meta", {})
        if isinstance(meta, dict):
            return str(meta.get("route_reason", "")).strip()
        return ""

    @property
    def verdict_status(self) -> str:
        return str(self.verdict.get("status", "")).strip() or "unknown"

    @property
    def verdict_sub_status(self) -> str:
        return str(self.verdict.get("sub_status", "")).strip() or "unknown"

    @property
    def max_relative_error_pct(self) -> float | None:
        return _coerce_float(self.verdict.get("max_relative_error_pct"))

    @property
    def applied_threshold_percent(self) -> float | None:
        return _coerce_float(self.verdict.get("applied_threshold_percent"))

    @property
    def iterations_used(self) -> int:
        verdict_it = _coerce_int(self.verdict.get("alignment_iterations_used"))
        if verdict_it is not None:
            return verdict_it
        iterations = self.iteration_log.get("iterations", [])
        if isinstance(iterations, list):
            return len(iterations)
        return 0

    @property
    def total_tokens(self) -> int | None:
        token_usage = self.token_summary.get("token_usage", {})
        if isinstance(token_usage, dict):
            return _coerce_int(token_usage.get("total_tokens_sum"))
        return _coerce_int(self.token_summary.get("total_tokens_sum"))

    @property
    def updated_at(self) -> str:
        updated = str(self.iteration_log.get("updated_at_utc", "")).strip()
        if updated:
            return updated
        if self.token_summary_path and self.token_summary_path.exists():
            return datetime.fromtimestamp(self.token_summary_path.stat().st_mtime).isoformat()
        return datetime.fromtimestamp(self.session_dir.stat().st_mtime).isoformat()

    @property
    def artifact_count(self) -> int:
        artifacts = self.session_state.get("artifact_records", [])
        return len(artifacts) if isinstance(artifacts, list) else 0


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _default_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _discover_session_dirs(project_root: Path) -> list[Path]:
    base = project_root / "shared" / "sessions"
    if not base.exists():
        return []
    return sorted((item for item in base.iterdir() if item.is_dir()), key=lambda p: p.stat().st_mtime, reverse=True)


def _load_session_snapshot(project_root: Path, session_dir: Path) -> SessionSnapshot:
    session_id = session_dir.name
    session_state_path = session_dir / "session_state.json"
    verdict_path = session_dir / "reproducibility_verdict.json"
    iteration_log_path = session_dir / "alignment_iteration_log.json"
    workflow_report_path = session_dir / "workflow_stage_report.md"
    token_summary_path = project_root / "results" / "sessions" / session_id / "llm_token_usage_summary.json"

    session_state = _safe_read_json(session_state_path)
    verdict = _safe_read_json(verdict_path)
    if not verdict and isinstance(session_state.get("meta"), dict):
        meta_verdict = session_state["meta"].get("reproducibility_verdict")
        if isinstance(meta_verdict, dict):
            verdict = dict(meta_verdict)

    iteration_log = _safe_read_json(iteration_log_path)
    token_summary = _safe_read_json(token_summary_path)

    task_contract_rel = str(session_state.get("task_contract_path", "")).strip()
    task_contract_path = (project_root / task_contract_rel) if task_contract_rel else None

    return SessionSnapshot(
        session_id=session_id,
        session_dir=session_dir,
        session_state=session_state,
        verdict=verdict,
        iteration_log=iteration_log,
        token_summary=token_summary,
        workflow_stage_report_path=workflow_report_path if workflow_report_path.exists() else None,
        token_summary_path=token_summary_path if token_summary_path.exists() else None,
        task_contract_path=task_contract_path if task_contract_path and task_contract_path.exists() else None,
    )


def _build_sessions_dataframe(snapshots: list[SessionSnapshot]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in snapshots:
        rows.append(
            {
                "session_id": item.session_id,
                "status": item.status,
                "verdict": item.verdict_status,
                "sub_status": item.verdict_sub_status,
                "execution_route": item.execution_route or "unknown",
                "iterations": item.iterations_used,
                "max_error_pct": item.max_relative_error_pct,
                "threshold_pct": item.applied_threshold_percent,
                "total_tokens": item.total_tokens,
                "artifacts": item.artifact_count,
                "paper_path": item.paper_path,
                "updated_at": item.updated_at,
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "session_id",
                "status",
                "verdict",
                "sub_status",
                "execution_route",
                "iterations",
                "max_error_pct",
                "threshold_pct",
                "total_tokens",
                "artifacts",
                "paper_path",
                "updated_at",
            ]
        )
    return pd.DataFrame(rows)


def _build_stage_gate_dataframe(snapshot: SessionSnapshot) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    iterations = snapshot.iteration_log.get("iterations", [])
    if isinstance(iterations, list):
        for iteration in iterations:
            it_num = _coerce_int(iteration.get("iteration"))
            step_results = iteration.get("step_results", [])
            if not isinstance(step_results, list):
                continue
            for step in step_results:
                if not isinstance(step, dict):
                    continue
                meta = step.get("meta", {})
                if not isinstance(meta, dict):
                    continue
                gate = meta.get("alignment_gate")
                if not isinstance(gate, dict):
                    continue
                rows.append(
                    {
                        "iteration": it_num,
                        "stage": str(gate.get("stage", "")).strip(),
                        "agent_step": str(step.get("step", "")).strip(),
                        "passed": bool(gate.get("passed", False)),
                        "measurable": bool(gate.get("measurable", False)),
                        "actual_error_pct": _coerce_float(gate.get("max_relative_error_pct")),
                        "threshold_pct": _coerce_float(gate.get("applied_threshold_percent")),
                        "metric_count": _coerce_int(gate.get("metric_count")),
                        "reason": str(gate.get("reason", "")).strip(),
                    }
                )
    if not rows:
        return pd.DataFrame(
            columns=[
                "iteration",
                "stage",
                "agent_step",
                "passed",
                "measurable",
                "actual_error_pct",
                "threshold_pct",
                "metric_count",
                "reason",
            ]
        )
    return pd.DataFrame(rows)


def _build_step_result_dataframe(snapshot: SessionSnapshot) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    iterations = snapshot.iteration_log.get("iterations", [])
    if not isinstance(iterations, list):
        return pd.DataFrame(columns=["iteration", "step", "status", "message"])

    for iteration in iterations:
        it_num = _coerce_int(iteration.get("iteration"))
        step_results = iteration.get("step_results", [])
        if not isinstance(step_results, list):
            continue
        for step in step_results:
            if not isinstance(step, dict):
                continue
            rows.append(
                {
                    "iteration": it_num,
                    "step": str(step.get("step", "")).strip(),
                    "status": str(step.get("status", "")).strip(),
                    "message": str(step.get("message", "")).strip(),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["iteration", "step", "status", "message"])
    return pd.DataFrame(rows)


def _build_artifact_dataframe(project_root: Path, snapshot: SessionSnapshot) -> pd.DataFrame:
    records = snapshot.session_state.get("artifact_records", [])
    rows: list[dict[str, Any]] = []
    if isinstance(records, list):
        for item in records:
            if not isinstance(item, dict):
                continue
            rel_path = str(item.get("rel_path", "")).strip()
            abs_path = (project_root / rel_path) if rel_path else None
            rows.append(
                {
                    "name": str(item.get("name", "")).strip(),
                    "type": str(item.get("artifact_type", "")).strip(),
                    "producer": str(item.get("producer", "")).strip(),
                    "required": bool(item.get("required", False)),
                    "rel_path": rel_path,
                    "exists": bool(abs_path and abs_path.exists()),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["name", "type", "producer", "required", "rel_path", "exists"])
    return pd.DataFrame(rows)


def _load_markdown(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _ensure_dashboard_user_tag() -> str:
    key = "dashboard_user_tag"
    cached = str(st.session_state.get(key, "")).strip()
    if cached:
        return cached
    generated = f"web-{uuid.uuid4().hex[:12]}"
    st.session_state[key] = generated
    return generated


def _render_session_summary(snapshot: SessionSnapshot) -> None:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Session Status", snapshot.status)
    c2.metric("Verdict", f"{snapshot.verdict_status}/{snapshot.verdict_sub_status}")
    c3.metric("Route", snapshot.execution_route or "unknown")
    c4.metric("Iterations", snapshot.iterations_used)
    token_text = "-" if snapshot.total_tokens is None else f"{snapshot.total_tokens:,}"
    c5.metric("Total Tokens", token_text)

    st.caption(f"Paper: {snapshot.paper_path or '-'}")
    st.caption(f"Route Reason: {snapshot.route_reason or '-'}")


def _sanitize_filename(name: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name or "").strip())
    text = text.strip("._")
    return text or "paper.pdf"


def _save_uploaded_paper(*, project_root: Path, uploaded_file: Any) -> Path:
    upload_dir = project_root / "papers" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    original_name = getattr(uploaded_file, "name", "paper.pdf")
    safe_name = _sanitize_filename(original_name)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = upload_dir / f"{timestamp}-{safe_name}"
    out_path.write_bytes(uploaded_file.getvalue())
    return out_path


_DOWNLOADABLE_SUFFIXES = {".md", ".csv", ".json", ".zip", ".png", ".pdf", ".tex"}
_JOB_ACTIVE_STATUSES = {JOB_STATUS_QUEUED, JOB_STATUS_RUNNING}


def _run_mode_label(mode: str) -> str:
    text = str(mode or "").strip()
    meta = _RUN_MODE_META.get(text, {})
    label = str(meta.get("label", "")).strip()
    if not label:
        return text
    return f"{text} ｜ {label}"


def _job_status_label(status: str) -> str:
    text = str(status or "").strip()
    mapped = _JOB_STATUS_META.get(text, "")
    if not mapped:
        return text or "unknown"
    return f"{text} ｜ {mapped}"


def _render_create_job_guide() -> None:
    st.markdown("#### 使用引导")
    g1, g2, g3 = st.columns(3)
    g1.info("第 1 步：上传 PDF 或填写 `paper_path`。二选一即可。")
    g2.info("第 2 步：选择 `run_mode`。首次建议用 `agentic_repro`。")
    g3.info("第 3 步：点击“创建任务并开始”，到下方 `Job Detail` 看状态与报告。")

    with st.expander("字段与选项说明（首次使用建议展开）", expanded=False):
        st.markdown(
            "\n".join(
                [
                    "- `上传论文 PDF`：最直观的入口，系统会自动保存到仓库 `papers/uploads/`。",
                    "- `paper_path`：如果论文已在仓库中，可直接填相对路径，如 `papers/xxx.pdf`。",
                    "- `instructions`：补充你的复现要求，例如“优先输出 cohort 纳排和对齐表”。",
                    "- `run_mode`：控制执行深度。",
                    "  - `agentic_repro`：完整链路（推荐）。",
                    "  - `plan_only`：仅规划，不执行统计。",
                    "  - `preset_real_run`：优先用预置流程快速执行。",
                    "- `config_path`：配置文件路径，默认 `configs/openclaw.agentic.yaml`。",
                    "- `session_id`：可选。留空会自动生成；填写可续接已有会话。",
                    "- `use_llm`：是否启用 LLM 做论文证据抽取与合同构建。",
                    "- `dry_run`：仅验证流程和参数，不做真实重计算。",
                ]
            )
        )


def _render_job_status_legend() -> None:
    with st.expander("任务状态说明", expanded=False):
        st.markdown(
            "\n".join(
                [
                    "- `queued`：任务已创建，等待 worker 执行。",
                    "- `running`：后台正在执行。",
                    "- `waiting_user_input`：需要你补答 follow-up 问题后继续。",
                    "- `completed`：任务完成，可查看报告和下载产物。",
                    "- `failed`：执行失败，可在 Job Detail 查看错误信息。",
                ]
            )
        )


def _build_job_artifact_dataframe(*, project_root: Path, artifacts: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not isinstance(artifacts, list):
        return pd.DataFrame(columns=["name", "type", "producer", "required", "rel_path", "exists", "size_bytes"])

    for item in artifacts:
        if not isinstance(item, dict):
            continue
        rel_path = str(item.get("rel_path", "")).strip()
        abs_path = project_root / rel_path if rel_path else None
        exists = bool(abs_path and abs_path.exists())
        size_bytes = abs_path.stat().st_size if exists and abs_path else None
        rows.append(
            {
                "name": str(item.get("name", "")).strip(),
                "type": str(item.get("artifact_type", "")).strip(),
                "producer": str(item.get("producer", "")).strip(),
                "required": bool(item.get("required", False)),
                "rel_path": rel_path,
                "exists": exists,
                "size_bytes": size_bytes,
            }
        )
    if not rows:
        return pd.DataFrame(columns=["name", "type", "producer", "required", "rel_path", "exists", "size_bytes"])
    return pd.DataFrame(rows)


def _build_jobs_dataframe(jobs: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for job in jobs:
        payload = job.get("request_payload", {})
        if not isinstance(payload, dict):
            payload = {}
        run_mode = str(payload.get("run_mode", "")).strip()
        response = job.get("last_response", {})
        if not isinstance(response, dict):
            response = {}
        execution = response.get("execution", {})
        execution_status = ""
        if isinstance(execution, dict):
            execution_status = str(execution.get("status", "")).strip()
        rows.append(
            {
                "job_id": str(job.get("job_id", "")).strip(),
                "status": str(job.get("status", "")).strip(),
                "status_label": _job_status_label(str(job.get("status", "")).strip()),
                "progress_stage": str(job.get("progress_stage", "")).strip(),
                "session_id": str(job.get("session_id", "")).strip(),
                "run_mode": run_mode,
                "run_mode_label": _run_mode_label(run_mode),
                "execution_status": execution_status,
                "updated_at": str(job.get("updated_at", "")).strip(),
                "elapsed_seconds": _coerce_float(job.get("elapsed_seconds")),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "job_id",
                "status",
                "status_label",
                "progress_stage",
                "session_id",
                "run_mode",
                "run_mode_label",
                "execution_status",
                "updated_at",
                "elapsed_seconds",
            ]
        )
    return pd.DataFrame(rows)


def _resolve_report_path_from_job(project_root: Path, job: dict[str, Any]) -> Path | None:
    rel = str(job.get("workflow_report_path", "")).strip()
    if rel:
        candidate = project_root / rel
        if candidate.exists():
            return candidate
    session_id = str(job.get("session_id", "")).strip()
    if session_id:
        fallback = project_root / "shared" / "sessions" / session_id / "workflow_stage_report.md"
        if fallback.exists():
            return fallback
    return None


def _render_job_downloads(*, project_root: Path, artifact_df: pd.DataFrame, job_id: str) -> None:
    if artifact_df.empty:
        st.info("当前任务还没有可下载工件。")
        return
    st.markdown("#### Download Artifacts")
    rendered = 0
    for idx, row in artifact_df.iterrows():
        rel_path = str(row.get("rel_path", "")).strip()
        if not rel_path:
            continue
        abs_path = project_root / rel_path
        if not abs_path.exists():
            continue
        suffix = abs_path.suffix.lower()
        if suffix not in _DOWNLOADABLE_SUFFIXES:
            continue
        try:
            data = abs_path.read_bytes()
        except OSError:
            continue
        label = f"Download {abs_path.name}"
        st.download_button(
            label=label,
            data=data,
            file_name=abs_path.name,
            key=f"download_{job_id}_{idx}",
        )
        rendered += 1
    if rendered == 0:
        st.info("当前工件暂不在可下载类型范围（md/csv/json/zip/png/pdf/tex）内。")


def _render_follow_up_form(*, project_root: Path, job: dict[str, Any]) -> None:
    job_id = str(job.get("job_id", "")).strip()
    follow_up_questions = job.get("follow_up_questions", [])
    if not isinstance(follow_up_questions, list) or not follow_up_questions:
        st.info("任务需要补充信息，但未提供结构化 follow-up 问题。")
        return

    st.markdown("#### Follow-up Questions")
    st.caption("请补齐下面问题后继续执行。提交后任务会自动重新入队。")

    with st.form(f"follow_up_form_{job_id}", clear_on_submit=False):
        answers: dict[str, str] = {}
        missing_required: list[str] = []
        for idx, item in enumerate(follow_up_questions):
            if not isinstance(item, dict):
                continue
            field = str(item.get("field", "")).strip() or f"field_{idx+1}"
            question = str(item.get("question", "")).strip() or field
            rationale = str(item.get("rationale", "")).strip()
            required = bool(item.get("required", True))
            label = f"{idx+1}. {question}"
            user_input = st.text_area(
                label,
                value="",
                height=90,
                key=f"follow_up_{job_id}_{field}_{idx}",
                help=rationale or None,
            )
            cleaned = user_input.strip()
            if cleaned:
                answers[field] = cleaned
            elif required:
                missing_required.append(field)
        submitted = st.form_submit_button("提交补答并继续运行", type="primary")

    if not submitted:
        return
    if missing_required:
        st.error("以下必填字段还未填写：" + ", ".join(missing_required))
        return
    if not answers:
        st.error("请至少填写一个 follow-up 字段后再提交。")
        return

    try:
        resume_job_with_answers(project_root=project_root, job_id=job_id, answers=answers)
        run_next_queued_job_async(project_root)
        st.success("补答已提交，任务已重新入队。")
        time.sleep(0.6)
        st.rerun()
    except Exception as exc:  # pragma: no cover - interactive runtime path
        st.error(f"继续任务失败：{exc}")


def _render_job_detail(*, project_root: Path, job: dict[str, Any]) -> None:
    payload = job.get("request_payload", {})
    if not isinstance(payload, dict):
        payload = {}
    response = job.get("last_response", {})
    if not isinstance(response, dict):
        response = {}

    execution = response.get("execution", {})
    execution_status = str(execution.get("status", "-")).strip() if isinstance(execution, dict) else "-"
    run_mode = str(payload.get("run_mode", "-")).strip() or "-"
    status = str(job.get("status", "-")).strip()

    top_cols = st.columns(5)
    top_cols[0].metric("status", _job_status_label(status))
    top_cols[1].metric("session_id", str(job.get("session_id", "-")))
    top_cols[2].metric("run_mode", _run_mode_label(run_mode))
    top_cols[3].metric("execution.status", execution_status or "-")
    elapsed = _coerce_float(job.get("elapsed_seconds"))
    top_cols[4].metric("elapsed (s)", "-" if elapsed is None else f"{elapsed:.3f}")

    st.caption("request: " + json.dumps(payload, ensure_ascii=False))
    error_text = str(job.get("error", "")).strip()
    if error_text:
        st.error(error_text)

    verdict = job.get("reproducibility_verdict", {})
    if isinstance(verdict, dict) and verdict:
        st.markdown("#### Reproducibility Verdict")
        st.json(verdict)

    if str(job.get("status", "")).strip() == JOB_STATUS_WAITING_USER_INPUT:
        _render_follow_up_form(project_root=project_root, job=job)

    artifact_df = _build_job_artifact_dataframe(project_root=project_root, artifacts=job.get("artifacts", []))
    st.markdown("#### Artifacts")
    st.dataframe(artifact_df, use_container_width=True, hide_index=True)

    if not artifact_df.empty:
        missing_required = artifact_df[(artifact_df["required"] == True) & (artifact_df["exists"] == False)]  # noqa: E712
        if not missing_required.empty:
            st.warning("以下 required artifacts 当前缺失：")
            st.dataframe(missing_required, use_container_width=True, hide_index=True)
    _render_job_downloads(project_root=project_root, artifact_df=artifact_df, job_id=str(job.get("job_id", "")))

    report_path = _resolve_report_path_from_job(project_root, job)
    st.markdown("#### Workflow Stage Report")
    if report_path and report_path.exists():
        markdown = _load_markdown(report_path)
        if markdown:
            st.markdown(markdown)
            try:
                report_bytes = report_path.read_bytes()
                st.download_button(
                    "Download workflow_stage_report.md",
                    data=report_bytes,
                    file_name=report_path.name,
                    key=f"download_report_{job.get('job_id', '')}",
                )
            except OSError:
                st.info("Workflow report 可预览，但下载读取失败。")
        else:
            st.info("workflow_stage_report.md 存在，但内容为空。")
    else:
        st.info("当前任务尚未生成 workflow_stage_report.md。")

    with st.expander("Raw Job JSON", expanded=False):
        st.json(job)


def _render_run_new_paper_panel(project_root: Path, owner_tag: str) -> None:
    run_next_queued_job_async(project_root, max_workers=1)
    worker_state = get_worker_state()

    st.subheader("Run New Paper · Job Center")
    st.caption("输入论文后创建异步任务，页面会自动展示任务状态、裁决、阶段报告与可下载工件。")
    st.caption(
        "Worker: "
        + ("running" if worker_state.get("running") else "idle")
        + f" | active_job_id: {str(worker_state.get('active_job_id', '') or '-')}"
    )
    st.caption("当前视图默认只展示本浏览器创建的任务与相关 session。")
    _render_create_job_guide()

    default_config_path = "configs/openclaw.agentic.yaml"
    with st.container():
        st.markdown("#### Create Job")
        with st.form("run_new_paper_job_form", clear_on_submit=False):
            col_a, col_b = st.columns(2)
            with col_a:
                uploaded_file = st.file_uploader(
                    "上传论文 PDF",
                    type=["pdf"],
                    help="直接上传论文文件。上传后会自动保存到 `papers/uploads/` 并作为 paper_path 使用。",
                )
                paper_path_input = st.text_input(
                    "或使用仓库内论文路径",
                    value="",
                    placeholder="papers/your-paper.pdf",
                    help="当论文已经在仓库里时使用，例如 `papers/s40001-026-03994-w.pdf`。",
                )
                instructions = st.text_area(
                    "运行说明（instructions）",
                    value="请严格按论文方法复现，输出阶段状态、门禁结果、对齐结论与报告。",
                    height=110,
                    help="告诉 Agent 你的偏好，例如“先给 cohort 纳排，再继续统计建模”。",
                )
            with col_b:
                run_mode = st.selectbox(
                    "Run Mode",
                    options=["agentic_repro", "plan_only", "preset_real_run"],
                    index=0,
                    format_func=_run_mode_label,
                    help="控制执行深度：完整复现 / 仅规划 / 预置快速执行。",
                )
                config_path = st.text_input(
                    "Config Path",
                    value=default_config_path,
                    help="运行配置文件路径。默认配置对大多数任务可直接使用。",
                )
                session_id = st.text_input(
                    "Session ID（可选）",
                    value="",
                    help="留空自动创建新 session。填写已有 session_id 可继续同一会话。",
                )
                use_llm = st.checkbox(
                    "Use LLM",
                    value=True,
                    help="开启后会使用 LLM 进行论文证据抽取与合同构建。",
                )
                dry_run = st.checkbox(
                    "Dry Run",
                    value=False,
                    help="只走流程校验，不做完整执行。适合先验证配置是否正确。",
                )
            submitted = st.form_submit_button("创建任务并开始", type="primary")

        if submitted:
            effective_paper_path = str(paper_path_input).strip()
            if uploaded_file is not None:
                saved_path = _save_uploaded_paper(project_root=project_root, uploaded_file=uploaded_file)
                try:
                    effective_paper_path = str(saved_path.relative_to(project_root))
                except ValueError:
                    effective_paper_path = str(saved_path)
            if not effective_paper_path:
                st.error("请先上传论文 PDF，或填写 `paper_path`。")
                return

            request_payload: dict[str, Any] = {
                "paper_path": effective_paper_path,
                "instructions": instructions.strip(),
                "run_mode": run_mode,
                "config_path": config_path.strip() or default_config_path,
                "use_llm": bool(use_llm),
                "dry_run": bool(dry_run),
            }
            if str(session_id).strip():
                request_payload["session_id"] = str(session_id).strip()

            try:
                job_id = create_job(project_root=project_root, request_payload=request_payload, owner_tag=owner_tag)
                st.session_state["dashboard_selected_job_id"] = job_id
                run_next_queued_job_async(project_root)
                st.success(f"任务已创建：{job_id}")
            except Exception as exc:  # pragma: no cover - interactive runtime path
                st.error(f"创建任务失败：{exc}")
                return

    with st.container():
        st.markdown("#### Job List")
        _render_job_status_legend()
        filter_col, refresh_col, auto_col = st.columns([2, 1, 1])
        with filter_col:
            status_filter = st.selectbox(
                "状态筛选",
                options=["all", JOB_STATUS_QUEUED, JOB_STATUS_RUNNING, JOB_STATUS_WAITING_USER_INPUT, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED],
                index=0,
            )
        with refresh_col:
            manual_refresh = st.button("手动刷新", use_container_width=True)
        with auto_col:
            auto_refresh = st.checkbox("运行中自动刷新", value=True)

        jobs = list_jobs(
            project_root=project_root,
            limit=200,
            status_filter="" if status_filter == "all" else status_filter,
            owner_tag=owner_tag,
        )
        jobs_df = _build_jobs_dataframe(jobs)
        jobs_display_df = jobs_df.rename(
            columns={
                "job_id": "Job ID",
                "status_label": "状态",
                "progress_stage": "阶段",
                "session_id": "Session",
                "run_mode_label": "Run Mode",
                "execution_status": "执行状态",
                "updated_at": "更新时间",
                "elapsed_seconds": "耗时(秒)",
            }
        )
        selected_columns = ["Job ID", "状态", "阶段", "Session", "Run Mode", "执行状态", "更新时间", "耗时(秒)"]
        st.dataframe(jobs_display_df[selected_columns], use_container_width=True, hide_index=True)

        selected_job_id = str(st.session_state.get("dashboard_selected_job_id", "")).strip()
        job_ids = [str(item.get("job_id", "")).strip() for item in jobs if str(item.get("job_id", "")).strip()]
        if not selected_job_id and job_ids:
            selected_job_id = job_ids[0]
        if job_ids:
            default_index = job_ids.index(selected_job_id) if selected_job_id in job_ids else 0
            selected_job_id = st.selectbox("查看任务详情", options=job_ids, index=default_index)
            st.session_state["dashboard_selected_job_id"] = selected_job_id
        else:
            st.info("当前没有任务。请先创建一个 job。")
            selected_job_id = ""

    with st.container():
        st.markdown("#### Job Detail")
        if not selected_job_id:
            st.info("暂无可查看的任务。")
            return
        try:
            job = get_job(project_root=project_root, job_id=selected_job_id)
        except Exception as exc:  # pragma: no cover - interactive runtime path
            st.error(f"读取任务失败：{exc}")
            return
        _render_job_detail(project_root=project_root, job=job)

    if manual_refresh:
        st.rerun()
    if auto_refresh and str(job.get("status", "")).strip() in _JOB_ACTIVE_STATUSES:
        time.sleep(2.5)
        st.rerun()


def _render_session_explorer(
    project_root: Path,
    snapshots: list[SessionSnapshot],
    related_session_ids: set[str],
) -> None:
    show_all_sessions = st.checkbox(
        "显示全部 session（管理员视角）",
        value=False,
        help="默认只显示当前浏览器用户相关的 session，勾选后显示仓库内全部 session。",
    )
    if not show_all_sessions:
        snapshots = [item for item in snapshots if item.session_id in related_session_ids]

    if not snapshots:
        if show_all_sessions:
            st.info("当前没有可展示的 session。你可以先在左侧“Run New Paper”里启动一次运行。")
        else:
            st.info("当前用户还没有相关 session。请先在左侧“Run New Paper”创建任务。")
        return

    sessions_df = _build_sessions_dataframe(snapshots)

    status_options = sorted(item for item in sessions_df["status"].dropna().unique().tolist())
    selected_status = st.sidebar.multiselect("Filter by Status", options=status_options, default=status_options)

    verdict_options = sorted(item for item in sessions_df["verdict"].dropna().unique().tolist())
    selected_verdicts = st.sidebar.multiselect("Filter by Verdict", options=verdict_options, default=verdict_options)

    search_text = st.sidebar.text_input("Search Session ID / Paper Path", value="").strip().lower()

    filtered_df = sessions_df.copy()
    if selected_status:
        filtered_df = filtered_df[filtered_df["status"].isin(selected_status)]
    if selected_verdicts:
        filtered_df = filtered_df[filtered_df["verdict"].isin(selected_verdicts)]
    if search_text:
        filtered_df = filtered_df[
            filtered_df["session_id"].str.lower().str.contains(search_text, na=False)
            | filtered_df["paper_path"].str.lower().str.contains(search_text, na=False)
        ]

    st.subheader("Session 总览")
    st.dataframe(
        filtered_df[
            [
                "session_id",
                "status",
                "verdict",
                "sub_status",
                "execution_route",
                "iterations",
                "max_error_pct",
                "threshold_pct",
                "total_tokens",
                "artifacts",
                "updated_at",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    if filtered_df.empty:
        st.warning("当前筛选条件下没有 session。")
        return

    default_session_id = str(filtered_df.iloc[0]["session_id"])
    selected_session_id = st.sidebar.selectbox(
        "Session Detail",
        options=filtered_df["session_id"].tolist(),
        index=0,
    )

    snapshot_by_id = {item.session_id: item for item in snapshots}
    snapshot = snapshot_by_id.get(selected_session_id) or snapshot_by_id[default_session_id]

    st.markdown("---")
    st.subheader(f"Session 详情: `{snapshot.session_id}`")
    _render_session_summary(snapshot)

    tab_gate, tab_steps, tab_artifacts, tab_verdict, tab_report = st.tabs(
        ["Stage Gates", "Steps", "Artifacts", "Verdict & Tokens", "Workflow Report"]
    )

    with tab_gate:
        gate_df = _build_stage_gate_dataframe(snapshot)
        st.dataframe(gate_df, use_container_width=True, hide_index=True)
        if not gate_df.empty:
            st.bar_chart(gate_df.set_index("stage")[["actual_error_pct", "threshold_pct"]])
        else:
            st.info("未找到阶段门禁数据。")

    with tab_steps:
        step_df = _build_step_result_dataframe(snapshot)
        st.dataframe(step_df, use_container_width=True, hide_index=True)

    with tab_artifacts:
        artifact_df = _build_artifact_dataframe(project_root, snapshot)
        st.dataframe(artifact_df, use_container_width=True, hide_index=True)
        required_missing = artifact_df[(artifact_df["required"] == True) & (artifact_df["exists"] == False)]  # noqa: E712
        if not required_missing.empty:
            st.warning("检测到缺失的 required artifact：")
            st.dataframe(required_missing, use_container_width=True, hide_index=True)

    with tab_verdict:
        st.markdown("#### Reproducibility Verdict")
        st.json(snapshot.verdict)
        st.markdown("#### Token Summary")
        if snapshot.token_summary:
            st.json(snapshot.token_summary)
        else:
            st.info("未找到 token summary。")

    with tab_report:
        markdown_text = _load_markdown(snapshot.workflow_stage_report_path)
        if markdown_text:
            st.markdown(markdown_text)
        else:
            st.info("未找到 workflow_stage_report.md。")


def _inject_dashboard_styles() -> None:
    st.markdown(
        """
<style>
div[data-testid="stMetric"] {
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  padding: 10px 12px;
}
div[data-testid="stMetricLabel"] > div {
  font-weight: 600;
}
div[data-testid="stExpander"] summary p {
  font-weight: 600;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(page_title="MIMIC Repro Dashboard", layout="wide")
    _inject_dashboard_styles()
    st.title("MIMIC Reproduction Harness Dashboard")
    st.caption("基于 session 工件的可视化控制台：阶段门禁、裁决、Token、交付产物")

    default_root = _default_project_root()
    root_input = st.sidebar.text_input("Project Root", value=str(default_root))
    project_root = Path(root_input).expanduser().resolve()
    owner_tag = _ensure_dashboard_user_tag()

    tab_run, tab_explorer = st.tabs(["Run New Paper", "Session Explorer"])

    with tab_run:
        _render_run_new_paper_panel(project_root, owner_tag=owner_tag)

    current_user_jobs = list_jobs(project_root=project_root, limit=200, owner_tag=owner_tag)
    related_session_ids = {
        str(item.get("session_id", "")).strip()
        for item in current_user_jobs
        if str(item.get("session_id", "")).strip()
    }

    session_dirs = _discover_session_dirs(project_root)
    snapshots = [_load_session_snapshot(project_root, session_dir) for session_dir in session_dirs]
    with tab_explorer:
        _render_session_explorer(project_root, snapshots, related_session_ids=related_session_ids)


if __name__ == "__main__":
    main()

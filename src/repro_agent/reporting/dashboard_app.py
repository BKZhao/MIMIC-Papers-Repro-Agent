from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from ..integrations.openclaw import handle_openclaw_request


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


def _build_response_artifact_dataframe(response: dict[str, Any]) -> pd.DataFrame:
    artifacts = response.get("artifacts", [])
    rows: list[dict[str, Any]] = []
    if isinstance(artifacts, list):
        for item in artifacts:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "name": str(item.get("name", "")).strip(),
                    "type": str(item.get("artifact_type", "")).strip(),
                    "producer": str(item.get("producer", "")).strip(),
                    "required": bool(item.get("required", False)),
                    "rel_path": str(item.get("rel_path", "")).strip(),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["name", "type", "producer", "required", "rel_path"])
    return pd.DataFrame(rows)


def _try_render_response_report(*, project_root: Path, response: dict[str, Any]) -> None:
    session_id = str(response.get("session_id", "")).strip()
    if not session_id:
        return
    report_path = project_root / "shared" / "sessions" / session_id / "workflow_stage_report.md"
    if not report_path.exists():
        return
    markdown = _load_markdown(report_path)
    if not markdown:
        return
    with st.expander("运行后生成的 Workflow Report", expanded=False):
        st.markdown(markdown)


def _render_run_new_paper_panel(project_root: Path) -> None:
    st.subheader("Run New Paper")
    st.caption("上传论文或输入论文路径，直接调用 Agent 执行，并返回状态与报告。")

    default_config_path = "configs/openclaw.agentic.yaml"
    with st.form("run_new_paper_form", clear_on_submit=False):
        col_a, col_b = st.columns(2)
        with col_a:
            uploaded_file = st.file_uploader("上传论文 PDF", type=["pdf"])
            paper_path_input = st.text_input("或使用仓库内论文路径", value="", placeholder="papers/your-paper.pdf")
            instructions = st.text_area(
                "运行说明（instructions）",
                value=(
                    "请严格按论文方法复现，输出阶段状态、门禁结果、对齐结论与报告。"
                ),
                height=110,
            )
        with col_b:
            run_mode = st.selectbox(
                "Run Mode",
                options=["agentic_repro", "plan_only", "preset_real_run"],
                index=0,
            )
            config_path = st.text_input("Config Path", value=default_config_path)
            session_id = st.text_input("Session ID（可选）", value="")
            use_llm = st.checkbox("Use LLM", value=True)
            dry_run = st.checkbox("Dry Run", value=False)

        submitted = st.form_submit_button("开始运行", type="primary")

    response = st.session_state.get("last_openclaw_response", {})
    last_request = st.session_state.get("last_openclaw_request", {})
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

        start_ts = time.perf_counter()
        with st.spinner("Agent 正在执行，请稍候..."):
            try:
                response = handle_openclaw_request(project_root=project_root, request=request_payload)
                elapsed = round(time.perf_counter() - start_ts, 3)
                response["_dashboard_elapsed_seconds"] = elapsed
                st.session_state["last_openclaw_response"] = response
                st.session_state["last_openclaw_request"] = request_payload
                st.success(f"运行完成，耗时 {elapsed}s")
            except Exception as exc:  # pragma: no cover - interactive runtime path
                st.session_state["last_openclaw_response"] = {}
                st.session_state["last_openclaw_request"] = request_payload
                st.error(f"运行失败：{exc}")
                return

    if response:
        st.markdown("#### 最近一次运行结果")
        top_cols = st.columns(5)
        top_cols[0].metric("status", str(response.get("status", "-")))
        top_cols[1].metric("session_id", str(response.get("session_id", "-")))
        top_cols[2].metric("run_mode", str(response.get("run_profile_used", "-")))
        execution = response.get("execution", {})
        if isinstance(execution, dict):
            top_cols[3].metric("execution.status", str(execution.get("status", "-")))
        else:
            top_cols[3].metric("execution.status", "-")
        elapsed_value = _coerce_float(response.get("_dashboard_elapsed_seconds"))
        top_cols[4].metric("elapsed (s)", "-" if elapsed_value is None else f"{elapsed_value:.3f}")

        st.caption(
            "request: "
            + json.dumps(last_request, ensure_ascii=False)
        )
        verdict = response.get("reproducibility_verdict", {})
        if isinstance(verdict, dict) and verdict:
            st.markdown("#### Reproducibility Verdict")
            st.json(verdict)

        artifact_df = _build_response_artifact_dataframe(response)
        if not artifact_df.empty:
            st.markdown("#### Artifacts")
            st.dataframe(artifact_df, use_container_width=True, hide_index=True)

        with st.expander("Raw Response JSON", expanded=False):
            st.json(response)

        _try_render_response_report(project_root=project_root, response=response)


def _render_session_explorer(project_root: Path, snapshots: list[SessionSnapshot]) -> None:
    if not snapshots:
        st.info("当前没有可展示的 session。你可以先在左侧“Run New Paper”里启动一次运行。")
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


def main() -> None:
    st.set_page_config(page_title="MIMIC Repro Dashboard", page_icon="🧪", layout="wide")
    st.title("🧪 MIMIC Reproduction Harness Dashboard")
    st.caption("基于 session 工件的可视化控制台：阶段门禁、裁决、Token、交付产物")

    default_root = _default_project_root()
    root_input = st.sidebar.text_input("Project Root", value=str(default_root))
    project_root = Path(root_input).expanduser().resolve()

    tab_run, tab_explorer = st.tabs(["Run New Paper", "Session Explorer"])

    with tab_run:
        _render_run_new_paper_panel(project_root)

    session_dirs = _discover_session_dirs(project_root)
    snapshots = [_load_session_snapshot(project_root, session_dir) for session_dir in session_dirs]
    with tab_explorer:
        _render_session_explorer(project_root, snapshots)


if __name__ == "__main__":
    main()

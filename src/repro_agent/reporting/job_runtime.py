from __future__ import annotations

import copy
import json
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..integrations.openclaw import handle_openclaw_request

JOB_STATUS_QUEUED = "queued"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_WAITING_USER_INPUT = "waiting_user_input"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_CANCELLED = "cancelled"

JOB_STATUSES = {
    JOB_STATUS_QUEUED,
    JOB_STATUS_RUNNING,
    JOB_STATUS_WAITING_USER_INPUT,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
    JOB_STATUS_CANCELLED,
}

DEFAULT_MAX_WORKERS = 1
DEFAULT_JOB_RETENTION = 200
JOB_DIR_REL_PATH = Path("shared") / "web_jobs"

_FILE_LOCK = threading.Lock()
_EXECUTOR: ThreadPoolExecutor | None = None
_ACTIVE_FUTURE: Future[None] | None = None
_ACTIVE_JOB_ID: str | None = None
_WORKER_LOCK = threading.Lock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def _jobs_dir(project_root: Path) -> Path:
    path = project_root.resolve() / JOB_DIR_REL_PATH
    path.mkdir(parents=True, exist_ok=True)
    return path


def _job_file(project_root: Path, job_id: str) -> Path:
    safe = str(job_id).strip()
    if not safe:
        raise ValueError("job_id cannot be empty")
    return _jobs_dir(project_root) / f"{safe}.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.write_text(text + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _deep_copy(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False))
    except (TypeError, ValueError):
        return copy.deepcopy(value)


def _normalize_answers(raw: dict[str, Any] | None) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    if not isinstance(raw, dict):
        return cleaned
    for key, value in raw.items():
        key_text = str(key).strip()
        value_text = str(value).strip()
        if key_text and value_text:
            cleaned[key_text] = value_text
    return cleaned


def _normalize_follow_up_questions(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field", "")).strip()
        question = str(item.get("question", "")).strip()
        rationale = str(item.get("rationale", "")).strip()
        if not field and not question:
            continue
        rows.append(
            {
                "field": field,
                "question": question,
                "rationale": rationale,
                "required": bool(item.get("required", True)),
            }
        )
    return rows


def _extract_workflow_report_rel_path(project_root: Path, session_id: str) -> str:
    if not session_id:
        return ""
    report_abs = project_root / "shared" / "sessions" / session_id / "workflow_stage_report.md"
    if not report_abs.exists():
        return ""
    try:
        return str(report_abs.relative_to(project_root))
    except ValueError:
        return str(report_abs)


def _job_elapsed_seconds(job: dict[str, Any]) -> float | None:
    started_text = str(job.get("started_at", "")).strip()
    finished_text = str(job.get("finished_at", "")).strip()
    if not started_text:
        return None
    try:
        started = datetime.fromisoformat(started_text.replace("Z", "+00:00"))
    except ValueError:
        return None
    target = finished_text or _utc_now_iso()
    try:
        ended = datetime.fromisoformat(target.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((ended - started).total_seconds(), 3)


def _is_waiting_for_follow_up(response: dict[str, Any]) -> bool:
    execution = response.get("execution", {})
    if not isinstance(execution, dict):
        return False
    return (
        str(execution.get("status", "")).strip().lower() == "skipped"
        and str(execution.get("reason", "")).strip().lower() == "task_not_ready"
        and bool(_normalize_follow_up_questions(response.get("follow_up_questions")))
    )


def _derive_progress_stage(response: dict[str, Any]) -> str:
    execution = response.get("execution", {})
    if isinstance(execution, dict):
        execution_status = str(execution.get("status", "")).strip()
        if execution_status:
            return f"execution:{execution_status}"
    response_status = str(response.get("status", "")).strip()
    if response_status:
        return f"response:{response_status}"
    return ""


def _sanitize_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("request_payload must be a JSON object")
    return _deep_copy(payload)


def _new_job_payload(*, job_id: str, request_payload: dict[str, Any], owner_tag: str = "") -> dict[str, Any]:
    now = _utc_now_iso()
    session_id = str(request_payload.get("session_id", "")).strip()
    return {
        "job_id": job_id,
        "created_at": now,
        "updated_at": now,
        "request_payload": _deep_copy(request_payload),
        "initial_request_payload": _deep_copy(request_payload),
        "session_id": session_id,
        "status": JOB_STATUS_QUEUED,
        "progress_stage": "queued",
        "error": "",
        "last_response": {},
        "follow_up_questions": [],
        "answers_history": [],
        "artifacts": [],
        "workflow_report_path": "",
        "reproducibility_verdict": {},
        "attempt_count": 0,
        "started_at": "",
        "finished_at": "",
        "elapsed_seconds": None,
        "owner_tag": str(owner_tag).strip(),
    }


def _validate_job_payload(job: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(job, dict):
        return {}
    status = str(job.get("status", "")).strip()
    if status not in JOB_STATUSES:
        job["status"] = JOB_STATUS_FAILED
    return job


def _load_job_locked(project_root: Path, job_id: str) -> tuple[Path, dict[str, Any]]:
    path = _job_file(project_root, job_id)
    job = _validate_job_payload(_read_json(path))
    if not job:
        raise FileNotFoundError(f"job not found: {job_id}")
    return path, job


def _save_job_locked(path: Path, job: dict[str, Any]) -> dict[str, Any]:
    job["updated_at"] = _utc_now_iso()
    job["elapsed_seconds"] = _job_elapsed_seconds(job)
    _write_json_atomic(path, job)
    return _deep_copy(job)


def _prune_old_jobs(project_root: Path, *, keep: int = DEFAULT_JOB_RETENTION) -> None:
    keep = max(1, _coerce_int(keep) or DEFAULT_JOB_RETENTION)
    jobs_dir = _jobs_dir(project_root)
    job_files = [path for path in jobs_dir.glob("*.json") if path.is_file()]
    if len(job_files) <= keep:
        return
    job_files.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    for stale_path in job_files[keep:]:
        try:
            stale_path.unlink()
        except OSError:
            continue


def create_job(project_root: Path, request_payload: dict[str, Any], owner_tag: str = "") -> str:
    project_root = project_root.resolve()
    normalized = _sanitize_request_payload(request_payload)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_id = f"job-{timestamp}-{uuid.uuid4().hex[:8]}"
    job = _new_job_payload(job_id=job_id, request_payload=normalized, owner_tag=owner_tag)
    path = _job_file(project_root, job_id)
    with _FILE_LOCK:
        _write_json_atomic(path, job)
        _prune_old_jobs(project_root)
    return job_id


def get_job(project_root: Path, job_id: str) -> dict[str, Any]:
    project_root = project_root.resolve()
    with _FILE_LOCK:
        path, job = _load_job_locked(project_root, job_id)
        job["job_file"] = str(path)
        return _deep_copy(job)


def list_jobs(
    project_root: Path,
    limit: int = DEFAULT_JOB_RETENTION,
    status_filter: str = "",
    owner_tag: str = "",
) -> list[dict[str, Any]]:
    project_root = project_root.resolve()
    jobs_dir = _jobs_dir(project_root)
    rows: list[dict[str, Any]] = []
    owner_filter = str(owner_tag).strip()
    for path in jobs_dir.glob("*.json"):
        job = _validate_job_payload(_read_json(path))
        if not job:
            continue
        job_status = str(job.get("status", "")).strip()
        if status_filter and job_status != str(status_filter).strip():
            continue
        if owner_filter and str(job.get("owner_tag", "")).strip() != owner_filter:
            continue
        rows.append(job)
    rows.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
    capped = max(1, _coerce_int(limit) or DEFAULT_JOB_RETENTION)
    return [_deep_copy(item) for item in rows[:capped]]


def _build_resume_request_payload(job: dict[str, Any], answers: dict[str, str]) -> dict[str, Any]:
    base = job.get("request_payload", {})
    if not isinstance(base, dict):
        base = {}
    session_id = str(job.get("session_id", "")).strip() or str(base.get("session_id", "")).strip()
    if not session_id:
        raise ValueError("Cannot resume a job without session_id")

    resumed: dict[str, Any] = {
        "session_id": session_id,
        "run_mode": str(base.get("run_mode", "agentic_repro")).strip() or "agentic_repro",
        "answers": answers,
    }
    for optional_key in ("config_path", "use_llm", "dry_run"):
        if optional_key in base:
            resumed[optional_key] = base[optional_key]
    return resumed


def resume_job_with_answers(project_root: Path, job_id: str, answers: dict[str, Any]) -> dict[str, Any]:
    project_root = project_root.resolve()
    cleaned_answers = _normalize_answers(answers if isinstance(answers, dict) else {})
    if not cleaned_answers:
        raise ValueError("answers cannot be empty")

    with _FILE_LOCK:
        path, job = _load_job_locked(project_root, job_id)
        if str(job.get("status", "")).strip() != JOB_STATUS_WAITING_USER_INPUT:
            raise ValueError(f"job {job_id} is not waiting for user input")

        resume_payload = _build_resume_request_payload(job, cleaned_answers)
        answers_history = job.get("answers_history", [])
        if not isinstance(answers_history, list):
            answers_history = []
        answers_history.append(
            {
                "submitted_at": _utc_now_iso(),
                "answers": cleaned_answers,
            }
        )
        job["answers_history"] = answers_history
        job["request_payload"] = resume_payload
        job["follow_up_questions"] = []
        job["status"] = JOB_STATUS_QUEUED
        job["progress_stage"] = "resume_queued"
        job["error"] = ""
        job["finished_at"] = ""
        saved = _save_job_locked(path, job)
    return saved


def _mark_job_running(project_root: Path, job_id: str) -> dict[str, Any]:
    with _FILE_LOCK:
        path, job = _load_job_locked(project_root, job_id)
        if str(job.get("status", "")).strip() == JOB_STATUS_CANCELLED:
            return _deep_copy(job)
        if not str(job.get("started_at", "")).strip():
            job["started_at"] = _utc_now_iso()
        job["finished_at"] = ""
        job["attempt_count"] = int(_coerce_int(job.get("attempt_count")) or 0) + 1
        job["status"] = JOB_STATUS_RUNNING
        job["progress_stage"] = "dispatching"
        job["error"] = ""
        return _save_job_locked(path, job)


def _finalize_job_success(project_root: Path, job_id: str, response: dict[str, Any]) -> dict[str, Any]:
    follow_up_questions = _normalize_follow_up_questions(response.get("follow_up_questions"))
    waiting_for_input = _is_waiting_for_follow_up(response)
    with _FILE_LOCK:
        path, job = _load_job_locked(project_root, job_id)
        session_id = str(response.get("session_id", "")).strip() or str(job.get("session_id", "")).strip()
        status = JOB_STATUS_WAITING_USER_INPUT if waiting_for_input else JOB_STATUS_COMPLETED

        response_status = str(response.get("status", "")).strip().lower()
        if response_status in {"failed", "error"}:
            status = JOB_STATUS_FAILED

        job["status"] = status
        job["session_id"] = session_id
        job["progress_stage"] = "awaiting_follow_up" if status == JOB_STATUS_WAITING_USER_INPUT else _derive_progress_stage(response)
        job["follow_up_questions"] = follow_up_questions if status == JOB_STATUS_WAITING_USER_INPUT else []
        job["last_response"] = _deep_copy(response)
        job["artifacts"] = _deep_copy(response.get("artifacts", []))
        verdict = response.get("reproducibility_verdict", {})
        job["reproducibility_verdict"] = _deep_copy(verdict) if isinstance(verdict, dict) else {}
        job["workflow_report_path"] = _extract_workflow_report_rel_path(project_root, session_id)
        job["finished_at"] = _utc_now_iso() if status in {JOB_STATUS_COMPLETED, JOB_STATUS_FAILED} else ""
        if status in {JOB_STATUS_COMPLETED, JOB_STATUS_WAITING_USER_INPUT}:
            job["error"] = ""
        return _save_job_locked(path, job)


def _finalize_job_failure(project_root: Path, job_id: str, error: Exception) -> dict[str, Any]:
    with _FILE_LOCK:
        path, job = _load_job_locked(project_root, job_id)
        job["status"] = JOB_STATUS_FAILED
        job["progress_stage"] = "execution_error"
        job["error"] = str(error).strip() or error.__class__.__name__
        job["finished_at"] = _utc_now_iso()
        return _save_job_locked(path, job)


def run_job_worker_once(project_root: Path, job_id: str) -> None:
    project_root = project_root.resolve()
    try:
        running_job = _mark_job_running(project_root, job_id)
    except FileNotFoundError:
        return

    if str(running_job.get("status", "")).strip() == JOB_STATUS_CANCELLED:
        return

    request_payload = running_job.get("request_payload", {})
    if not isinstance(request_payload, dict):
        _finalize_job_failure(project_root, job_id, ValueError("Invalid request_payload in job"))
        return

    try:
        response = handle_openclaw_request(project_root=project_root, request=request_payload)
    except Exception as exc:
        _finalize_job_failure(project_root, job_id, exc)
        return

    _finalize_job_success(project_root, job_id, response)


def _ensure_executor(max_workers: int = DEFAULT_MAX_WORKERS) -> ThreadPoolExecutor:
    global _EXECUTOR
    with _WORKER_LOCK:
        if _EXECUTOR is None:
            workers = max(1, _coerce_int(max_workers) or DEFAULT_MAX_WORKERS)
            _EXECUTOR = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="paper-repro-web-job")
        return _EXECUTOR


def _next_queued_job_id(project_root: Path) -> str | None:
    rows = list_jobs(project_root, limit=DEFAULT_JOB_RETENTION, status_filter=JOB_STATUS_QUEUED)
    if not rows:
        return None
    rows.sort(key=lambda item: str(item.get("created_at", "")))
    return str(rows[0].get("job_id", "")).strip() or None


def run_next_queued_job_async(project_root: Path, *, max_workers: int = DEFAULT_MAX_WORKERS) -> str | None:
    project_root = project_root.resolve()
    global _ACTIVE_FUTURE, _ACTIVE_JOB_ID
    with _WORKER_LOCK:
        if _ACTIVE_FUTURE is not None:
            if _ACTIVE_FUTURE.done():
                _ACTIVE_FUTURE = None
                _ACTIVE_JOB_ID = None
            else:
                return _ACTIVE_JOB_ID

        queued_job_id = _next_queued_job_id(project_root)
        if not queued_job_id:
            return None

        executor = _ensure_executor(max_workers=max_workers)
        _ACTIVE_JOB_ID = queued_job_id
        _ACTIVE_FUTURE = executor.submit(run_job_worker_once, project_root, queued_job_id)
        return _ACTIVE_JOB_ID


def get_worker_state() -> dict[str, Any]:
    with _WORKER_LOCK:
        running = bool(_ACTIVE_FUTURE is not None and not _ACTIVE_FUTURE.done())
        return {
            "running": running,
            "active_job_id": _ACTIVE_JOB_ID if running else "",
        }

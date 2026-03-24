from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import AgentRun, ArtifactRecord, SessionState, TaskContract


class LocalRuntime:
    """Small local runtime for deterministic artifact writes and run events."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.events_path = self.project_root / "results" / "run_events.jsonl"
        self.agent_runs_path = self.project_root / "results" / "agent_runs.jsonl"
        self.artifacts_path = self.project_root / "results" / "artifacts.jsonl"

    def ensure_layout(self) -> None:
        for rel in ("papers", "shared", "results", "shared/sessions"):
            (self.project_root / rel).mkdir(parents=True, exist_ok=True)

    def emit_event(self, step: str, status: str, message: str, meta: dict[str, Any] | None = None) -> None:
        payload = {
            "timestamp_utc": datetime.now(tz=UTC).isoformat(),
            "step": step,
            "status": status,
            "message": message,
            "meta": meta or {},
        }
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def write_text(self, rel_path: str, content: str) -> str:
        path = self.project_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return rel_path

    def write_json(self, rel_path: str, payload: dict[str, Any]) -> str:
        path = self.project_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return rel_path

    def write_csv(self, rel_path: str, rows: list[dict[str, Any]], fieldnames: list[str]) -> str:
        path = self.project_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return rel_path

    def read_json(self, rel_path: str) -> dict[str, Any]:
        path = self.project_root / rel_path
        return json.loads(path.read_text(encoding="utf-8"))

    def read_csv(self, rel_path: str) -> list[dict[str, str]]:
        path = self.project_root / rel_path
        with path.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def session_dir(self, session_id: str) -> Path:
        path = self.project_root / "shared" / "sessions" / session_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_task_contract(self, rel_path: str, contract: TaskContract) -> str:
        return self.write_json(rel_path, contract.as_dict())

    def write_session_state(self, session: SessionState) -> str:
        rel_path = f"shared/sessions/{session.session_id}/session_state.json"
        return self.write_json(rel_path, session.as_dict())

    def read_session_state(self, session_id: str) -> SessionState:
        rel_path = f"shared/sessions/{session_id}/session_state.json"
        return SessionState.from_dict(self.read_json(rel_path))

    def append_agent_run(self, agent_run: AgentRun) -> None:
        self.agent_runs_path.parent.mkdir(parents=True, exist_ok=True)
        with self.agent_runs_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(agent_run.as_dict(), ensure_ascii=False) + "\n")

    def record_artifact(self, artifact: ArtifactRecord) -> None:
        self.artifacts_path.parent.mkdir(parents=True, exist_ok=True)
        with self.artifacts_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(artifact.as_dict(), ensure_ascii=False) + "\n")

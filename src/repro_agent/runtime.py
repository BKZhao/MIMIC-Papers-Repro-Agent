from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class LocalRuntime:
    """Small local runtime for deterministic artifact writes and run events."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.events_path = self.project_root / "results" / "run_events.jsonl"

    def ensure_layout(self) -> None:
        for rel in ("papers", "shared", "results"):
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


from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def default_codex_skill_bridge_manifest_path(project_root: Path) -> Path:
    return (project_root / "openclaw" / "skills" / "codex_skill_bridge.yaml").resolve()


def load_codex_skill_bridge_manifest(project_root: Path) -> dict[str, Any]:
    path = default_codex_skill_bridge_manifest_path(project_root)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        payload = {}

    category_groups = payload.get("category_groups")
    stage_bridges = payload.get("openclaw_stage_bridges")

    payload["repo_skill_root"] = str(payload.get("repo_skill_root", ".codex/skills")).strip() or ".codex/skills"
    payload["project_skill_count"] = _coerce_non_negative_int(payload.get("project_skill_count"))
    if payload["project_skill_count"] == 0:
        payload["project_skill_count"] = _count_category_skills(category_groups)
    payload["category_group_count"] = len(category_groups) if isinstance(category_groups, dict) else 0
    payload["stage_bridge_count"] = len(stage_bridges) if isinstance(stage_bridges, dict) else 0
    payload["source_path"] = str(path)
    return payload


def _count_category_skills(category_groups: Any) -> int:
    if not isinstance(category_groups, dict):
        return 0
    names: set[str] = set()
    for item in category_groups.values():
        if not isinstance(item, dict):
            continue
        skills = item.get("skills")
        if not isinstance(skills, list):
            continue
        for skill in skills:
            name = str(skill).strip()
            if name:
                names.add(name)
    return len(names)


def _coerce_non_negative_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0


__all__ = [
    "default_codex_skill_bridge_manifest_path",
    "load_codex_skill_bridge_manifest",
]

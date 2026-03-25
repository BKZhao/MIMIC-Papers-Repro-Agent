from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SkillContract:
    name: str
    title: str
    stage: str
    path: str
    execution_preference: str
    purpose: str = ""
    primary_entrypoints: tuple[str, ...] = ()
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()
    reads: tuple[str, ...] = ()
    writes: tuple[str, ...] = ()
    guardrails: tuple[str, ...] = ()
    fails_when: tuple[str, ...] = ()
    success_criteria: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "title": self.title,
            "stage": self.stage,
            "path": self.path,
            "execution_preference": self.execution_preference,
            "purpose": self.purpose,
            "primary_entrypoints": list(self.primary_entrypoints),
            "inputs": list(self.inputs),
            "outputs": list(self.outputs),
            "reads": list(self.reads),
            "writes": list(self.writes),
            "guardrails": list(self.guardrails),
            "fails_when": list(self.fails_when),
            "success_criteria": list(self.success_criteria),
        }


@dataclass(frozen=True)
class SkillContractManifest:
    agent_name: str
    version: str
    skills: dict[str, SkillContract]
    source_path: str

    def skill_count(self) -> int:
        return len(self.skills)

    def as_dict(self) -> dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "version": self.version,
            "skill_count": self.skill_count(),
            "skills": {name: contract.as_dict() for name, contract in self.skills.items()},
            "source_path": self.source_path,
        }


def default_skill_contract_manifest_path(project_root: Path) -> Path:
    return (project_root / "openclaw" / "skills" / "skills_manifest.yaml").resolve()


def load_skill_contract_manifest(project_root: Path) -> SkillContractManifest:
    path = default_skill_contract_manifest_path(project_root)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    skills_raw = payload.get("skills", {})
    skills: dict[str, SkillContract] = {}
    for key, item in skills_raw.items():
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", key)).strip() or str(key).strip()
        skills[name] = SkillContract(
            name=name,
            title=str(item.get("title", name.replace("_", " ").title())).strip(),
            stage=str(item.get("stage", "unknown")).strip(),
            path=str(item.get("path", "")).strip(),
            execution_preference=str(item.get("execution_preference", "hybrid")).strip(),
            purpose=str(item.get("purpose", "")).strip(),
            primary_entrypoints=_as_tuple(item.get("primary_entrypoints")),
            inputs=_as_tuple(item.get("inputs")),
            outputs=_as_tuple(item.get("outputs")),
            reads=_as_tuple(item.get("reads")),
            writes=_as_tuple(item.get("writes")),
            guardrails=_as_tuple(item.get("guardrails")),
            fails_when=_as_tuple(item.get("fails_when")),
            success_criteria=_as_tuple(item.get("success_criteria")),
        )
    return SkillContractManifest(
        agent_name=str(payload.get("agent_name", "paper-repro-scientist")).strip(),
        version=str(payload.get("version", "unknown")).strip(),
        skills=skills,
        source_path=str(path),
    )


def list_skill_contracts(manifest: SkillContractManifest) -> list[SkillContract]:
    return [manifest.skills[name] for name in sorted(manifest.skills)]


def get_skill_contract(manifest: SkillContractManifest, name: str) -> SkillContract | None:
    return manifest.skills.get(name)


def _as_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    text = str(value).strip()
    return (text,) if text else ()

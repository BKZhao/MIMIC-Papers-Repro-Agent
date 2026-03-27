from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SemanticVariable:
    name: str
    category: str
    dataset_field: str
    source_name: str
    aliases: tuple[str, ...]
    description: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "dataset_field": self.dataset_field,
            "source_name": self.source_name,
            "aliases": list(self.aliases),
            "description": self.description,
        }


@dataclass(frozen=True)
class SemanticRegistry:
    dataset: str
    version: str
    variables: dict[str, SemanticVariable]
    categories: dict[str, list[str]]
    source_path: str

    def variable_count(self) -> int:
        return len(self.variables)

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "version": self.version,
            "variable_count": self.variable_count(),
            "categories": {key: list(value) for key, value in self.categories.items()},
            "source_path": self.source_path,
        }


def default_mimic_semantic_registry_path(project_root: Path) -> Path:
    return (project_root / "configs" / "mimic_variable_semantics.yaml").resolve()


def load_mimic_semantic_registry(project_root: Path) -> SemanticRegistry:
    path = default_mimic_semantic_registry_path(project_root)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    categories_raw = payload.get("categories", {})
    variables: dict[str, SemanticVariable] = {}
    categories: dict[str, list[str]] = {}
    for category, entries in categories_raw.items():
        if not isinstance(entries, dict):
            continue
        ordered_names: list[str] = []
        for key, item in entries.items():
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", key)).strip() or str(key).strip()
            aliases = tuple(str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip())
            variable = SemanticVariable(
                name=name,
                category=str(category),
                dataset_field=str(item.get("dataset_field", "")).strip(),
                source_name=str(item.get("source_name", "")).strip(),
                aliases=aliases,
                description=str(item.get("description", "")).strip(),
            )
            variables[name] = variable
            ordered_names.append(name)
        categories[str(category)] = ordered_names
    version = str(payload.get("version", "unknown"))
    dataset = str(payload.get("dataset", "mimic_iv"))
    return SemanticRegistry(
        dataset=dataset,
        version=version,
        variables=variables,
        categories=categories,
        source_path=str(path),
    )


def resolve_semantic_variable(registry: SemanticRegistry, variable_name: str) -> SemanticVariable | None:
    normalized = _normalize(variable_name)
    for variable in registry.variables.values():
        if normalized == _normalize(variable.name):
            return variable
        if any(normalized == _normalize(alias) for alias in variable.aliases):
            return variable
    return None


def semantic_candidates(registry: SemanticRegistry, variable_name: str) -> list[SemanticVariable]:
    normalized = _normalize(variable_name)
    matches: list[SemanticVariable] = []
    for variable in registry.variables.values():
        if normalized in {_normalize(variable.name), *(_normalize(alias) for alias in variable.aliases)}:
            matches.append(variable)
    return matches


def _normalize(text: str) -> str:
    return "_".join(str(text).strip().lower().replace("-", " ").split())

from __future__ import annotations

from dataclasses import dataclass

from .config import PipelineConfig


@dataclass(frozen=True)
class SkillDefinition:
    name: str
    source: str
    location: str


DEFAULT_EXTERNAL_SKILLS: dict[str, str] = {
    "pyhealth": ".codex/skills/pyhealth",
    "clinicaltrials-database": ".codex/skills/clinicaltrials-database",
    "clinical-reports": ".codex/skills/clinical-reports",
    "statistical-analysis": ".codex/skills/statistical-analysis",
}

DEFAULT_LOCAL_SKILLS: dict[str, str] = {
    "task-spec-normalizer": "local:task-spec-normalizer",
    "clinical-variable-role-parser": "local:clinical-variable-role-parser",
    "mimic-cohort-builder": "local:mimic-cohort-builder",
    "mimic-variable-mapper": "local:mimic-variable-mapper",
    "clinical-survival-analysis": "local:clinical-survival-analysis",
    "table-figure-compiler": "local:table-figure-compiler",
    "paper-alignment-verifier": "local:paper-alignment-verifier",
    "git-github-update": "local:git-github-update",
}

DEFAULT_AGENT_SKILL_MAP: dict[str, list[str]] = {
    "paper_parser_agent": ["task-spec-normalizer", "clinical-reports"],
    "study_design_agent": ["task-spec-normalizer", "clinical-variable-role-parser"],
    "cohort_agent": ["mimic-cohort-builder", "mimic-variable-mapper", "pyhealth"],
    "feature_agent": ["mimic-variable-mapper", "pyhealth"],
    "stats_agent": ["clinical-survival-analysis", "statistical-analysis"],
    "figure_agent": ["table-figure-compiler", "clinical-reports"],
    "verify_agent": ["paper-alignment-verifier", "clinical-reports"],
    "report_agent": ["clinical-reports", "table-figure-compiler"],
    "git_update_agent": ["git-github-update"],
}


def build_skill_registry(config: PipelineConfig) -> dict[str, SkillDefinition]:
    registry: dict[str, SkillDefinition] = {}
    external = dict(DEFAULT_EXTERNAL_SKILLS)
    external.update(config.skill_registry.external)
    local = dict(DEFAULT_LOCAL_SKILLS)
    local.update(config.skill_registry.local)
    for name, location in external.items():
        registry[name] = SkillDefinition(name=name, source="external", location=location)
    for name, location in local.items():
        registry[name] = SkillDefinition(name=name, source="local", location=location)
    return registry


def resolve_agent_skills(config: PipelineConfig, agent_name: str) -> list[str]:
    route = config.agent_routes.get(agent_name)
    if route and route.allowed_skills:
        return list(route.allowed_skills)

    merged = dict(DEFAULT_AGENT_SKILL_MAP)
    merged.update(config.skill_registry.agent_skill_map)
    return list(merged.get(agent_name, []))

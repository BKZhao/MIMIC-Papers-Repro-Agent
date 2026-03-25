from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Any

import yaml

from .contracts import ExecutionMode, InteractionMode


@dataclass
class RunConfig:
    name: str
    paper_path: str
    doi: str
    dataset: str
    dry_run: bool = True
    execution_mode: ExecutionMode = ExecutionMode.DETERMINISTIC
    interaction_mode: InteractionMode = InteractionMode.BATCH
    task_contract_path: str = "shared/task_contract.json"
    session_id: str = ""


@dataclass
class QualityGates:
    expected_cohort_size: int
    cohort_tolerance_percent: float
    max_fail_metrics: int


@dataclass
class LLMConfig:
    provider: str = "siliconflow"
    base_url: str = "https://api.siliconflow.cn/v1"
    default_model: str = "Qwen/Qwen2.5-72B-Instruct"
    api_key_env: str = "SILICONFLOW_API_KEY"
    temperature: float = 0.0
    max_tokens: int = 4000
    timeout_seconds: int = 60
    enabled: bool = True


@dataclass
class AgentRouteConfig:
    provider: str = ""
    model: str = ""
    temperature: float | None = None
    max_tokens: int | None = None
    allowed_skills: list[str] = field(default_factory=list)
    tool_boundary: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class SkillRegistryConfig:
    external: dict[str, str] = field(default_factory=dict)
    local: dict[str, str] = field(default_factory=dict)
    agent_skill_map: dict[str, list[str]] = field(default_factory=dict)
    fallback_behavior: str = "warn"


@dataclass
class DatasetAdapterConfig:
    default_adapter: str = "mimic_iv"
    adapters: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class ArtifactPolicy:
    shared_dir: str = "shared"
    results_dir: str = "results"
    session_root: str = "shared/sessions"
    write_agent_runs: bool = True
    write_task_contract: bool = True


@dataclass
class VerificationPolicy:
    compare_targets: bool = True
    enable_alignment_diagnostics: bool = True
    stop_on_fail: bool = True
    max_missing_metrics: int = 0


@dataclass
class PipelineConfig:
    run: RunConfig
    quality_gates: QualityGates
    targets: list[dict[str, Any]]
    agents: dict[str, dict[str, Any]]
    llm: LLMConfig = field(default_factory=LLMConfig)
    agent_routes: dict[str, AgentRouteConfig] = field(default_factory=dict)
    skill_registry: SkillRegistryConfig = field(default_factory=SkillRegistryConfig)
    dataset_adapters: DatasetAdapterConfig = field(default_factory=DatasetAdapterConfig)
    artifact_policy: ArtifactPolicy = field(default_factory=ArtifactPolicy)
    verification_policy: VerificationPolicy = field(default_factory=VerificationPolicy)


def _as_dict(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _as_list_of_str(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if raw is None:
        return []
    text = str(raw).strip()
    return [text] if text else []


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip())
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _resolve_llm_api_key_env(configured_env_name: str) -> str:
    explicit_override = os.getenv("LLM_API_KEY_ENV", "").strip()
    if explicit_override:
        return explicit_override
    configured_env_name = configured_env_name.strip() or "SILICONFLOW_API_KEY"
    if os.getenv(configured_env_name, "").strip():
        return configured_env_name
    if os.getenv("OPENAI_API_KEY", "").strip():
        return "OPENAI_API_KEY"
    return configured_env_name


def _parse_execution_mode(raw: Any) -> ExecutionMode:
    try:
        return ExecutionMode(str(raw or ExecutionMode.DETERMINISTIC.value))
    except ValueError:
        return ExecutionMode.DETERMINISTIC


def _parse_interaction_mode(raw: Any) -> InteractionMode:
    try:
        return InteractionMode(str(raw or InteractionMode.BATCH.value))
    except ValueError:
        return InteractionMode.BATCH


def load_pipeline_config(path: Path) -> PipelineConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    run_raw = _as_dict(raw.get("run"))
    gates_raw = _as_dict(raw.get("quality_gates"))
    llm_raw = _as_dict(raw.get("llm"))
    routes_raw = _as_dict(raw.get("agent_routes"))
    skills_raw = _as_dict(raw.get("skill_registry"))
    adapters_raw = _as_dict(raw.get("dataset_adapters"))
    artifact_raw = _as_dict(raw.get("artifact_policy"))
    verify_raw = _as_dict(raw.get("verification_policy"))

    run = RunConfig(
        name=str(run_raw.get("name", "paper-repro-run")),
        paper_path=str(run_raw.get("paper_path", "papers/paper.md")),
        doi=str(run_raw.get("doi", "unknown")),
        dataset=str(run_raw.get("dataset", "unknown")),
        dry_run=bool(run_raw.get("dry_run", True)),
        execution_mode=_parse_execution_mode(run_raw.get("execution_mode")),
        interaction_mode=_parse_interaction_mode(run_raw.get("interaction_mode")),
        task_contract_path=str(run_raw.get("task_contract_path", "shared/task_contract.json")),
        session_id=str(run_raw.get("session_id", "")),
    )
    gates = QualityGates(
        expected_cohort_size=int(gates_raw.get("expected_cohort_size", 0)),
        cohort_tolerance_percent=float(gates_raw.get("cohort_tolerance_percent", 5)),
        max_fail_metrics=int(gates_raw.get("max_fail_metrics", 0)),
    )
    llm = LLMConfig(
        provider=_env_str("LLM_PROVIDER", str(llm_raw.get("provider", "siliconflow"))),
        base_url=_env_str("LLM_BASE_URL", str(llm_raw.get("base_url", "https://api.siliconflow.cn/v1"))),
        default_model=_env_str("LLM_DEFAULT_MODEL", str(llm_raw.get("default_model", "Qwen/Qwen2.5-72B-Instruct"))),
        api_key_env=_resolve_llm_api_key_env(str(llm_raw.get("api_key_env", "SILICONFLOW_API_KEY"))),
        temperature=_env_float("LLM_TEMPERATURE", float(llm_raw.get("temperature", 0.0))),
        max_tokens=_env_int("LLM_MAX_TOKENS", int(llm_raw.get("max_tokens", 4000))),
        timeout_seconds=_env_int("LLM_TIMEOUT_SECONDS", int(llm_raw.get("timeout_seconds", 60))),
        enabled=_env_bool("LLM_ENABLED", bool(llm_raw.get("enabled", True))),
    )

    targets = raw.get("targets", [])
    if not isinstance(targets, list):
        targets = []

    agents = _as_dict(raw.get("agents"))
    agent_routes = {
        str(name): AgentRouteConfig(
            provider=str(_as_dict(cfg).get("provider", "")),
            model=str(_as_dict(cfg).get("model", "")),
            temperature=float(_as_dict(cfg)["temperature"]) if "temperature" in _as_dict(cfg) else None,
            max_tokens=int(_as_dict(cfg)["max_tokens"]) if "max_tokens" in _as_dict(cfg) else None,
            allowed_skills=_as_list_of_str(_as_dict(cfg).get("allowed_skills")),
            tool_boundary=_as_list_of_str(_as_dict(cfg).get("tool_boundary")),
            enabled=bool(_as_dict(cfg).get("enabled", True)),
        )
        for name, cfg in routes_raw.items()
    }
    skill_registry = SkillRegistryConfig(
        external={str(k): str(v) for k, v in _as_dict(skills_raw.get("external")).items()},
        local={str(k): str(v) for k, v in _as_dict(skills_raw.get("local")).items()},
        agent_skill_map={
            str(k): _as_list_of_str(v)
            for k, v in _as_dict(skills_raw.get("agent_skill_map")).items()
        },
        fallback_behavior=str(skills_raw.get("fallback_behavior", "warn")),
    )
    dataset_adapters = DatasetAdapterConfig(
        default_adapter=str(adapters_raw.get("default_adapter", "mimic_iv")),
        adapters={str(k): _as_dict(v) for k, v in _as_dict(adapters_raw.get("adapters")).items()},
    )
    artifact_policy = ArtifactPolicy(
        shared_dir=str(artifact_raw.get("shared_dir", "shared")),
        results_dir=str(artifact_raw.get("results_dir", "results")),
        session_root=str(artifact_raw.get("session_root", "shared/sessions")),
        write_agent_runs=bool(artifact_raw.get("write_agent_runs", True)),
        write_task_contract=bool(artifact_raw.get("write_task_contract", True)),
    )
    verification_policy = VerificationPolicy(
        compare_targets=bool(verify_raw.get("compare_targets", True)),
        enable_alignment_diagnostics=bool(verify_raw.get("enable_alignment_diagnostics", True)),
        stop_on_fail=bool(verify_raw.get("stop_on_fail", True)),
        max_missing_metrics=int(verify_raw.get("max_missing_metrics", 0)),
    )

    return PipelineConfig(
        run=run,
        quality_gates=gates,
        targets=targets,
        agents=agents,
        llm=llm,
        agent_routes=agent_routes,
        skill_registry=skill_registry,
        dataset_adapters=dataset_adapters,
        artifact_policy=artifact_policy,
        verification_policy=verification_policy,
    )

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class StepStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    BLOCKED = "blocked"
    SKIPPED = "skipped"


class ExecutionMode(str, Enum):
    DETERMINISTIC = "deterministic"
    AGENTIC = "agentic"


class InteractionMode(str, Enum):
    BATCH = "batch"
    CHAT = "chat"


class VariableRole(str, Enum):
    EXPOSURE = "exposure"
    OUTCOME = "outcome"
    CONTROL = "control"
    SUBGROUP = "subgroup"
    TIME = "time"
    ID = "id"
    DERIVED = "derived"


@dataclass
class AgentStep:
    name: str
    enabled: bool = True
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ArtifactRecord:
    name: str
    rel_path: str
    artifact_type: str
    producer: str
    required: bool = True
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DatasetSpec:
    name: str = "unknown"
    adapter: str = "mimic_iv"
    source_type: str = "postgres"
    connector_env_prefix: str = "MIMIC_PG"
    version: str = "unknown"
    schemas: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> DatasetSpec:
        payload = data or {}
        return cls(
            name=str(payload.get("name", "unknown")),
            adapter=str(payload.get("adapter", "mimic_iv")),
            source_type=str(payload.get("source_type", "postgres")),
            connector_env_prefix=str(payload.get("connector_env_prefix", "MIMIC_PG")),
            version=str(payload.get("version", "unknown")),
            schemas=_as_str_list(payload.get("schemas")),
            meta=_as_dict(payload.get("meta")),
        )


@dataclass
class CohortSpec:
    population: str = ""
    inclusion_criteria: list[str] = field(default_factory=list)
    exclusion_criteria: list[str] = field(default_factory=list)
    diagnosis_logic: str = ""
    screening_steps: list[str] = field(default_factory=list)
    first_stay_only: bool | None = None
    min_age: int | None = None
    max_age: int | None = None
    min_icu_los_hours: int | None = None
    max_admit_to_icu_hours: int | None = None
    required_measurements: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> CohortSpec:
        payload = data or {}
        return cls(
            population=str(payload.get("population", "")),
            inclusion_criteria=_as_str_list(payload.get("inclusion_criteria")),
            exclusion_criteria=_as_str_list(payload.get("exclusion_criteria")),
            diagnosis_logic=str(payload.get("diagnosis_logic", "")),
            screening_steps=_as_str_list(payload.get("screening_steps")),
            first_stay_only=_as_optional_bool(payload.get("first_stay_only")),
            min_age=_as_optional_int(payload.get("min_age")),
            max_age=_as_optional_int(payload.get("max_age")),
            min_icu_los_hours=_as_optional_int(payload.get("min_icu_los_hours")),
            max_admit_to_icu_hours=_as_optional_int(payload.get("max_admit_to_icu_hours")),
            required_measurements=_as_str_list(payload.get("required_measurements")),
            meta=_as_dict(payload.get("meta")),
        )


@dataclass
class VariableSpec:
    name: str
    role: VariableRole
    label: str = ""
    description: str = ""
    dataset_field: str = ""
    source_name: str = ""
    transform: str = ""
    formula: str = ""
    unit: str = ""
    required: bool = True
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["role"] = self.role.value
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VariableSpec:
        payload = data or {}
        return cls(
            name=str(payload.get("name", "")).strip(),
            role=_parse_variable_role(payload.get("role")),
            label=str(payload.get("label", "")),
            description=str(payload.get("description", "")),
            dataset_field=str(payload.get("dataset_field", "")),
            source_name=str(payload.get("source_name", "")),
            transform=str(payload.get("transform", "")),
            formula=str(payload.get("formula", "")),
            unit=str(payload.get("unit", "")),
            required=bool(payload.get("required", True)),
            meta=_as_dict(payload.get("meta")),
        )


@dataclass
class ModelSpec:
    name: str
    family: str
    exposure_variables: list[str] = field(default_factory=list)
    outcome_variables: list[str] = field(default_factory=list)
    control_variables: list[str] = field(default_factory=list)
    subgroup_variables: list[str] = field(default_factory=list)
    time_variable: str = ""
    description: str = ""
    options: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ModelSpec:
        payload = data or {}
        return cls(
            name=str(payload.get("name", "")).strip(),
            family=str(payload.get("family", "")).strip(),
            exposure_variables=_as_str_list(payload.get("exposure_variables")),
            outcome_variables=_as_str_list(payload.get("outcome_variables")),
            control_variables=_as_str_list(payload.get("control_variables")),
            subgroup_variables=_as_str_list(payload.get("subgroup_variables")),
            time_variable=str(payload.get("time_variable", "")),
            description=str(payload.get("description", "")),
            options=_as_dict(payload.get("options")),
        )


@dataclass
class OutputSpec:
    name: str
    kind: str
    fmt: str = "json"
    description: str = ""
    required: bool = True
    model_refs: list[str] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["format"] = payload.pop("fmt")
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OutputSpec:
        payload = data or {}
        return cls(
            name=str(payload.get("name", "")).strip(),
            kind=str(payload.get("kind", "")).strip(),
            fmt=str(payload.get("format", payload.get("fmt", "json"))).strip(),
            description=str(payload.get("description", "")),
            required=bool(payload.get("required", True)),
            model_refs=_as_str_list(payload.get("model_refs")),
            options=_as_dict(payload.get("options")),
        )


@dataclass
class AgentRun:
    agent_name: str
    status: StepStatus
    message: str
    provider: str = ""
    model: str = ""
    selected_skills: list[str] = field(default_factory=list)
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status.value
        return payload


@dataclass
class SessionState:
    session_id: str
    task_id: str
    paper_path: str = ""
    instructions: str = ""
    status: str = "draft"
    messages: list[dict[str, str]] = field(default_factory=list)
    task_contract_path: str = ""
    artifact_records: list[ArtifactRecord] = field(default_factory=list)
    agent_runs: list[AgentRun] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "task_id": self.task_id,
            "paper_path": self.paper_path,
            "instructions": self.instructions,
            "status": self.status,
            "messages": list(self.messages),
            "task_contract_path": self.task_contract_path,
            "artifact_records": [artifact.as_dict() for artifact in self.artifact_records],
            "agent_runs": [agent_run.as_dict() for agent_run in self.agent_runs],
            "meta": dict(self.meta),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> SessionState:
        payload = data or {}
        return cls(
            session_id=str(payload.get("session_id", "")),
            task_id=str(payload.get("task_id", "")),
            paper_path=str(payload.get("paper_path", "")),
            instructions=str(payload.get("instructions", "")),
            status=str(payload.get("status", "draft")),
            messages=[dict(item) for item in payload.get("messages", []) if isinstance(item, dict)],
            task_contract_path=str(payload.get("task_contract_path", "")),
            artifact_records=[
                ArtifactRecord(**item) for item in payload.get("artifact_records", []) if isinstance(item, dict)
            ],
            agent_runs=[
                AgentRun(
                    agent_name=str(item.get("agent_name", "")),
                    status=StepStatus(str(item.get("status", StepStatus.SKIPPED.value))),
                    message=str(item.get("message", "")),
                    provider=str(item.get("provider", "")),
                    model=str(item.get("model", "")),
                    selected_skills=_as_str_list(item.get("selected_skills")),
                    inputs=_as_str_list(item.get("inputs")),
                    outputs=_as_str_list(item.get("outputs")),
                    meta=_as_dict(item.get("meta")),
                )
                for item in payload.get("agent_runs", [])
                if isinstance(item, dict)
            ],
            meta=_as_dict(payload.get("meta")),
        )


@dataclass
class TaskContract:
    task_id: str
    title: str
    execution_mode: ExecutionMode = ExecutionMode.AGENTIC
    interaction_mode: InteractionMode = InteractionMode.CHAT
    source_paper_path: str = ""
    instructions: str = ""
    dataset: DatasetSpec = field(default_factory=DatasetSpec)
    cohort: CohortSpec = field(default_factory=CohortSpec)
    variables: list[VariableSpec] = field(default_factory=list)
    models: list[ModelSpec] = field(default_factory=list)
    outputs: list[OutputSpec] = field(default_factory=list)
    verification_targets: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "execution_mode": self.execution_mode.value,
            "interaction_mode": self.interaction_mode.value,
            "source_paper_path": self.source_paper_path,
            "instructions": self.instructions,
            "dataset": self.dataset.as_dict(),
            "cohort": self.cohort.as_dict(),
            "variables": [item.as_dict() for item in self.variables],
            "models": [item.as_dict() for item in self.models],
            "outputs": [item.as_dict() for item in self.outputs],
            "verification_targets": list(self.verification_targets),
            "notes": list(self.notes),
            "meta": dict(self.meta),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> TaskContract:
        payload = data or {}
        return cls(
            task_id=str(payload.get("task_id", "task-unknown")),
            title=str(payload.get("title", "Untitled task")),
            execution_mode=ExecutionMode(str(payload.get("execution_mode", ExecutionMode.AGENTIC.value))),
            interaction_mode=InteractionMode(str(payload.get("interaction_mode", InteractionMode.CHAT.value))),
            source_paper_path=str(payload.get("source_paper_path", "")),
            instructions=str(payload.get("instructions", "")),
            dataset=DatasetSpec.from_dict(_as_dict(payload.get("dataset"))),
            cohort=CohortSpec.from_dict(_as_dict(payload.get("cohort"))),
            variables=[VariableSpec.from_dict(item) for item in payload.get("variables", []) if isinstance(item, dict)],
            models=[ModelSpec.from_dict(item) for item in payload.get("models", []) if isinstance(item, dict)],
            outputs=[OutputSpec.from_dict(item) for item in payload.get("outputs", []) if isinstance(item, dict)],
            verification_targets=[
                dict(item) for item in payload.get("verification_targets", []) if isinstance(item, dict)
            ],
            notes=_as_str_list(payload.get("notes")),
            meta=_as_dict(payload.get("meta")),
        )


@dataclass
class StepResult:
    step: str
    status: StepStatus
    message: str
    outputs: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "status": self.status.value,
            "message": self.message,
            "outputs": self.outputs,
            "meta": self.meta,
        }


@dataclass
class RunSummary:
    run_id: str
    status: StepStatus
    step_results: list[StepResult]

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "steps": [step.as_dict() for step in self.step_results],
        }


def _as_dict(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _as_str_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if raw is None:
        return []
    text = str(raw).strip()
    return [text] if text else []


def _as_optional_bool(raw: Any) -> bool | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _as_optional_int(raw: Any) -> int | None:
    try:
        if raw is None or raw == "":
            return None
        return int(raw)
    except (TypeError, ValueError):
        return None


def _parse_variable_role(raw: Any) -> VariableRole:
    text = str(raw or VariableRole.DERIVED.value).strip().lower()
    try:
        return VariableRole(text)
    except ValueError:
        return VariableRole.DERIVED

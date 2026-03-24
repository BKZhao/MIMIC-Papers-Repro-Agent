from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class StepStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    BLOCKED = "blocked"
    SKIPPED = "skipped"


@dataclass
class AgentStep:
    name: str
    enabled: bool = True
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)


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


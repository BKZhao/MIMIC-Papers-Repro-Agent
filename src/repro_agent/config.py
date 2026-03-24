from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class RunConfig:
    name: str
    paper_path: str
    doi: str
    dataset: str
    dry_run: bool = True


@dataclass
class QualityGates:
    expected_cohort_size: int
    cohort_tolerance_percent: float
    max_fail_metrics: int


@dataclass
class PipelineConfig:
    run: RunConfig
    quality_gates: QualityGates
    targets: list[dict[str, Any]]
    agents: dict[str, dict[str, Any]]


def _as_dict(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def load_pipeline_config(path: Path) -> PipelineConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    run_raw = _as_dict(raw.get("run"))
    gates_raw = _as_dict(raw.get("quality_gates"))

    run = RunConfig(
        name=str(run_raw.get("name", "paper-repro-run")),
        paper_path=str(run_raw.get("paper_path", "papers/paper.md")),
        doi=str(run_raw.get("doi", "unknown")),
        dataset=str(run_raw.get("dataset", "unknown")),
        dry_run=bool(run_raw.get("dry_run", True)),
    )
    gates = QualityGates(
        expected_cohort_size=int(gates_raw.get("expected_cohort_size", 0)),
        cohort_tolerance_percent=float(gates_raw.get("cohort_tolerance_percent", 5)),
        max_fail_metrics=int(gates_raw.get("max_fail_metrics", 0)),
    )

    targets = raw.get("targets", [])
    if not isinstance(targets, list):
        targets = []

    agents = _as_dict(raw.get("agents"))
    return PipelineConfig(run=run, quality_gates=gates, targets=targets, agents=agents)


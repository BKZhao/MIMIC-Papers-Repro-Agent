from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .paper_contract import build_paper_alignment_contract


VerificationTargetsBuilder = Callable[[Path | None], list[dict[str, Any]]]


@dataclass(frozen=True)
class PaperPreset:
    key: str
    title: str
    description: str
    dataset_adapter: str
    execution_backend: str
    doi: str = "unknown"
    default_expected_cohort_size: int = 0
    supported_domains: tuple[str, ...] = ()
    required_terms: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    verification_targets_builder: VerificationTargetsBuilder | None = None

    def matches(self, haystack: str) -> bool:
        lowered = haystack.lower()
        return all(term.lower() in lowered for term in self.required_terms)

    def verification_targets(self, project_root: Path | None = None) -> list[dict[str, Any]]:
        if self.verification_targets_builder is None:
            return []
        return [dict(item) for item in self.verification_targets_builder(project_root)]

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "dataset_adapter": self.dataset_adapter,
            "execution_backend": self.execution_backend,
            "doi": self.doi,
            "default_expected_cohort_size": self.default_expected_cohort_size,
            "supported_domains": list(self.supported_domains),
            "required_terms": list(self.required_terms),
            "notes": list(self.notes),
            "verification_target_count": len(self.verification_targets()),
        }


def _mimic_tyg_sepsis_targets(project_root: Path | None = None) -> list[dict[str, Any]]:
    contract = build_paper_alignment_contract(project_root=project_root)
    return [dict(item) for item in contract.get("metric_targets", [])]


MIMIC_TYG_SEPSIS_PRESET = PaperPreset(
    key="mimic_tyg_sepsis",
    title="MIMIC-IV TyG Sepsis Reproduction",
    description=(
        "Preset for the MIMIC-IV TyG index and sepsis mortality paper with deterministic "
        "cohort extraction, analysis dataset expansion, survival analysis, and paper-alignment verification."
    ),
    dataset_adapter="mimic_iv",
    execution_backend="deterministic_bridge",
    doi="10.1038/s41598-024-75050-8",
    default_expected_cohort_size=1742,
    supported_domains=("mimic", "clinical_observational", "survival_analysis"),
    required_terms=("mimic", "tyg", "sepsis"),
    notes=(
        "This is the first fully executable preset in the repository.",
        "Preset detection is intentionally conservative to avoid false positives.",
    ),
    verification_targets_builder=_mimic_tyg_sepsis_targets,
)


BUILTIN_PRESETS: tuple[PaperPreset, ...] = (MIMIC_TYG_SEPSIS_PRESET,)


def list_builtin_presets() -> list[PaperPreset]:
    return list(BUILTIN_PRESETS)


def get_paper_preset(key: str | None) -> PaperPreset | None:
    normalized = (key or "").strip().lower()
    if not normalized:
        return None
    for preset in BUILTIN_PRESETS:
        if preset.key == normalized:
            return preset
    return None


def detect_paper_preset(
    *,
    dataset_label: str,
    instructions: str,
    materials: dict[str, str],
) -> PaperPreset | None:
    haystack = "\n".join(
        [
            dataset_label,
            instructions,
            *materials.values(),
        ]
    ).lower()
    for preset in BUILTIN_PRESETS:
        if preset.matches(haystack):
            return preset
    return None


def preset_verification_targets(
    preset_key: str | None,
    project_root: Path | None = None,
) -> list[dict[str, Any]]:
    preset = get_paper_preset(preset_key)
    if preset is None:
        return []
    return preset.verification_targets(project_root)


def preset_execution_backend(preset_key: str | None) -> str:
    preset = get_paper_preset(preset_key)
    if preset is None:
        return "spec_only"
    return preset.execution_backend

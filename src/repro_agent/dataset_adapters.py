from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .contracts import TaskContract, VariableRole


@dataclass(frozen=True)
class AdapterSupport:
    adapter_name: str
    planning_supported: bool = True
    execution_supported: bool = False
    execution_backend: str = "spec_only"
    notes: list[str] = field(default_factory=list)
    missing_capabilities: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "adapter_name": self.adapter_name,
            "planning_supported": self.planning_supported,
            "execution_supported": self.execution_supported,
            "execution_backend": self.execution_backend,
            "notes": list(self.notes),
            "missing_capabilities": list(self.missing_capabilities),
        }


class DatasetAdapter:
    name = "generic"

    def describe_contract(self, contract: TaskContract) -> AdapterSupport:
        return AdapterSupport(
            adapter_name=self.name,
            planning_supported=True,
            execution_supported=False,
            execution_backend="spec_only",
            notes=[
                "Generic task planning is available.",
                "Executable dataset-specific extraction is not implemented for this adapter yet.",
            ],
            missing_capabilities=[
                "dataset-specific cohort SQL compilation",
                "variable-to-field mapping",
                "model-ready feature extraction",
            ],
        )

    def compile_cohort_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        cohort = contract.cohort
        return {
            "dataset": contract.dataset.as_dict(),
            "population": cohort.population,
            "diagnosis_logic": cohort.diagnosis_logic,
            "screening_steps": list(cohort.screening_steps),
            "inclusion_criteria": list(cohort.inclusion_criteria),
            "exclusion_criteria": list(cohort.exclusion_criteria),
            "first_stay_only": cohort.first_stay_only,
            "min_age": cohort.min_age,
            "max_age": cohort.max_age,
            "min_icu_los_hours": cohort.min_icu_los_hours,
            "max_admit_to_icu_hours": cohort.max_admit_to_icu_hours,
            "required_measurements": list(cohort.required_measurements),
            "meta": dict(cohort.meta),
        }

    def compile_feature_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        by_role: dict[str, list[dict[str, Any]]] = {}
        for role in VariableRole:
            by_role[role.value] = [
                item.as_dict()
                for item in contract.variables
                if item.role == role
            ]
        return {
            "dataset": contract.dataset.as_dict(),
            "variables_by_role": by_role,
            "variable_count": len(contract.variables),
        }

    def compile_model_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        return {
            "dataset": contract.dataset.as_dict(),
            "models": [item.as_dict() for item in contract.models],
            "requested_outputs": [item.as_dict() for item in contract.outputs],
            "verification_targets": list(contract.verification_targets),
        }


class MimicIVAdapter(DatasetAdapter):
    name = "mimic_iv"

    def describe_contract(self, contract: TaskContract) -> AdapterSupport:
        notes = [
            "MIMIC-IV planning is supported through a generic task contract.",
            "The adapter can describe ICU/hospital time semantics, first-stay logic, and measurement requirements.",
        ]
        missing: list[str] = []
        execution_supported = False
        execution_backend = "spec_only"

        if contract.meta.get("preset") == "mimic_tyg_sepsis":
            execution_supported = True
            execution_backend = "deterministic_bridge"
            notes.append("This contract matches the built-in TyG sepsis preset and can run through the deterministic bridge.")
        else:
            missing.extend(
                [
                    "generic SQL compiler from CohortSpec to executable MIMIC SQL",
                    "generic variable mapper from VariableSpec to MIMIC fields",
                    "generic model-to-feature extraction for arbitrary MIMIC studies",
                ]
            )
            notes.append("Non-preset MIMIC studies can already be planned and persisted, but full execution still needs the generic SQL/feature compiler.")

        return AdapterSupport(
            adapter_name=self.name,
            planning_supported=True,
            execution_supported=execution_supported,
            execution_backend=execution_backend,
            notes=notes,
            missing_capabilities=missing,
        )

    def compile_cohort_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        payload = super().compile_cohort_blueprint(contract)
        payload["dataset_semantics"] = {
            "hospital_time_anchor": "mimiciv_hosp.admissions.admittime/dischtime/deathtime",
            "icu_time_anchor": "mimiciv_icu.icustays.intime/outtime",
            "first_stay_logic": "subject-level or stay-level first ICU stay depending on contract",
            "derived_schema_candidates": ["mimiciv_derived"],
        }
        return payload

    def compile_feature_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        payload = super().compile_feature_blueprint(contract)
        payload["mapping_notes"] = [
            "Prefer explicit dataset_field mappings when present.",
            "Otherwise resolve through local variable mapping rules for MIMIC-IV.",
            "Derived variables should preserve source_name and formula when available.",
        ]
        return payload

    def compile_model_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        payload = super().compile_model_blueprint(contract)
        payload["supported_model_families"] = [
            "baseline_table",
            "logistic_regression",
            "cox_regression",
            "kaplan_meier",
            "logrank_test",
            "restricted_cubic_spline",
            "subgroup_analysis",
            "interaction_analysis",
        ]
        return payload


def get_dataset_adapter(adapter_name: str) -> DatasetAdapter:
    normalized = (adapter_name or "").strip().lower()
    if normalized in {"mimic", "mimic_iv", "mimic-iv"}:
        return MimicIVAdapter()
    return DatasetAdapter()

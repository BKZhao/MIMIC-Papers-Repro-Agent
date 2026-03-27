from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .analysis.router import resolve_clinical_analysis_route
from .contracts import TaskContract, VariableRole
from .paper.presets import get_paper_preset


@dataclass(frozen=True)
class AdapterSupport:
    adapter_name: str
    planning_supported: bool = True
    execution_supported: bool = False
    execution_backend: str = "spec_only"
    notes: list[str] = field(default_factory=list)
    missing_capabilities: list[str] = field(default_factory=list)
    paper_target_dataset_version: str = ""
    execution_environment_dataset_version: str = ""
    configured_dataset_version: str = ""
    execution_year_window: str = ""
    dataset_version_mismatch: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "adapter_name": self.adapter_name,
            "planning_supported": self.planning_supported,
            "execution_supported": self.execution_supported,
            "execution_backend": self.execution_backend,
            "notes": list(self.notes),
            "missing_capabilities": list(self.missing_capabilities),
            "paper_target_dataset_version": self.paper_target_dataset_version,
            "execution_environment_dataset_version": self.execution_environment_dataset_version,
            "configured_dataset_version": self.configured_dataset_version,
            "execution_year_window": self.execution_year_window,
            "dataset_version_mismatch": self.dataset_version_mismatch,
        }


class DatasetAdapter:
    name = "generic"

    def describe_contract(self, contract: TaskContract) -> AdapterSupport:
        target_version = str(contract.meta.get("paper_target_dataset_version", "")).strip()
        execution_environment_version = str(
            contract.meta.get("execution_environment_dataset_version", contract.meta.get("configured_dataset_version", contract.dataset.version))
        ).strip()
        execution_year_window = str(contract.meta.get("execution_year_window", "")).strip()
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
            paper_target_dataset_version=target_version,
            execution_environment_dataset_version=execution_environment_version,
            configured_dataset_version=execution_environment_version,
            execution_year_window=execution_year_window,
            dataset_version_mismatch=bool(contract.meta.get("dataset_version_mismatch", False)),
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
        analysis_route = resolve_clinical_analysis_route(contract)
        target_version = str(contract.meta.get("paper_target_dataset_version", "")).strip()
        execution_environment_version = str(
            contract.meta.get("execution_environment_dataset_version", contract.meta.get("configured_dataset_version", contract.dataset.version))
        ).strip()
        execution_year_window = str(contract.meta.get("execution_year_window", "")).strip()
        experimental_profile = str(contract.meta.get("experimental_profile", "")).strip()
        analysis_dataset_rel = str(contract.meta.get("analysis_dataset_rel", "")).strip()
        auto_binary_profile = str(contract.meta.get("auto_binary_profile", "")).strip().lower()
        requires_trajectory_modeling = bool(contract.meta.get("requires_longitudinal_trajectory_modeling"))
        notes = [
            "MIMIC-IV planning is supported through a generic task contract.",
            "The adapter can describe ICU/hospital time semantics, first-stay logic, and measurement requirements.",
        ]
        missing: list[str] = []
        execution_supported = False
        execution_backend = "spec_only"
        preset = get_paper_preset(contract.meta.get("preset"))

        if preset is not None:
            execution_supported = True
            execution_backend = preset.execution_backend
            notes.append(
                f"This contract matches the built-in preset '{preset.title}' and can run through the deterministic bridge."
            )
        elif experimental_profile == "mimic_tyg_stroke_nondiabetic":
            execution_supported = True
            execution_backend = "profile_survival_bridge"
            notes.extend(
                [
                    "This contract is routed into the experimental non-diabetic ischemic stroke TyG execution profile.",
                    "The engine can build a stroke-specific MIMIC-IV cohort, derive the paper-aligned TyG quartiles, and run deterministic Cox, Kaplan-Meier, restricted cubic spline, and subgroup analyses across the reported mortality horizons.",
                    "The execution route stays paper-driven at the control plane while keeping numeric results, SQL extraction, and figure generation in deterministic local tooling.",
                ]
            )
            missing.extend(
                [
                    "fasting laboratory semantics not guaranteed",
                    "paper-identical MICE and PSM sensitivity analysis not implemented",
                    "IV-tPA and mechanical thrombectomy flags are code-title approximations",
                ]
            )
        elif experimental_profile == "mimic_hr_trajectory_sepsis" and requires_trajectory_modeling:
            execution_supported = True
            execution_backend = "trajectory_python_bridge"
            notes.extend(
                [
                    "This contract is routed into the experimental heart-rate trajectory execution profile.",
                    "The engine can extract the repeated hourly heart-rate panel, derive trajectory classes with the local Python backend, and bridge those labels into KM and Cox outputs.",
                    "The execution posture is method-aligned rather than paper-identical, and all outputs must preserve that fidelity boundary explicitly.",
                ]
            )
            missing.extend(
                [
                    "paper-identical LGMM not implemented",
                    "raw-event fidelity not guaranteed",
                    "missing-data handling not paper-identical",
                ]
            )
        elif _can_route_to_hybrid_binary_runner(
            contract=contract,
            analysis_route=analysis_route,
            analysis_dataset_rel=analysis_dataset_rel,
            auto_binary_profile=auto_binary_profile,
        ):
            execution_supported = True
            execution_backend = "hybrid_binary_runner"
            notes.extend(
                [
                    "This contract can run through the hybrid binary-outcome execution path.",
                    "The local runner can execute baseline summaries, logistic regression tables, ROC summaries, calibration curves, decision curves, and selected paper-guided visualization families for binary-outcome studies.",
                    "For ARF-style Cox/nomogram prediction papers, this route executes a binary-outcome approximation with an explicit method-gap note in the report.",
                ]
            )
            if analysis_dataset_rel:
                notes.append(
                    "The analysis dataset was supplied explicitly via contract meta.analysis_dataset_rel."
                )
            elif auto_binary_profile == "mimic_arf_nomogram_v1":
                notes.append(
                    "No precomputed analysis dataset was supplied; the runner will auto-materialize the ARF binary dataset before stats execution."
                )
        else:
            missing.extend(
                [
                    "generic SQL compiler from CohortSpec to executable MIMIC SQL",
                    "generic variable mapper from VariableSpec to MIMIC fields",
                    "generic model-to-feature extraction for arbitrary MIMIC studies",
                ]
            )
            notes.append("Non-preset MIMIC studies can already be planned and persisted, but full execution still needs the generic SQL/feature compiler.")
        if requires_trajectory_modeling:
            notes.extend(
                [
                    "This paper requires longitudinal trajectory modeling rather than only single-baseline predictors.",
                    "The paper-required clustering method is LGMM, while the local engine uses a Python-only trajectory mixture route.",
                    "The output contract must keep the distinction between executable artifacts and paper-identical fidelity explicit.",
                ]
            )
        study_template = str(contract.meta.get("study_template_title") or contract.meta.get("study_template") or "").strip()
        if study_template:
            notes.append(f"Inferred study template: {study_template}.")
        mapped_count = int(contract.meta.get("semantic_mapped_variable_count", 0) or 0)
        if mapped_count > 0:
            notes.append(f"Semantic registry matched {mapped_count} variables in the current contract.")
        if target_version:
            notes.append(f"Paper original dataset version: {target_version}.")
        if execution_environment_version:
            notes.append(f"Execution environment dataset version: {execution_environment_version}.")
        if execution_year_window:
            notes.append(f"Execution year window: {execution_year_window}.")
        if contract.meta.get("dataset_version_mismatch"):
            notes.append(
                "Version mismatch detected: "
                + f"paper original dataset version is {target_version or 'unknown'} "
                + f"while the execution environment dataset version is {execution_environment_version or 'unknown'}."
            )

        return AdapterSupport(
            adapter_name=self.name,
            planning_supported=True,
            execution_supported=execution_supported,
            execution_backend=execution_backend,
            notes=notes,
            missing_capabilities=missing,
            paper_target_dataset_version=target_version,
            execution_environment_dataset_version=execution_environment_version,
            configured_dataset_version=execution_environment_version,
            execution_year_window=execution_year_window,
            dataset_version_mismatch=bool(contract.meta.get("dataset_version_mismatch", False)),
        )

    def compile_cohort_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        payload = super().compile_cohort_blueprint(contract)
        payload["dataset_semantics"] = {
            "hospital_time_anchor": "mimiciv_hosp.admissions.admittime/dischtime/deathtime",
            "icu_time_anchor": "mimiciv_icu.icustays.intime/outtime",
            "first_stay_logic": "subject-level or stay-level first ICU stay depending on contract",
            "derived_schema_candidates": ["mimiciv_derived"],
        }
        payload["preset"] = {
            "key": contract.meta.get("preset", ""),
            "title": contract.meta.get("preset_title", ""),
            "execution_backend": contract.meta.get("execution_backend", "spec_only"),
        }
        return payload

    def compile_feature_blueprint(self, contract: TaskContract) -> dict[str, Any]:
        payload = super().compile_feature_blueprint(contract)
        payload["mapping_notes"] = [
            "Prefer explicit dataset_field mappings when present.",
            "Otherwise resolve through local variable mapping rules for MIMIC-IV.",
            "Derived variables should preserve source_name and formula when available.",
        ]
        payload["semantic_mapping_summary"] = {
            "mapped_variable_count": len([item for item in contract.variables if item.dataset_field]),
            "unmapped_variables": [
                item.name
                for item in contract.variables
                if not item.dataset_field and item.role != VariableRole.ID
            ],
            "registry": dict(contract.meta.get("semantic_registry", {})),
        }
        if contract.meta.get("requires_longitudinal_trajectory_modeling"):
            payload["trajectory_feature_plan"] = {
                "measurement_window_hours": int(contract.meta.get("trajectory_measurement_window_hours", 10) or 10),
                "measurement_interval_hours": int(contract.meta.get("trajectory_measurement_interval_hours", 1) or 1),
                "derived_exposure": "heart_rate_trajectory_class",
                "raw_panel_variable": "heart_rate_hourly_panel_10h",
            }
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
            "trajectory_mixture_model",
        ]
        payload["study_template"] = {
            "key": contract.meta.get("study_template", ""),
            "title": contract.meta.get("study_template_title", ""),
            "suggested_outputs": list(contract.meta.get("study_template_suggested_outputs", [])),
        }
        payload["execution_backend"] = contract.meta.get("execution_backend", "spec_only")
        if contract.meta.get("requires_longitudinal_trajectory_modeling"):
            payload["paper_required_methods"] = list(contract.meta.get("paper_required_methods", []))
            payload["trajectory_engine_plan"] = {
                "paper_required_method": "LGMM",
                "supported_backend": contract.meta.get("engine_supported_trajectory_backend", ""),
                "execution_backend": contract.meta.get("execution_backend", ""),
                "fidelity": contract.meta.get("fidelity", ""),
                "method_gap": contract.meta.get("method_gap", ""),
            }
            payload["fidelity_gaps"] = [
                "paper-identical LGMM not implemented",
                "raw-event fidelity not guaranteed",
                "missing-data handling not paper-identical",
            ]
        return payload


def get_dataset_adapter(adapter_name: str) -> DatasetAdapter:
    normalized = _normalize_adapter_key(adapter_name)
    if normalized in {"mimic", "mimic_iv", "postgres", "postgresql", "relational_database", "sql", "database"}:
        return MimicIVAdapter()
    return DatasetAdapter()


def _normalize_adapter_key(value: str) -> str:
    normalized = "".join(ch if ch.isalnum() else "_" for ch in (value or "").strip().lower())
    normalized = "_".join(part for part in normalized.split("_") if part)
    if normalized in {"mimiciv", "mimic_4", "mimic_iv"}:
        return "mimic_iv"
    return normalized


def _can_route_to_hybrid_binary_runner(
    *,
    contract: TaskContract,
    analysis_route: Any,
    analysis_dataset_rel: str,
    auto_binary_profile: str,
) -> bool:
    if not analysis_route.requested_families:
        return False

    requested_families = set(analysis_route.requested_families)
    binary_core_families = {
        "baseline_table",
        "descriptive_statistics",
        "hypothesis_testing",
        "logistic_regression",
        "roc_analysis",
        "calibration_curve",
        "decision_curve_analysis",
        "distribution_comparison",
        "heatmap_visualization",
        "missingness_report",
    }
    bridge_families = {"cox_regression", "nomogram_prediction"}
    if not requested_families.issubset(binary_core_families | bridge_families):
        return False

    has_binary_signal = bool(
        requested_families
        & {
            "logistic_regression",
            "roc_analysis",
            "calibration_curve",
            "decision_curve_analysis",
            "cox_regression",
            "nomogram_prediction",
        }
    )
    if not has_binary_signal:
        return False

    if not _contract_has_binary_outcome(contract):
        return False

    if analysis_dataset_rel:
        return True
    return auto_binary_profile == "mimic_arf_nomogram_v1"


def _contract_has_binary_outcome(contract: TaskContract) -> bool:
    outcome_variables = [item for item in contract.variables if item.role == VariableRole.OUTCOME]
    if not outcome_variables:
        return False

    for variable in outcome_variables:
        transform = str(variable.transform or "").lower()
        name = str(variable.name or "").lower()
        description = str(variable.description or "").lower()
        unit = str(variable.unit or "").strip().lower()
        if "binary" in transform:
            return True
        if ("mortality" in name or "death" in name or "mortality" in description or "death" in description) and any(
            marker in name or marker in description for marker in ("28", "30", "90", "365", "1_year", "1year")
        ):
            return True
        if unit in {"", "none"} and ("mortality" in name or "death" in name):
            return True
    return False

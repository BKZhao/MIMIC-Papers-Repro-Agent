from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..contracts import TaskContract
from ..registry.skills import ClinicalAnalysisFamily, get_core_clinical_analysis_family


MODEL_FAMILY_TO_ANALYSIS_FAMILY: dict[str, str] = {
    "anova": "hypothesis_testing",
    "bayesian_survival": "bayesian_survival",
    "calibration_curve": "calibration_curve",
    "competing_risk": "competing_risk",
    "cox_regression": "cox_regression",
    "decision_curve_analysis": "decision_curve_analysis",
    "deep_survival_prediction": "deep_survival_prediction",
    "distribution_comparison": "distribution_comparison",
    "heatmap_visualization": "heatmap_visualization",
    "interaction_analysis": "subgroup_forest",
    "kaplan_meier": "kaplan_meier",
    "lasso_feature_selection": "lasso_feature_selection",
    "logistic_regression": "logistic_regression",
    "logrank_test": "kaplan_meier",
    "machine_learning_prediction": "machine_learning_prediction",
    "mixed_effects": "mixed_effects",
    "nomogram_prediction": "nomogram_prediction",
    "propensity_score_matching": "propensity_score_matching",
    "roc_analysis": "roc_analysis",
    "restricted_cubic_spline": "restricted_cubic_spline",
    "shap_explainability": "shap_explainability",
    "subgroup_analysis": "subgroup_forest",
    "trajectory_mixture_model": "trajectory_survival",
}

OUTPUT_TO_ANALYSIS_FAMILY: dict[str, str] = {
    "baseline_table": "baseline_table",
    "cohort_flowchart_figure": "cohort_flowchart",
    "calibration_figure": "calibration_curve",
    "cif_figure": "competing_risk",
    "cox_results_table": "cox_regression",
    "decision_curve_figure": "decision_curve_analysis",
    "distribution_figure": "distribution_comparison",
    "heatmap_figure": "heatmap_visualization",
    "km_figure": "kaplan_meier",
    "lasso_trace_figure": "lasso_feature_selection",
    "love_plot_figure": "propensity_score_matching",
    "missingness_report": "missingness_report",
    "nomogram_figure": "nomogram_prediction",
    "posterior_survival_figure": "bayesian_survival",
    "rcs_figure": "restricted_cubic_spline",
    "roc_figure": "roc_analysis",
    "shap_figure": "shap_explainability",
    "subgroup_figure": "subgroup_forest",
    "time_auc_figure": "deep_survival_prediction",
    "trajectory_figure": "trajectory_survival",
    "trajectory_table": "trajectory_survival",
}

PAPER_SIGNAL_TO_ANALYSIS_FAMILY: dict[str, str] = {
    "auc": "roc_analysis",
    "bayesian survival": "bayesian_survival",
    "boxplot": "distribution_comparison",
    "calibration": "calibration_curve",
    "calibration curve": "calibration_curve",
    "cif": "competing_risk",
    "cluster heatmap": "heatmap_visualization",
    "competing risk": "competing_risk",
    "correlation matrix": "heatmap_visualization",
    "dca": "decision_curve_analysis",
    "decision curve": "decision_curve_analysis",
    "deepsurv": "deep_survival_prediction",
    "density plot": "distribution_comparison",
    "fine-gray": "competing_risk",
    "forest plot": "subgroup_forest",
    "flowchart": "cohort_flowchart",
    "heatmap": "heatmap_visualization",
    "histogram": "distribution_comparison",
    "iptw": "iptw_weighting",
    "integrated discrimination improvement": "nri_idi_comparison",
    "lasso": "lasso_feature_selection",
    "machine learning": "machine_learning_prediction",
    "love plot": "propensity_score_matching",
    "mice": "multiple_imputation",
    "mixed effects": "mixed_effects",
    "mixed model": "mixed_effects",
    "multiple imputation": "multiple_imputation",
    "net reclassification improvement": "nri_idi_comparison",
    "nomogram": "nomogram_prediction",
    "paired plot": "distribution_comparison",
    "participant selection": "cohort_flowchart",
    "patient selection": "cohort_flowchart",
    "propensity score matching": "propensity_score_matching",
    "psm": "propensity_score_matching",
    "pycox": "deep_survival_prediction",
    "random forest": "machine_learning_prediction",
    "receiver operating characteristic": "roc_analysis",
    "roc": "roc_analysis",
    "shap": "shap_explainability",
    "shapley": "shap_explainability",
    "strip plot": "distribution_comparison",
    "svm": "machine_learning_prediction",
    "swarm plot": "distribution_comparison",
    "time-dependent auc": "deep_survival_prediction",
    "violin plot": "distribution_comparison",
    "volcano plot": "bioinformatics_extension",
    "xgboost": "machine_learning_prediction",
    "descriptive statistics": "descriptive_statistics",
}


@dataclass(frozen=True)
class PaperArtifactIntent:
    source_label: str
    output_name: str
    analysis_family: str
    support_level: str
    execution_mode: str
    preferred_libraries: tuple[str, ...]
    required_skills: tuple[str, ...]
    supplemental_codex_skills: tuple[str, ...]
    figure_style_hints: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_label": self.source_label,
            "output_name": self.output_name,
            "analysis_family": self.analysis_family,
            "support_level": self.support_level,
            "execution_mode": self.execution_mode,
            "preferred_libraries": list(self.preferred_libraries),
            "required_skills": list(self.required_skills),
            "supplemental_codex_skills": list(self.supplemental_codex_skills),
            "figure_style_hints": list(self.figure_style_hints),
        }


@dataclass(frozen=True)
class ClinicalAnalysisRoute:
    requested_families: tuple[str, ...]
    native_supported_families: tuple[str, ...]
    llm_compiled_families: tuple[str, ...]
    planning_reference_families: tuple[str, ...]
    unresolved_model_families: tuple[str, ...]
    unresolved_paper_signals: tuple[str, ...]
    paper_requested_output_names: tuple[str, ...]
    paper_figure_intents: tuple[PaperArtifactIntent, ...]
    paper_table_intents: tuple[PaperArtifactIntent, ...]
    figure_style_hints: tuple[str, ...]
    preferred_libraries: tuple[str, ...]
    required_skills: tuple[str, ...]
    supplemental_codex_skills: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "requested_families": list(self.requested_families),
            "native_supported_families": list(self.native_supported_families),
            "llm_compiled_families": list(self.llm_compiled_families),
            "planning_reference_families": list(self.planning_reference_families),
            "unresolved_model_families": list(self.unresolved_model_families),
            "unresolved_paper_signals": list(self.unresolved_paper_signals),
            "paper_requested_output_names": list(self.paper_requested_output_names),
            "paper_figure_intents": [item.as_dict() for item in self.paper_figure_intents],
            "paper_table_intents": [item.as_dict() for item in self.paper_table_intents],
            "figure_style_hints": list(self.figure_style_hints),
            "preferred_libraries": list(self.preferred_libraries),
            "required_skills": list(self.required_skills),
            "supplemental_codex_skills": list(self.supplemental_codex_skills),
        }


def resolve_clinical_analysis_route(contract: TaskContract) -> ClinicalAnalysisRoute:
    requested: list[str] = []
    unresolved_models: list[str] = []
    unresolved_signals: list[str] = []
    family_details: list[ClinicalAnalysisFamily] = []
    paper_requested_output_names: list[str] = []
    paper_figure_intents: list[PaperArtifactIntent] = []
    paper_table_intents: list[PaperArtifactIntent] = []

    for model in contract.models:
        normalized = (model.family or "").strip().lower()
        if not normalized:
            continue
        resolved = _resolve_analysis_family_key(normalized)
        if resolved:
            requested.append(resolved)
            detail = get_core_clinical_analysis_family(resolved)
            if detail is not None:
                family_details.append(detail)
        else:
            unresolved_models.append(normalized)

    for output in contract.outputs:
        normalized = (output.name or output.kind or "").strip().lower()
        resolved = OUTPUT_TO_ANALYSIS_FAMILY.get(normalized)
        if not resolved:
            continue
        requested.append(resolved)
        detail = get_core_clinical_analysis_family(resolved)
        if detail is not None:
            family_details.append(detail)

    for raw_signal in contract.meta.get("paper_required_methods", []):
        signal = str(raw_signal).strip().lower()
        if not signal:
            continue
        resolved = _resolve_signal_family(signal)
        if resolved:
            requested.append(resolved)
            detail = get_core_clinical_analysis_family(resolved)
            if detail is not None:
                family_details.append(detail)
        else:
            unresolved_signals.append(signal)

    if any(item.kind == "missingness_report" for item in contract.outputs):
        detail = get_core_clinical_analysis_family("missingness_report")
        if detail is not None:
            requested.append(detail.key)
            family_details.append(detail)

    for item in _collect_paper_artifact_manifest(contract):
        output_name = str(item.get("output_name", "")).strip()
        source_label = str(item.get("source_label", "")).strip()
        if output_name:
            paper_requested_output_names.append(output_name)
        resolved = OUTPUT_TO_ANALYSIS_FAMILY.get(output_name)
        if not resolved and source_label:
            resolved = _resolve_signal_family(source_label.lower())
        detail = get_core_clinical_analysis_family(resolved) if resolved else None
        if detail is not None:
            requested.append(detail.key)
            family_details.append(detail)
        elif source_label:
            unresolved_signals.append(source_label.lower())

        intent = PaperArtifactIntent(
            source_label=source_label,
            output_name=output_name,
            analysis_family=detail.key if detail is not None else (resolved or ""),
            support_level=detail.support_level if detail is not None else "unmapped",
            execution_mode=detail.execution_mode if detail is not None else "unresolved",
            preferred_libraries=detail.preferred_libraries if detail is not None else (),
            required_skills=detail.required_skills if detail is not None else (),
            supplemental_codex_skills=detail.supplemental_codex_skills if detail is not None else (),
            figure_style_hints=tuple(
                _dedupe(
                    [
                        *(_as_str_list(item.get("style_hints"))),
                        *(detail.figure_style_hints if detail is not None else ()),
                    ]
                )
            ),
        )
        if _is_figure_output_name(output_name):
            paper_figure_intents.append(intent)
        else:
            paper_table_intents.append(intent)

    requested = _dedupe(requested)
    family_details = _dedupe_family_details(family_details)
    native_supported = tuple(detail.key for detail in family_details if detail.support_level == "native_supported")
    llm_compiled = tuple(detail.key for detail in family_details if detail.support_level == "llm_compiled_then_execute")
    planning_reference = tuple(
        detail.key
        for detail in family_details
        if detail.execution_mode == "planning_and_scaffold_only"
    )
    preferred_libraries = tuple(
        _dedupe([library for detail in family_details for library in detail.preferred_libraries])
    )
    required_skills = tuple(
        _dedupe([skill for detail in family_details for skill in detail.required_skills])
    )
    supplemental_codex_skills = tuple(
        _dedupe([skill for detail in family_details for skill in detail.supplemental_codex_skills])
    )
    figure_style_hints = tuple(
        _dedupe(
            [
                hint
                for detail in family_details
                for hint in detail.figure_style_hints
            ]
            + [
                hint
                for intent in paper_figure_intents
                for hint in intent.figure_style_hints
            ]
        )
    )
    return ClinicalAnalysisRoute(
        requested_families=tuple(requested),
        native_supported_families=native_supported,
        llm_compiled_families=llm_compiled,
        planning_reference_families=planning_reference,
        unresolved_model_families=tuple(_dedupe(unresolved_models)),
        unresolved_paper_signals=tuple(_dedupe(unresolved_signals)),
        paper_requested_output_names=tuple(_dedupe(paper_requested_output_names)),
        paper_figure_intents=tuple(paper_figure_intents),
        paper_table_intents=tuple(paper_table_intents),
        figure_style_hints=figure_style_hints,
        preferred_libraries=preferred_libraries,
        required_skills=required_skills,
        supplemental_codex_skills=supplemental_codex_skills,
    )


def _resolve_analysis_family_key(value: str) -> str:
    direct = get_core_clinical_analysis_family(value)
    if direct is not None:
        return direct.key
    return MODEL_FAMILY_TO_ANALYSIS_FAMILY.get(value, "")


def _resolve_signal_family(signal: str) -> str:
    direct = get_core_clinical_analysis_family(signal)
    if direct is not None:
        return direct.key
    for pattern, family_key in PAPER_SIGNAL_TO_ANALYSIS_FAMILY.items():
        if pattern in signal:
            return family_key
    return ""


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _dedupe_family_details(values: list[ClinicalAnalysisFamily]) -> list[ClinicalAnalysisFamily]:
    seen: set[str] = set()
    ordered: list[ClinicalAnalysisFamily] = []
    for value in values:
        if value.key in seen:
            continue
        seen.add(value.key)
        ordered.append(value)
    return ordered


def _collect_paper_artifact_manifest(contract: TaskContract) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _append(output_name: str, source_label: str, style_hints: list[str]) -> None:
        key = (output_name, source_label)
        if key in seen:
            return
        seen.add(key)
        manifest.append(
            {
                "output_name": output_name,
                "source_label": source_label,
                "style_hints": style_hints,
            }
        )

    for raw_manifest_key in ("paper_evidence_figure_manifest", "paper_figure_manifest"):
        raw_manifest = contract.meta.get(raw_manifest_key, [])
        if not isinstance(raw_manifest, list):
            continue
        for item in raw_manifest:
            if not isinstance(item, dict):
                continue
            output_name = str(item.get("output_name", "")).strip()
            source_label = str(
                item.get("source_label")
                or item.get("paper_figure_key")
                or item.get("figure_key")
                or ""
            ).strip()
            style_hints = _as_str_list(item.get("style_hints"))
            if output_name:
                _append(output_name, source_label, style_hints)

    for output in contract.outputs:
        output_name = str(output.name or output.kind).strip()
        if not output_name:
            continue
        source_label = str(
            output.options.get("paper_evidence_source_label")
            or output.options.get("paper_figure_key")
            or output.options.get("paper_source_label")
            or ""
        ).strip()
        style_hints = _as_str_list(output.options.get("paper_style_hints"))
        if source_label or output.options.get("paper_driven"):
            _append(output_name, source_label, style_hints)

    return manifest


def _is_figure_output_name(value: str) -> bool:
    return str(value).strip().lower().endswith("_figure")


def _as_str_list(value: Any) -> list[str]:
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []

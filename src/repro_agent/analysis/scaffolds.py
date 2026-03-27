from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..contracts import TaskContract, VariableRole
from ..registry.skills import ClinicalAnalysisFamily, get_core_clinical_analysis_family
from .router import ClinicalAnalysisRoute, resolve_clinical_analysis_route


@dataclass(frozen=True)
class HybridScaffoldBundle:
    analysis_spec: dict[str, Any]
    figure_spec: dict[str, Any]
    executor_scaffold: str


def build_hybrid_scaffold_bundle(
    contract: TaskContract,
    *,
    route: ClinicalAnalysisRoute | None = None,
) -> HybridScaffoldBundle:
    resolved_route = route or resolve_clinical_analysis_route(contract)
    family_details = _resolve_family_details(resolved_route)
    analysis_spec = _build_analysis_spec(contract, resolved_route, family_details)
    figure_spec = _build_figure_spec(contract, resolved_route, family_details)
    executor_scaffold = _render_executor_scaffold(contract, resolved_route, family_details)
    return HybridScaffoldBundle(
        analysis_spec=analysis_spec,
        figure_spec=figure_spec,
        executor_scaffold=executor_scaffold,
    )


def _build_analysis_spec(
    contract: TaskContract,
    route: ClinicalAnalysisRoute,
    family_details: list[ClinicalAnalysisFamily],
) -> dict[str, Any]:
    return {
        "spec_version": "1",
        "spec_kind": "analysis_spec",
        "title": contract.title,
        "task_id": contract.task_id,
        "dataset": contract.dataset.as_dict(),
        "cohort": contract.cohort.as_dict(),
        "requested_families": list(route.requested_families),
        "llm_compiled_families": list(route.llm_compiled_families),
        "planning_reference_families": list(route.planning_reference_families),
        "unresolved_model_families": list(route.unresolved_model_families),
        "unresolved_paper_signals": list(route.unresolved_paper_signals),
        "variables_by_role": _variables_by_role(contract),
        "models": [item.as_dict() for item in contract.models],
        "outputs": [item.as_dict() for item in contract.outputs],
        "paper_required_methods": list(contract.meta.get("paper_required_methods", [])),
        "family_specs": [
            {
                "key": item.key,
                "description": item.description,
                "execution_mode": item.execution_mode,
                "support_level": item.support_level,
                "primary_outputs": list(item.primary_outputs),
                "scaffold_outputs": list(item.scaffold_outputs),
                "preferred_libraries": list(item.preferred_libraries),
                "notes": list(item.notes),
            }
            for item in family_details
        ],
    }


def _build_figure_spec(
    contract: TaskContract,
    route: ClinicalAnalysisRoute,
    family_details: list[ClinicalAnalysisFamily],
) -> dict[str, Any]:
    requested_figure_outputs = [
        {
            "name": item.name,
            "kind": item.kind,
            "format": item.fmt,
            "required": item.required,
        }
        for item in contract.outputs
        if item.kind.endswith("figure")
    ]
    figure_families = [
        item.key
        for item in family_details
        if any(output.lower().endswith(".png") or "figure" in output.lower() for output in item.primary_outputs)
        or "figure_spec.json" in item.scaffold_outputs
    ]
    return {
        "spec_version": "1",
        "spec_kind": "figure_spec",
        "title": contract.title,
        "task_id": contract.task_id,
        "requested_figure_outputs": requested_figure_outputs,
        "figure_families": _dedupe(figure_families),
        "preferred_libraries": list(route.preferred_libraries),
        "notes": [
            "Generate figures from reproduced data only.",
            "If a plot family is still scaffold-only, keep the artifact labeled as a draft executor specification.",
        ],
    }


def _render_executor_scaffold(
    contract: TaskContract,
    route: ClinicalAnalysisRoute,
    family_details: list[ClinicalAnalysisFamily],
) -> str:
    exposures = [item.name for item in contract.variables if item.role == VariableRole.EXPOSURE]
    outcomes = [item.name for item in contract.variables if item.role == VariableRole.OUTCOME]
    controls = [item.name for item in contract.variables if item.role == VariableRole.CONTROL]
    lines = [
        '"""Auto-generated hybrid execution scaffold.',
        "",
        "This file is a deterministic scaffold draft for method families that are recognized",
        "by the agent but not yet wired into the native execution backend.",
        "",
        "Replace the TODO blocks with real local execution code. Do not fabricate results.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from pathlib import Path",
        "",
        "import pandas as pd",
        "",
        f"REQUESTED_FAMILIES = {list(route.requested_families)!r}",
        f"LLM_COMPILED_FAMILIES = {list(route.llm_compiled_families)!r}",
        f"EXPOSURE_VARIABLES = {exposures!r}",
        f"OUTCOME_VARIABLES = {outcomes!r}",
        f"CONTROL_VARIABLES = {controls!r}",
        "",
        "",
        "def load_analysis_dataset(path: str | Path) -> pd.DataFrame:",
        '    """Load the prepared analysis dataset before fitting real models."""',
        "    return pd.read_csv(path)",
        "",
    ]
    for family in family_details:
        function_name = f"run_{family.key}"
        lines.extend(
            [
                f"def {function_name}(df: pd.DataFrame) -> None:",
                f'    """Execute the {family.key} family with real local computation."""',
                "    # TODO: implement the paper-aligned local executor.",
                f"    # Preferred libraries: {', '.join(family.preferred_libraries) or 'pandas'}",
                f"    # Notes: {'; '.join(family.notes) or 'none'}",
                '    raise NotImplementedError("Scaffold only: implement real local execution before use.")',
                "",
            ]
        )
    lines.extend(
        [
            "def main() -> None:",
            '    raise SystemExit("This scaffold is a draft executor specification and should not be run unchanged.")',
            "",
            "",
            'if __name__ == "__main__":',
            "    main()",
            "",
        ]
    )
    return "\n".join(lines)


def _resolve_family_details(route: ClinicalAnalysisRoute) -> list[ClinicalAnalysisFamily]:
    families: list[ClinicalAnalysisFamily] = []
    for key in list(route.llm_compiled_families) + list(route.planning_reference_families):
        item = get_core_clinical_analysis_family(key)
        if item is not None:
            families.append(item)
    return families


def _variables_by_role(contract: TaskContract) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {role.value: [] for role in VariableRole}
    for item in contract.variables:
        grouped[item.role.value].append(
            {
                "name": item.name,
                "dataset_field": item.dataset_field,
                "required": item.required,
                "source_name": item.source_name,
                "transform": item.transform,
            }
        )
    return grouped


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

from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from repro_agent.contracts import (  # noqa: E402
    CohortSpec,
    DatasetSpec,
    ModelSpec,
    TaskContract,
    VariableRole,
    VariableSpec,
)
from repro_agent.openclaw_bridge import describe_openclaw_integration, plan_task  # noqa: E402
from repro_agent.preset_registry import detect_paper_preset  # noqa: E402
from repro_agent.semantic_registry import load_mimic_semantic_registry, resolve_semantic_variable  # noqa: E402
from repro_agent.skill_contracts import load_skill_contract_manifest  # noqa: E402
from repro_agent.study_templates import infer_study_template  # noqa: E402


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _scaffold_temp_project(project_root: Path) -> None:
    _write_text(
        project_root / "configs" / "agentic.example.yaml",
        (ROOT / "configs" / "agentic.example.yaml").read_text(encoding="utf-8"),
    )
    _write_text(
        project_root / "configs" / "mimic_variable_semantics.yaml",
        (ROOT / "configs" / "mimic_variable_semantics.yaml").read_text(encoding="utf-8"),
    )
    _write_text(
        project_root / "papers" / "paper.md",
        textwrap.dedent(
            """
            MIMIC-IV sepsis cohort study focused on the TyG index and mortality.
            Participants were adult sepsis patients in the ICU.
            The paper reports Cox regression, Kaplan-Meier curves, restricted cubic splines, and subgroup analysis.
            """
        ).strip()
        + "\n",
    )
    _write_text(
        project_root / "openclaw" / "skills" / "skills_manifest.yaml",
        (ROOT / "openclaw" / "skills" / "skills_manifest.yaml").read_text(encoding="utf-8"),
    )
    for skill_doc in (ROOT / "openclaw" / "skills").glob("*/SKILL.md"):
        _write_text(
            project_root / skill_doc.relative_to(ROOT),
            skill_doc.read_text(encoding="utf-8"),
        )


class PresetRegistryTests(unittest.TestCase):
    def test_detects_mimic_tyg_sepsis_preset(self) -> None:
        preset = detect_paper_preset(
            dataset_label="MIMIC-IV v2.2",
            instructions="Please reproduce the TyG index sepsis paper from MIMIC.",
            materials={"paper.md": "This MIMIC study examines TyG among sepsis ICU patients."},
        )
        self.assertIsNotNone(preset)
        assert preset is not None
        self.assertEqual(preset.key, "mimic_tyg_sepsis")


class SemanticRegistryTests(unittest.TestCase):
    def test_load_and_resolve_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            registry = load_mimic_semantic_registry(project_root)
            self.assertGreater(registry.variable_count(), 20)

            glucose = resolve_semantic_variable(registry, "fbg")
            self.assertIsNotNone(glucose)
            assert glucose is not None
            self.assertEqual(glucose.name, "blood_glucose")

            tyg = resolve_semantic_variable(registry, "tyg")
            self.assertIsNotNone(tyg)
            assert tyg is not None
            self.assertEqual(tyg.dataset_field, "analysis_dataset.tyg_index")


class StudyTemplateTests(unittest.TestCase):
    def test_infers_baseline_subgroup_spline_template(self) -> None:
        contract = TaskContract(
            task_id="task-1",
            title="TyG sepsis paper",
            dataset=DatasetSpec(name="MIMIC-IV", adapter="mimic_iv"),
            cohort=CohortSpec(population="adult sepsis ICU patients"),
            variables=[
                VariableSpec(name="tyg_index", role=VariableRole.EXPOSURE),
                VariableSpec(name="in_hospital_mortality", role=VariableRole.OUTCOME),
            ],
            models=[
                ModelSpec(name="cox", family="cox_regression"),
                ModelSpec(name="km", family="kaplan_meier"),
                ModelSpec(name="rcs", family="restricted_cubic_spline"),
                ModelSpec(name="subgroup", family="subgroup_analysis"),
            ],
        )
        template = infer_study_template(contract)
        self.assertIsNotNone(template)
        assert template is not None
        self.assertEqual(template.key, "baseline_subgroup_spline")


class OpenClawBridgeTests(unittest.TestCase):
    def test_describe_openclaw_integration_reports_bridge_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = describe_openclaw_integration(project_root)
            self.assertEqual(payload["agent_name"], "paper-repro-scientist")
            self.assertEqual(payload["recommended_config"], "configs/openclaw.agentic.yaml")
            self.assertEqual(payload["recommended_real_run_config"], "configs/openclaw.mimic-real-run.yaml")
            self.assertEqual(payload["capability_summary"]["overall_readiness"], "partial_real_execution")
            self.assertTrue(payload["agent_contract"]["single_entrypoint"])
            self.assertIn("preset_real_run", payload["run_profiles"])
            self.assertIn("paper_intake_and_contract", payload["skill_paths"])
            self.assertIn("result_figure_generation", payload["skill_paths"])
            self.assertEqual(payload["workflow_contract"]["phase_count"], 5)
            self.assertEqual(payload["workflow_contract"]["phases"][0]["key"], "paper_extraction")
            self.assertEqual(payload["skill_contract_manifest"]["skill_count"], 7)
            self.assertEqual(payload["skill_contracts"]["git_update"]["stage"], "ops")
            self.assertGreater(payload["semantic_registry"]["variable_count"], 20)
            interfaces = {item["name"]: item for item in payload["interfaces"]}
            self.assertIn("profile", interfaces["extract_analysis_dataset"]["inputs"])

    def test_plan_task_without_llm_returns_preset_and_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/paper.md",
                instructions=(
                    "自变量: TyG index; 因变量: in-hospital mortality, ICU mortality; "
                    "模型: Cox, Kaplan-Meier, RCS, subgroup"
                ),
                session_id="session-test-openclaw",
                use_llm=False,
            )

            self.assertEqual(payload["execution_backend"], "deterministic_bridge")
            self.assertTrue(payload["execution_supported"])
            self.assertEqual(payload["preset"]["key"], "mimic_tyg_sepsis")
            self.assertEqual(payload["study_template"]["key"], "baseline_subgroup_spline")
            self.assertEqual(payload["session_id"], "session-test-openclaw")

            contract = payload["task_contract"]
            self.assertEqual(contract["meta"]["execution_backend"], "deterministic_bridge")
            self.assertGreater(contract["meta"]["semantic_mapped_variable_count"], 0)
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "task_contract.json").exists()
            )

            by_name = {item["name"]: item for item in contract["variables"]}
            self.assertEqual(by_name["tyg_index"]["dataset_field"], "analysis_dataset.tyg_index")

    def test_plan_task_parses_single_line_structured_instructions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/paper.md",
                instructions=(
                    "Exposure: TyG index. Outcomes: in-hospital mortality, ICU mortality. "
                    "Controls: age, sex. Models: Cox, Kaplan-Meier, RCS, subgroup. "
                    "Outputs: baseline table, KM figure, RCS figure. "
                    "Cohort logic: adult sepsis patients in the first ICU stay."
                ),
                session_id="session-structured-parse",
                use_llm=False,
            )

            contract = payload["task_contract"]
            exposures = [item["name"] for item in contract["variables"] if item["role"] == "exposure"]
            outcomes = [item["name"] for item in contract["variables"] if item["role"] == "outcome"]
            controls = [item["name"] for item in contract["variables"] if item["role"] == "control"]
            model_families = [item["family"] for item in contract["models"]]
            output_names = [item["name"] for item in contract["outputs"]]

            self.assertEqual(exposures, ["tyg_index"])
            self.assertEqual(sorted(outcomes), ["icu_mortality", "in_hospital_mortality"])
            self.assertEqual(sorted(controls), ["age", "sex"])
            self.assertEqual(
                model_families,
                ["cox_regression", "kaplan_meier", "restricted_cubic_spline", "subgroup_analysis"],
            )
            self.assertIn("baseline_table", output_names)
            self.assertIn("km_figure", output_names)
            self.assertIn("rcs_figure", output_names)
            self.assertIn("adult sepsis patients in the first ICU stay", contract["cohort"]["inclusion_criteria"][0])
            self.assertEqual(payload["missing_high_impact_fields"], [])


class SkillContractManifestTests(unittest.TestCase):
    def test_load_skill_contract_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            manifest = load_skill_contract_manifest(project_root)

            self.assertEqual(manifest.agent_name, "paper-repro-scientist")
            self.assertEqual(manifest.skill_count(), 7)
            self.assertIn("paper_intake_and_contract", manifest.skills)
            self.assertEqual(manifest.skills["paper_intake_and_contract"].stage, "paper_extraction")
            self.assertIn("result_figure_generation", manifest.skills)
            self.assertEqual(
                manifest.skills["mimic_cohort_execution"].execution_preference,
                "deterministic_preferred",
            )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import importlib
import json
import sys
import tempfile
import textwrap
import unittest
from dataclasses import replace
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from repro_agent.contracts import (  # noqa: E402
    CohortSpec,
    DatasetSpec,
    ModelSpec,
    OutputSpec,
    TaskContract,
    VariableRole,
    VariableSpec,
)
from repro_agent.agentic.runner import AgentRunner  # noqa: E402
from repro_agent.config import load_pipeline_config  # noqa: E402
from repro_agent.dataset_adapters import get_dataset_adapter  # noqa: E402
from repro_agent.openclaw_bridge import continue_session, describe_openclaw_integration, plan_task  # noqa: E402
from repro_agent.openclaw_bridge import get_openclaw_request_template, handle_openclaw_request  # noqa: E402
from repro_agent.analysis.router import resolve_clinical_analysis_route  # noqa: E402
from repro_agent.paper.builder import normalize_task_contract  # noqa: E402
from repro_agent.paper.presets import detect_paper_preset  # noqa: E402
from repro_agent.registry.codex_skill_bridge import load_codex_skill_bridge_manifest  # noqa: E402
from repro_agent.registry.semantic import load_mimic_semantic_registry, resolve_semantic_variable  # noqa: E402
from repro_agent.registry.skill_contracts import load_skill_contract_manifest  # noqa: E402
from repro_agent.paper.templates import infer_study_template  # noqa: E402
from repro_agent.sql.cohort import build_tyg_stroke_cohort_sql, build_tyg_stroke_funnel_sql  # noqa: E402


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
        project_root / "papers" / "trajectory.md",
        textwrap.dedent(
            """
            Influence of heart rate trajectory in 30-day mortality in sepsis patients: a retrospective study based on the MIMIC-IV database.
            Data were extracted from the MIMIC-IV database (MIMIC-IV 2.0) between 2008 and 2019 and the measured heart rate of sepsis patients 10 h post-admission to ICU was extracted, with 1 h between each measurement.
            Latent growth mixture modelling (LGMM) was used to classify heart rate trajectories, while Kaplan-Meier and Cox proportional hazards models were employed to analyze differences in survival between groups.
            The outcome was 30-day mortality among sepsis patients admitted to the ICU.
            """
        ).strip()
        + "\n",
    )
    _write_text(
        project_root / "papers" / "nlr.md",
        textwrap.dedent(
            """
            Neutrophil-to-lymphocyte ratio and 28-day mortality in elderly sepsis patients from the MIMIC-IV database.
            Methods: Data were obtained from the MIMIC-IV database (MIMIC-IV, version 3.1) between 2008 and 2019.
            The paper reports Cox regression, Kaplan-Meier analysis, subgroup analysis, and restricted cubic spline analysis.
            """
        ).strip()
        + "\n",
    )
    _write_text(
        project_root / "papers" / "stroke_tyg.md",
        textwrap.dedent(
            """
            Association of triglyceride-glucose index with all-cause mortality in critically ill non-diabetic ischemic stroke:
            a retrospective cohort study based on the MIMIC-IV database.
            Methods: Data were obtained from the MIMIC-IV database (MIMIC-IV, version 3.1).
            The cohort included non-diabetic adult patients with ischemic stroke.
            Outcomes included ICU, in-hospital, 30-day, 90-day, 180-day, and 1-year all-cause mortality.
            The paper reports Cox regression, Kaplan-Meier analysis, restricted cubic spline analysis, and subgroup analysis.
            """
        ).strip()
        + "\n",
    )
    _write_text(
        project_root / "papers" / "arf.md",
        textwrap.dedent(
            """
            During the period from 2002 to 2017, the diagnosis rate per 100,000 adults increased substantially.
            Based on Medical Information Mart for Intensive Care IV-3.1 (MIMIC-IV-3.1) in the United States, this study constructed and verified a short-term death risk prediction model for patients with acute respiratory failure.
            The inclusion criteria were as follows: age > 18 years and first ICU admission with acute respiratory failure.
            The outcome was 28-day mortality. The model construction used multifactorial Cox regression and a prognostic nomogram.
            """
        ).strip()
        + "\n",
    )
    _write_text(
        project_root / "openclaw" / "skills" / "skills_manifest.yaml",
        (ROOT / "openclaw" / "skills" / "skills_manifest.yaml").read_text(encoding="utf-8"),
    )
    _write_text(
        project_root / "openclaw" / "skills" / "codex_skill_bridge.yaml",
        (ROOT / "openclaw" / "skills" / "codex_skill_bridge.yaml").read_text(encoding="utf-8"),
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

    def test_does_not_misclassify_stroke_tyg_paper_as_sepsis_preset(self) -> None:
        preset = detect_paper_preset(
            dataset_label="MIMIC-IV v3.1",
            instructions="Please reproduce this TyG sepsis study from MIMIC.",
            materials={
                "stroke_tyg.md": (
                    "Association of triglyceride-glucose index with all-cause mortality in critically ill "
                    "non-diabetic ischemic stroke based on the MIMIC-IV database."
                )
            },
        )
        self.assertIsNone(preset)


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


class PackageFacadeTests(unittest.TestCase):
    def test_new_package_facades_import_cleanly(self) -> None:
        modules = [
            "repro_agent.core.config",
            "repro_agent.paper.builder",
            "repro_agent.agentic.decision",
            "repro_agent.analysis.router",
            "repro_agent.registry.skills",
            "repro_agent.sql.cohort",
            "repro_agent.integrations.openclaw",
        ]
        for module_name in modules:
            imported = importlib.import_module(module_name)
            self.assertIsNotNone(imported)


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

    def test_infers_longitudinal_trajectory_survival_template(self) -> None:
        contract = TaskContract(
            task_id="task-trajectory",
            title="Heart rate trajectory sepsis paper",
            dataset=DatasetSpec(name="MIMIC-IV", adapter="mimic_iv"),
            cohort=CohortSpec(population="sepsis ICU patients"),
            variables=[
                VariableSpec(name="heart_rate_trajectory_class", role=VariableRole.EXPOSURE),
                VariableSpec(name="mortality_30d", role=VariableRole.OUTCOME),
            ],
            models=[
                ModelSpec(name="trajectory", family="trajectory_mixture_model"),
                ModelSpec(name="km", family="kaplan_meier"),
                ModelSpec(name="cox", family="cox_regression"),
            ],
        )
        template = infer_study_template(contract)
        self.assertIsNotNone(template)
        assert template is not None
        self.assertEqual(template.key, "longitudinal_trajectory_survival")


class ClinicalAnalysisRouterTests(unittest.TestCase):
    def test_resolve_hybrid_and_native_analysis_families(self) -> None:
        contract = TaskContract(
            task_id="task-router",
            title="ARF nomogram paper",
            dataset=DatasetSpec(name="MIMIC-IV", adapter="mimic_iv"),
            cohort=CohortSpec(population="adult ICU patients with ARF"),
            variables=[
                VariableSpec(name="mortality_28d", role=VariableRole.OUTCOME),
            ],
            models=[
                ModelSpec(name="cox", family="cox_regression"),
                ModelSpec(name="km", family="kaplan_meier"),
            ],
            meta={
                "paper_required_methods": [
                    "nomogram",
                    "LASSO",
                    "calibration curve",
                    "DCA",
                    "SHAP",
                ]
            },
        )

        route = resolve_clinical_analysis_route(contract)
        payload = route.as_dict()
        self.assertIn("cox_regression", payload["native_supported_families"])
        self.assertIn("kaplan_meier", payload["native_supported_families"])
        self.assertIn("nomogram_prediction", payload["llm_compiled_families"])
        self.assertIn("lasso_feature_selection", payload["llm_compiled_families"])
        self.assertIn("calibration_curve", payload["llm_compiled_families"])
        self.assertIn("decision_curve_analysis", payload["llm_compiled_families"])
        self.assertIn("shap_explainability", payload["llm_compiled_families"])
        self.assertIn("statsmodels", payload["preferred_libraries"])
        self.assertIn("matplotlib", payload["preferred_libraries"])


class StrokeTygCohortSqlTests(unittest.TestCase):
    def test_stroke_tyg_sql_uses_time_aware_diabetes_history_filters(self) -> None:
        cohort_sql = build_tyg_stroke_cohort_sql(mode="icd", has_sepsis3_flag=False)
        funnel_sql = build_tyg_stroke_funnel_sql(mode="icd", has_sepsis3_flag=False)

        for sql_text in (cohort_sql, funnel_sql):
            self.assertIn("diabetic_hadm_history", sql_text)
            self.assertIn("antidiabetic_hadm_history", sql_text)
            self.assertIn("AND d.admittime <= l.admittime", sql_text)
            self.assertIn("AND r.admittime < l.admittime", sql_text)
            self.assertNotIn("SELECT DISTINCT rx.subject_id", sql_text)

    def test_stroke_tyg_funnel_sql_keeps_dischtime_available_for_admission_anchor(self) -> None:
        from repro_agent.sql.cohort import PAPER_MIMIC_TYG_STROKE_PROFILE  # noqa: E402

        admission_profile = replace(PAPER_MIMIC_TYG_STROKE_PROFILE, lab_anchor="admission")
        funnel_sql = build_tyg_stroke_funnel_sql(
            mode="icd",
            has_sepsis3_flag=False,
            profile=admission_profile,
        )

        self.assertIn("a.dischtime", funnel_sql)
        self.assertIn("COALESCE(l.dischtime, l.outtime)", funnel_sql)

    def test_stroke_tyg_sql_requires_paired_tg_glucose_labs(self) -> None:
        cohort_sql = build_tyg_stroke_cohort_sql(mode="icd", has_sepsis3_flag=False)
        funnel_sql = build_tyg_stroke_funnel_sql(mode="icd", has_sepsis3_flag=False)

        for sql_text in (cohort_sql, funnel_sql):
            self.assertIn("glu.charttime = tg.charttime", sql_text)


class PaperEvidenceOutputPreferenceTests(unittest.TestCase):
    def test_paper_evidence_can_request_participant_flowchart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            config = load_pipeline_config(project_root / "configs" / "agentic.example.yaml")

            contract = TaskContract(
                task_id="task-flowchart",
                title="stroke flowchart paper",
                source_paper_path="papers/stroke_tyg.md",
                dataset=DatasetSpec(name="MIMIC-IV v3.1", adapter="mimic_iv", version="MIMIC-IV v3.1"),
                cohort=CohortSpec(population="non-diabetic ischemic stroke ICU patients"),
                variables=[
                    VariableSpec(name="tyg_index", role=VariableRole.EXPOSURE),
                    VariableSpec(name="mortality_30d", role=VariableRole.OUTCOME),
                ],
                models=[ModelSpec(name="cox", family="cox_regression")],
                outputs=[OutputSpec(name="model_results_table", kind="model_results_table", fmt="csv")],
                meta={
                    "paper_evidence": {
                        "title": "Stroke TyG paper",
                        "paper_target_dataset_version": "MIMIC-IV v3.1",
                        "requested_figures": [
                            "Flowchart of participant selection",
                            "Kaplan-Meier survival curves by TyG quartiles",
                        ],
                        "requested_tables": [],
                    }
                },
            )

            normalized = normalize_task_contract(contract, config=config, project_root=project_root)
            output_names = [item.name for item in normalized.outputs]
            self.assertIn("cohort_flowchart_figure", output_names)
            self.assertIn("km_figure", output_names)

            route = resolve_clinical_analysis_route(normalized)
            figure_outputs = {item["output_name"] for item in route.as_dict()["paper_figure_intents"]}
            self.assertIn("cohort_flowchart_figure", figure_outputs)
            self.assertIn("cohort_flowchart", route.as_dict()["requested_families"])

    def test_paper_evidence_figure_intent_overrides_heuristic_figure_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            config = load_pipeline_config(project_root / "configs" / "agentic.example.yaml")

            contract = TaskContract(
                task_id="task-stroke-tyg",
                title="triglyceride glucose index",
                source_paper_path="papers/stroke_tyg.md",
                dataset=DatasetSpec(name="MIMIC-IV v3.1", adapter="mimic_iv", version="MIMIC-IV v3.1"),
                cohort=CohortSpec(population="non-diabetic ischemic stroke ICU patients"),
                variables=[
                    VariableSpec(name="tyg_index", role=VariableRole.EXPOSURE),
                    VariableSpec(name="mortality_30d", role=VariableRole.OUTCOME),
                ],
                models=[
                    ModelSpec(name="cox", family="cox_regression"),
                ],
                outputs=[
                    OutputSpec(name="model_results_table", kind="model_results_table", fmt="csv"),
                    OutputSpec(name="rcs_figure", kind="rcs_figure", fmt="png"),
                    OutputSpec(name="subgroup_figure", kind="subgroup_figure", fmt="png"),
                ],
                meta={
                    "paper_evidence": {
                        "title": "Association of triglyceride-glucose index with all-cause mortality in critically ill non-diabetic ischemic stroke",
                        "paper_target_dataset_version": "MIMIC-IV v3.1",
                        "requested_figures": [
                            "Kaplan-Meier survival analysis curves of ACM by quartiles of TyG index"
                        ],
                        "requested_tables": [],
                    }
                },
            )

            normalized = normalize_task_contract(contract, config=config, project_root=project_root)
            output_names = [item.name for item in normalized.outputs]

            self.assertEqual(
                normalized.title,
                "Association of triglyceride-glucose index with all-cause mortality in critically ill non-diabetic ischemic stroke",
            )
            self.assertIn("km_figure", output_names)
            self.assertIn("model_results_table", output_names)
            self.assertNotIn("rcs_figure", output_names)
            self.assertNotIn("subgroup_figure", output_names)
            self.assertIn("paper_evidence_figure_manifest", normalized.meta)

    def test_analysis_route_surfaces_paper_driven_figure_intents_and_skills(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            config = load_pipeline_config(project_root / "configs" / "agentic.example.yaml")

            contract = TaskContract(
                task_id="task-paper-figure-route",
                title="prediction paper",
                source_paper_path="papers/arf.md",
                dataset=DatasetSpec(name="MIMIC-IV v3.1", adapter="mimic_iv", version="MIMIC-IV v3.1"),
                cohort=CohortSpec(population="adult ICU patients"),
                variables=[
                    VariableSpec(name="mortality_28d", role=VariableRole.OUTCOME),
                ],
                models=[
                    ModelSpec(name="cox", family="cox_regression"),
                ],
                outputs=[
                    OutputSpec(name="model_results_table", kind="model_results_table", fmt="csv"),
                ],
                meta={
                    "paper_evidence": {
                        "title": "ARF prognostic modeling paper",
                        "paper_target_dataset_version": "MIMIC-IV v3.1",
                        "requested_figures": [
                            "Kaplan-Meier survival curves with number at risk for 28-day mortality",
                            "Calibration curve for the nomogram",
                            "SHAP beeswarm plot for feature importance",
                            "Heatmap of correlation matrix between predictors",
                        ],
                        "requested_tables": [
                            "Multivariable Cox regression table",
                        ],
                    }
                },
            )

            normalized = normalize_task_contract(contract, config=config, project_root=project_root)
            route = resolve_clinical_analysis_route(normalized)
            payload = route.as_dict()

            self.assertIn("kaplan_meier", payload["requested_families"])
            self.assertIn("calibration_curve", payload["requested_families"])
            self.assertIn("shap_explainability", payload["requested_families"])
            self.assertIn("heatmap_visualization", payload["requested_families"])
            self.assertIn("cox_regression", payload["requested_families"])
            self.assertIn("scientific-visualization", payload["supplemental_codex_skills"])
            self.assertIn("number_at_risk", payload["figure_style_hints"])

            figure_outputs = {item["output_name"] for item in payload["paper_figure_intents"]}
            self.assertIn("km_figure", figure_outputs)
            self.assertIn("calibration_figure", figure_outputs)
            self.assertIn("shap_figure", figure_outputs)
            self.assertIn("heatmap_figure", figure_outputs)

            table_families = {item["analysis_family"] for item in payload["paper_table_intents"]}
            self.assertIn("cox_regression", table_families)


class OpenClawBridgeTests(unittest.TestCase):
    def test_describe_openclaw_integration_reports_bridge_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = describe_openclaw_integration(project_root)
            self.assertEqual(payload["agent_name"], "paper-repro-scientist")
            self.assertEqual(payload["architecture_posture"], "hybrid_llm_plus_deterministic")
            self.assertEqual(payload["recommended_config"], "configs/openclaw.agentic.yaml")
            self.assertEqual(payload["recommended_real_run_config"], "configs/openclaw.mimic-real-run.yaml")
            self.assertEqual(payload["soul_path"], "openclaw/SOUL.MD")
            self.assertEqual(payload["agents_path"], "openclaw/AGENTS.md")
            self.assertEqual(payload["capability_summary"]["overall_readiness"], "partial_real_execution")
            self.assertTrue(payload["agent_contract"]["single_entrypoint"])
            self.assertIn("preset_real_run", payload["run_profiles"])
            self.assertIn("paper_intake_and_contract", payload["skill_paths"])
            self.assertIn("result_figure_generation", payload["skill_paths"])
            self.assertIn("longitudinal_trajectory_execution", payload["skill_paths"])
            self.assertEqual(payload["workflow_contract"]["phase_count"], 5)
            self.assertEqual(payload["workflow_contract"]["phases"][0]["key"], "paper_extraction")
            self.assertEqual(payload["skill_contract_manifest"]["skill_count"], 7)
            self.assertTrue(payload["bridge_artifacts"]["codex_skill_bridge"]["present"])
            self.assertEqual(payload["bridge_artifacts"]["codex_skill_bridge"]["project_skill_root"], ".codex/skills")
            self.assertEqual(payload["bridge_artifacts"]["codex_skill_bridge"]["project_skill_count"], 46)
            self.assertEqual(payload["bridge_artifacts"]["codex_skill_bridge"]["category_group_count"], 5)
            self.assertEqual(payload["bridge_artifacts"]["codex_skill_bridge"]["stage_bridge_count"], 5)
            self.assertEqual(payload["codex_skill_bridge"]["repo_skill_root"], ".codex/skills")
            self.assertIn("writing_review_and_research", payload["codex_skill_bridge"]["category_groups"])
            self.assertIn("dataset_and_modeling", payload["codex_skill_bridge"]["openclaw_stage_bridges"])
            self.assertGreater(payload["semantic_registry"]["variable_count"], 20)
            clinical_registry = payload["clinical_analysis_registry"]
            self.assertEqual(clinical_registry["trajectory_survival"]["support_level"], "native_supported")
            self.assertEqual(clinical_registry["trajectory_survival"]["maturity"], "experimental")
            self.assertEqual(clinical_registry["logistic_regression"]["support_level"], "llm_compiled_then_execute")
            self.assertIn("executor_scaffold.py", clinical_registry["logistic_regression"]["scaffold_outputs"])
            self.assertIn("roc_analysis", clinical_registry)
            interfaces = {item["name"]: item for item in payload["interfaces"]}
            self.assertIn("profile", interfaces["extract_analysis_dataset"]["inputs"])
            self.assertIn("agent_decision", interfaces["plan_task"]["outputs"])
            self.assertIn("analysis_family_route", interfaces["plan_task"]["outputs"])
            self.assertIn("paper_spec_surface", interfaces["plan_task"]["outputs"])
            self.assertIn("analysis_spec_surface", interfaces["plan_task"]["outputs"])
            self.assertIn("llm_execution_plan", interfaces["plan_task"]["outputs"])

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
            self.assertEqual(payload["recommended_run_profile"], "preset_real_run")
            self.assertEqual(payload["agent_decision"]["mode"], "deterministic_preset_run")
            self.assertIn("stats_agent", payload["selected_agent_sequence"])
            self.assertEqual(payload["follow_up_questions"], [])
            self.assertEqual(payload["conversation_turns"], 2)
            self.assertIn("deterministic_preset_run", payload["agent_reply"])
            self.assertIn("llm_execution_plan", payload)
            self.assertIn("analysis_family_route", payload)
            self.assertIn("paper_spec_surface", payload)
            self.assertIn("analysis_spec_surface", payload)
            self.assertIn("cox_regression", payload["analysis_family_route"]["native_supported_families"])
            self.assertEqual(payload["paper_spec_surface"]["surface_kind"], "paper_spec_surface")
            self.assertEqual(payload["analysis_spec_surface"]["surface_kind"], "analysis_spec_surface")
            self.assertTrue(payload["analysis_family_route_path"].endswith("analysis_family_route.json"))
            self.assertTrue(payload["paper_spec_surface_path"].endswith("paper_spec_surface.json"))
            self.assertTrue(payload["analysis_spec_surface_path"].endswith("analysis_spec_surface.json"))
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "analysis_family_route.json").exists()
            )
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "paper_spec_surface.json").exists()
            )
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "analysis_spec_surface.json").exists()
            )

            contract = payload["task_contract"]
            self.assertEqual(contract["meta"]["execution_backend"], "deterministic_bridge")
            self.assertGreater(contract["meta"]["semantic_mapped_variable_count"], 0)
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "task_contract.json").exists()
            )
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "agent_decision.json").exists()
            )
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "agent_execution_plan.md").exists()
            )
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-test-openclaw" / "agent_reply.md").exists()
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
            self.assertEqual(payload["agent_decision"]["mode"], "deterministic_preset_run")
            self.assertEqual(payload["recommended_run_profile"], "preset_real_run")

    def test_plan_task_returns_follow_up_questions_when_contract_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            _write_text(
                project_root / "papers" / "minimal.md",
                "MIMIC observational study in ICU patients.\n",
            )
            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/minimal.md",
                instructions="Please read the paper and prepare a plan.",
                session_id="session-incomplete-contract",
                use_llm=False,
            )

            self.assertIn("exposure_variables", payload["missing_high_impact_fields"])
            self.assertIn("outcome_variables", payload["missing_high_impact_fields"])
            self.assertEqual(payload["agent_decision"]["mode"], "needs_contract_completion")
            self.assertEqual(payload["recommended_run_profile"], "plan_only")
            self.assertGreaterEqual(len(payload["follow_up_questions"]), 2)
            question_fields = {item["field"] for item in payload["follow_up_questions"]}
            self.assertIn("exposure_variables", question_fields)
            self.assertIn("outcome_variables", question_fields)

    def test_continue_session_applies_answers_and_promotes_to_preset_route(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            _write_text(
                project_root / "papers" / "minimal.md",
                "MIMIC observational study in ICU patients.\n",
            )
            initial = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/minimal.md",
                instructions="Please read the paper and prepare a plan.",
                session_id="session-follow-up",
                use_llm=False,
            )

            self.assertEqual(initial["agent_decision"]["mode"], "needs_contract_completion")
            self.assertGreaterEqual(len(initial["follow_up_questions"]), 2)

            updated = continue_session(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                session_id="session-follow-up",
                answers={
                    "exposure_variables": "TyG index",
                    "outcome_variables": "in-hospital mortality, ICU mortality",
                    "models": "Cox, Kaplan-Meier, RCS, subgroup",
                    "outputs": "baseline table, KM figure, RCS figure, subgroup figure",
                    "cohort_logic": "adult sepsis patients in the first ICU stay",
                },
            )

            self.assertEqual(updated["missing_high_impact_fields"], [])
            self.assertEqual(updated["agent_decision"]["mode"], "deterministic_preset_run")
            self.assertEqual(updated["recommended_run_profile"], "preset_real_run")
            self.assertEqual(updated["preset"]["key"], "mimic_tyg_sepsis")
            self.assertEqual(updated["follow_up_questions"], [])
            self.assertGreaterEqual(updated["conversation_turns"], 4)
            self.assertIn("preset_real_run", updated["agent_reply"])

            contract = updated["task_contract"]
            mapped_fields = {
                item["dataset_field"]
                for item in contract["variables"]
                if item.get("dataset_field")
            }
            self.assertIn("analysis_dataset.tyg_index", mapped_fields)

            session_payload = json.loads(
                (project_root / "shared" / "sessions" / "session-follow-up" / "session_state.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertGreaterEqual(len(session_payload["messages"]), 4)
            self.assertEqual(session_payload["messages"][0]["role"], "user")
            self.assertEqual(session_payload["messages"][-1]["role"], "assistant")
            self.assertEqual(session_payload["messages"][-1]["kind"], "agent_reply")

    def test_plan_task_marks_trajectory_paper_as_experimental_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/trajectory.md",
                instructions="Please read the paper and prepare the reproduction plan.",
                session_id="session-trajectory-planning",
                use_llm=False,
            )

            contract = payload["task_contract"]
            model_families = [item["family"] for item in contract["models"]]
            output_names = [item["name"] for item in contract["outputs"]]
            outcome_names = [item["name"] for item in contract["variables"] if item["role"] == "outcome"]

            self.assertIsNone(payload["preset"])
            self.assertEqual(payload["study_template"]["key"], "longitudinal_trajectory_survival")
            self.assertEqual(payload["agent_decision"]["mode"], "agentic_execution")
            self.assertEqual(payload["recommended_run_profile"], "agentic_repro")
            self.assertTrue(payload["execution_supported"])
            self.assertEqual(payload["execution_backend"], "trajectory_python_bridge")
            self.assertEqual(payload["paper_target_dataset_version"], "MIMIC-IV v2.0")
            self.assertEqual(payload["execution_environment_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(payload["execution_year_window"], "2008-2019")
            self.assertTrue(payload["dataset_version_mismatch"])
            self.assertEqual(payload["missing_high_impact_fields"], [])
            self.assertIn("trajectory_mixture_model", model_families)
            self.assertIn("kaplan_meier", model_families)
            self.assertIn("cox_regression", model_families)
            self.assertIn("trajectory_table", output_names)
            self.assertIn("trajectory_figure", output_names)
            self.assertIn("km_figure", output_names)
            self.assertIn("model_results_table", output_names)
            self.assertIn("mortality_30d", outcome_names)
            self.assertTrue(contract["meta"]["requires_longitudinal_trajectory_modeling"])
            self.assertEqual(contract["meta"]["experimental_profile"], "mimic_hr_trajectory_sepsis")
            self.assertEqual(contract["meta"]["paper_target_dataset_version"], "MIMIC-IV v2.0")
            self.assertEqual(contract["meta"]["execution_environment_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(contract["meta"]["execution_year_window"], "2008-2019")
            self.assertIn("latent_growth_mixture_model", contract["meta"]["paper_required_methods"])
            self.assertEqual(contract["meta"]["engine_supported_trajectory_backend"], "python_only_mixture_route_v1")

            missing_capabilities = set(payload["agent_decision"]["missing_capabilities"])
            self.assertIn("paper-identical LGMM not implemented", missing_capabilities)
            self.assertIn("raw-event fidelity not guaranteed", missing_capabilities)
            self.assertIn("missing-data handling not paper-identical", missing_capabilities)
            self.assertIn("trajectory", payload["agent_reply"].lower())
            self.assertIn("python", payload["agent_reply"].lower())
            self.assertIn("LGMM", payload["agent_reply"])

    def test_plan_task_routes_stroke_tyg_paper_to_experimental_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/stroke_tyg.md",
                instructions="Please reproduce this TyG sepsis study from MIMIC.",
                session_id="session-stroke-tyg",
                use_llm=False,
            )

            self.assertIsNone(payload["preset"])
            self.assertTrue(payload["execution_supported"])
            self.assertEqual(payload["execution_backend"], "profile_survival_bridge")
            self.assertEqual(payload["paper_target_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(payload["recommended_run_profile"], "agentic_repro")
            self.assertEqual(payload["agent_decision"]["mode"], "agentic_execution")
            self.assertNotIn("preset", payload["task_contract"]["meta"])
            self.assertEqual(payload["task_contract"]["meta"]["paper_target_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(payload["task_contract"]["meta"]["experimental_profile"], "mimic_tyg_stroke_nondiabetic")
            self.assertIn("kaplan_meier", payload["task_contract"]["meta"]["required_analysis_families"])
            self.assertIn("restricted_cubic_spline", payload["task_contract"]["meta"]["required_analysis_families"])
            self.assertIn("fidelity", payload["task_contract"]["meta"])

    def test_sql_adapter_alias_keeps_stroke_tyg_contract_on_mimic_execution_route(self) -> None:
        contract = TaskContract(
            task_id="stroke-sql-alias",
            title="Stroke TyG SQL alias normalization",
            dataset=DatasetSpec(
                name="MIMIC-IV",
                adapter="sql",
                source_type="relational_database",
                connector_env_prefix="MIMIC_PG",
                version="MIMIC-IV v3.1",
                schemas=["mimiciv_hosp", "mimiciv_icu", "mimiciv_derived"],
            ),
            cohort=CohortSpec(population="non-diabetic adult ischemic stroke ICU patients"),
            variables=[
                VariableSpec(name="tyg_index", role=VariableRole.EXPOSURE, required=True),
                VariableSpec(name="mortality_30d", role=VariableRole.OUTCOME, required=True),
                VariableSpec(name="time_to_event_30d_days", role=VariableRole.TIME, required=True),
            ],
            models=[
                ModelSpec(
                    name="cox_regression",
                    family="cox_regression",
                    exposure_variables=["tyg_index"],
                    outcome_variables=["mortality_30d"],
                    time_variable="time_to_event_30d_days",
                )
            ],
            outputs=[OutputSpec(name="km_figure", kind="km_figure", fmt="png", required=True)],
            meta={
                "experimental_profile": "mimic_tyg_stroke_nondiabetic",
                "paper_target_dataset_version": "MIMIC-IV v3.1",
                "execution_environment_dataset_version": "MIMIC-IV v3.1",
                "configured_dataset_version": "MIMIC-IV v3.1",
                "execution_year_window": "2008-2022",
            },
        )

        adapter = get_dataset_adapter(contract.dataset.adapter)
        support = adapter.describe_contract(contract)

        self.assertEqual(adapter.name, "mimic_iv")
        self.assertTrue(support.execution_supported)
        self.assertEqual(support.execution_backend, "profile_survival_bridge")
        self.assertIn("MIMIC-IV", " ".join(support.notes))

    def test_plan_task_extracts_nlr_paper_source_dataset_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/nlr.md",
                instructions="Please read the paper and prepare the reproduction plan.",
                session_id="session-nlr-version",
                use_llm=False,
            )

            self.assertEqual(payload["paper_target_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(payload["execution_environment_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(payload["execution_year_window"], "2008-2019")
            self.assertFalse(payload["dataset_version_mismatch"])

    def test_plan_task_extracts_arf_hyphenated_dataset_version_without_false_year_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/arf.md",
                instructions="Please read the paper and prepare the reproduction plan.",
                session_id="session-arf-version",
                use_llm=False,
            )

            self.assertEqual(payload["paper_target_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(payload["execution_environment_dataset_version"], "MIMIC-IV v3.1")
            self.assertEqual(payload["execution_year_window"], "")
            self.assertFalse(payload["dataset_version_mismatch"])

    def test_handle_openclaw_request_plan_only_returns_contract_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = handle_openclaw_request(
                project_root=project_root,
                request={
                    "paper_path": "papers/paper.md",
                    "instructions": "Please plan the reproduction from this paper.",
                    "session_id": "session-openclaw-plan",
                    "use_llm": False,
                    "run_mode": "plan_only",
                    "config_path": "configs/agentic.example.yaml",
                },
            )

            self.assertEqual(payload["session_id"], "session-openclaw-plan")
            self.assertEqual(payload["run_profile_used"], "plan_only")
            self.assertEqual(payload["agent_decision"]["mode"], "deterministic_preset_run")
            self.assertTrue(payload["execution_supported"])
            self.assertNotIn("execution", payload)
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-openclaw-plan" / "task_contract.json").exists()
            )

    def test_handle_openclaw_request_agentic_repro_auto_runs_when_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = handle_openclaw_request(
                project_root=project_root,
                request={
                    "paper_path": "papers/paper.md",
                    "instructions": "Please execute this supported preset route.",
                    "session_id": "session-openclaw-run",
                    "use_llm": False,
                    "run_mode": "agentic_repro",
                    "dry_run": True,
                    "config_path": "configs/agentic.example.yaml",
                },
            )

            self.assertEqual(payload["session_id"], "session-openclaw-run")
            self.assertEqual(payload["run_profile_used"], "agentic_repro")
            self.assertIn("execution", payload)
            self.assertIn(payload["execution"]["status"], {"success", "blocked"})
            self.assertEqual(payload["status"], payload["execution"]["status"])
            self.assertIn("artifacts", payload)
            self.assertTrue(
                (project_root / "shared" / "sessions" / "session-openclaw-run" / "session_state.json").exists()
            )

    def test_handle_openclaw_request_continue_only_updates_existing_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            _write_text(
                project_root / "papers" / "minimal.md",
                "MIMIC observational study in ICU patients.\n",
            )
            initial = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.example.yaml",
                paper_path="papers/minimal.md",
                instructions="Prepare a plan.",
                session_id="session-openclaw-follow-up",
                use_llm=False,
            )
            self.assertEqual(initial["agent_decision"]["mode"], "needs_contract_completion")

            updated = handle_openclaw_request(
                project_root=project_root,
                request={
                    "session_id": "session-openclaw-follow-up",
                    "run_mode": "plan_only",
                    "config_path": "configs/agentic.example.yaml",
                    "answers": {
                        "exposure_variables": "TyG index",
                        "outcome_variables": "in-hospital mortality",
                        "models": "Cox, Kaplan-Meier, RCS, subgroup",
                        "outputs": "baseline table, KM figure, RCS figure",
                        "cohort_logic": "adult sepsis patients in the first ICU stay",
                    },
                },
            )

            self.assertEqual(updated["run_profile_used"], "plan_only")
            self.assertEqual(updated["missing_high_impact_fields"], [])
            self.assertEqual(updated["agent_decision"]["mode"], "deterministic_preset_run")
            self.assertEqual(updated["recommended_run_profile"], "preset_real_run")
            self.assertNotIn("execution", updated)

    def test_handle_openclaw_request_surfaces_unknown_field_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            payload = handle_openclaw_request(
                project_root=project_root,
                request={
                    "paper_path": "papers/paper.md",
                    "instructions": "Please plan this paper.",
                    "session_id": "session-openclaw-warning",
                    "run_mode": "plan_only",
                    "config_path": "configs/agentic.example.yaml",
                    "use_llm": False,
                    "unexpected_field": "should be ignored",
                },
            )

            self.assertEqual(payload["run_profile_used"], "plan_only")
            self.assertIn("request_warnings", payload)
            self.assertIn("unexpected_field", " ".join(payload["request_warnings"]))

    def test_handle_openclaw_request_rejects_invalid_boolean_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            with self.assertRaisesRegex(ValueError, "use_llm"):
                handle_openclaw_request(
                    project_root=project_root,
                    request={
                        "paper_path": "papers/paper.md",
                        "instructions": "Please plan this paper.",
                        "session_id": "session-openclaw-bool",
                        "run_mode": "plan_only",
                        "config_path": "configs/agentic.example.yaml",
                        "use_llm": "maybe",
                    },
                )

    def test_get_openclaw_request_template_returns_expected_defaults(self) -> None:
        template = get_openclaw_request_template("agentic_repro")
        self.assertEqual(template["run_mode"], "agentic_repro")
        self.assertEqual(template["config_path"], "configs/openclaw.agentic.yaml")
        self.assertIn("instructions", template)

    def test_get_openclaw_request_template_rejects_unknown_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "Invalid template mode"):
            get_openclaw_request_template("bad_mode")

    def test_plan_task_surfaces_dataset_version_mismatch_for_trajectory_paper(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            config_text = (project_root / "configs" / "agentic.example.yaml").read_text(encoding="utf-8")
            _write_text(
                project_root / "configs" / "agentic.mimic22.yaml",
                config_text.replace("MIMIC-IV v3.1", "MIMIC-IV v2.2"),
            )

            payload = plan_task(
                project_root=project_root,
                config_path=project_root / "configs" / "agentic.mimic22.yaml",
                paper_path="papers/trajectory.md",
                instructions="Please read the paper and prepare the reproduction plan.",
                session_id="session-trajectory-mismatch",
                use_llm=False,
            )

            self.assertTrue(payload["dataset_version_mismatch"])
            self.assertEqual(payload["configured_dataset_version"], "MIMIC-IV v2.2")
            self.assertEqual(payload["execution_environment_dataset_version"], "MIMIC-IV v2.2")
            self.assertEqual(payload["paper_target_dataset_version"], "MIMIC-IV v2.0")
            self.assertEqual(payload["execution_year_window"], "2008-2019")
            self.assertTrue(payload["agent_decision"]["dataset_version_mismatch"])
            self.assertIn("version mismatch", payload["agent_reply"].lower())


class HybridScaffoldRunnerTests(unittest.TestCase):
    def test_runner_emits_hybrid_scaffold_bundle_for_llm_compiled_families(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            config = load_pipeline_config(project_root / "configs" / "agentic.example.yaml")
            runner = AgentRunner(project_root=project_root, config=config)
            contract = TaskContract(
                task_id="task-hybrid-arf",
                title="ARF nomogram scaffold test",
                source_paper_path="papers/arf.md",
                instructions="Prepare an executable scaffold for an ARF prediction paper.",
                dataset=DatasetSpec(name="MIMIC-IV", adapter="mimic_iv", version="MIMIC-IV v3.1"),
                cohort=CohortSpec(
                    population="adult ICU patients with acute respiratory failure",
                    inclusion_criteria=["Age >= 18 years and first ICU admission with acute respiratory failure"],
                    first_stay_only=True,
                    min_age=18,
                ),
                variables=[
                    VariableSpec(name="albumin", role=VariableRole.EXPOSURE),
                    VariableSpec(name="mortality_28d", role=VariableRole.OUTCOME),
                    VariableSpec(name="age", role=VariableRole.CONTROL, required=False),
                    VariableSpec(name="sex", role=VariableRole.CONTROL, required=False),
                ],
                models=[
                    ModelSpec(
                        name="cox",
                        family="cox_regression",
                        exposure_variables=["albumin"],
                        outcome_variables=["mortality_28d"],
                        control_variables=["age", "sex"],
                    ),
                    ModelSpec(
                        name="logistic",
                        family="logistic_regression",
                        exposure_variables=["albumin"],
                        outcome_variables=["mortality_28d"],
                        control_variables=["age", "sex"],
                    ),
                ],
                outputs=[
                    OutputSpec(name="model_results_table", kind="model_results_table", fmt="csv"),
                    OutputSpec(name="roc_figure", kind="roc_figure", fmt="png", required=False),
                ],
                meta={
                    "paper_required_methods": [
                        "nomogram",
                        "LASSO",
                        "calibration curve",
                        "DCA",
                    ],
                    "paper_target_dataset_version": "MIMIC-IV v3.1",
                    "execution_environment_dataset_version": "MIMIC-IV v3.1",
                },
            )

            result = runner.run_task(contract, dry_run=False)

            self.assertEqual(result.summary.status.value, "success")
            stats_step = next(item for item in result.summary.step_results if item.step == "stats_agent")
            self.assertEqual(stats_step.status.value, "success")
            self.assertIn("Hybrid scaffold artifacts", stats_step.message)

            session_dir = project_root / "shared" / "sessions" / result.session_id
            analysis_spec_path = session_dir / "analysis_spec.json"
            figure_spec_path = session_dir / "figure_spec.json"
            executor_scaffold_path = session_dir / "executor_scaffold.py"

            self.assertTrue(analysis_spec_path.exists())
            self.assertTrue(figure_spec_path.exists())
            self.assertTrue(executor_scaffold_path.exists())

            analysis_spec = json.loads(analysis_spec_path.read_text(encoding="utf-8"))
            figure_spec = json.loads(figure_spec_path.read_text(encoding="utf-8"))
            executor_scaffold = executor_scaffold_path.read_text(encoding="utf-8")

            self.assertIn("logistic_regression", analysis_spec["llm_compiled_families"])
            self.assertIn("nomogram_prediction", analysis_spec["llm_compiled_families"])
            self.assertIn("lasso_feature_selection", analysis_spec["llm_compiled_families"])
            self.assertIn("calibration_curve", analysis_spec["llm_compiled_families"])
            self.assertIn("decision_curve_analysis", analysis_spec["llm_compiled_families"])
            self.assertIn("statsmodels", figure_spec["preferred_libraries"])
            self.assertIn("matplotlib", figure_spec["preferred_libraries"])
            self.assertIn("run_logistic_regression", executor_scaffold)
            self.assertIn("run_nomogram_prediction", executor_scaffold)
            self.assertIn("run_calibration_curve", executor_scaffold)
            self.assertIn("run_decision_curve_analysis", executor_scaffold)

            output_names = set(stats_step.outputs)
            self.assertIn(f"shared/sessions/{result.session_id}/model_blueprint.json", output_names)
            self.assertIn(f"shared/sessions/{result.session_id}/analysis_spec.json", output_names)
            self.assertIn(f"shared/sessions/{result.session_id}/figure_spec.json", output_names)
            self.assertIn(f"shared/sessions/{result.session_id}/executor_scaffold.py", output_names)


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
            self.assertIn("longitudinal_trajectory_execution", manifest.skills)
            self.assertEqual(
                manifest.skills["mimic_cohort_execution"].execution_preference,
                "deterministic_preferred",
            )


class CodexSkillBridgeTests(unittest.TestCase):
    def test_load_codex_skill_bridge_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_temp_project(project_root)
            bridge = load_codex_skill_bridge_manifest(project_root)

            self.assertEqual(bridge["repo_skill_root"], ".codex/skills")
            self.assertEqual(bridge["project_skill_count"], 46)
            self.assertEqual(bridge["category_group_count"], 5)
            self.assertEqual(bridge["stage_bridge_count"], 5)
            self.assertIn("clinical_statistics_and_econometrics", bridge["category_groups"])
            self.assertIn("workflow_and_repo_ops", bridge["category_groups"])
            self.assertIn("codex-autoresearch", bridge["category_groups"]["workflow_and_repo_ops"]["skills"])
            self.assertIn("omics_reproduction", bridge["future_extension_lanes"])


if __name__ == "__main__":
    unittest.main()

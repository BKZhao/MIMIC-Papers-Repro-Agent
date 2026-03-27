from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from repro_agent.agentic.runner import AgentRunner  # noqa: E402
from repro_agent.config import load_pipeline_config  # noqa: E402
from repro_agent.contracts import (  # noqa: E402
    CohortSpec,
    DatasetSpec,
    ModelSpec,
    OutputSpec,
    TaskContract,
    VariableRole,
    VariableSpec,
)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _scaffold_project(project_root: Path) -> None:
    _write_text(
        project_root / "configs" / "agentic.example.yaml",
        (ROOT / "configs" / "agentic.example.yaml").read_text(encoding="utf-8"),
    )
    _write_text(
        project_root / "papers" / "arf.md",
        textwrap.dedent(
            """
            Clinical MIMIC paper for acute respiratory failure.
            The paper reports logistic regression and ROC analysis for 28-day mortality.
            """
        ).strip()
        + "\n",
    )


def _write_synthetic_binary_dataset(path: Path, n_rows: int = 180) -> None:
    rng = np.random.default_rng(42)
    age = rng.normal(67, 11, n_rows).clip(18, 95)
    bmi = rng.normal(27.5, 5.2, n_rows).clip(14, 55)
    albumin = rng.normal(3.3, 0.55, n_rows).clip(1.4, 5.5)
    sex = rng.choice(["M", "F"], size=n_rows, p=[0.58, 0.42])
    linear = -2.8 + 0.028 * age - 0.75 * albumin + 0.018 * bmi + 0.22 * (sex == "M")
    probability = 1.0 / (1.0 + np.exp(-linear))
    mortality = rng.binomial(1, probability)

    df = pd.DataFrame(
        {
            "age": age.round(2),
            "bmi": bmi.round(2),
            "albumin": albumin.round(2),
            "sex": sex,
            "mortality_28d": mortality.astype(int),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_synthetic_cox_dataset(path: Path, n_rows: int = 220) -> None:
    rng = np.random.default_rng(123)
    age = rng.normal(66, 10, n_rows).clip(18, 95)
    bmi = rng.normal(27, 4.8, n_rows).clip(14, 55)
    albumin = rng.normal(3.2, 0.5, n_rows).clip(1.2, 5.2)
    sex = rng.choice(["M", "F"], size=n_rows, p=[0.57, 0.43])

    # Simulate time-to-event process with informative censoring at 28 days.
    linear = 0.028 * age - 0.62 * albumin + 0.012 * bmi + 0.18 * (sex == "M")
    baseline_hazard = 0.055
    event_time = -np.log(np.clip(rng.uniform(size=n_rows), 1e-6, 1.0)) / (baseline_hazard * np.exp(linear))
    censor_time = rng.uniform(7.0, 28.0, size=n_rows)
    observed_time = np.minimum(event_time, censor_time)
    event = (event_time <= censor_time).astype(int)

    df = pd.DataFrame(
        {
            "age": age.round(2),
            "bmi": bmi.round(2),
            "albumin": albumin.round(2),
            "sex": sex,
            "time_to_event_28d_days": np.maximum(observed_time, 1.0 / 24.0).round(4),
            "mortality_28d": event.astype(int),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


class HybridBinaryRunnerTests(unittest.TestCase):
    def test_runner_executes_hybrid_binary_outcome_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_project(project_root)
            dataset_rel = "shared/demo_binary_analysis_dataset.csv"
            _write_synthetic_binary_dataset(project_root / dataset_rel)

            config = load_pipeline_config(project_root / "configs" / "agentic.example.yaml")
            runner = AgentRunner(project_root=project_root, config=config)
            contract = TaskContract(
                task_id="task-binary-001",
                title="Synthetic ARF binary outcome test",
                source_paper_path="papers/arf.md",
                instructions="Run logistic regression and ROC on the prepared analysis dataset.",
                dataset=DatasetSpec(name="MIMIC-IV", adapter="mimic_iv", version="MIMIC-IV v3.1"),
                cohort=CohortSpec(
                    population="adult ICU patients with acute respiratory failure",
                    inclusion_criteria=["Age >= 18 years"],
                    first_stay_only=True,
                    min_age=18,
                ),
                variables=[
                    VariableSpec(name="albumin", role=VariableRole.EXPOSURE),
                    VariableSpec(name="mortality_28d", role=VariableRole.OUTCOME),
                    VariableSpec(name="age", role=VariableRole.CONTROL, required=False),
                    VariableSpec(name="sex", role=VariableRole.CONTROL, required=False),
                    VariableSpec(name="bmi", role=VariableRole.CONTROL, required=False),
                ],
                models=[
                    ModelSpec(
                        name="model_1",
                        family="logistic_regression",
                        exposure_variables=["albumin"],
                        outcome_variables=["mortality_28d"],
                    ),
                    ModelSpec(
                        name="model_2",
                        family="logistic_regression",
                        exposure_variables=["albumin"],
                        outcome_variables=["mortality_28d"],
                        control_variables=["age", "sex", "bmi"],
                    ),
                ],
                outputs=[
                    OutputSpec(name="baseline_table", kind="baseline_table", fmt="csv"),
                    OutputSpec(name="model_results_table", kind="model_results_table", fmt="csv"),
                    OutputSpec(name="roc_figure", kind="roc_figure", fmt="png"),
                    OutputSpec(name="reproduction_report", kind="reproduction_report", fmt="md"),
                ],
                meta={
                    "analysis_dataset_rel": dataset_rel,
                    "paper_target_dataset_version": "MIMIC-IV v3.1",
                    "execution_environment_dataset_version": "MIMIC-IV v3.1",
                    "paper_required_methods": ["logistic regression", "ROC"],
                },
            )

            decision = runner.build_agent_decision(contract)
            self.assertTrue(decision.execution_supported)
            self.assertEqual(decision.execution_backend, "hybrid_binary_runner")

            result = runner.run_task(contract, dry_run=False)

            self.assertEqual(result.summary.status.value, "success")
            stats_step = next(item for item in result.summary.step_results if item.step == "stats_agent")
            self.assertEqual(stats_step.status.value, "success")
            self.assertIn("hybrid binary-outcome runner", stats_step.message)

            session_dir = project_root / "shared" / "sessions" / result.session_id / "binary_outcome"
            results_dir = project_root / "results" / "sessions" / result.session_id / "binary_outcome"
            baseline_path = session_dir / "baseline_table.csv"
            logistic_path = session_dir / "logistic_models.csv"
            roc_summary_path = session_dir / "roc_summary.json"
            report_path = session_dir / "reproduction_report.md"
            roc_plot_path = results_dir / "roc.png"

            self.assertTrue(baseline_path.exists())
            self.assertTrue(logistic_path.exists())
            self.assertTrue(roc_summary_path.exists())
            self.assertTrue(report_path.exists())
            self.assertTrue(roc_plot_path.exists())

            logistic_df = pd.read_csv(logistic_path)
            self.assertIn("model_1", set(logistic_df["model_name"]))
            self.assertIn("model_2", set(logistic_df["model_name"]))
            self.assertIn("odds_ratio", logistic_df.columns)
            self.assertTrue((logistic_df["odds_ratio"] > 0).all())

            roc_summary = json.loads(roc_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(roc_summary["model_count"], 2)
            self.assertGreater(float(roc_summary["best_model_auc"]), 0.6)

            output_names = set(stats_step.outputs)
            self.assertIn(f"shared/sessions/{result.session_id}/binary_outcome/logistic_models.csv", output_names)
            self.assertIn(f"results/sessions/{result.session_id}/binary_outcome/roc.png", output_names)

    def test_runner_executes_extended_prediction_figures_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_project(project_root)
            dataset_rel = "shared/demo_binary_analysis_dataset.csv"
            _write_synthetic_binary_dataset(project_root / dataset_rel, n_rows=220)

            config = load_pipeline_config(project_root / "configs" / "agentic.example.yaml")
            runner = AgentRunner(project_root=project_root, config=config)
            contract = TaskContract(
                task_id="task-binary-extended-001",
                title="Synthetic ARF extended figure test",
                source_paper_path="papers/arf.md",
                instructions=(
                    "Run logistic regression, ROC, calibration curve, decision curve, "
                    "distribution figure, and heatmap on the prepared analysis dataset."
                ),
                dataset=DatasetSpec(name="MIMIC-IV", adapter="mimic_iv", version="MIMIC-IV v3.1"),
                cohort=CohortSpec(
                    population="adult ICU patients with acute respiratory failure",
                    inclusion_criteria=["Age >= 18 years"],
                    first_stay_only=True,
                    min_age=18,
                ),
                variables=[
                    VariableSpec(name="albumin", role=VariableRole.EXPOSURE),
                    VariableSpec(name="mortality_28d", role=VariableRole.OUTCOME),
                    VariableSpec(name="age", role=VariableRole.CONTROL, required=False),
                    VariableSpec(name="sex", role=VariableRole.CONTROL, required=False),
                    VariableSpec(name="bmi", role=VariableRole.CONTROL, required=False),
                ],
                models=[
                    ModelSpec(
                        name="model_1",
                        family="logistic_regression",
                        exposure_variables=["albumin"],
                        outcome_variables=["mortality_28d"],
                    ),
                    ModelSpec(
                        name="model_2",
                        family="logistic_regression",
                        exposure_variables=["albumin"],
                        outcome_variables=["mortality_28d"],
                        control_variables=["age", "sex", "bmi"],
                    ),
                ],
                outputs=[
                    OutputSpec(name="baseline_table", kind="baseline_table", fmt="csv"),
                    OutputSpec(name="model_results_table", kind="model_results_table", fmt="csv"),
                    OutputSpec(name="roc_figure", kind="roc_figure", fmt="png"),
                    OutputSpec(name="calibration_figure", kind="calibration_figure", fmt="png"),
                    OutputSpec(name="decision_curve_figure", kind="decision_curve_figure", fmt="png"),
                    OutputSpec(
                        name="distribution_figure",
                        kind="distribution_figure",
                        fmt="png",
                        options={"paper_style_hints": ["violin_plot", "raw_points_overlay"]},
                    ),
                    OutputSpec(name="heatmap_figure", kind="heatmap_figure", fmt="png"),
                    OutputSpec(name="reproduction_report", kind="reproduction_report", fmt="md"),
                ],
                meta={
                    "analysis_dataset_rel": dataset_rel,
                    "paper_target_dataset_version": "MIMIC-IV v3.1",
                    "execution_environment_dataset_version": "MIMIC-IV v3.1",
                    "paper_required_methods": [
                        "logistic regression",
                        "ROC",
                        "calibration curve",
                        "decision curve",
                        "heatmap",
                    ],
                },
            )

            decision = runner.build_agent_decision(contract)
            self.assertTrue(decision.execution_supported)
            self.assertEqual(decision.execution_backend, "hybrid_binary_runner")

            result = runner.run_task(contract, dry_run=False)

            self.assertEqual(result.summary.status.value, "success")
            stats_step = next(item for item in result.summary.step_results if item.step == "stats_agent")
            figure_step = next(item for item in result.summary.step_results if item.step == "figure_agent")
            self.assertEqual(stats_step.status.value, "success")
            self.assertEqual(figure_step.status.value, "success")

            session_dir = project_root / "shared" / "sessions" / result.session_id / "binary_outcome"
            results_dir = project_root / "results" / "sessions" / result.session_id / "binary_outcome"

            calibration_summary_path = session_dir / "calibration_summary.json"
            dca_summary_path = session_dir / "dca_summary.json"
            distribution_summary_path = session_dir / "distribution_summary.json"
            heatmap_summary_path = session_dir / "heatmap_summary.json"
            heatmap_matrix_path = session_dir / "heatmap_matrix.csv"
            calibration_plot_path = results_dir / "calibration_curve.png"
            decision_curve_plot_path = results_dir / "decision_curve.png"
            distribution_plot_path = results_dir / "distribution.png"
            heatmap_plot_path = results_dir / "heatmap.png"

            for path in (
                calibration_summary_path,
                dca_summary_path,
                distribution_summary_path,
                heatmap_summary_path,
                heatmap_matrix_path,
                calibration_plot_path,
                decision_curve_plot_path,
                distribution_plot_path,
                heatmap_plot_path,
            ):
                self.assertTrue(path.exists(), msg=f"Missing artifact: {path}")

            calibration_summary = json.loads(calibration_summary_path.read_text(encoding="utf-8"))
            dca_summary = json.loads(dca_summary_path.read_text(encoding="utf-8"))
            distribution_summary = json.loads(distribution_summary_path.read_text(encoding="utf-8"))
            heatmap_summary = json.loads(heatmap_summary_path.read_text(encoding="utf-8"))

            self.assertGreater(float(calibration_summary["brier_score"]), 0.0)
            self.assertGreater(len(calibration_summary["calibration_points"]), 2)
            self.assertGreater(len(dca_summary["threshold_points"]), 5)
            self.assertIn(distribution_summary["plot_type"], {"violin_plot", "box_plot", "grouped_bar"})
            self.assertTrue(heatmap_summary["matrix_written"])

            figure_outputs = set(figure_step.outputs)
            self.assertIn(f"results/sessions/{result.session_id}/binary_outcome/calibration_curve.png", figure_outputs)
            self.assertIn(f"results/sessions/{result.session_id}/binary_outcome/decision_curve.png", figure_outputs)
            self.assertIn(f"results/sessions/{result.session_id}/binary_outcome/distribution.png", figure_outputs)
            self.assertIn(f"results/sessions/{result.session_id}/binary_outcome/heatmap.png", figure_outputs)

    def test_runner_executes_cox_regression_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _scaffold_project(project_root)
            dataset_rel = "shared/demo_cox_analysis_dataset.csv"
            _write_synthetic_cox_dataset(project_root / dataset_rel, n_rows=260)

            config = load_pipeline_config(project_root / "configs" / "agentic.example.yaml")
            runner = AgentRunner(project_root=project_root, config=config)
            contract = TaskContract(
                task_id="task-binary-cox-001",
                title="Synthetic ARF Cox execution test",
                source_paper_path="papers/arf.md",
                instructions="Run Cox regression and export hazard-ratio table on the prepared analysis dataset.",
                dataset=DatasetSpec(name="MIMIC-IV", adapter="mimic_iv", version="MIMIC-IV v3.1"),
                cohort=CohortSpec(
                    population="adult ICU patients with acute respiratory failure",
                    inclusion_criteria=["Age >= 18 years"],
                    first_stay_only=True,
                    min_age=18,
                ),
                variables=[
                    VariableSpec(name="albumin", role=VariableRole.EXPOSURE),
                    VariableSpec(name="mortality_28d", role=VariableRole.OUTCOME),
                    VariableSpec(name="time_to_event_28d_days", role=VariableRole.TIME),
                    VariableSpec(name="age", role=VariableRole.CONTROL, required=False),
                    VariableSpec(name="sex", role=VariableRole.CONTROL, required=False),
                    VariableSpec(name="bmi", role=VariableRole.CONTROL, required=False),
                ],
                models=[
                    ModelSpec(
                        name="cox_model_1",
                        family="cox_regression",
                        exposure_variables=["albumin"],
                        outcome_variables=["mortality_28d"],
                        control_variables=["age", "sex", "bmi"],
                        time_variable="time_to_event_28d_days",
                    ),
                ],
                outputs=[
                    OutputSpec(name="baseline_table", kind="baseline_table", fmt="csv"),
                    OutputSpec(name="cox_results_table", kind="cox_results_table", fmt="csv"),
                    OutputSpec(name="reproduction_report", kind="reproduction_report", fmt="md"),
                ],
                meta={
                    "analysis_dataset_rel": dataset_rel,
                    "paper_target_dataset_version": "MIMIC-IV v3.1",
                    "execution_environment_dataset_version": "MIMIC-IV v3.1",
                    "paper_required_methods": ["cox regression"],
                },
            )

            decision = runner.build_agent_decision(contract)
            self.assertTrue(decision.execution_supported)
            self.assertEqual(decision.execution_backend, "hybrid_binary_runner")

            result = runner.run_task(contract, dry_run=False)

            self.assertEqual(result.summary.status.value, "success")
            stats_step = next(item for item in result.summary.step_results if item.step == "stats_agent")
            self.assertEqual(stats_step.status.value, "success")

            session_dir = project_root / "shared" / "sessions" / result.session_id / "binary_outcome"
            cox_path = session_dir / "cox_results_table.csv"
            cox_summary_path = session_dir / "cox_summary.json"
            report_path = session_dir / "reproduction_report.md"

            self.assertTrue(cox_path.exists())
            self.assertTrue(cox_summary_path.exists())
            self.assertTrue(report_path.exists())

            cox_df = pd.read_csv(cox_path)
            self.assertIn("cox_model_1", set(cox_df["model_name"]))
            self.assertIn("hazard_ratio", cox_df.columns)
            self.assertTrue((cox_df["hazard_ratio"] > 0).all())

            cox_summary = json.loads(cox_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(int(cox_summary.get("model_count", 0)), 1)
            self.assertGreater(float(cox_summary["best_model_c_index"]), 0.5)

            output_names = set(stats_step.outputs)
            self.assertIn(f"shared/sessions/{result.session_id}/binary_outcome/cox_results_table.csv", output_names)


if __name__ == "__main__":
    unittest.main()

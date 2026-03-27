from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from repro_agent.analysis.profile_stats import run_profile_stats  # noqa: E402


class TrajectoryProfileStatsTests(unittest.TestCase):
    def test_run_profile_stats_dispatches_trajectory_workflow(self) -> None:
        rng = np.random.default_rng(42)
        patterns = {
            "p1": np.array([80, 80, 81, 80, 79, 80, 80, 81, 80, 80], dtype=float),
            "p2": np.array([92, 92, 93, 92, 91, 92, 92, 93, 92, 92], dtype=float),
            "p3": np.array([112, 108, 104, 100, 96, 92, 88, 84, 80, 76], dtype=float),
            "p4": np.array([110, 110, 111, 111, 110, 109, 110, 111, 110, 110], dtype=float),
            "p5": np.array([68, 64, 70, 66, 63, 67, 71, 65, 64, 66], dtype=float),
            "p6": np.array([126, 128, 130, 127, 129, 128, 127, 129, 130, 128], dtype=float),
        }
        rows: list[dict[str, object]] = []
        stay_id = 1
        for class_index, (pattern_name, base_pattern) in enumerate(patterns.items(), start=1):
            for sample_index in range(14):
                panel = base_pattern + rng.normal(0, 1.5, size=10)
                mortality = 1 if sample_index < class_index + 2 else 0
                duration = float(max(1.0, 30.0 - class_index * 2 - sample_index * 0.2))
                row: dict[str, object] = {
                    "subject_id": stay_id,
                    "hadm_id": 100000 + stay_id,
                    "stay_id": stay_id,
                    "age": 58 + class_index * 3 + sample_index % 4,
                    "bmi": 24.0 + class_index * 0.4,
                    "sofa_score": 6 + class_index,
                    "apsiii": 45 + class_index * 5,
                    "gcs_score": 14 - (class_index % 2),
                    "temperature": 36.5 + class_index * 0.2,
                    "hemoglobin": 10.5 + class_index * 0.3,
                    "neutrophils_abs": 7.0 + class_index * 0.8,
                    "pt": 13.0 + class_index * 0.4,
                    "ptt": 32.0 + class_index * 0.7,
                    "lactate": 1.8 + class_index * 0.2,
                    "gender": "M" if sample_index % 2 == 0 else "F",
                    "race": "WHITE" if sample_index % 3 == 0 else "BLACK",
                    "marital_status": "MARRIED" if sample_index % 4 else "SINGLE",
                    "peripheral_vascular_disease": 1 if class_index in {3, 4} else 0,
                    "liver_disease": 1 if class_index in {4, 6} else 0,
                    "mechanical_ventilation": 1 if class_index >= 4 else 0,
                    "renal_replacement_therapy": 1 if class_index >= 5 else 0,
                    "vasopressor_use": 1 if class_index >= 3 else 0,
                    "beta_blocker_use": 1 if pattern_name == "p1" else 0,
                    "charlson_score": 3 + class_index * 0.5,
                    "mortality_30d": mortality,
                    "time_to_event_30d_days": duration,
                }
                for hour_index, value in enumerate(panel, start=1):
                    row[f"heart_rate_hour_{hour_index}"] = round(float(value), 6)
                row["heart_rate_initial"] = row["heart_rate_hour_1"]
                row["heart_rate_mean_10h"] = round(float(np.mean(panel)), 6)
                rows.append(row)
                stay_id += 1

        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            (project_root / "shared").mkdir(parents=True, exist_ok=True)
            dataset_path = project_root / "shared" / "mimic_hr_trajectory_sepsis_analysis_dataset.csv"
            missingness_path = project_root / "shared" / "mimic_hr_trajectory_sepsis_analysis_missingness.json"
            pd.DataFrame(rows).to_csv(dataset_path, index=False)
            missingness_path.write_text(json.dumps({"missingness": []}), encoding="utf-8")

            result = run_profile_stats(
                project_root=project_root,
                profile_key="mimic_hr_trajectory_sepsis",
                analysis_dataset_rel="shared/mimic_hr_trajectory_sepsis_analysis_dataset.csv",
                missingness_rel="shared/mimic_hr_trajectory_sepsis_analysis_missingness.json",
                artifact_subdir="runs/test_trajectory",
            )

            self.assertEqual(result.profile_key, "mimic_hr_trajectory_sepsis")
            self.assertEqual(result.row_count, len(rows))
            self.assertIn("class_count", result.metrics)
            self.assertIn("logrank_p_value", result.metrics)
            expected_artifacts = {
                "shared/runs/test_trajectory/mimic_hr_trajectory_sepsis_trajectory_table.csv",
                "shared/runs/test_trajectory/mimic_hr_trajectory_sepsis_cox_models.csv",
                "shared/runs/test_trajectory/mimic_hr_trajectory_sepsis_reproduction_report.md",
                "results/runs/test_trajectory/mimic_hr_trajectory_sepsis_trajectory.png",
                "results/runs/test_trajectory/mimic_hr_trajectory_sepsis_km.png",
            }
            self.assertTrue(expected_artifacts.issubset(set(result.outputs)))
            for rel_path in expected_artifacts:
                self.assertTrue((project_root / rel_path).exists(), rel_path)

            report_text = (
                project_root
                / "shared"
                / "runs"
                / "test_trajectory"
                / "mimic_hr_trajectory_sepsis_reproduction_report.md"
            ).read_text(encoding="utf-8")
            self.assertIn("LGMM", report_text)
            self.assertIn("method-aligned", report_text)
            self.assertIn("derived-first with raw fallback", report_text)
            self.assertIn("Current cohort vs paper", report_text)


if __name__ == "__main__":
    unittest.main()

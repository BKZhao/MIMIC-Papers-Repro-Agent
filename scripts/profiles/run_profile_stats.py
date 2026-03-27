#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from repro_agent.analysis.profile_stats import run_profile_stats  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run paper-profile-driven statistics and figure generation.")
    parser.add_argument("--project-root", type=str, default=".")
    parser.add_argument("--profile", type=str, required=True)
    parser.add_argument("--analysis-dataset", type=str, default="")
    parser.add_argument("--missingness", type=str, default="")
    parser.add_argument("--artifact-subdir", type=str, default="")
    parser.add_argument("--execution-environment-version", type=str, default="")
    parser.add_argument("--execution-year-window", type=str, default="")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    analysis_dataset_rel = args.analysis_dataset or f"shared/{args.profile}_analysis_dataset.csv"
    missingness_rel = args.missingness or f"shared/{args.profile}_analysis_missingness.json"

    result = run_profile_stats(
        project_root=project_root,
        profile_key=args.profile,
        analysis_dataset_rel=analysis_dataset_rel,
        missingness_rel=missingness_rel,
        artifact_subdir=args.artifact_subdir,
        execution_environment_dataset_version=args.execution_environment_version,
        execution_year_window=args.execution_year_window,
    )
    print(
        json.dumps(
            {
                "profile": result.profile_key,
                "analysis_dataset": result.analysis_dataset_rel,
                "row_count": result.row_count,
                "artifact_subdir": args.artifact_subdir,
                "outputs": result.outputs,
                "metrics": result.metrics,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

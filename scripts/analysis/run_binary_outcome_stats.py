#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from repro_agent.analysis.binary_outcome import run_binary_outcome_analysis_workflow  # noqa: E402
from repro_agent.contracts import TaskContract  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run hybrid binary-outcome statistics for a prepared analysis dataset.")
    parser.add_argument("--project-root", type=str, default=".")
    parser.add_argument("--contract", type=str, required=True, help="Path to a TaskContract JSON file.")
    parser.add_argument("--analysis-dataset", type=str, required=True)
    parser.add_argument("--artifact-subdir", type=str, default="")
    parser.add_argument("--missingness", type=str, default="")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    contract_path = (project_root / args.contract).resolve()
    contract = TaskContract.from_dict(json.loads(contract_path.read_text(encoding="utf-8")))
    result = run_binary_outcome_analysis_workflow(
        project_root=project_root,
        contract=contract,
        analysis_dataset_rel=args.analysis_dataset,
        artifact_subdir=args.artifact_subdir,
        missingness_rel=args.missingness,
    )
    print(
        json.dumps(
            {
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

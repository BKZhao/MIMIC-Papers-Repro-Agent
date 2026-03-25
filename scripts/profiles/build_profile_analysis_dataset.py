#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from repro_agent.analysis_dataset_sql import (  # noqa: E402
    build_nlr_analysis_dataset_sql,
    build_tyg_analysis_dataset_sql,
)
from repro_agent.cohort_sql import PAPER_MIMIC_NLR_PROFILE, PAPER_MIMIC_TYG_PROFILE, TygSepsisCohortProfile  # noqa: E402
from repro_agent.paper_profiles import get_paper_execution_profile  # noqa: E402


def load_env_file(project_root: Path) -> None:
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def pg_cfg() -> dict[str, str]:
    return {
        "host": os.getenv("MIMIC_PG_HOST", "").strip(),
        "port": os.getenv("MIMIC_PG_PORT", "5432").strip(),
        "db": os.getenv("MIMIC_PG_DB", "").strip(),
        "user": os.getenv("MIMIC_PG_USER", "").strip(),
        "password": os.getenv("MIMIC_PG_PASSWORD", "").strip(),
        "sslmode": os.getenv("MIMIC_PG_SSLMODE", "disable").strip(),
    }


def conn_string(cfg: dict[str, str]) -> str:
    return (
        f"host={cfg['host']} port={cfg['port']} user={cfg['user']} "
        f"dbname={cfg['db']} sslmode={cfg['sslmode']}"
    )


def run_scalar(cfg: dict[str, str], sql: str) -> str:
    cmd = ["psql", conn_string(cfg), "-v", "ON_ERROR_STOP=1", "-tA", "-c", sql]
    env = os.environ.copy()
    env["PGPASSWORD"] = cfg["password"]
    completed = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "psql scalar query failed")
    return completed.stdout.strip()


def run_copy(cfg: dict[str, str], sql: str, output_path: Path) -> list[dict[str, str]]:
    cmd = ["psql", conn_string(cfg), "-v", "ON_ERROR_STOP=1", "-c", f"COPY ({sql}) TO STDOUT WITH CSV HEADER"]
    env = os.environ.copy()
    env["PGPASSWORD"] = cfg["password"]
    completed = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "psql copy failed")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(completed.stdout, encoding="utf-8")
    return list(csv.DictReader(io.StringIO(completed.stdout)))


def detect_sepsis3_support(cfg: dict[str, str]) -> tuple[bool, bool]:
    table_sql = """
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'mimiciv_derived'
      AND table_name = 'sepsis3'
) AS has_table;
"""
    has_table = run_scalar(cfg, table_sql).lower() in {"t", "true", "1"}
    if not has_table:
        return False, False

    col_sql = """
SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'mimiciv_derived'
      AND table_name = 'sepsis3'
      AND column_name = 'sepsis3'
) AS has_flag;
"""
    has_flag = run_scalar(cfg, col_sql).lower() in {"t", "true", "1"}
    return True, has_flag


def summarize_missingness(rows: list[dict[str, str]], non_predictor_columns: set[str]) -> dict[str, object]:
    if not rows:
        return {
            "row_count": 0,
            "predictor_columns": 0,
            "columns_above_30_percent_missing": [],
            "columns_above_50_percent_missing": [],
            "missingness": [],
        }

    fieldnames = list(rows[0].keys())
    predictor_columns = [name for name in fieldnames if name not in non_predictor_columns]
    summary_rows: list[dict[str, object]] = []

    for column in predictor_columns:
        missing_count = sum(1 for row in rows if not str(row.get(column, "")).strip())
        missing_ratio = missing_count / len(rows)
        summary_rows.append(
            {
                "column": column,
                "missing_count": missing_count,
                "missing_ratio": round(missing_ratio, 6),
                "above_30_percent_missing": missing_ratio > 0.30,
                "above_50_percent_missing": missing_ratio > 0.50,
            }
        )

    summary_rows.sort(key=lambda item: (-float(item["missing_ratio"]), str(item["column"])))
    return {
        "row_count": len(rows),
        "predictor_columns": len(predictor_columns),
        "columns_above_30_percent_missing": [
            item["column"] for item in summary_rows if bool(item["above_30_percent_missing"])
        ],
        "columns_above_50_percent_missing": [
            item["column"] for item in summary_rows if bool(item["above_50_percent_missing"])
        ],
        "missingness": summary_rows,
    }


def apply_profile_overrides(
    profile: TygSepsisCohortProfile,
    *,
    admit_year_start: int | None,
    admit_year_end: int | None,
) -> TygSepsisCohortProfile:
    if admit_year_start is None and admit_year_end is None:
        return profile
    return replace(
        profile,
        admit_year_start=admit_year_start,
        admit_year_end=admit_year_end,
    )


def resolve_profile_sql(
    profile_key: str,
    *,
    mode: str,
    has_sepsis3_flag: bool,
    cohort_profile: TygSepsisCohortProfile,
) -> tuple[str, str, set[str]]:
    if profile_key == "mimic_tyg_sepsis":
        return (
            cohort_profile.name,
            build_tyg_analysis_dataset_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=cohort_profile),
            {
                "subject_id",
                "hadm_id",
                "stay_id",
                "sepsis3_flag",
                "suspected_infection_time",
                "sofa_time",
                "tyg_quartile",
                "hospital_survival_hours",
                "icu_survival_hours",
                "hospital_los_hours",
                "icu_los_hours",
                "in_hospital_mortality",
                "icu_mortality",
            },
        )
    if profile_key == "mimic_nlr_sepsis_elderly":
        return (
            cohort_profile.name,
            build_nlr_analysis_dataset_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=cohort_profile),
            {
                "subject_id",
                "hadm_id",
                "stay_id",
                "sepsis3_flag",
                "suspected_infection_time",
                "sofa_time",
                "nlr_charttime",
                "nlr_quartile",
                "mortality_28d",
                "time_to_event_28d_hours",
                "time_to_event_28d_days",
            },
        )
    raise SystemExit(f"Unsupported profile key: {profile_key}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a paper-profile-driven analysis dataset from MIMIC-IV.")
    parser.add_argument("--project-root", type=str, default=".")
    parser.add_argument("--profile", type=str, required=True)
    parser.add_argument("--output", type=str, default="")
    parser.add_argument("--missingness-output", type=str, default="")
    parser.add_argument("--sepsis-source", choices=["auto", "derived", "icd"], default="auto")
    parser.add_argument("--admit-year-start", type=int, default=None)
    parser.add_argument("--admit-year-end", type=int, default=None)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    load_env_file(project_root)
    cfg = pg_cfg()
    required = [key for key in ("host", "port", "db", "user", "password") if not cfg[key]]
    if required:
        raise SystemExit(f"Missing DB env vars: {required}")

    has_sepsis3_table, has_sepsis3_flag = detect_sepsis3_support(cfg)
    requested_mode = args.sepsis_source
    if requested_mode == "auto":
        mode = "derived" if has_sepsis3_table else "icd"
    else:
        mode = requested_mode

    if mode == "derived" and not has_sepsis3_table:
        raise SystemExit(
            "Requested --sepsis-source=derived but mimiciv_derived.sepsis3 is not available. "
            "Use --sepsis-source=icd or --sepsis-source=auto for fallback mode."
        )

    profile = get_paper_execution_profile(args.profile)
    if args.profile == "mimic_tyg_sepsis":
        cohort_profile = apply_profile_overrides(
            PAPER_MIMIC_TYG_PROFILE,
            admit_year_start=args.admit_year_start,
            admit_year_end=args.admit_year_end,
        )
    elif args.profile == "mimic_nlr_sepsis_elderly":
        cohort_profile = apply_profile_overrides(
            PAPER_MIMIC_NLR_PROFILE,
            admit_year_start=args.admit_year_start,
            admit_year_end=args.admit_year_end,
        )
    else:
        raise SystemExit(f"Unsupported profile key: {args.profile}")
    output_rel = args.output or f"shared/{args.profile}_analysis_dataset.csv"
    missingness_rel = args.missingness_output or f"shared/{args.profile}_analysis_missingness.json"

    profile_name, dataset_sql, non_predictor_columns = resolve_profile_sql(
        args.profile,
        mode=mode,
        has_sepsis3_flag=has_sepsis3_flag,
        cohort_profile=cohort_profile,
    )
    output_path = project_root / output_rel
    missingness_path = project_root / missingness_rel

    rows = run_copy(cfg, dataset_sql, output_path)
    missingness = summarize_missingness(rows, non_predictor_columns=non_predictor_columns)
    payload = {
        "profile": profile.as_dict() if profile is not None else {"key": args.profile},
        "cohort_profile": profile_name,
        "sepsis_source": mode,
        "sepsis_source_requested": requested_mode,
        "row_count": len(rows),
        "column_count": len(rows[0]) if rows else 0,
        "cohort_filters": {
            "admit_year_start": cohort_profile.admit_year_start,
            "admit_year_end": cohort_profile.admit_year_end,
        },
        "missingness": missingness,
    }

    missingness_path.parent.mkdir(parents=True, exist_ok=True)
    missingness_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nAnalysis dataset written to: {output_path}")
    print(f"Missingness report written to: {missingness_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

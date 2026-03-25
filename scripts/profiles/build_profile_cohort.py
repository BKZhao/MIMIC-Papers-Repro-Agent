#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from repro_agent.cohort_sql import (  # noqa: E402
    PAPER_MIMIC_NLR_PROFILE,
    PAPER_MIMIC_TYG_PROFILE,
    TygSepsisCohortProfile,
    build_nlr_sepsis_cohort_sql,
    build_nlr_sepsis_funnel_sql,
    build_tyg_sepsis_cohort_sql,
    build_tyg_sepsis_funnel_sql,
)
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


def run_sql(cfg: dict[str, str], sql: str) -> str:
    cmd = ["psql", conn_string(cfg), "-v", "ON_ERROR_STOP=1", "-c", sql]
    env = os.environ.copy()
    env["PGPASSWORD"] = cfg["password"]
    completed = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "psql command failed")
    return completed.stdout


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


def parse_counts(output: str) -> dict[str, int]:
    lines = [ln.rstrip() for ln in output.splitlines() if ln.strip()]
    if len(lines) < 3:
        return {}
    headers = [h.strip() for h in lines[0].split("|")]
    values = [v.strip() for v in lines[2].split("|")]
    result: dict[str, int] = {}
    for header, value in zip(headers, values):
        try:
            result[header] = int(value)
        except ValueError:
            continue
    return result


def build_alignment_payload(profile_key: str, rows: list[dict[str, str]]) -> dict[str, object]:
    profile = get_paper_execution_profile(profile_key)
    expected_final_n = profile.expected_final_n if profile is not None else 0
    payload: dict[str, object] = {
        "profile": profile.as_dict() if profile is not None else {"key": profile_key},
        "actual": {
            "n_final": len(rows),
        },
        "target": {
            "n_final": expected_final_n,
        },
        "deviations": {
            "n_final": len(rows) - expected_final_n,
        },
    }
    quartile_column = profile.predictor_quartile_column if profile is not None else ""
    if quartile_column and rows and quartile_column in rows[0]:
        counts = Counter(str(row.get(quartile_column, "")).strip() for row in rows)
        payload["actual"][quartile_column] = {name: counts.get(name, 0) for name in ("Q1", "Q2", "Q3", "Q4")}
    return payload


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
) -> tuple[str, str, str]:
    if profile_key == "mimic_tyg_sepsis":
        return (
            cohort_profile.name,
            build_tyg_sepsis_cohort_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=cohort_profile),
            build_tyg_sepsis_funnel_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=cohort_profile),
        )
    if profile_key == "mimic_nlr_sepsis_elderly":
        return (
            cohort_profile.name,
            build_nlr_sepsis_cohort_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=cohort_profile),
            build_nlr_sepsis_funnel_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=cohort_profile),
        )
    raise SystemExit(f"Unsupported profile key: {profile_key}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a paper-profile-driven cohort from MIMIC-IV.")
    parser.add_argument("--project-root", type=str, default=".")
    parser.add_argument("--profile", type=str, required=True)
    parser.add_argument("--output", type=str, default="")
    parser.add_argument("--funnel-output", type=str, default="")
    parser.add_argument("--alignment-output", type=str, default="")
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
    output_rel = args.output or f"shared/{args.profile}_cohort.csv"
    funnel_rel = args.funnel_output or f"shared/{args.profile}_cohort_funnel.json"
    alignment_rel = args.alignment_output or f"shared/{args.profile}_cohort_alignment.json"

    profile_name, cohort_sql, funnel_sql = resolve_profile_sql(
        args.profile,
        mode=mode,
        has_sepsis3_flag=has_sepsis3_flag,
        cohort_profile=cohort_profile,
    )
    output_path = project_root / output_rel
    funnel_path = project_root / funnel_rel
    alignment_path = project_root / alignment_rel

    rows = run_copy(cfg, cohort_sql, output_path)
    funnel_counts = parse_counts(run_sql(cfg, funnel_sql))
    alignment_payload = build_alignment_payload(args.profile, rows)
    funnel_payload = {
        "profile": profile.as_dict() if profile is not None else {"key": args.profile},
        "cohort_profile": profile_name,
        "sepsis_source": mode,
        "sepsis_source_requested": requested_mode,
        "n_output_rows": len(rows),
        "counts": funnel_counts,
        "notes": {
            "derived_available": has_sepsis3_table,
            "derived_has_sepsis3_flag": has_sepsis3_flag,
            "admit_year_start": cohort_profile.admit_year_start,
            "admit_year_end": cohort_profile.admit_year_end,
        },
    }

    funnel_path.parent.mkdir(parents=True, exist_ok=True)
    alignment_path.parent.mkdir(parents=True, exist_ok=True)
    funnel_path.write_text(json.dumps(funnel_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    alignment_path.write_text(json.dumps(alignment_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(funnel_payload, indent=2, ensure_ascii=False))
    print(f"\nCohort written to: {output_path}")
    print(f"Funnel written to: {funnel_path}")
    print(f"Alignment written to: {alignment_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

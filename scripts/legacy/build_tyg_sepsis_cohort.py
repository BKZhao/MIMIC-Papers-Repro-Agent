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
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from repro_agent.cohort_sql import (
    GLUCOSE_ITEMIDS,
    PAPER_MIMIC_TYG_PROFILE,
    TG_ITEMIDS,
    build_tyg_sepsis_cohort_sql,
    build_tyg_sepsis_funnel_sql,
)
from repro_agent.paper_contract import build_paper_alignment_contract


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
    for h, v in zip(headers, values):
        try:
            result[h] = int(v)
        except ValueError:
            continue
    return result


def build_alignment_payload(rows: list[dict[str, str]], contract: dict[str, object]) -> dict[str, object]:
    cohort_targets = dict(contract.get("cohort_targets", {}))
    target_n = int(cohort_targets.get("final_n", 0) or 0)
    target_hosp = int(cohort_targets.get("in_hospital_mortality_n", 0) or 0)
    target_icu = int(cohort_targets.get("icu_mortality_n", 0) or 0)
    target_q = dict(cohort_targets.get("tyg_quartile_target_counts", {}))

    quartile_counts = Counter(str(row.get("tyg_quartile", "")).strip() for row in rows)
    tyg_values: dict[str, list[float]] = {"Q1": [], "Q2": [], "Q3": [], "Q4": []}
    for row in rows:
        quartile = str(row.get("tyg_quartile", "")).strip()
        try:
            value = float(str(row.get("tyg_index", "")).strip())
        except ValueError:
            continue
        if quartile in tyg_values:
            tyg_values[quartile].append(value)

    observed_bounds: dict[str, dict[str, float | None]] = {}
    for quartile, values in tyg_values.items():
        if values:
            observed_bounds[quartile] = {
                "min_tyg_index": round(min(values), 6),
                "max_tyg_index": round(max(values), 6),
            }
        else:
            observed_bounds[quartile] = {"min_tyg_index": None, "max_tyg_index": None}

    n_final = len(rows)
    n_hospital = sum(int(str(row.get("hospital_mortality", "0")).strip() or "0") for row in rows)
    n_icu = sum(int(str(row.get("icu_mortality", "0")).strip() or "0") for row in rows)

    return {
        "actual": {
            "n_final": n_final,
            "n_hospital_death": n_hospital,
            "n_icu_death": n_icu,
            "tyg_quartile_counts": {quartile: quartile_counts.get(quartile, 0) for quartile in ("Q1", "Q2", "Q3", "Q4")},
            "observed_tyg_bounds": observed_bounds,
        },
        "target": {
            "n_final": target_n,
            "n_hospital_death": target_hosp,
            "n_icu_death": target_icu,
            "tyg_quartile_counts": target_q,
        },
        "deviations": {
            "n_final": n_final - target_n,
            "n_hospital_death": n_hospital - target_hosp,
            "n_icu_death": n_icu - target_icu,
            "tyg_quartile_counts": {
                quartile: quartile_counts.get(quartile, 0) - int(target_q.get(quartile, 0) or 0)
                for quartile in ("Q1", "Q2", "Q3", "Q4")
            },
        },
        "flags": {
            "quartile_counts_match_target": all(
                quartile_counts.get(quartile, 0) == int(target_q.get(quartile, 0) or 0)
                for quartile in ("Q1", "Q2", "Q3", "Q4")
            ),
            "n_final_match_target": n_final == target_n,
            "death_counts_match_target": n_hospital == target_hosp and n_icu == target_icu,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the TyG sepsis cohort according to the paper criteria.")
    parser.add_argument("--project-root", type=str, default=".")
    parser.add_argument("--output", type=str, default="shared/cohort.csv")
    parser.add_argument("--funnel-output", type=str, default="shared/cohort_funnel.json")
    parser.add_argument("--alignment-output", type=str, default="shared/cohort_alignment.json")
    parser.add_argument("--sepsis-source", choices=["auto", "derived", "icd"], default="auto")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    load_env_file(project_root)
    cfg = pg_cfg()
    required = [k for k in ("host", "port", "db", "user", "password") if not cfg[k]]
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

    output_path = project_root / args.output
    funnel_path = project_root / args.funnel_output
    alignment_path = project_root / args.alignment_output

    cohort_query = build_tyg_sepsis_cohort_sql(
        mode=mode,
        has_sepsis3_flag=has_sepsis3_flag,
        profile=PAPER_MIMIC_TYG_PROFILE,
    )
    funnel_query = build_tyg_sepsis_funnel_sql(
        mode=mode,
        has_sepsis3_flag=has_sepsis3_flag,
        profile=PAPER_MIMIC_TYG_PROFILE,
    )

    rows = run_copy(cfg, cohort_query, output_path)
    n_rows = len(rows)
    funnel = parse_counts(run_sql(cfg, funnel_query))
    contract = build_paper_alignment_contract()
    alignment_payload = build_alignment_payload(rows, contract)

    funnel_payload = {
        "sepsis_source": mode,
        "sepsis_source_requested": requested_mode,
        "n_output_rows": n_rows,
        "counts": funnel,
        "notes": {
            "derived_available": has_sepsis3_table,
            "derived_has_sepsis3_flag": has_sepsis3_flag,
            "cohort_profile": PAPER_MIMIC_TYG_PROFILE.name,
            "min_age_years": PAPER_MIMIC_TYG_PROFILE.min_age_years,
            "max_age_years": PAPER_MIMIC_TYG_PROFILE.max_age_years,
            "max_admit_to_icu_hours": PAPER_MIMIC_TYG_PROFILE.max_admit_to_icu_hours,
            "min_icu_los_hours": PAPER_MIMIC_TYG_PROFILE.min_icu_los_hours,
            "require_hospital_time_records": PAPER_MIMIC_TYG_PROFILE.require_hospital_time_records,
            "lab_anchor": PAPER_MIMIC_TYG_PROFILE.lab_anchor,
            "baseline_lab_window_hours": PAPER_MIMIC_TYG_PROFILE.baseline_lab_window_hours,
            "tg_itemids": list(TG_ITEMIDS),
            "glucose_itemids": list(GLUCOSE_ITEMIDS),
        },
    }
    funnel_path.parent.mkdir(parents=True, exist_ok=True)
    funnel_path.write_text(json.dumps(funnel_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    alignment_path.parent.mkdir(parents=True, exist_ok=True)
    alignment_path.write_text(json.dumps(alignment_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(funnel_payload, indent=2, ensure_ascii=False))
    print("\nAlignment:")
    print(json.dumps(alignment_payload, indent=2, ensure_ascii=False))
    print(f"\nCohort written to: {output_path}")
    print(f"Funnel written to: {funnel_path}")
    print(f"Alignment written to: {alignment_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

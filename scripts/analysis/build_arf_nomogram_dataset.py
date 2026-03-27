#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
from pathlib import Path
from typing import Any


NON_PREDICTOR_COLUMNS: set[str] = {
    "subject_id",
    "hadm_id",
    "stay_id",
    "gender",
    "arf_primary_icd_code",
    "arf_primary_icd_version",
    "time_to_event_28d_days",
    "mortality_28day",
}


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


def run_copy(
    cfg: dict[str, str],
    sql: str,
    output_path: Path,
    *,
    write_csv: bool = True,
) -> list[dict[str, str]]:
    cmd = ["psql", conn_string(cfg), "-v", "ON_ERROR_STOP=1", "-f", "-"]
    env = os.environ.copy()
    env["PGPASSWORD"] = cfg["password"]
    completed = subprocess.run(
        cmd,
        env=env,
        text=True,
        capture_output=True,
        input=f"COPY ({sql}) TO STDOUT WITH CSV HEADER;\n",
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "psql copy failed")
    if write_csv:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(completed.stdout, encoding="utf-8")
    return list(csv.DictReader(io.StringIO(completed.stdout)))


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "<na>", "nat"}


def summarize_missingness(rows: list[dict[str, str]]) -> dict[str, object]:
    if not rows:
        return {
            "row_count": 0,
            "predictor_columns": 0,
            "columns_above_30_percent_missing": [],
            "columns_above_50_percent_missing": [],
            "missingness": [],
        }

    fieldnames = list(rows[0].keys())
    predictor_columns = [name for name in fieldnames if name not in NON_PREDICTOR_COLUMNS]
    missing_rows: list[dict[str, object]] = []
    for column in predictor_columns:
        missing_count = sum(1 for row in rows if _is_missing_value(row.get(column)))
        missing_ratio = missing_count / len(rows)
        missing_rows.append(
            {
                "column": column,
                "missing_count": missing_count,
                "missing_ratio": round(missing_ratio, 6),
                "above_30_percent_missing": missing_ratio > 0.30,
                "above_50_percent_missing": missing_ratio > 0.50,
            }
        )
    missing_rows.sort(key=lambda item: (-float(item["missing_ratio"]), str(item["column"])))
    return {
        "row_count": len(rows),
        "predictor_columns": len(predictor_columns),
        "columns_above_30_percent_missing": [
            item["column"] for item in missing_rows if bool(item["above_30_percent_missing"])
        ],
        "columns_above_50_percent_missing": [
            item["column"] for item in missing_rows if bool(item["above_50_percent_missing"])
        ],
        "missingness": missing_rows,
    }


def summarize_arf_codes(rows: list[dict[str, str]], top_n: int = 30) -> dict[str, object]:
    if not rows:
        return {"unique_codes": 0, "top_codes": []}

    counts: dict[str, int] = {}
    event_counts: dict[str, int] = {}
    for row in rows:
        code = str(row.get("arf_primary_icd_code", "")).strip() or "UNKNOWN"
        counts[code] = counts.get(code, 0) + 1
        event_flag = str(row.get("mortality_28day", "")).strip().lower()
        if event_flag in {"1", "1.0", "true", "t"}:
            event_counts[code] = event_counts.get(code, 0) + 1

    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    top_codes = []
    for code, n in ordered[: max(1, top_n)]:
        events = int(event_counts.get(code, 0))
        top_codes.append(
            {
                "code": code,
                "n": int(n),
                "events": events,
                "event_rate": round((events / n), 6) if n > 0 else 0.0,
            }
        )

    return {"unique_codes": len(counts), "top_codes": top_codes}


def build_arf_dataset_sql() -> str:
    return """
WITH first_icu AS (
    SELECT DISTINCT ON (i.subject_id)
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY i.subject_id, i.intime
),
adult_first_icu AS (
    SELECT
        f.subject_id,
        f.hadm_id,
        f.stay_id,
        f.intime,
        f.outtime,
        p.anchor_age AS age,
        p.gender,
        a.deathtime,
        p.dod,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours
    FROM first_icu f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = f.hadm_id
    WHERE p.anchor_age > 18
),
arf_primary_diagnosis AS (
    SELECT DISTINCT ON (d.hadm_id)
        d.hadm_id,
        d.icd_code,
        d.icd_version
    FROM mimiciv_hosp.diagnoses_icd d
    WHERE d.seq_num = 1
      AND (
        (d.icd_version = 10 AND d.icd_code LIKE 'J96%')
        OR
        (d.icd_version = 9 AND d.icd_code IN ('51881', '51882', '51884', '518.81', '518.82', '518.84'))
      )
    ORDER BY d.hadm_id, d.icd_version DESC, d.icd_code
),
cohort AS (
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.intime,
        c.age,
        c.gender,
        dx.icd_code AS arf_primary_icd_code,
        dx.icd_version AS arf_primary_icd_version,
        CASE
            WHEN COALESCE(c.deathtime, c.dod) IS NOT NULL
             AND COALESCE(c.deathtime, c.dod) >= c.intime
             AND COALESCE(c.deathtime, c.dod) <= c.intime + INTERVAL '28 day'
            THEN GREATEST(
                EXTRACT(EPOCH FROM (COALESCE(c.deathtime, c.dod) - c.intime)) / 86400.0,
                1.0 / 24.0
            )
            ELSE 28.0
        END AS time_to_event_28d_days,
        CASE
            WHEN COALESCE(c.deathtime, c.dod) IS NOT NULL
             AND COALESCE(c.deathtime, c.dod) <= c.intime + INTERVAL '28 day'
            THEN 1 ELSE 0
        END AS mortality_28day
    FROM adult_first_icu c
    JOIN arf_primary_diagnosis dx
        ON dx.hadm_id = c.hadm_id
    WHERE c.icu_los_hours >= 48
)
SELECT
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    c.age,
    c.gender,
    c.arf_primary_icd_code,
    c.arf_primary_icd_version,
    c.time_to_event_28d_days,
    fl.wbc_max AS wbc,
    fl.glucose_max AS glucose,
    fdv.temperature_mean AS temperature,
    ch.metastatic_solid_tumor,
    ch.malignant_cancer,
    ch.diabetes_without_cc,
    ch.cerebrovascular_disease,
    ch.dementia,
    aps.apsiii,
    sap.sapsii,
    oas.oasis,
    c.mortality_28day
FROM cohort c
LEFT JOIN mimiciv_derived.first_day_lab fl
    ON fl.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.first_day_vitalsign fdv
    ON fdv.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.charlson ch
    ON ch.hadm_id = c.hadm_id
LEFT JOIN mimiciv_derived.apsiii aps
    ON aps.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.sapsii sap
    ON sap.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.oasis oas
    ON oas.stay_id = c.stay_id
ORDER BY c.subject_id, c.stay_id
"""


def build_arf_funnel_sql() -> str:
    return """
WITH first_icu AS (
    SELECT DISTINCT ON (i.subject_id)
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY i.subject_id, i.intime
),
adult_first_icu AS (
    SELECT
        f.subject_id,
        f.hadm_id,
        f.stay_id,
        f.intime,
        f.outtime,
        p.anchor_age AS age,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours
    FROM first_icu f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    WHERE p.anchor_age > 18
),
arf_primary_diagnosis AS (
    SELECT DISTINCT d.hadm_id
    FROM mimiciv_hosp.diagnoses_icd d
    WHERE d.seq_num = 1
      AND (
        (d.icd_version = 10 AND d.icd_code LIKE 'J96%')
        OR
        (d.icd_version = 9 AND d.icd_code IN ('51881', '51882', '51884', '518.81', '518.82', '518.84'))
      )
),
cohort AS (
    SELECT c.subject_id, c.hadm_id, c.stay_id
    FROM adult_first_icu c
    JOIN arf_primary_diagnosis dx
      ON dx.hadm_id = c.hadm_id
    WHERE c.icu_los_hours >= 48
),
analysis_rows AS (
    SELECT
        c.stay_id,
        fl.wbc_max AS wbc,
        fl.glucose_max AS glucose,
        fdv.temperature_mean AS temperature
    FROM cohort c
    LEFT JOIN mimiciv_derived.first_day_lab fl
      ON fl.stay_id = c.stay_id
    LEFT JOIN mimiciv_derived.first_day_vitalsign fdv
      ON fdv.stay_id = c.stay_id
)
SELECT stage, n
FROM (
    SELECT 1 AS ord, 'first_icu_subject_stays' AS stage, COUNT(*)::bigint AS n FROM first_icu
    UNION ALL
    SELECT 2 AS ord, 'adult_first_icu' AS stage, COUNT(*)::bigint AS n FROM adult_first_icu
    UNION ALL
    SELECT 3 AS ord, 'primary_arf_diagnosis' AS stage, COUNT(*)::bigint AS n FROM adult_first_icu a JOIN arf_primary_diagnosis d ON d.hadm_id = a.hadm_id
    UNION ALL
    SELECT 4 AS ord, 'icu_los_ge_48h' AS stage, COUNT(*)::bigint AS n FROM cohort
    UNION ALL
    SELECT 5 AS ord, 'core_predictors_nonnull' AS stage, COUNT(*)::bigint AS n
    FROM analysis_rows
    WHERE wbc IS NOT NULL AND glucose IS NOT NULL AND temperature IS NOT NULL
) s
ORDER BY ord
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Build ARF nomogram binary analysis dataset from MIMIC-IV.")
    parser.add_argument("--project-root", type=str, default=".")
    parser.add_argument("--output", type=str, default="shared/arf_nomogram/analysis_dataset.csv")
    parser.add_argument("--missingness-output", type=str, default="shared/arf_nomogram/analysis_missingness.json")
    parser.add_argument("--funnel-output", type=str, default="shared/arf_nomogram/cohort_funnel.json")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    load_env_file(project_root)
    cfg = pg_cfg()
    required = [key for key in ("host", "port", "db", "user", "password") if not cfg[key]]
    if required:
        raise SystemExit(f"Missing DB env vars: {required}")

    dataset_rows = run_copy(cfg, build_arf_dataset_sql(), (project_root / args.output).resolve(), write_csv=True)
    funnel_rows = run_copy(cfg, build_arf_funnel_sql(), (project_root / args.funnel_output).resolve(), write_csv=False)
    arf_code_summary = summarize_arf_codes(dataset_rows, top_n=30)
    funnel_path = (project_root / args.funnel_output).resolve()
    funnel_payload = {
        "profile": "mimic_arf_nomogram_v1",
        "stages": [
            {
                "stage": row.get("stage", ""),
                "n": int(row.get("n", "0") or 0),
            }
            for row in funnel_rows
        ],
        "arf_code_summary": arf_code_summary,
    }
    funnel_path.parent.mkdir(parents=True, exist_ok=True)
    funnel_path.write_text(json.dumps(funnel_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    missingness_payload = summarize_missingness(dataset_rows)
    missingness_path = (project_root / args.missingness_output).resolve()
    missingness_path.parent.mkdir(parents=True, exist_ok=True)
    missingness_path.write_text(
        json.dumps(
            {
                "profile": "mimic_arf_nomogram_v1",
                "row_count": len(dataset_rows),
                "missingness": missingness_payload.get("missingness", []),
                "columns_above_30_percent_missing": missingness_payload.get("columns_above_30_percent_missing", []),
                "columns_above_50_percent_missing": missingness_payload.get("columns_above_50_percent_missing", []),
                "predictor_columns": missingness_payload.get("predictor_columns", 0),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    event_count = sum(
        1
        for row in dataset_rows
        if str(row.get("mortality_28day", "")).strip() in {"1", "1.0", "true", "True", "t"}
    )
    event_rate = (event_count / len(dataset_rows)) if dataset_rows else 0.0
    payload = {
        "profile": "mimic_arf_nomogram_v1",
        "analysis_dataset": args.output,
        "missingness_output": args.missingness_output,
        "funnel_output": args.funnel_output,
        "row_count": len(dataset_rows),
        "event_count": event_count,
        "event_rate": round(event_rate, 6),
        "funnel": funnel_payload["stages"],
        "arf_code_summary": arf_code_summary,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

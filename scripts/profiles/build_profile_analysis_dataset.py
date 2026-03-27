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
from typing import Any

import pandas as pd

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from repro_agent.sql.analysis_dataset import (  # noqa: E402
    build_nlr_analysis_dataset_sql,
    build_tyg_stroke_analysis_dataset_sql,
    build_tyg_analysis_dataset_sql,
)
from repro_agent.sql.cohort import (  # noqa: E402
    PAPER_MIMIC_HEART_RATE_TRAJECTORY_PROFILE,
    PAPER_MIMIC_NLR_PROFILE,
    PAPER_MIMIC_TYG_STROKE_PROFILE,
    PAPER_MIMIC_TYG_PROFILE,
    TygSepsisCohortProfile,
)
from repro_agent.paper.profiles import get_paper_execution_profile  # noqa: E402


TRAJECTORY_PANEL_COLUMNS: tuple[str, ...] = tuple(f"heart_rate_hour_{index}" for index in range(1, 11))
TRAJECTORY_REQUIRED_COLUMNS: tuple[str, ...] = (
    "subject_id",
    "hadm_id",
    "stay_id",
    "mortality_30d",
    "time_to_event_30d_days",
    "hourly_measurement_count",
    *TRAJECTORY_PANEL_COLUMNS,
)
TRAJECTORY_FINAL_COLUMN_ORDER: tuple[str, ...] = (
    "subject_id",
    "hadm_id",
    "stay_id",
    "age",
    "gender",
    "race",
    "insurance",
    "marital_status",
    "height_cm",
    "weight_kg",
    "bmi",
    "sofa_score",
    "apsiii",
    "gcs_score",
    "temperature",
    "hemoglobin",
    "neutrophils_abs",
    "pt",
    "ptt",
    "lactate",
    "peripheral_vascular_disease",
    "cerebrovascular_disease",
    "liver_disease",
    "charlson_score",
    "mechanical_ventilation",
    "renal_replacement_therapy",
    "vasopressor_use",
    "beta_blocker_use",
    "heart_rate_initial",
    "heart_rate_mean_10h",
    "heart_rate_delta_10h",
    *TRAJECTORY_PANEL_COLUMNS,
    "sepsis3_flag",
    "suspected_infection_time",
    "sofa_time",
    "first_heart_rate_charttime",
    "hourly_measurement_count",
    "mortality_30d",
    "time_to_event_30d_days",
    "icu_los_days",
    "hospital_los_days",
)
TRAJECTORY_NON_PREDICTOR_COLUMNS: set[str] = {
    "subject_id",
    "hadm_id",
    "stay_id",
    "sepsis3_flag",
    "suspected_infection_time",
    "sofa_time",
    "first_heart_rate_charttime",
    "hourly_measurement_count",
    "mortality_30d",
    "time_to_event_30d_days",
    "icu_los_days",
    "hospital_los_days",
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


def run_scalar(cfg: dict[str, str], sql: str) -> str:
    cmd = ["psql", conn_string(cfg), "-v", "ON_ERROR_STOP=1", "-tA", "-c", sql]
    env = os.environ.copy()
    env["PGPASSWORD"] = cfg["password"]
    completed = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "psql scalar query failed")
    return completed.stdout.strip()


def run_copy(cfg: dict[str, str], sql: str, output_path: Path) -> list[dict[str, str]]:
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
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(completed.stdout, encoding="utf-8")
    return list(csv.DictReader(io.StringIO(completed.stdout)))


def run_query_df(cfg: dict[str, str], sql: str) -> pd.DataFrame:
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
        raise RuntimeError(completed.stderr.strip() or "psql dataframe query failed")
    if not completed.stdout.strip():
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(completed.stdout))


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


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "<na>", "nat"}


def summarize_missingness(rows: list[dict[str, Any]], non_predictor_columns: set[str]) -> dict[str, object]:
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
        missing_count = sum(1 for row in rows if _is_missing_value(row.get(column)))
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
    if profile_key == "mimic_tyg_stroke_nondiabetic":
        return (
            cohort_profile.name,
            build_tyg_stroke_analysis_dataset_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=cohort_profile),
            {
                "subject_id",
                "hadm_id",
                "stay_id",
                "insurance",
                "marital_status",
                "race",
                "gender",
                "tyg_quartile",
                "icu_mortality",
                "in_hospital_mortality",
                "mortality_30d",
                "mortality_90d",
                "mortality_180d",
                "mortality_1y",
                "time_to_icu_event_days",
                "time_to_in_hospital_event_days",
                "time_to_event_30d_days",
                "time_to_event_90d_days",
                "time_to_event_180d_days",
                "time_to_event_1y_days",
                "icu_los_days",
                "hospital_los_days",
            },
        )
    raise SystemExit(f"Unsupported profile key for SQL builder: {profile_key}")


def resolve_seed_cohort_path(project_root: Path, *, output_path: Path, profile_key: str) -> Path:
    candidates = [
        output_path.with_name("cohort.csv"),
        project_root / "shared" / "runs" / profile_key / "cohort.csv",
        project_root / f"shared/{profile_key}_cohort.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError(
        f"Unable to locate seed cohort CSV for {profile_key}. Checked: "
        + ", ".join(str(path) for path in candidates)
    )


def _trajectory_seed_values_sql(seed_df: pd.DataFrame) -> str:
    rows: list[str] = []
    for subject_id, hadm_id, stay_id in seed_df.loc[:, ["subject_id", "hadm_id", "stay_id"]].itertuples(index=False, name=None):
        rows.append(f"({int(subject_id)}, {int(hadm_id)}, {int(stay_id)})")
    return ",\n        ".join(rows)


def _trajectory_seed_cte_sql(seed_df: pd.DataFrame) -> str:
    return (
        "seed(subject_id, hadm_id, stay_id) AS (\n"
        "    VALUES\n"
        f"        {_trajectory_seed_values_sql(seed_df)}\n"
        ")"
    )


def _trajectory_seed_ctx_cte_sql(seed_df: pd.DataFrame) -> str:
    return f"""
{_trajectory_seed_cte_sql(seed_df)},
seed_ctx AS (
    SELECT
        s.subject_id,
        s.hadm_id,
        s.stay_id,
        i.intime,
        i.outtime,
        a.admittime,
        a.dischtime,
        a.deathtime,
        a.insurance
    FROM seed s
    JOIN mimiciv_icu.icustays i
        ON i.stay_id = s.stay_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = s.hadm_id
)
""".strip()


def _trajectory_context_sql(seed_df: pd.DataFrame) -> str:
    return f"""
WITH {_trajectory_seed_ctx_cte_sql(seed_df)}
SELECT
    sc.stay_id,
    sc.insurance
FROM seed_ctx sc
ORDER BY sc.stay_id
""".strip()


def _trajectory_scores_sql(seed_df: pd.DataFrame) -> str:
    return f"""
WITH {_trajectory_seed_cte_sql(seed_df)}
SELECT
    s.stay_id,
    round(fd_height.height::numeric, 3) AS height_cm,
    round(fd_weight.weight::numeric, 3) AS weight_kg,
    round(fd_sofa.sofa::numeric, 6) AS sofa_score_derived,
    round(aps.apsiii::numeric, 6) AS apsiii,
    round(fd_gcs.gcs_min::numeric, 6) AS gcs_score
FROM seed s
LEFT JOIN mimiciv_derived.first_day_height fd_height
    ON fd_height.stay_id = s.stay_id
LEFT JOIN mimiciv_derived.first_day_weight fd_weight
    ON fd_weight.stay_id = s.stay_id
LEFT JOIN mimiciv_derived.first_day_sofa fd_sofa
    ON fd_sofa.stay_id = s.stay_id
LEFT JOIN mimiciv_derived.apsiii aps
    ON aps.stay_id = s.stay_id
LEFT JOIN mimiciv_derived.first_day_gcs fd_gcs
    ON fd_gcs.stay_id = s.stay_id
ORDER BY s.stay_id
""".strip()


def _trajectory_vitals_labs_sql(seed_df: pd.DataFrame) -> str:
    return f"""
WITH {_trajectory_seed_ctx_cte_sql(seed_df)},
cbc_raw AS (
    SELECT
        sc.hadm_id,
        d.charttime,
        d.specimen_id,
        d.hemoglobin,
        ROW_NUMBER() OVER (
            PARTITION BY sc.hadm_id
            ORDER BY d.charttime NULLS LAST, d.specimen_id NULLS LAST
        ) AS rn
    FROM seed_ctx sc
    JOIN mimiciv_derived.complete_blood_count d
        ON d.hadm_id = sc.hadm_id
    WHERE d.charttime >= sc.intime
      AND d.charttime <= sc.intime + INTERVAL '24 hours'
      AND d.hemoglobin IS NOT NULL
),
cbc AS (
    SELECT
        hadm_id,
        hemoglobin
    FROM cbc_raw
    WHERE rn = 1
),
diff_raw AS (
    SELECT
        sc.hadm_id,
        d.charttime,
        d.specimen_id,
        d.neutrophils_abs,
        ROW_NUMBER() OVER (
            PARTITION BY sc.hadm_id
            ORDER BY d.charttime NULLS LAST, d.specimen_id NULLS LAST
        ) AS rn
    FROM seed_ctx sc
    JOIN mimiciv_derived.blood_differential d
        ON d.hadm_id = sc.hadm_id
    WHERE d.charttime >= sc.intime
      AND d.charttime <= sc.intime + INTERVAL '24 hours'
      AND d.neutrophils_abs IS NOT NULL
),
diff AS (
    SELECT
        hadm_id,
        neutrophils_abs
    FROM diff_raw
    WHERE rn = 1
),
coag_raw AS (
    SELECT
        sc.hadm_id,
        d.charttime,
        d.specimen_id,
        d.pt,
        d.ptt,
        ROW_NUMBER() OVER (
            PARTITION BY sc.hadm_id
            ORDER BY d.charttime NULLS LAST, d.specimen_id NULLS LAST
        ) AS rn
    FROM seed_ctx sc
    JOIN mimiciv_derived.coagulation d
        ON d.hadm_id = sc.hadm_id
    WHERE d.charttime >= sc.intime
      AND d.charttime <= sc.intime + INTERVAL '24 hours'
      AND (d.pt IS NOT NULL OR d.ptt IS NOT NULL)
),
coag AS (
    SELECT
        hadm_id,
        pt,
        ptt
    FROM coag_raw
    WHERE rn = 1
),
bg_raw AS (
    SELECT
        sc.hadm_id,
        d.charttime,
        d.lactate,
        ROW_NUMBER() OVER (
            PARTITION BY sc.hadm_id
            ORDER BY d.charttime NULLS LAST
        ) AS rn
    FROM seed_ctx sc
    JOIN mimiciv_derived.bg d
        ON d.hadm_id = sc.hadm_id
    WHERE d.charttime >= sc.intime
      AND d.charttime <= sc.intime + INTERVAL '24 hours'
      AND d.lactate IS NOT NULL
),
bg AS (
    SELECT
        hadm_id,
        lactate
    FROM bg_raw
    WHERE rn = 1
)
SELECT
    sc.stay_id,
    round(vs.temperature_mean::numeric, 6) AS temperature,
    round(cbc.hemoglobin::numeric, 6) AS hemoglobin,
    round(diff.neutrophils_abs::numeric, 6) AS neutrophils_abs,
    round(coag.pt::numeric, 6) AS pt,
    round(coag.ptt::numeric, 6) AS ptt,
    round(bg.lactate::numeric, 6) AS lactate
FROM seed_ctx sc
LEFT JOIN mimiciv_derived.first_day_vitalsign vs
    ON vs.stay_id = sc.stay_id
LEFT JOIN cbc
    ON cbc.hadm_id = sc.hadm_id
LEFT JOIN diff
    ON diff.hadm_id = sc.hadm_id
LEFT JOIN coag
    ON coag.hadm_id = sc.hadm_id
LEFT JOIN bg
    ON bg.hadm_id = sc.hadm_id
ORDER BY sc.stay_id
""".strip()


def _trajectory_comorbidity_intervention_sql(seed_df: pd.DataFrame) -> str:
    return f"""
WITH {_trajectory_seed_ctx_cte_sql(seed_df)},
charlson_flags AS (
    SELECT
        sc.hadm_id,
        COALESCE(ch.peripheral_vascular_disease, 0) AS peripheral_vascular_disease,
        COALESCE(ch.cerebrovascular_disease, 0) AS cerebrovascular_disease,
        CASE
            WHEN COALESCE(ch.mild_liver_disease, 0) = 1
              OR COALESCE(ch.severe_liver_disease, 0) = 1
            THEN 1 ELSE 0
        END AS liver_disease,
        round(ch.charlson_comorbidity_index::numeric, 6) AS charlson_score
    FROM seed_ctx sc
    LEFT JOIN mimiciv_derived.charlson ch
        ON ch.hadm_id = sc.hadm_id
),
rrt_flag AS (
    SELECT DISTINCT
        sc.stay_id,
        1 AS renal_replacement_therapy
    FROM seed_ctx sc
    JOIN mimiciv_derived.rrt d
        ON d.stay_id = sc.stay_id
    WHERE d.charttime >= sc.intime
      AND d.charttime <= sc.outtime
      AND COALESCE(d.dialysis_present, 0) = 1
),
ventilation_flag AS (
    SELECT DISTINCT
        sc.stay_id,
        1 AS mechanical_ventilation
    FROM seed_ctx sc
    JOIN mimiciv_derived.ventilation d
        ON d.stay_id = sc.stay_id
    WHERE d.starttime <= sc.outtime
      AND COALESCE(d.endtime, sc.outtime) >= sc.intime
      AND COALESCE(d.ventilation_status, '') <> ''
),
vasopressor_flag AS (
    SELECT DISTINCT
        sc.stay_id,
        1 AS vasopressor_use
    FROM seed_ctx sc
    JOIN mimiciv_derived.vasoactive_agent d
        ON d.stay_id = sc.stay_id
    WHERE d.starttime <= sc.outtime
      AND COALESCE(d.endtime, sc.outtime) >= sc.intime
      AND (
          d.dopamine IS NOT NULL
          OR d.epinephrine IS NOT NULL
          OR d.norepinephrine IS NOT NULL
          OR d.phenylephrine IS NOT NULL
          OR d.vasopressin IS NOT NULL
          OR d.dobutamine IS NOT NULL
          OR d.milrinone IS NOT NULL
      )
)
SELECT
    sc.stay_id,
    COALESCE(charlson_flags.peripheral_vascular_disease, 0) AS peripheral_vascular_disease,
    COALESCE(charlson_flags.cerebrovascular_disease, 0) AS cerebrovascular_disease,
    COALESCE(charlson_flags.liver_disease, 0) AS liver_disease,
    round(charlson_flags.charlson_score::numeric, 6) AS charlson_score,
    CASE WHEN ventilation_flag.mechanical_ventilation IS NULL THEN 0 ELSE 1 END AS mechanical_ventilation,
    CASE WHEN rrt_flag.renal_replacement_therapy IS NULL THEN 0 ELSE 1 END AS renal_replacement_therapy,
    CASE WHEN vasopressor_flag.vasopressor_use IS NULL THEN 0 ELSE 1 END AS vasopressor_use,
    NULL::integer AS beta_blocker_use
FROM seed_ctx sc
LEFT JOIN charlson_flags
    ON charlson_flags.hadm_id = sc.hadm_id
LEFT JOIN rrt_flag
    ON rrt_flag.stay_id = sc.stay_id
LEFT JOIN ventilation_flag
    ON ventilation_flag.stay_id = sc.stay_id
LEFT JOIN vasopressor_flag
    ON vasopressor_flag.stay_id = sc.stay_id
ORDER BY sc.stay_id
""".strip()


def _coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _prepare_trajectory_seed_dataframe(seed_df: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in TRAJECTORY_REQUIRED_COLUMNS if column not in seed_df.columns]
    if missing:
        raise RuntimeError(f"Trajectory seed cohort is missing required columns: {missing}")

    df = seed_df.copy()
    if "gender" not in df.columns and "sex" in df.columns:
        df = df.rename(columns={"sex": "gender"})

    numeric_columns = [
        "age",
        "sofa_score",
        "admit_to_icu_hours",
        "icu_los_hours",
        "hospital_los_hours",
        "hourly_measurement_count",
        "mortality_30d",
        "time_to_event_30d_days",
        *TRAJECTORY_PANEL_COLUMNS,
    ]
    _coerce_numeric(df, [column for column in numeric_columns if column in df.columns])

    df["heart_rate_initial"] = pd.to_numeric(df["heart_rate_hour_1"], errors="coerce")
    df["heart_rate_mean_10h"] = df.loc[:, list(TRAJECTORY_PANEL_COLUMNS)].mean(axis=1)
    df["heart_rate_delta_10h"] = pd.to_numeric(df["heart_rate_hour_10"], errors="coerce") - pd.to_numeric(
        df["heart_rate_hour_1"], errors="coerce"
    )
    if "icu_los_hours" in df.columns:
        df["icu_los_days"] = pd.to_numeric(df["icu_los_hours"], errors="coerce") / 24.0
    if "hospital_los_hours" in df.columns:
        df["hospital_los_days"] = pd.to_numeric(df["hospital_los_hours"], errors="coerce") / 24.0
    if "insurance" not in df.columns:
        df["insurance"] = pd.NA
    df["beta_blocker_use"] = pd.NA
    return df


def _execute_trajectory_block(
    cfg: dict[str, str],
    *,
    seed_count: int,
    block_name: str,
    sql: str,
    expected_columns: list[str],
    fallback_sql: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_used = "mimiciv_derived"
    fallback_attempted = False
    try:
        df = run_query_df(cfg, sql)
    except Exception as exc:
        if fallback_sql is None:
            return pd.DataFrame(columns=["stay_id", *expected_columns]), {
                "status": "failed",
                "row_count": 0,
                "columns_added": expected_columns,
                "source": source_used,
                "fallback_attempted": False,
                "error": str(exc),
            }
        fallback_attempted = True
        source_used = "raw_fallback"
        try:
            df = run_query_df(cfg, fallback_sql)
        except Exception as fallback_exc:
            return pd.DataFrame(columns=["stay_id", *expected_columns]), {
                "status": "failed",
                "row_count": 0,
                "columns_added": expected_columns,
                "source": source_used,
                "fallback_attempted": True,
                "error": f"primary error: {exc}; fallback error: {fallback_exc}",
            }

    if df.empty:
        return pd.DataFrame(columns=["stay_id", *expected_columns]), {
            "status": "failed",
            "row_count": 0,
            "columns_added": expected_columns,
            "source": source_used,
            "fallback_attempted": fallback_attempted,
            "error": f"{block_name} query returned no rows",
        }

    if "stay_id" not in df.columns:
        return pd.DataFrame(columns=["stay_id", *expected_columns]), {
            "status": "failed",
            "row_count": 0,
            "columns_added": expected_columns,
            "source": source_used,
            "fallback_attempted": fallback_attempted,
            "error": f"{block_name} query did not return stay_id",
        }

    status = "success"
    issues: list[str] = []
    missing_columns = [column for column in expected_columns if column not in df.columns]
    if missing_columns:
        status = "partial"
        issues.append(f"missing columns: {missing_columns}")
    all_missing_columns = [
        column
        for column in expected_columns
        if column in df.columns and df[column].isna().all()
    ]
    if all_missing_columns:
        status = "partial"
        issues.append(f"all values missing: {all_missing_columns}")
    if len(df) != seed_count:
        status = "partial"
        issues.append(f"row_count {len(df)} differs from seed_count {seed_count}")

    return df, {
        "status": status,
        "row_count": int(len(df)),
        "columns_added": expected_columns,
        "source": source_used,
        "fallback_attempted": fallback_attempted,
        "error": "; ".join(issues),
    }


def build_trajectory_analysis_dataset(
    *,
    project_root: Path,
    cfg: dict[str, str],
    output_path: Path,
    missingness_path: Path,
    profile_key: str,
    profile_name: str,
    requested_mode: str,
    resolved_mode: str,
    cohort_profile: TygSepsisCohortProfile,
) -> dict[str, Any]:
    seed_path = resolve_seed_cohort_path(project_root, output_path=output_path, profile_key=profile_key)
    seed_raw = pd.read_csv(seed_path)
    merged = _prepare_trajectory_seed_dataframe(seed_raw)

    profile = get_paper_execution_profile(profile_key)
    seed_count = int(len(merged))
    block_status: dict[str, dict[str, Any]] = {
        "cohort_seed_hr_panel": {
            "status": "success",
            "row_count": seed_count,
            "columns_added": [
                "age",
                "gender",
                "race",
                "marital_status",
                "sepsis3_flag",
                "suspected_infection_time",
                "sofa_time",
                "first_heart_rate_charttime",
                "hourly_measurement_count",
                "mortality_30d",
                "time_to_event_30d_days",
                "heart_rate_initial",
                "heart_rate_mean_10h",
                "heart_rate_delta_10h",
                *TRAJECTORY_PANEL_COLUMNS,
            ],
            "source": "seed_cohort_csv",
            "fallback_attempted": False,
            "error": "",
        }
    }
    partial_fidelity_notes: list[str] = []
    source_strategy = {
        "mode": "derived_first_with_raw_block_fallback",
        "default_source": "mimiciv_derived",
        "fallback_policy": "fallback is attempted only per block when a primary query fails",
        "seed_cohort_path": str(seed_path.relative_to(project_root)),
    }

    blocks = [
        {
            "name": "demographics_admission_icu_context",
            "sql": _trajectory_context_sql(seed_raw),
            "expected_columns": ["insurance"],
            "critical": False,
        },
        {
            "name": "first_day_scores_anthropometrics",
            "sql": _trajectory_scores_sql(seed_raw),
            "expected_columns": ["height_cm", "weight_kg", "sofa_score_derived", "apsiii", "gcs_score"],
            "critical": False,
        },
        {
            "name": "first_day_vitals_labs",
            "sql": _trajectory_vitals_labs_sql(seed_raw),
            "expected_columns": ["temperature", "hemoglobin", "neutrophils_abs", "pt", "ptt", "lactate"],
            "critical": False,
        },
        {
            "name": "comorbidity_intervention_flags",
            "sql": _trajectory_comorbidity_intervention_sql(seed_raw),
            "expected_columns": [
                "peripheral_vascular_disease",
                "cerebrovascular_disease",
                "liver_disease",
                "charlson_score",
                "mechanical_ventilation",
                "renal_replacement_therapy",
                "vasopressor_use",
                "beta_blocker_use",
            ],
            "critical": False,
        },
    ]

    for block in blocks:
        block_df, diagnostics = _execute_trajectory_block(
            cfg,
            seed_count=seed_count,
            block_name=str(block["name"]),
            sql=str(block["sql"]),
            expected_columns=list(block["expected_columns"]),
        )
        block_status[str(block["name"])] = diagnostics

        if diagnostics["status"] == "failed":
            if bool(block["critical"]):
                raise RuntimeError(f"Critical trajectory block failed: {block['name']} -> {diagnostics['error']}")
            for column in block["expected_columns"]:
                if column not in merged.columns:
                    merged[column] = pd.NA
            partial_fidelity_notes.append(f"{block['name']}: {diagnostics['error']}")
            continue

        merged = merged.merge(block_df, on="stay_id", how="left")
        if diagnostics["status"] == "partial":
            partial_fidelity_notes.append(f"{block['name']}: {diagnostics['error']}")

    if "sofa_score_derived" in merged.columns:
        merged["sofa_score"] = pd.to_numeric(merged["sofa_score_derived"], errors="coerce").combine_first(
            pd.to_numeric(merged.get("sofa_score"), errors="coerce")
        )
        merged = merged.drop(columns=["sofa_score_derived"])

    merged["height_cm"] = pd.to_numeric(merged.get("height_cm"), errors="coerce")
    merged["weight_kg"] = pd.to_numeric(merged.get("weight_kg"), errors="coerce")
    merged["bmi"] = pd.NA
    valid_bmi = (merged["height_cm"] > 0) & (merged["weight_kg"] > 0)
    merged.loc[valid_bmi, "bmi"] = merged.loc[valid_bmi, "weight_kg"] / (
        (merged.loc[valid_bmi, "height_cm"] / 100.0) ** 2
    )

    for column in (
        "sofa_score",
        "apsiii",
        "gcs_score",
        "temperature",
        "hemoglobin",
        "neutrophils_abs",
        "pt",
        "ptt",
        "lactate",
        "charlson_score",
        "bmi",
    ):
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")
    for column in (
        "peripheral_vascular_disease",
        "cerebrovascular_disease",
        "liver_disease",
        "mechanical_ventilation",
        "renal_replacement_therapy",
        "vasopressor_use",
        "beta_blocker_use",
    ):
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")

    for column in TRAJECTORY_FINAL_COLUMN_ORDER:
        if column not in merged.columns:
            merged[column] = pd.NA
    final_df = merged.loc[:, list(TRAJECTORY_FINAL_COLUMN_ORDER)].copy()

    critical_missing = [column for column in TRAJECTORY_REQUIRED_COLUMNS if column not in final_df.columns]
    if critical_missing:
        raise RuntimeError(f"Trajectory dataset is missing critical columns after staged build: {critical_missing}")

    empty_critical = [column for column in TRAJECTORY_REQUIRED_COLUMNS if final_df[column].isna().all()]
    if empty_critical:
        raise RuntimeError(f"Trajectory dataset has critical columns with all values missing: {empty_critical}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_path, index=False)

    rows = final_df.to_dict(orient="records")
    missingness = summarize_missingness(rows, non_predictor_columns=TRAJECTORY_NON_PREDICTOR_COLUMNS)
    payload = {
        "profile": profile.as_dict() if profile is not None else {"key": profile_key},
        "cohort_profile": profile_name,
        "sepsis_source": resolved_mode,
        "sepsis_source_requested": requested_mode,
        "row_count": int(len(final_df)),
        "column_count": int(len(final_df.columns)),
        "cohort_filters": {
            "admit_year_start": cohort_profile.admit_year_start,
            "admit_year_end": cohort_profile.admit_year_end,
        },
        "source_strategy": source_strategy,
        "block_status": block_status,
        "partial_fidelity": bool(partial_fidelity_notes),
        "partial_fidelity_notes": partial_fidelity_notes,
        "missingness": missingness,
    }

    missingness_path.parent.mkdir(parents=True, exist_ok=True)
    missingness_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


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
    elif args.profile == "mimic_hr_trajectory_sepsis":
        cohort_profile = apply_profile_overrides(
            PAPER_MIMIC_HEART_RATE_TRAJECTORY_PROFILE,
            admit_year_start=args.admit_year_start,
            admit_year_end=args.admit_year_end,
        )
    elif args.profile == "mimic_tyg_stroke_nondiabetic":
        cohort_profile = apply_profile_overrides(
            PAPER_MIMIC_TYG_STROKE_PROFILE,
            admit_year_start=args.admit_year_start,
            admit_year_end=args.admit_year_end,
        )
    else:
        raise SystemExit(f"Unsupported profile key: {args.profile}")

    output_rel = args.output or f"shared/{args.profile}_analysis_dataset.csv"
    missingness_rel = args.missingness_output or f"shared/{args.profile}_analysis_missingness.json"
    output_path = project_root / output_rel
    missingness_path = project_root / missingness_rel

    if args.profile == "mimic_hr_trajectory_sepsis":
        payload = build_trajectory_analysis_dataset(
            project_root=project_root,
            cfg=cfg,
            output_path=output_path,
            missingness_path=missingness_path,
            profile_key=args.profile,
            profile_name=cohort_profile.name,
            requested_mode=requested_mode,
            resolved_mode=mode,
            cohort_profile=cohort_profile,
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print(f"\nAnalysis dataset written to: {output_path}")
        print(f"Missingness report written to: {missingness_path}")
        return 0

    profile_name, dataset_sql, non_predictor_columns = resolve_profile_sql(
        args.profile,
        mode=mode,
        has_sepsis3_flag=has_sepsis3_flag,
        cohort_profile=cohort_profile,
    )

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

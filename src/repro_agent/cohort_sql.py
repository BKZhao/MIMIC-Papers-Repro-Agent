from __future__ import annotations

from dataclasses import dataclass

from .paper_contract import PAPER_TYG_QUARTILE_BOUNDS


@dataclass(frozen=True)
class TygSepsisCohortProfile:
    name: str
    min_age_years: int = 18
    max_age_years: int | None = None
    max_admit_to_icu_hours: int | None = None
    min_icu_los_hours: int = 48
    require_hospital_time_records: bool = True
    lab_anchor: str = "icu"
    baseline_lab_window_hours: int = 24


PAPER_MIMIC_TYG_PROFILE = TygSepsisCohortProfile(
    name="paper_mimic_tyg",
    min_age_years=18,
    max_age_years=None,
    max_admit_to_icu_hours=None,
    min_icu_los_hours=48,
    require_hospital_time_records=False,
    lab_anchor="admission",
    baseline_lab_window_hours=48,
)

TG_ITEMIDS: tuple[int, ...] = (51000,)
GLUCOSE_ITEMIDS: tuple[int, ...] = (50931,)


def build_tyg_sepsis_cohort_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_TYG_PROFILE,
) -> str:
    return f"""
WITH first_icu_subject AS (
    SELECT DISTINCT ON (i.subject_id)
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY i.subject_id, i.intime, i.stay_id
),
adult_first_icu AS (
    SELECT
        f.subject_id,
        f.hadm_id,
        f.stay_id,
        f.intime,
        f.outtime,
        p.anchor_age AS age,
        p.gender AS sex,
        a.admittime,
        a.dischtime,
        a.deathtime,
        a.hospital_expire_flag,
        EXTRACT(EPOCH FROM (f.intime - a.admittime)) / 3600.0 AS admit_to_icu_hours,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours,
        EXTRACT(EPOCH FROM (a.dischtime - a.admittime)) / 3600.0 AS hospital_los_hours
    FROM first_icu_subject f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = f.hadm_id
    WHERE {_age_clause(profile)}
),
admit_gap_filtered AS (
    SELECT *
    FROM adult_first_icu
    WHERE admit_to_icu_hours IS NOT NULL
      AND {_admit_gap_clause(profile)}
),
sepsis_cohort AS (
{_sepsis_select(mode=mode, has_sepsis3_flag=has_sepsis3_flag)}
),
hospital_time_filtered AS (
    SELECT *
    FROM sepsis_cohort
    WHERE {_hospital_time_clause(profile)}
),
los_filtered AS (
    SELECT *
    FROM hospital_time_filtered
    WHERE icu_los_hours IS NOT NULL
      AND icu_los_hours >= {profile.min_icu_los_hours}
),
tg AS (
    SELECT
        l.stay_id,
        le.valuenum AS tg,
        ROW_NUMBER() OVER (PARTITION BY l.stay_id ORDER BY le.charttime, le.labevent_id) AS rn
    FROM los_filtered l
    JOIN mimiciv_hosp.labevents le
        ON le.hadm_id = l.hadm_id
    WHERE le.itemid IN ({_int_list(TG_ITEMIDS)})
      AND le.valuenum IS NOT NULL
      AND le.charttime >= {_lab_window_lower_bound(profile)}
      AND le.charttime <= { _lab_window_upper_bound(profile) }
),
glu AS (
    SELECT
        l.stay_id,
        le.valuenum AS glucose,
        ROW_NUMBER() OVER (PARTITION BY l.stay_id ORDER BY le.charttime, le.labevent_id) AS rn
    FROM los_filtered l
    JOIN mimiciv_hosp.labevents le
        ON le.hadm_id = l.hadm_id
    WHERE le.itemid IN ({_int_list(GLUCOSE_ITEMIDS)})
      AND le.valuenum IS NOT NULL
      AND le.charttime >= {_lab_window_lower_bound(profile)}
      AND le.charttime <= { _lab_window_upper_bound(profile) }
),
final_raw AS (
    SELECT
        l.subject_id,
        l.hadm_id,
        l.stay_id,
        l.age,
        l.sex,
        l.sepsis3_flag,
        l.suspected_infection_time,
        l.sofa_time,
        l.sofa_score,
        l.admit_to_icu_hours,
        l.icu_los_hours,
        l.hospital_los_hours,
        ln((tg.tg * glu.glucose) / 2.0) AS tyg_index,
        CASE
            WHEN l.deathtime IS NOT NULL
             AND l.deathtime >= l.admittime
             AND (l.dischtime IS NULL OR l.deathtime <= l.dischtime)
            THEN 1 ELSE 0
        END AS hospital_mortality,
        CASE
            WHEN l.deathtime IS NOT NULL
             AND l.deathtime BETWEEN l.intime AND l.outtime
            THEN 1 ELSE 0
        END AS icu_mortality,
        EXTRACT(
            EPOCH FROM (
                COALESCE(
                    CASE
                        WHEN l.deathtime IS NOT NULL
                         AND l.deathtime >= l.admittime
                         AND (l.dischtime IS NULL OR l.deathtime <= l.dischtime)
                        THEN l.deathtime
                    END,
                    l.dischtime,
                    l.outtime
                ) - l.admittime
            )
        ) / 3600.0 AS hospital_survival_hours,
        EXTRACT(
            EPOCH FROM (
                COALESCE(
                    CASE
                        WHEN l.deathtime IS NOT NULL
                         AND l.deathtime BETWEEN l.intime AND l.outtime
                        THEN l.deathtime
                    END,
                    l.outtime
                ) - l.intime
            )
        ) / 3600.0 AS icu_survival_hours
    FROM los_filtered l
    JOIN tg
        ON tg.stay_id = l.stay_id
       AND tg.rn = 1
    JOIN glu
        ON glu.stay_id = l.stay_id
       AND glu.rn = 1
    WHERE tg.tg > 0
      AND glu.glucose > 0
)
SELECT
    subject_id,
    hadm_id,
    stay_id,
    age,
    sex,
    sepsis3_flag,
    suspected_infection_time,
    sofa_time,
    sofa_score,
    round(admit_to_icu_hours::numeric, 3) AS admit_to_icu_hours,
    round(icu_los_hours::numeric, 3) AS icu_los_hours,
    round(hospital_los_hours::numeric, 3) AS hospital_los_hours,
    round(hospital_survival_hours::numeric, 3) AS hospital_survival_hours,
    round(icu_survival_hours::numeric, 3) AS icu_survival_hours,
    round(tyg_index::numeric, 6) AS tyg_index,
    {_paper_tyg_quartile_case_sql("tyg_index")} AS tyg_quartile,
    hospital_mortality,
    icu_mortality
FROM final_raw
"""


def build_tyg_sepsis_funnel_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_TYG_PROFILE,
) -> str:
    return f"""
WITH first_icu_subject AS (
    SELECT DISTINCT ON (i.subject_id)
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY i.subject_id, i.intime, i.stay_id
),
adult_first_icu AS (
    SELECT
        f.subject_id,
        f.hadm_id,
        f.stay_id,
        f.intime,
        f.outtime,
        p.anchor_age AS age,
        a.admittime,
        a.dischtime,
        EXTRACT(EPOCH FROM (f.intime - a.admittime)) / 3600.0 AS admit_to_icu_hours,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours,
        EXTRACT(EPOCH FROM (a.dischtime - a.admittime)) / 3600.0 AS hospital_los_hours
    FROM first_icu_subject f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = f.hadm_id
    WHERE {_age_clause(profile)}
),
admit_gap_filtered AS (
    SELECT *
    FROM adult_first_icu
    WHERE admit_to_icu_hours IS NOT NULL
      AND {_admit_gap_clause(profile)}
),
sepsis_cohort AS (
{_sepsis_select(mode=mode, has_sepsis3_flag=has_sepsis3_flag, include_extra_columns=False)}
),
hospital_time_filtered AS (
    SELECT *
    FROM sepsis_cohort
    WHERE {_hospital_time_clause(profile)}
),
los_filtered AS (
    SELECT *
    FROM hospital_time_filtered
    WHERE icu_los_hours IS NOT NULL
      AND icu_los_hours >= {profile.min_icu_los_hours}
),
tg AS (
    SELECT DISTINCT l.stay_id
    FROM los_filtered l
    JOIN mimiciv_hosp.labevents le
        ON le.hadm_id = l.hadm_id
    WHERE le.itemid IN ({_int_list(TG_ITEMIDS)})
      AND le.valuenum IS NOT NULL
      AND le.charttime >= {_lab_window_lower_bound(profile)}
      AND le.charttime <= { _lab_window_upper_bound(profile) }
),
glu AS (
    SELECT DISTINCT l.stay_id
    FROM los_filtered l
    JOIN mimiciv_hosp.labevents le
        ON le.hadm_id = l.hadm_id
    WHERE le.itemid IN ({_int_list(GLUCOSE_ITEMIDS)})
      AND le.valuenum IS NOT NULL
      AND le.charttime >= {_lab_window_lower_bound(profile)}
      AND le.charttime <= { _lab_window_upper_bound(profile) }
),
final_cohort AS (
    SELECT l.stay_id
    FROM los_filtered l
    JOIN tg USING (stay_id)
    JOIN glu USING (stay_id)
)
SELECT
    (SELECT COUNT(*) FROM first_icu_subject) AS n_initial_first_icu,
    (SELECT COUNT(*) FROM adult_first_icu) AS n_after_age,
    (SELECT COUNT(*) FROM admit_gap_filtered) AS n_after_admit_gap,
    (SELECT COUNT(*) FROM sepsis_cohort) AS n_after_sepsis,
    (SELECT COUNT(*) FROM hospital_time_filtered) AS n_after_hospital_time,
    (SELECT COUNT(*) FROM los_filtered) AS n_after_icu_los,
    (SELECT COUNT(*) FROM final_cohort) AS n_final_with_tg_glucose
"""


def _age_clause(profile: TygSepsisCohortProfile) -> str:
    clauses = [f"p.anchor_age >= {profile.min_age_years}"]
    if profile.max_age_years is not None:
        clauses.append(f"p.anchor_age <= {profile.max_age_years}")
    return " AND ".join(clauses)


def _admit_gap_clause(profile: TygSepsisCohortProfile) -> str:
    if profile.max_admit_to_icu_hours is None:
        return "TRUE"
    return f"admit_to_icu_hours <= {profile.max_admit_to_icu_hours}"


def _hospital_time_clause(profile: TygSepsisCohortProfile) -> str:
    if not profile.require_hospital_time_records:
        return "TRUE"
    return (
        "admittime IS NOT NULL "
        "AND dischtime IS NOT NULL "
        "AND intime IS NOT NULL "
        "AND outtime IS NOT NULL "
        "AND hospital_los_hours IS NOT NULL"
    )


def _lab_window_upper_bound(profile: TygSepsisCohortProfile) -> str:
    if profile.lab_anchor == "admission":
        return (
            "LEAST(COALESCE(l.dischtime, l.outtime), l.admittime + "
            f"INTERVAL '{profile.baseline_lab_window_hours} hours')"
        )
    return (
        "LEAST(l.outtime, l.intime + "
        f"INTERVAL '{profile.baseline_lab_window_hours} hours')"
    )


def _lab_window_lower_bound(profile: TygSepsisCohortProfile) -> str:
    if profile.lab_anchor == "admission":
        return "l.admittime"
    return "l.intime"


def _int_list(values: tuple[int, ...]) -> str:
    return ", ".join(str(value) for value in values)


def _paper_tyg_quartile_case_sql(expr: str) -> str:
    q1_max, q2_max, q3_max = PAPER_TYG_QUARTILE_BOUNDS
    return (
        "CASE "
        f"WHEN {expr} <= {q1_max} THEN 'Q1' "
        f"WHEN {expr} <= {q2_max} THEN 'Q2' "
        f"WHEN {expr} <= {q3_max} THEN 'Q3' "
        "ELSE 'Q4' "
        "END"
    )


def _sepsis_select(mode: str, has_sepsis3_flag: bool, include_extra_columns: bool = True) -> str:
    if mode == "derived":
        extra_columns = _derived_extra_columns(has_sepsis3_flag) if include_extra_columns else ""
        flag_filter = "AND s3.sepsis3 IS TRUE" if has_sepsis3_flag else ""
        return f"""    SELECT
        a.*
{extra_columns}
    FROM admit_gap_filtered a
    JOIN mimiciv_derived.sepsis3 s3
        ON s3.stay_id = a.stay_id
       {flag_filter}"""

    extra_columns = _icd_extra_columns() if include_extra_columns else ""
    return f"""    SELECT
        a.*
{extra_columns}
    FROM admit_gap_filtered a
    WHERE EXISTS (
        SELECT 1
        FROM mimiciv_hosp.diagnoses_icd d
        WHERE d.hadm_id = a.hadm_id
          AND (
              (d.icd_version = 9 AND d.icd_code IN ('99591', '99592', '78552'))
              OR (d.icd_version = 10 AND (
                    d.icd_code LIKE 'A40%%'
                    OR d.icd_code LIKE 'A41%%'
                    OR d.icd_code = 'R6520'
                    OR d.icd_code = 'R6521'
              ))
          )
    )"""


def _derived_extra_columns(has_sepsis3_flag: bool) -> str:
    if has_sepsis3_flag:
        return """
        , CASE WHEN s3.sepsis3 IS TRUE THEN 1 ELSE 0 END AS sepsis3_flag
        , s3.suspected_infection_time
        , s3.sofa_time
        , s3.sofa_score"""
    return """
        , 1::int AS sepsis3_flag
        , s3.suspected_infection_time
        , s3.sofa_time
        , s3.sofa_score"""


def _icd_extra_columns() -> str:
    return """
        , NULL::int AS sepsis3_flag
        , NULL::timestamp AS suspected_infection_time
        , NULL::timestamp AS sofa_time
        , NULL::double precision AS sofa_score"""

from __future__ import annotations

from dataclasses import dataclass

from ..paper.contract import PAPER_TYG_QUARTILE_BOUNDS


@dataclass(frozen=True)
class TygSepsisCohortProfile:
    name: str
    min_age_years: int = 18
    max_age_years: int | None = None
    admit_year_start: int | None = None
    admit_year_end: int | None = None
    first_icu_scope: str = "subject"
    max_admit_to_icu_hours: int | None = None
    min_icu_los_hours: int = 48
    require_hospital_time_records: bool = True
    lab_anchor: str = "icu"
    baseline_lab_window_hours: int = 24
    exclude_multiple_icu_stays: bool = False
    trajectory_window_hours: int = 0


PAPER_MIMIC_TYG_PROFILE = TygSepsisCohortProfile(
    name="paper_mimic_tyg",
    min_age_years=18,
    max_age_years=None,
    first_icu_scope="subject",
    max_admit_to_icu_hours=None,
    min_icu_los_hours=48,
    require_hospital_time_records=False,
    lab_anchor="admission",
    baseline_lab_window_hours=48,
)

PAPER_MIMIC_NLR_PROFILE = TygSepsisCohortProfile(
    name="paper_mimic_nlr",
    min_age_years=65,
    max_age_years=None,
    first_icu_scope="hadm",
    max_admit_to_icu_hours=None,
    min_icu_los_hours=24,
    require_hospital_time_records=False,
    lab_anchor="icu",
    baseline_lab_window_hours=24,
)

PAPER_MIMIC_HEART_RATE_TRAJECTORY_PROFILE = TygSepsisCohortProfile(
    name="paper_mimic_hr_trajectory",
    min_age_years=18,
    max_age_years=90,
    admit_year_start=2008,
    admit_year_end=2019,
    first_icu_scope="subject",
    max_admit_to_icu_hours=None,
    min_icu_los_hours=48,
    require_hospital_time_records=False,
    lab_anchor="icu",
    baseline_lab_window_hours=24,
    exclude_multiple_icu_stays=True,
    trajectory_window_hours=10,
)

TG_ITEMIDS: tuple[int, ...] = (51000,)
GLUCOSE_ITEMIDS: tuple[int, ...] = (50931,)
NLR_QUARTILE_BOUNDS: tuple[float, float, float] = (4.9, 8.8, 16.0)
STROKE_TYG_QUARTILE_BOUNDS: tuple[float, float, float] = (8.27, 8.62, 8.98)

STROKE_ISCHEMIC_ICD9_CODES: tuple[str, ...] = (
    "43301",
    "43311",
    "43321",
    "43331",
    "43381",
    "43391",
    "43401",
    "43411",
    "43491",
    "436",
)
STROKE_ISCHEMIC_ICD10_PREFIXES: tuple[str, ...] = ("I63",)
DIABETES_ICD9_PREFIXES: tuple[str, ...] = ("249", "250")
DIABETES_ICD10_PREFIXES: tuple[str, ...] = ("E08", "E09", "E10", "E11", "E13")


PAPER_MIMIC_TYG_STROKE_PROFILE = TygSepsisCohortProfile(
    name="paper_mimic_tyg_stroke",
    min_age_years=18,
    max_age_years=None,
    admit_year_start=2008,
    admit_year_end=2022,
    first_icu_scope="subject",
    max_admit_to_icu_hours=None,
    min_icu_los_hours=3,
    require_hospital_time_records=False,
    lab_anchor="admission",
    baseline_lab_window_hours=24,
)


def _stroke_non_diabetic_filter_ctes() -> str:
    return f"""
diabetic_hadm_history AS (
    SELECT DISTINCT
        dx.hadm_id,
        a2.subject_id,
        a2.admittime
    FROM mimiciv_hosp.diagnoses_icd dx
    JOIN mimiciv_hosp.admissions a2
        ON a2.hadm_id = dx.hadm_id
    WHERE {_diabetes_dx_match_sql("dx")}
),
antidiabetic_hadm_history AS (
    SELECT DISTINCT
        rx.hadm_id,
        rx.subject_id,
        a2.admittime
    FROM mimiciv_hosp.prescriptions rx
    JOIN mimiciv_hosp.admissions a2
        ON a2.hadm_id = rx.hadm_id
    WHERE {_antidiabetic_drug_match_sql("rx")}
),
diabetic_excluded_stays AS (
    SELECT DISTINCT l.stay_id
    FROM los_filtered l
    JOIN diabetic_hadm_history d
        ON d.subject_id = l.subject_id
       AND d.admittime <= l.admittime
),
antidiabetic_excluded_stays AS (
    SELECT DISTINCT l.stay_id
    FROM los_filtered l
    JOIN antidiabetic_hadm_history r
        ON r.subject_id = l.subject_id
       AND r.admittime < l.admittime
),
non_diabetic_filtered AS (
    SELECT l.*
    FROM los_filtered l
    LEFT JOIN diabetic_excluded_stays d
        ON d.stay_id = l.stay_id
    LEFT JOIN antidiabetic_excluded_stays r
        ON r.stay_id = l.stay_id
    WHERE d.stay_id IS NULL
      AND r.stay_id IS NULL
)"""


def build_tyg_sepsis_cohort_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_TYG_PROFILE,
) -> str:
    return f"""
WITH first_icu_index AS (
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
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
    FROM first_icu_index f
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
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
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


def build_nlr_sepsis_cohort_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_NLR_PROFILE,
) -> str:
    return f"""
WITH first_icu_index AS (
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
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
        p.dod AS patient_dod,
        a.admittime,
        a.dischtime,
        a.deathtime,
        a.hospital_expire_flag,
        a.race,
        EXTRACT(EPOCH FROM (f.intime - a.admittime)) / 3600.0 AS admit_to_icu_hours,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours,
        EXTRACT(EPOCH FROM (a.dischtime - a.admittime)) / 3600.0 AS hospital_los_hours
    FROM first_icu_index f
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
charlson_exclusions AS (
    SELECT
        l.hadm_id,
        COALESCE(ch.malignant_cancer, 0) AS malignant_cancer,
        COALESCE(ch.metastatic_solid_tumor, 0) AS metastatic_solid_tumor,
        COALESCE(ch.aids, 0) AS aids,
        COALESCE(ch.rheumatic_disease, 0) AS rheumatic_disease
    FROM los_filtered l
    LEFT JOIN mimiciv_derived.charlson ch
        ON ch.hadm_id = l.hadm_id
),
eligible_filtered AS (
    SELECT l.*
    FROM los_filtered l
    LEFT JOIN charlson_exclusions x
        ON x.hadm_id = l.hadm_id
    WHERE COALESCE(x.malignant_cancer, 0) = 0
      AND COALESCE(x.metastatic_solid_tumor, 0) = 0
      AND COALESCE(x.aids, 0) = 0
      AND COALESCE(x.rheumatic_disease, 0) = 0
),
nlr_raw AS (
    SELECT
        l.stay_id,
        l.hadm_id,
        d.charttime,
        d.specimen_id,
        d.neutrophils_abs,
        d.lymphocytes_abs
    FROM eligible_filtered l
    JOIN mimiciv_derived.blood_differential d
        ON d.hadm_id = l.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound(profile)}
      AND d.charttime <= {_lab_window_upper_bound(profile)}
      AND d.neutrophils_abs IS NOT NULL
      AND d.neutrophils_abs > 0
      AND d.lymphocytes_abs IS NOT NULL
      AND d.lymphocytes_abs > 0
),
nlr_measurement AS (
    SELECT DISTINCT ON (stay_id)
        stay_id,
        hadm_id,
        charttime AS nlr_charttime,
        neutrophils_abs,
        lymphocytes_abs,
        neutrophils_abs / lymphocytes_abs AS nlr
    FROM nlr_raw
    ORDER BY stay_id, charttime NULLS LAST, specimen_id NULLS LAST
),
final_raw AS (
    SELECT
        l.subject_id,
        l.hadm_id,
        l.stay_id,
        l.age,
        l.sex,
        l.race,
        l.sepsis3_flag,
        l.suspected_infection_time,
        l.sofa_time,
        l.sofa_score,
        l.admit_to_icu_hours,
        l.icu_los_hours,
        l.hospital_los_hours,
        nlr.nlr_charttime,
        nlr.neutrophils_abs,
        nlr.lymphocytes_abs,
        nlr.nlr,
        CASE
            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '28 days'
            THEN 1 ELSE 0
        END AS mortality_28d,
        LEAST(
            672.0,
            EXTRACT(
                EPOCH FROM (
                    COALESCE(
                        CASE
                            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
                             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
                             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '28 days'
                            THEN COALESCE(l.deathtime, l.patient_dod)
                        END,
                        l.intime + INTERVAL '28 days'
                    ) - l.intime
                )
            ) / 3600.0
        ) AS time_to_event_28d_hours
    FROM eligible_filtered l
    JOIN nlr_measurement nlr
        ON nlr.stay_id = l.stay_id
    WHERE nlr.nlr > 0
)
SELECT
    subject_id,
    hadm_id,
    stay_id,
    age,
    sex,
    race,
    sepsis3_flag,
    suspected_infection_time,
    sofa_time,
    round(sofa_score::numeric, 6) AS sofa_score,
    round(admit_to_icu_hours::numeric, 3) AS admit_to_icu_hours,
    round(icu_los_hours::numeric, 3) AS icu_los_hours,
    round(hospital_los_hours::numeric, 3) AS hospital_los_hours,
    nlr_charttime,
    round(neutrophils_abs::numeric, 6) AS neutrophil_count,
    round(lymphocytes_abs::numeric, 6) AS lymphocyte_count,
    round(nlr::numeric, 6) AS nlr,
    {_quartile_case_sql("nlr", NLR_QUARTILE_BOUNDS)} AS nlr_quartile,
    mortality_28d,
    round(time_to_event_28d_hours::numeric, 6) AS time_to_event_28d_hours,
    round((time_to_event_28d_hours / 24.0)::numeric, 6) AS time_to_event_28d_days
FROM final_raw
"""


def build_tyg_stroke_cohort_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_TYG_STROKE_PROFILE,
) -> str:
    return f"""
WITH first_icu_index AS (
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
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
        p.dod AS patient_dod,
        a.admittime,
        a.dischtime,
        a.deathtime,
        a.hospital_expire_flag,
        a.race,
        a.insurance,
        a.marital_status,
        EXTRACT(EPOCH FROM (f.intime - a.admittime)) / 3600.0 AS admit_to_icu_hours,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours,
        EXTRACT(EPOCH FROM (a.dischtime - a.admittime)) / 3600.0 AS hospital_los_hours
    FROM first_icu_index f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = f.hadm_id
    WHERE {_age_clause(profile)}
),
stroke_primary_hadm AS (
    SELECT DISTINCT dx.hadm_id
    FROM mimiciv_hosp.diagnoses_icd dx
    WHERE COALESCE(dx.seq_num, 1) = 1
      AND {_stroke_ischemic_dx_match_sql("dx")}
),
stroke_primary_filtered AS (
    SELECT a.*
    FROM adult_first_icu a
    JOIN stroke_primary_hadm s
        ON s.hadm_id = a.hadm_id
),
los_filtered AS (
    SELECT *
    FROM stroke_primary_filtered
    WHERE icu_los_hours IS NOT NULL
      AND icu_los_hours >= {profile.min_icu_los_hours}
),
{_stroke_non_diabetic_filter_ctes()},
paired_lab_raw AS (
    SELECT
        l.stay_id,
        tg.charttime,
        tg.labevent_id AS tg_labevent_id,
        glu.labevent_id AS glu_labevent_id,
        tg.valuenum AS triglycerides,
        glu.valuenum AS glucose,
        ROW_NUMBER() OVER (
            PARTITION BY l.stay_id
            ORDER BY tg.charttime NULLS LAST, tg.labevent_id NULLS LAST, glu.labevent_id NULLS LAST
        ) AS rn
    FROM non_diabetic_filtered l
    JOIN mimiciv_hosp.labevents tg
        ON tg.hadm_id = l.hadm_id
    JOIN mimiciv_hosp.labevents glu
        ON glu.hadm_id = l.hadm_id
       AND glu.itemid IN ({_int_list(GLUCOSE_ITEMIDS)})
       AND glu.valuenum IS NOT NULL
       AND glu.valuenum > 0
       AND glu.charttime = tg.charttime
    WHERE tg.itemid IN ({_int_list(TG_ITEMIDS)})
      AND tg.valuenum IS NOT NULL
      AND tg.valuenum > 0
      AND tg.charttime >= {_lab_window_lower_bound(profile)}
      AND tg.charttime <= {_lab_window_upper_bound(profile)}
),
paired_labs AS (
    SELECT
        stay_id,
        triglycerides,
        glucose
    FROM paired_lab_raw
    WHERE rn = 1
),
final_raw AS (
    SELECT
        l.subject_id,
        l.hadm_id,
        l.stay_id,
        l.age,
        l.sex,
        l.race,
        l.insurance,
        l.marital_status,
        l.admit_to_icu_hours,
        l.icu_los_hours,
        l.hospital_los_hours,
        paired_labs.triglycerides,
        paired_labs.glucose AS blood_glucose,
        ln((paired_labs.triglycerides * paired_labs.glucose) / 2.0) AS tyg_index,
        CASE
            WHEN l.deathtime IS NOT NULL
             AND l.deathtime BETWEEN l.intime AND l.outtime
            THEN 1 ELSE 0
        END AS icu_mortality,
        CASE
            WHEN l.deathtime IS NOT NULL
             AND l.deathtime >= l.admittime
             AND (l.dischtime IS NULL OR l.deathtime <= l.dischtime)
            THEN 1 ELSE 0
        END AS in_hospital_mortality,
        CASE
            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '30 days'
            THEN 1 ELSE 0
        END AS mortality_30d,
        CASE
            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '90 days'
            THEN 1 ELSE 0
        END AS mortality_90d,
        CASE
            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '180 days'
            THEN 1 ELSE 0
        END AS mortality_180d,
        CASE
            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '365 days'
            THEN 1 ELSE 0
        END AS mortality_1y,
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
        ) / 86400.0 AS time_to_icu_event_days,
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
                ) - l.intime
            )
        ) / 86400.0 AS time_to_in_hospital_event_days,
        LEAST(
            30.0,
            EXTRACT(
                EPOCH FROM (
                    COALESCE(
                        CASE
                            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
                             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
                             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '30 days'
                            THEN COALESCE(l.deathtime, l.patient_dod)
                        END,
                        l.intime + INTERVAL '30 days'
                    ) - l.intime
                )
            ) / 86400.0
        ) AS time_to_event_30d_days,
        LEAST(
            90.0,
            EXTRACT(
                EPOCH FROM (
                    COALESCE(
                        CASE
                            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
                             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
                             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '90 days'
                            THEN COALESCE(l.deathtime, l.patient_dod)
                        END,
                        l.intime + INTERVAL '90 days'
                    ) - l.intime
                )
            ) / 86400.0
        ) AS time_to_event_90d_days,
        LEAST(
            180.0,
            EXTRACT(
                EPOCH FROM (
                    COALESCE(
                        CASE
                            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
                             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
                             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '180 days'
                            THEN COALESCE(l.deathtime, l.patient_dod)
                        END,
                        l.intime + INTERVAL '180 days'
                    ) - l.intime
                )
            ) / 86400.0
        ) AS time_to_event_180d_days,
        LEAST(
            365.0,
            EXTRACT(
                EPOCH FROM (
                    COALESCE(
                        CASE
                            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
                             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
                             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '365 days'
                            THEN COALESCE(l.deathtime, l.patient_dod)
                        END,
                        l.intime + INTERVAL '365 days'
                    ) - l.intime
                )
            ) / 86400.0
        ) AS time_to_event_1y_days
    FROM non_diabetic_filtered l
    JOIN paired_labs
        ON paired_labs.stay_id = l.stay_id
)
SELECT
    subject_id,
    hadm_id,
    stay_id,
    age,
    sex,
    race,
    insurance,
    marital_status,
    round(admit_to_icu_hours::numeric, 3) AS admit_to_icu_hours,
    round(icu_los_hours::numeric, 3) AS icu_los_hours,
    round(hospital_los_hours::numeric, 3) AS hospital_los_hours,
    round(triglycerides::numeric, 6) AS triglycerides,
    round(blood_glucose::numeric, 6) AS blood_glucose,
    round(tyg_index::numeric, 6) AS tyg_index,
    {_quartile_case_sql("tyg_index", STROKE_TYG_QUARTILE_BOUNDS)} AS tyg_quartile,
    icu_mortality,
    in_hospital_mortality,
    mortality_30d,
    mortality_90d,
    mortality_180d,
    mortality_1y,
    round(time_to_icu_event_days::numeric, 6) AS time_to_icu_event_days,
    round(time_to_in_hospital_event_days::numeric, 6) AS time_to_in_hospital_event_days,
    round(time_to_event_30d_days::numeric, 6) AS time_to_event_30d_days,
    round(time_to_event_90d_days::numeric, 6) AS time_to_event_90d_days,
    round(time_to_event_180d_days::numeric, 6) AS time_to_event_180d_days,
    round(time_to_event_1y_days::numeric, 6) AS time_to_event_1y_days
FROM final_raw
"""


def build_tyg_stroke_funnel_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_TYG_STROKE_PROFILE,
) -> str:
    return f"""
WITH first_icu_index AS (
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
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
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours
    FROM first_icu_index f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = f.hadm_id
    WHERE {_age_clause(profile)}
),
stroke_primary_hadm AS (
    SELECT DISTINCT dx.hadm_id
    FROM mimiciv_hosp.diagnoses_icd dx
    WHERE COALESCE(dx.seq_num, 1) = 1
      AND {_stroke_ischemic_dx_match_sql("dx")}
),
stroke_primary_filtered AS (
    SELECT a.*
    FROM adult_first_icu a
    JOIN stroke_primary_hadm s
        ON s.hadm_id = a.hadm_id
),
los_filtered AS (
    SELECT *
    FROM stroke_primary_filtered
    WHERE icu_los_hours IS NOT NULL
      AND icu_los_hours >= {profile.min_icu_los_hours}
),
{_stroke_non_diabetic_filter_ctes()},
paired_labs AS (
    SELECT DISTINCT l.stay_id
    FROM non_diabetic_filtered l
    JOIN mimiciv_hosp.labevents tg
        ON tg.hadm_id = l.hadm_id
    JOIN mimiciv_hosp.labevents glu
        ON glu.hadm_id = l.hadm_id
       AND glu.itemid IN ({_int_list(GLUCOSE_ITEMIDS)})
       AND glu.valuenum IS NOT NULL
       AND glu.valuenum > 0
       AND glu.charttime = tg.charttime
    WHERE tg.itemid IN ({_int_list(TG_ITEMIDS)})
      AND tg.valuenum IS NOT NULL
      AND tg.valuenum > 0
      AND tg.charttime >= {_lab_window_lower_bound(profile)}
      AND tg.charttime <= {_lab_window_upper_bound(profile)}
),
final_cohort AS (
    SELECT stay_id
    FROM paired_labs
)
SELECT
    (SELECT COUNT(*) FROM first_icu_index) AS n_initial_first_icu,
    (SELECT COUNT(*) FROM adult_first_icu) AS n_after_age,
    (SELECT COUNT(*) FROM stroke_primary_filtered) AS n_after_primary_ischemic_stroke,
    (SELECT COUNT(*) FROM los_filtered) AS n_after_icu_los,
    (SELECT COUNT(*) FROM non_diabetic_filtered) AS n_after_non_diabetic_exclusion,
    (SELECT COUNT(*) FROM final_cohort) AS n_final_with_day1_tyg
"""


def build_nlr_sepsis_funnel_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_NLR_PROFILE,
) -> str:
    return f"""
WITH first_icu_index AS (
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
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
    FROM first_icu_index f
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
eligible_filtered AS (
    SELECT l.*
    FROM los_filtered l
    LEFT JOIN mimiciv_derived.charlson ch
        ON ch.hadm_id = l.hadm_id
    WHERE COALESCE(ch.malignant_cancer, 0) = 0
      AND COALESCE(ch.metastatic_solid_tumor, 0) = 0
      AND COALESCE(ch.aids, 0) = 0
      AND COALESCE(ch.rheumatic_disease, 0) = 0
),
nlr_measurement AS (
    SELECT DISTINCT l.stay_id
    FROM eligible_filtered l
    JOIN mimiciv_derived.blood_differential d
        ON d.hadm_id = l.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound(profile)}
      AND d.charttime <= {_lab_window_upper_bound(profile)}
      AND d.neutrophils_abs IS NOT NULL
      AND d.neutrophils_abs > 0
      AND d.lymphocytes_abs IS NOT NULL
      AND d.lymphocytes_abs > 0
)
SELECT
    (SELECT COUNT(*) FROM first_icu_index) AS n_initial_first_icu,
    (SELECT COUNT(*) FROM adult_first_icu) AS n_after_age,
    (SELECT COUNT(*) FROM admit_gap_filtered) AS n_after_admit_gap,
    (SELECT COUNT(*) FROM sepsis_cohort) AS n_after_sepsis,
    (SELECT COUNT(*) FROM hospital_time_filtered) AS n_after_hospital_time,
    (SELECT COUNT(*) FROM los_filtered) AS n_after_icu_los,
    (SELECT COUNT(*) FROM eligible_filtered) AS n_after_exclusion_flags,
    (SELECT COUNT(*) FROM nlr_measurement) AS n_final_with_nlr
"""


def build_hr_trajectory_sepsis_cohort_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_HEART_RATE_TRAJECTORY_PROFILE,
) -> str:
    hours = max(int(profile.trajectory_window_hours or 0), 10)
    return f"""
WITH icu_counts AS (
    SELECT
        subject_id,
        COUNT(*) AS icu_stay_count
    FROM mimiciv_icu.icustays
    GROUP BY subject_id
),
first_icu_index AS (
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
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
        p.dod AS patient_dod,
        a.admittime,
        a.dischtime,
        a.deathtime,
        a.marital_status,
        a.race,
        c.icu_stay_count,
        EXTRACT(EPOCH FROM (f.intime - a.admittime)) / 3600.0 AS admit_to_icu_hours,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours,
        EXTRACT(EPOCH FROM (a.dischtime - a.admittime)) / 3600.0 AS hospital_los_hours
    FROM first_icu_index f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = f.hadm_id
    LEFT JOIN icu_counts c
        ON c.subject_id = f.subject_id
    WHERE {_age_clause(profile)}
),
single_icu_filtered AS (
    SELECT *
    FROM adult_first_icu
    WHERE {("COALESCE(icu_stay_count, 0) = 1" if profile.exclude_multiple_icu_stays else "TRUE")}
),
admit_gap_filtered AS (
    SELECT *
    FROM single_icu_filtered
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
hourly_hr_raw AS (
    SELECT
        l.stay_id,
        bucket.hour_index,
        v.charttime,
        v.heart_rate,
        ROW_NUMBER() OVER (
            PARTITION BY l.stay_id, bucket.hour_index
            ORDER BY v.charttime NULLS LAST
        ) AS rn
    FROM los_filtered l
    CROSS JOIN generate_series(1, {hours}) AS bucket(hour_index)
    LEFT JOIN mimiciv_derived.vitalsign v
        ON v.stay_id = l.stay_id
       AND v.heart_rate IS NOT NULL
       AND v.charttime >= l.intime + make_interval(hours => bucket.hour_index - 1)
       AND v.charttime < l.intime + make_interval(hours => bucket.hour_index)
),
hourly_hr AS (
    SELECT
        stay_id,
        hour_index,
        charttime,
        heart_rate
    FROM hourly_hr_raw
    WHERE rn = 1
      AND heart_rate IS NOT NULL
),
hourly_panel AS (
    SELECT
        stay_id,
        COUNT(*) AS hourly_measurement_count,
        MIN(charttime) AS first_heart_rate_charttime,
        {_trajectory_panel_case_sql(hours)}
    FROM hourly_hr
    GROUP BY stay_id
    HAVING COUNT(*) = {hours}
),
final_raw AS (
    SELECT
        l.subject_id,
        l.hadm_id,
        l.stay_id,
        l.age,
        l.sex,
        l.race,
        l.marital_status,
        l.sepsis3_flag,
        l.suspected_infection_time,
        l.sofa_time,
        l.sofa_score,
        l.admit_to_icu_hours,
        l.icu_los_hours,
        l.hospital_los_hours,
        hp.hourly_measurement_count,
        hp.first_heart_rate_charttime,
        {_trajectory_cte_column_sql("hp", hours)}
        CASE
            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '30 days'
            THEN 1 ELSE 0
        END AS mortality_30d,
        LEAST(
            30.0,
            EXTRACT(
                EPOCH FROM (
                    COALESCE(
                        CASE
                            WHEN COALESCE(l.deathtime, l.patient_dod) IS NOT NULL
                             AND COALESCE(l.deathtime, l.patient_dod) >= l.intime
                             AND COALESCE(l.deathtime, l.patient_dod) <= l.intime + INTERVAL '30 days'
                            THEN COALESCE(l.deathtime, l.patient_dod)
                        END,
                        l.intime + INTERVAL '30 days'
                    ) - l.intime
                )
            ) / 86400.0
        ) AS time_to_event_30d_days
    FROM los_filtered l
    JOIN hourly_panel hp
        ON hp.stay_id = l.stay_id
)
SELECT
    subject_id,
    hadm_id,
    stay_id,
    age,
    sex,
    race,
    marital_status,
    sepsis3_flag,
    suspected_infection_time,
    sofa_time,
    round(sofa_score::numeric, 6) AS sofa_score,
    round(admit_to_icu_hours::numeric, 3) AS admit_to_icu_hours,
    round(icu_los_hours::numeric, 3) AS icu_los_hours,
    round(hospital_los_hours::numeric, 3) AS hospital_los_hours,
    hourly_measurement_count,
    first_heart_rate_charttime,
    {_trajectory_select_columns_sql(hours)}
    mortality_30d,
    round(time_to_event_30d_days::numeric, 6) AS time_to_event_30d_days
FROM final_raw
"""


def build_hr_trajectory_sepsis_funnel_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_HEART_RATE_TRAJECTORY_PROFILE,
) -> str:
    hours = max(int(profile.trajectory_window_hours or 0), 10)
    return f"""
WITH icu_counts AS (
    SELECT
        subject_id,
        COUNT(*) AS icu_stay_count
    FROM mimiciv_icu.icustays
    GROUP BY subject_id
),
first_icu_index AS (
    SELECT DISTINCT ON ({_first_icu_partition_expr(profile)})
        i.subject_id,
        i.hadm_id,
        i.stay_id,
        i.intime,
        i.outtime
    FROM mimiciv_icu.icustays i
    ORDER BY {_first_icu_order_expr(profile)}
),
adult_first_icu AS (
    SELECT
        f.subject_id,
        f.hadm_id,
        f.stay_id,
        f.intime,
        f.outtime,
        p.anchor_age AS age,
        c.icu_stay_count,
        a.admittime,
        a.dischtime,
        EXTRACT(EPOCH FROM (f.intime - a.admittime)) / 3600.0 AS admit_to_icu_hours,
        EXTRACT(EPOCH FROM (f.outtime - f.intime)) / 3600.0 AS icu_los_hours,
        EXTRACT(EPOCH FROM (a.dischtime - a.admittime)) / 3600.0 AS hospital_los_hours
    FROM first_icu_index f
    JOIN mimiciv_hosp.patients p
        ON p.subject_id = f.subject_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = f.hadm_id
    LEFT JOIN icu_counts c
        ON c.subject_id = f.subject_id
    WHERE {_age_clause(profile)}
),
single_icu_filtered AS (
    SELECT *
    FROM adult_first_icu
    WHERE {("COALESCE(icu_stay_count, 0) = 1" if profile.exclude_multiple_icu_stays else "TRUE")}
),
admit_gap_filtered AS (
    SELECT *
    FROM single_icu_filtered
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
hourly_hr AS (
    SELECT
        l.stay_id,
        bucket.hour_index,
        MIN(v.charttime) AS first_charttime
    FROM los_filtered l
    CROSS JOIN generate_series(1, {hours}) AS bucket(hour_index)
    LEFT JOIN mimiciv_derived.vitalsign v
        ON v.stay_id = l.stay_id
       AND v.heart_rate IS NOT NULL
       AND v.charttime >= l.intime + make_interval(hours => bucket.hour_index - 1)
       AND v.charttime < l.intime + make_interval(hours => bucket.hour_index)
    GROUP BY l.stay_id, bucket.hour_index
),
hourly_panel AS (
    SELECT
        stay_id,
        COUNT(first_charttime) AS hourly_measurement_count
    FROM hourly_hr
    GROUP BY stay_id
    HAVING COUNT(first_charttime) = {hours}
)
SELECT
    (SELECT COUNT(*) FROM first_icu_index) AS n_initial_first_icu,
    (SELECT COUNT(*) FROM adult_first_icu) AS n_after_age,
    (SELECT COUNT(*) FROM single_icu_filtered) AS n_after_single_icu_filter,
    (SELECT COUNT(*) FROM admit_gap_filtered) AS n_after_admit_gap,
    (SELECT COUNT(*) FROM sepsis_cohort) AS n_after_sepsis,
    (SELECT COUNT(*) FROM hospital_time_filtered) AS n_after_hospital_time,
    (SELECT COUNT(*) FROM los_filtered) AS n_after_icu_los,
    (SELECT COUNT(*) FROM hourly_panel) AS n_final_with_hourly_hr_panel
"""


def _age_clause(profile: TygSepsisCohortProfile) -> str:
    clauses = [f"p.anchor_age >= {profile.min_age_years}"]
    if profile.max_age_years is not None:
        clauses.append(f"p.anchor_age <= {profile.max_age_years}")
    if profile.admit_year_start is not None:
        clauses.append(
            "COALESCE(NULLIF(split_part(p.anchor_year_group, ' - ', 2), '')::int, "
            f"EXTRACT(YEAR FROM a.admittime)::int) >= {profile.admit_year_start}"
        )
    if profile.admit_year_end is not None:
        clauses.append(
            "COALESCE(NULLIF(split_part(p.anchor_year_group, ' - ', 1), '')::int, "
            f"EXTRACT(YEAR FROM a.admittime)::int) <= {profile.admit_year_end}"
        )
    return " AND ".join(clauses)


def _first_icu_partition_expr(profile: TygSepsisCohortProfile) -> str:
    return "i.hadm_id" if profile.first_icu_scope == "hadm" else "i.subject_id"


def _first_icu_order_expr(profile: TygSepsisCohortProfile) -> str:
    partition = "i.hadm_id" if profile.first_icu_scope == "hadm" else "i.subject_id"
    return f"{partition}, i.intime, i.stay_id"


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


def _quartile_case_sql(expr: str, bounds: tuple[float, float, float]) -> str:
    q1_max, q2_max, q3_max = bounds
    return (
        "CASE "
        f"WHEN {expr} <= {q1_max} THEN 'Q1' "
        f"WHEN {expr} <= {q2_max} THEN 'Q2' "
        f"WHEN {expr} <= {q3_max} THEN 'Q3' "
        "ELSE 'Q4' "
        "END"
    )


def _stroke_ischemic_dx_match_sql(alias: str) -> str:
    icd9_clause = " OR ".join(f"{alias}.icd_code = '{code}'" for code in STROKE_ISCHEMIC_ICD9_CODES)
    icd10_clause = " OR ".join(f"{alias}.icd_code LIKE '{prefix}%'" for prefix in STROKE_ISCHEMIC_ICD10_PREFIXES)
    return (
        f"(({alias}.icd_version = 9 AND ({icd9_clause})) "
        f"OR ({alias}.icd_version = 10 AND ({icd10_clause})))"
    )


def _diabetes_dx_match_sql(alias: str) -> str:
    icd9_clause = " OR ".join(f"{alias}.icd_code LIKE '{prefix}%'" for prefix in DIABETES_ICD9_PREFIXES)
    icd10_clause = " OR ".join(f"{alias}.icd_code LIKE '{prefix}%'" for prefix in DIABETES_ICD10_PREFIXES)
    return (
        f"(({alias}.icd_version = 9 AND ({icd9_clause})) "
        f"OR ({alias}.icd_version = 10 AND ({icd10_clause})))"
    )


def _antidiabetic_drug_match_sql(alias: str) -> str:
    patterns = (
        "insulin",
        "metformin",
        "glipizide",
        "glyburide",
        "glimepiride",
        "pioglitazone",
        "rosiglitazone",
        "sitagliptin",
        "linagliptin",
        "alogliptin",
        "saxagliptin",
        "empagliflozin",
        "dapagliflozin",
        "canagliflozin",
        "liraglutide",
        "semaglutide",
        "exenatide",
        "dulaglutide",
    )
    clauses = [f"LOWER(COALESCE({alias}.drug, '')) LIKE '%{pattern}%'" for pattern in patterns]
    exclusions = [
        f"LOWER(COALESCE({alias}.drug, '')) NOT LIKE '%catheter clearance%'",
        f"LOWER(COALESCE({alias}.drug, '')) NOT LIKE '%external ventricular drain%'",
    ]
    return "(" + " OR ".join(clauses) + ") AND " + " AND ".join(exclusions)


def _trajectory_panel_case_sql(hours: int) -> str:
    return ",\n        ".join(
        f"MAX(CASE WHEN hour_index = {hour} THEN heart_rate END) AS heart_rate_hour_{hour}"
        for hour in range(1, hours + 1)
    )


def _trajectory_select_columns_sql(hours: int) -> str:
    return "".join(
        f"round(heart_rate_hour_{hour}::numeric, 6) AS heart_rate_hour_{hour},\n    "
        for hour in range(1, hours + 1)
    )


def _trajectory_cte_column_sql(alias: str, hours: int) -> str:
    return "".join(
        f"{alias}.heart_rate_hour_{hour},\n        "
        for hour in range(1, hours + 1)
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

from __future__ import annotations

from dataclasses import dataclass

from .cohort_sql import (
    GLUCOSE_ITEMIDS,
    NLR_QUARTILE_BOUNDS,
    PAPER_MIMIC_NLR_PROFILE,
    PAPER_MIMIC_TYG_PROFILE,
    TG_ITEMIDS,
    TygSepsisCohortProfile,
    build_nlr_sepsis_cohort_sql,
    build_tyg_sepsis_cohort_sql,
)


HBA1C_ITEMIDS: tuple[int, ...] = (50852,)
ALBUMIN_BLOOD_ITEMIDS: tuple[int, ...] = (50862, 52022, 53085, 53138)
TOTAL_CHOLESTEROL_ITEMIDS: tuple[int, ...] = (50907,)
HDL_ITEMIDS: tuple[int, ...] = (50904,)
LDL_ITEMIDS: tuple[int, ...] = (50905, 50906)
URINE_ALBUMIN_ITEMIDS: tuple[int, ...] = (51069, 52703)
URINE_GLUCOSE_ITEMIDS: tuple[int, ...] = (51084,)
ANALYSIS_LABEVENT_ITEMIDS: tuple[int, ...] = (
    *ALBUMIN_BLOOD_ITEMIDS,
    *GLUCOSE_ITEMIDS,
    *HBA1C_ITEMIDS,
    *TG_ITEMIDS,
    *TOTAL_CHOLESTEROL_ITEMIDS,
    *HDL_ITEMIDS,
    *LDL_ITEMIDS,
    *URINE_GLUCOSE_ITEMIDS,
    *URINE_ALBUMIN_ITEMIDS,
)


@dataclass(frozen=True)
class DiagnosisCodePrefixes:
    icd9: tuple[str, ...] = ()
    icd10: tuple[str, ...] = ()


COMORBIDITY_CODE_PREFIXES: dict[str, DiagnosisCodePrefixes] = {
    "hypertension": DiagnosisCodePrefixes(
        icd9=("401",),
        icd10=("I10",),
    ),
    "type2_diabetes": DiagnosisCodePrefixes(
        icd9=("250",),
        icd10=("E11",),
    ),
    "heart_failure": DiagnosisCodePrefixes(
        icd9=("428", "40201", "40211", "40291", "40401", "40403", "40411", "40413", "40491", "40493"),
        icd10=("I50",),
    ),
    "myocardial_infarction": DiagnosisCodePrefixes(
        icd9=("410", "412"),
        icd10=("I21", "I22", "I252"),
    ),
    "malignant_tumor": DiagnosisCodePrefixes(
        icd9=("140", "141", "142", "143", "144", "145", "146", "147", "148", "149", "150", "151", "152", "153",
              "154", "155", "156", "157", "158", "159", "160", "161", "162", "163", "164", "165", "170", "171",
              "172", "174", "175", "176", "179", "180", "181", "182", "183", "184", "185", "186", "187", "188",
              "189", "190", "191", "192", "193", "194", "195", "196", "197", "198", "199", "200", "201", "202",
              "203", "204", "205", "206", "207", "208"),
        icd10=("C",),
    ),
    "chronic_renal_failure": DiagnosisCodePrefixes(
        icd9=("585", "586", "403", "404"),
        icd10=("N18", "N19", "I120", "I130", "I131", "I132"),
    ),
    "acute_renal_failure": DiagnosisCodePrefixes(
        icd9=("584",),
        icd10=("N17",),
    ),
    "cirrhosis": DiagnosisCodePrefixes(
        icd9=("5712", "5715", "5716"),
        icd10=("K703", "K74"),
    ),
    "hepatitis": DiagnosisCodePrefixes(
        icd9=("070", "5714"),
        icd10=("B15", "B16", "B17", "B18", "B19", "K73", "K75"),
    ),
    "tuberculosis": DiagnosisCodePrefixes(
        icd9=("010", "011", "012", "013", "014", "015", "016", "017", "018"),
        icd10=("A15", "A16", "A17", "A18", "A19"),
    ),
    "pneumonia": DiagnosisCodePrefixes(
        icd9=("480", "481", "482", "483", "484", "485", "486", "4870"),
        icd10=("J12", "J13", "J14", "J15", "J16", "J17", "J18", "J69"),
    ),
    "copd": DiagnosisCodePrefixes(
        icd9=("490", "491", "492", "4932", "494", "496"),
        icd10=("J41", "J42", "J43", "J44", "J47"),
    ),
    "hyperlipidemia": DiagnosisCodePrefixes(
        icd9=("272",),
        icd10=("E78",),
    ),
    "stroke": DiagnosisCodePrefixes(
        icd9=("430", "431", "432", "433", "434", "435", "436", "437", "438"),
        icd10=("I60", "I61", "I62", "I63", "I64", "I65", "I66", "I67", "I68", "I69", "G45"),
    ),
}


def build_tyg_analysis_dataset_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_TYG_PROFILE,
) -> str:
    cohort_sql = build_tyg_sepsis_cohort_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=profile)
    return f"""
WITH cohort AS (
{_indent_sql(cohort_sql, 4)}
),
cohort_ctx AS (
    SELECT
        c.*,
        i.intime,
        i.outtime,
        a.admittime,
        a.dischtime,
        a.deathtime,
        a.insurance,
        a.race,
        a.marital_status
    FROM cohort c
    JOIN mimiciv_icu.icustays i
        ON i.stay_id = c.stay_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = c.hadm_id
),
dx_flags AS (
    SELECT
        c.hadm_id,
{_diagnosis_flag_select_sql()}
    FROM cohort_ctx c
    LEFT JOIN mimiciv_hosp.diagnoses_icd dx
        ON dx.hadm_id = c.hadm_id
    GROUP BY c.hadm_id
),
cbc_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.wbc,
        d.rbc,
        d.rdw,
        d.platelet,
        d.hemoglobin,
        d.mcv,
        d.hematocrit
    FROM cohort_ctx c
    JOIN mimiciv_derived.complete_blood_count d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
cbc AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        wbc,
        rbc,
        rdw,
        platelet,
        hemoglobin,
        mcv,
        hematocrit
    FROM cbc_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
diff_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.neutrophils_abs,
        d.lymphocytes_abs
    FROM cohort_ctx c
    JOIN mimiciv_derived.blood_differential d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
diff AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        neutrophils_abs,
        lymphocytes_abs
    FROM diff_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
chem_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.albumin,
        d.globulin,
        d.total_protein,
        d.aniongap,
        d.bun,
        d.calcium,
        d.chloride,
        d.creatinine,
        d.glucose,
        d.sodium,
        d.potassium
    FROM cohort_ctx c
    JOIN mimiciv_derived.chemistry d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
chem AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
albumin,
        globulin,
        total_protein,
        aniongap,
        bun,
        calcium,
        chloride,
        creatinine,
        glucose,
        sodium,
        potassium
    FROM chem_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
bg_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.ph,
        d.pco2,
        d.po2,
        d.lactate,
        d.totalco2,
        d.calcium,
        d.chloride,
        d.glucose,
        d.potassium
    FROM cohort_ctx c
    JOIN mimiciv_derived.bg d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
bg AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        ph,
        pco2,
        po2,
        lactate,
        totalco2,
        calcium,
        chloride,
        glucose,
        potassium
    FROM bg_raw
    ORDER BY hadm_id, charttime NULLS LAST
),
coag_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.thrombin,
        d.fibrinogen,
        d.ptt,
        d.inr,
        d.d_dimer
    FROM cohort_ctx c
    JOIN mimiciv_derived.coagulation d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
coag AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        thrombin,
        fibrinogen,
        ptt,
        inr,
        d_dimer
    FROM coag_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
enzyme_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.bilirubin_total,
        d.bilirubin_direct,
        d.bilirubin_indirect,
        d.ast,
        d.alt,
        d.ld_ldh,
        d.ck_cpk,
        d.ck_mb
    FROM cohort_ctx c
    JOIN mimiciv_derived.enzyme d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
enzyme AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        bilirubin_total,
        bilirubin_direct,
        bilirubin_indirect,
        ast,
        alt,
        ld_ldh,
        ck_cpk,
        ck_mb
    FROM enzyme_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
cardiac_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.troponin_t,
        d.ck_mb,
        d.ntprobnp
    FROM cohort_ctx c
    JOIN mimiciv_derived.cardiac_marker d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
cardiac AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        troponin_t,
        ck_mb,
        ntprobnp
    FROM cardiac_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
labevents_raw AS (
    SELECT
        c.hadm_id,
        le.itemid,
        le.charttime,
        le.labevent_id,
        le.valuenum
    FROM cohort_ctx c
    JOIN mimiciv_hosp.labevents le
        ON le.hadm_id = c.hadm_id
    WHERE le.itemid IN ({_int_list(ANALYSIS_LABEVENT_ITEMIDS)})
      AND le.valuenum IS NOT NULL
      AND le.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND le.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
albumin_lab AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS albumin
    FROM labevents_raw
    WHERE itemid IN ({_int_list(ALBUMIN_BLOOD_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
glucose_lab AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS blood_glucose
    FROM labevents_raw
    WHERE itemid IN ({_int_list(GLUCOSE_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
hba1c AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS hba1c
    FROM labevents_raw
    WHERE itemid IN ({_int_list(HBA1C_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
triglycerides AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS triglycerides
    FROM labevents_raw
    WHERE itemid IN ({_int_list(TG_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
total_cholesterol AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS total_cholesterol
    FROM labevents_raw
    WHERE itemid IN ({_int_list(TOTAL_CHOLESTEROL_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
hdl AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS hdl_cholesterol
    FROM labevents_raw
    WHERE itemid IN ({_int_list(HDL_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
ldl AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS ldl_cholesterol
    FROM labevents_raw
    WHERE itemid IN ({_int_list(LDL_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
urine_glucose AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS urine_glucose
    FROM labevents_raw
    WHERE itemid IN ({_int_list(URINE_GLUCOSE_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
urine_albumin AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        valuenum AS urine_albumin
    FROM labevents_raw
    WHERE itemid IN ({_int_list(URINE_ALBUMIN_ITEMIDS)})
    ORDER BY hadm_id, charttime NULLS LAST, labevent_id
),
crrt_flag AS (
    SELECT DISTINCT
        c.stay_id,
        1 AS crrt_present
    FROM cohort_ctx c
    JOIN mimiciv_derived.crrt d
        ON d.stay_id = c.stay_id
    WHERE d.charttime >= c.intime
      AND d.charttime <= c.outtime
),
ventilation_flag AS (
    SELECT DISTINCT
        c.stay_id,
        1 AS ventilation_present
    FROM cohort_ctx c
    JOIN mimiciv_derived.ventilation d
        ON d.stay_id = c.stay_id
    WHERE d.starttime <= c.outtime
      AND COALESCE(d.endtime, c.outtime) >= c.intime
)
SELECT
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    c.age,
    c.sex AS gender,
    c.insurance,
    c.race,
    c.marital_status,
    round(fd_height.height::numeric, 3) AS height_cm,
    round(fd_weight.weight::numeric, 3) AS weight_kg,
    CASE
        WHEN fd_height.height > 0 AND fd_weight.weight > 0
        THEN round((fd_weight.weight / power(fd_height.height / 100.0, 2))::numeric, 6)
        ELSE NULL
    END AS bmi,
    COALESCE(dx.hypertension, 0) AS hypertension,
    COALESCE(dx.type2_diabetes, 0) AS type2_diabetes,
    COALESCE(dx.heart_failure, 0) AS heart_failure,
    COALESCE(dx.myocardial_infarction, 0) AS myocardial_infarction,
    COALESCE(dx.malignant_tumor, 0) AS malignant_tumor,
    COALESCE(dx.chronic_renal_failure, 0) AS chronic_renal_failure,
    COALESCE(dx.acute_renal_failure, 0) AS acute_renal_failure,
    COALESCE(dx.cirrhosis, 0) AS cirrhosis,
    COALESCE(dx.hepatitis, 0) AS hepatitis,
    COALESCE(dx.tuberculosis, 0) AS tuberculosis,
    COALESCE(dx.pneumonia, 0) AS pneumonia,
    COALESCE(dx.copd, 0) AS copd,
    COALESCE(dx.hyperlipidemia, 0) AS hyperlipidemia,
    COALESCE(dx.stroke, 0) AS stroke,
    round(cbc.wbc::numeric, 6) AS white_blood_cell_count,
    round(cbc.rbc::numeric, 6) AS red_blood_cell_count,
    round(cbc.rdw::numeric, 6) AS rdw,
    round(diff.neutrophils_abs::numeric, 6) AS neutrophil_count,
    round(diff.lymphocytes_abs::numeric, 6) AS lymphocyte_count,
    round(cbc.platelet::numeric, 6) AS platelet_count,
    round(cbc.hemoglobin::numeric, 6) AS hemoglobin_count,
    round(cbc.mcv::numeric, 6) AS mean_corpuscular_volume,
    round(cbc.hematocrit::numeric, 6) AS hematocrit,
    round(COALESCE(chem.albumin, albumin_lab.albumin)::numeric, 6) AS albumin,
    round(chem.globulin::numeric, 6) AS globulin,
    round(chem.total_protein::numeric, 6) AS total_protein,
    round(chem.sodium::numeric, 6) AS sodium,
    round(COALESCE(bg.potassium, chem.potassium)::numeric, 6) AS potassium,
    round(COALESCE(bg.calcium, chem.calcium)::numeric, 6) AS calcium,
    round(COALESCE(bg.chloride, chem.chloride)::numeric, 6) AS chloride,
    round(COALESCE(glucose_lab.blood_glucose, chem.glucose, bg.glucose)::numeric, 6) AS blood_glucose,
    round(hba1c.hba1c::numeric, 6) AS hba1c,
    round(chem.aniongap::numeric, 6) AS anion_gap,
    round(bg.ph::numeric, 6) AS blood_ph,
    round(bg.pco2::numeric, 6) AS arterial_pco2,
    round(bg.po2::numeric, 6) AS arterial_po2,
    round(bg.lactate::numeric, 6) AS lactate,
    round(bg.totalco2::numeric, 6) AS total_carbon_dioxide,
    round(coag.thrombin::numeric, 6) AS thrombin_time,
    round(coag.fibrinogen::numeric, 6) AS fibrinogen,
    round(coag.ptt::numeric, 6) AS partial_thromboplastin_time,
    round(coag.inr::numeric, 6) AS international_normalized_ratio,
    round(coag.d_dimer::numeric, 6) AS d_dimer,
    round(triglycerides.triglycerides::numeric, 6) AS triglycerides,
    round(total_cholesterol.total_cholesterol::numeric, 6) AS total_cholesterol,
    round(hdl.hdl_cholesterol::numeric, 6) AS high_density_lipoprotein,
    round(ldl.ldl_cholesterol::numeric, 6) AS low_density_lipoprotein,
    round(enzyme.bilirubin_total::numeric, 6) AS total_bilirubin,
    round(enzyme.bilirubin_direct::numeric, 6) AS direct_bilirubin,
    round(enzyme.bilirubin_indirect::numeric, 6) AS indirect_bilirubin,
    round(enzyme.ast::numeric, 6) AS aspartate_aminotransferase,
    round(enzyme.alt::numeric, 6) AS alanine_aminotransferase,
    round(chem.bun::numeric, 6) AS urea_nitrogen,
    round(chem.creatinine::numeric, 6) AS creatinine,
    round(enzyme.ld_ldh::numeric, 6) AS lactate_dehydrogenase,
    round(enzyme.ck_cpk::numeric, 6) AS creatine_kinase,
    round(COALESCE(cardiac.ck_mb, enzyme.ck_mb)::numeric, 6) AS creatine_kinase_isoenzyme,
    round(cardiac.troponin_t::numeric, 6) AS troponin_t,
    round(cardiac.ntprobnp::numeric, 6) AS ntprobnp,
    round(urine_glucose.urine_glucose::numeric, 6) AS urinary_sugar,
    round(urine_albumin.urine_albumin::numeric, 6) AS urinary_albumin,
    CASE WHEN crrt_flag.crrt_present IS NULL THEN 0 ELSE 1 END AS continuous_renal_replacement_therapy,
    CASE WHEN ventilation_flag.ventilation_present IS NULL THEN 0 ELSE 1 END AS mechanical_ventilation,
    round(fd_sofa.sofa::numeric, 6) AS sofa_score,
    round(aps.apsiii::numeric, 6) AS apache_iii_score,
    round(saps.sapsii::numeric, 6) AS saps_ii_score,
    round(oasis.oasis::numeric, 6) AS oasis_score,
    round(charlson.charlson_comorbidity_index::numeric, 6) AS charlson_score,
    round(sirs.sirs::numeric, 6) AS sirs_score,
    round(fd_gcs.gcs_min::numeric, 6) AS gcs_score,
    round(c.hospital_survival_hours::numeric, 6) AS hospital_survival_hours,
    round(c.icu_survival_hours::numeric, 6) AS icu_survival_hours,
    round(c.hospital_los_hours::numeric, 6) AS hospital_los_hours,
    round(c.icu_los_hours::numeric, 6) AS icu_los_hours,
    c.hospital_mortality AS in_hospital_mortality,
    c.icu_mortality AS icu_mortality,
    round(c.tyg_index::numeric, 6) AS tyg_index,
    c.tyg_quartile,
    c.sepsis3_flag,
    c.suspected_infection_time,
    c.sofa_time,
    round(c.sofa_score::numeric, 6) AS sepsis3_sofa_score
FROM cohort_ctx c
LEFT JOIN dx_flags dx
    ON dx.hadm_id = c.hadm_id
LEFT JOIN mimiciv_derived.first_day_height fd_height
    ON fd_height.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.first_day_weight fd_weight
    ON fd_weight.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.first_day_sofa fd_sofa
    ON fd_sofa.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.apsiii aps
    ON aps.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.sapsii saps
    ON saps.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.oasis oasis
    ON oasis.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.charlson charlson
    ON charlson.hadm_id = c.hadm_id
LEFT JOIN mimiciv_derived.sirs sirs
    ON sirs.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.first_day_gcs fd_gcs
    ON fd_gcs.stay_id = c.stay_id
LEFT JOIN cbc
    ON cbc.hadm_id = c.hadm_id
LEFT JOIN diff
    ON diff.hadm_id = c.hadm_id
LEFT JOIN chem
    ON chem.hadm_id = c.hadm_id
LEFT JOIN bg
    ON bg.hadm_id = c.hadm_id
LEFT JOIN coag
    ON coag.hadm_id = c.hadm_id
LEFT JOIN enzyme
    ON enzyme.hadm_id = c.hadm_id
LEFT JOIN cardiac
    ON cardiac.hadm_id = c.hadm_id
LEFT JOIN albumin_lab
    ON albumin_lab.hadm_id = c.hadm_id
LEFT JOIN glucose_lab
    ON glucose_lab.hadm_id = c.hadm_id
LEFT JOIN hba1c
    ON hba1c.hadm_id = c.hadm_id
LEFT JOIN triglycerides
    ON triglycerides.hadm_id = c.hadm_id
LEFT JOIN total_cholesterol
    ON total_cholesterol.hadm_id = c.hadm_id
LEFT JOIN hdl
    ON hdl.hadm_id = c.hadm_id
LEFT JOIN ldl
    ON ldl.hadm_id = c.hadm_id
LEFT JOIN urine_glucose
    ON urine_glucose.hadm_id = c.hadm_id
LEFT JOIN urine_albumin
    ON urine_albumin.hadm_id = c.hadm_id
LEFT JOIN crrt_flag
    ON crrt_flag.stay_id = c.stay_id
LEFT JOIN ventilation_flag
    ON ventilation_flag.stay_id = c.stay_id
ORDER BY c.stay_id
"""


def build_nlr_analysis_dataset_sql(
    mode: str,
    has_sepsis3_flag: bool,
    profile: TygSepsisCohortProfile = PAPER_MIMIC_NLR_PROFILE,
) -> str:
    cohort_sql = build_nlr_sepsis_cohort_sql(mode=mode, has_sepsis3_flag=has_sepsis3_flag, profile=profile)
    q1_max, q2_max, q3_max = NLR_QUARTILE_BOUNDS
    return f"""
WITH cohort AS (
{_indent_sql(cohort_sql, 4)}
),
cohort_ctx AS (
    SELECT
        c.*,
        i.intime,
        i.outtime,
        a.admittime,
        a.dischtime,
        a.insurance,
        a.marital_status
    FROM cohort c
    JOIN mimiciv_icu.icustays i
        ON i.stay_id = c.stay_id
    JOIN mimiciv_hosp.admissions a
        ON a.hadm_id = c.hadm_id
),
cbc_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.wbc,
        d.rbc,
        d.platelet,
        d.hemoglobin
    FROM cohort_ctx c
    JOIN mimiciv_derived.complete_blood_count d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
cbc AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        wbc,
        rbc,
        platelet,
        hemoglobin
    FROM cbc_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
chem_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.bun,
        d.calcium,
        d.creatinine,
        d.glucose,
        d.sodium,
        d.potassium
    FROM cohort_ctx c
    JOIN mimiciv_derived.chemistry d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
chem AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        bun,
        calcium,
        creatinine,
        glucose,
        sodium,
        potassium
    FROM chem_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
bg_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.calcium,
        d.glucose,
        d.potassium,
        d.ph,
        d.lactate
    FROM cohort_ctx c
    JOIN mimiciv_derived.bg d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
bg AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        calcium,
        glucose,
        potassium,
        ph,
        lactate
    FROM bg_raw
    ORDER BY hadm_id, charttime NULLS LAST
),
coag_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.inr
    FROM cohort_ctx c
    JOIN mimiciv_derived.coagulation d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
coag AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        inr
    FROM coag_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
enzyme_raw AS (
    SELECT
        c.hadm_id,
        d.charttime,
        d.specimen_id,
        d.alt
    FROM cohort_ctx c
    JOIN mimiciv_derived.enzyme d
        ON d.hadm_id = c.hadm_id
    WHERE d.charttime >= {_lab_window_lower_bound_sql("c", profile)}
      AND d.charttime <= {_lab_window_upper_bound_sql("c", profile)}
),
enzyme AS (
    SELECT DISTINCT ON (hadm_id)
        hadm_id,
        alt
    FROM enzyme_raw
    ORDER BY hadm_id, charttime NULLS LAST, specimen_id NULLS LAST
),
charlson_flags AS (
    SELECT
        c.hadm_id,
        COALESCE(ch.myocardial_infarct, 0) AS myocardial_infarct,
        COALESCE(ch.congestive_heart_failure, 0) AS congestive_heart_failure,
        COALESCE(ch.cerebrovascular_disease, 0) AS cerebrovascular_disease,
        COALESCE(ch.chronic_pulmonary_disease, 0) AS chronic_pulmonary_disease,
        COALESCE(ch.mild_liver_disease, 0) AS mild_liver_disease,
        COALESCE(ch.severe_liver_disease, 0) AS severe_liver_disease,
        COALESCE(ch.renal_disease, 0) AS renal_disease,
        COALESCE(ch.diabetes_without_cc, 0) AS diabetes_without_cc,
        COALESCE(ch.diabetes_with_cc, 0) AS diabetes_with_cc
    FROM cohort_ctx c
    LEFT JOIN mimiciv_derived.charlson ch
        ON ch.hadm_id = c.hadm_id
),
rrt_flag AS (
    SELECT DISTINCT
        c.stay_id,
        1 AS renal_replacement_therapy
    FROM cohort_ctx c
    JOIN mimiciv_derived.rrt d
        ON d.stay_id = c.stay_id
    WHERE d.charttime >= c.intime
      AND d.charttime <= c.outtime
      AND COALESCE(d.dialysis_present, 0) = 1
),
ventilation_flag AS (
    SELECT DISTINCT
        c.stay_id,
        1 AS mechanical_ventilation
    FROM cohort_ctx c
    JOIN mimiciv_derived.ventilation d
        ON d.stay_id = c.stay_id
    WHERE d.starttime <= c.outtime
      AND COALESCE(d.endtime, c.outtime) >= c.intime
      AND COALESCE(d.ventilation_status, '') <> ''
),
vasopressor_flag AS (
    SELECT DISTINCT
        c.stay_id,
        1 AS vasopressor_use
    FROM cohort_ctx c
    JOIN mimiciv_derived.vasoactive_agent d
        ON d.stay_id = c.stay_id
    WHERE d.starttime <= c.outtime
      AND COALESCE(d.endtime, c.outtime) >= c.intime
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
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    c.age,
    c.sex AS gender,
    c.race,
    c.insurance,
    c.marital_status,
    round(fd_height.height::numeric, 3) AS height_cm,
    round(fd_weight.weight::numeric, 3) AS weight_kg,
    CASE
        WHEN fd_height.height > 0 AND fd_weight.weight > 0
        THEN round((fd_weight.weight / power(fd_height.height / 100.0, 2))::numeric, 6)
        ELSE NULL
    END AS bmi,
    round(vs.sbp_mean::numeric, 6) AS sbp,
    round(vs.dbp_mean::numeric, 6) AS dbp,
    round(vs.resp_rate_mean::numeric, 6) AS resp_rate,
    round(vs.heart_rate_mean::numeric, 6) AS heart_rate,
    round(vs.temperature_mean::numeric, 6) AS temperature,
    round(vs.spo2_mean::numeric, 6) AS spo2,
    round(cbc.wbc::numeric, 6) AS white_blood_cell_count,
    round(cbc.rbc::numeric, 6) AS red_blood_cell_count,
    round(c.neutrophil_count::numeric, 6) AS neutrophil_count,
    round(c.lymphocyte_count::numeric, 6) AS lymphocyte_count,
    round(c.nlr::numeric, 6) AS nlr,
    CASE
        WHEN c.nlr <= {q1_max} THEN 'Q1'
        WHEN c.nlr <= {q2_max} THEN 'Q2'
        WHEN c.nlr <= {q3_max} THEN 'Q3'
        ELSE 'Q4'
    END AS nlr_quartile,
    round(cbc.platelet::numeric, 6) AS platelet_count,
    round(cbc.hemoglobin::numeric, 6) AS hemoglobin,
    round(enzyme.alt::numeric, 6) AS alanine_aminotransferase,
    round(coag.inr::numeric, 6) AS international_normalized_ratio,
    round(chem.creatinine::numeric, 6) AS creatinine,
    round(chem.bun::numeric, 6) AS urea_nitrogen,
    round(bg.lactate::numeric, 6) AS lactate,
    round(bg.ph::numeric, 6) AS blood_ph,
    round(COALESCE(chem.glucose, vs.glucose_mean)::numeric, 6) AS blood_glucose,
    round(chem.sodium::numeric, 6) AS sodium,
    round(COALESCE(bg.potassium, chem.potassium)::numeric, 6) AS potassium,
    round(COALESCE(bg.calcium, chem.calcium)::numeric, 6) AS calcium,
    COALESCE(charlson_flags.congestive_heart_failure, 0) AS chf,
    COALESCE(charlson_flags.cerebrovascular_disease, 0) AS cvd,
    COALESCE(charlson_flags.chronic_pulmonary_disease, 0) AS copd,
    COALESCE(charlson_flags.myocardial_infarct, 0) AS mi,
    CASE
        WHEN COALESCE(charlson_flags.mild_liver_disease, 0) = 1
          OR COALESCE(charlson_flags.severe_liver_disease, 0) = 1
        THEN 1 ELSE 0
    END AS liver_disease,
    COALESCE(charlson_flags.renal_disease, 0) AS renal_disease,
    CASE
        WHEN COALESCE(charlson_flags.diabetes_without_cc, 0) = 1
          OR COALESCE(charlson_flags.diabetes_with_cc, 0) = 1
        THEN 1 ELSE 0
    END AS diabetes,
    round(fd_sofa.sofa::numeric, 6) AS sofa_score,
    round(saps.sapsii::numeric, 6) AS saps_ii_score,
    CASE WHEN ventilation_flag.mechanical_ventilation IS NULL THEN 0 ELSE 1 END AS mechanical_ventilation,
    CASE WHEN rrt_flag.renal_replacement_therapy IS NULL THEN 0 ELSE 1 END AS renal_replacement_therapy,
    CASE WHEN vasopressor_flag.vasopressor_use IS NULL THEN 0 ELSE 1 END AS vasopressor_use,
    c.sepsis3_flag,
    c.suspected_infection_time,
    c.sofa_time,
    c.nlr_charttime,
    c.mortality_28d,
    round(c.time_to_event_28d_hours::numeric, 6) AS time_to_event_28d_hours,
    round(c.time_to_event_28d_days::numeric, 6) AS time_to_event_28d_days,
    round((c.icu_los_hours / 24.0)::numeric, 6) AS icu_los_days,
    round((c.hospital_los_hours / 24.0)::numeric, 6) AS hospital_los_days
FROM cohort_ctx c
LEFT JOIN mimiciv_derived.first_day_height fd_height
    ON fd_height.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.first_day_weight fd_weight
    ON fd_weight.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.first_day_vitalsign vs
    ON vs.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.first_day_sofa fd_sofa
    ON fd_sofa.stay_id = c.stay_id
LEFT JOIN mimiciv_derived.sapsii saps
    ON saps.stay_id = c.stay_id
LEFT JOIN cbc
    ON cbc.hadm_id = c.hadm_id
LEFT JOIN chem
    ON chem.hadm_id = c.hadm_id
LEFT JOIN bg
    ON bg.hadm_id = c.hadm_id
LEFT JOIN coag
    ON coag.hadm_id = c.hadm_id
LEFT JOIN enzyme
    ON enzyme.hadm_id = c.hadm_id
LEFT JOIN charlson_flags
    ON charlson_flags.hadm_id = c.hadm_id
LEFT JOIN rrt_flag
    ON rrt_flag.stay_id = c.stay_id
LEFT JOIN ventilation_flag
    ON ventilation_flag.stay_id = c.stay_id
LEFT JOIN vasopressor_flag
    ON vasopressor_flag.stay_id = c.stay_id
ORDER BY c.stay_id
"""


def _indent_sql(sql: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(f"{pad}{line}" if line else "" for line in sql.splitlines())


def _int_list(values: tuple[int, ...]) -> str:
    return ", ".join(str(value) for value in values)


def _lab_window_lower_bound_sql(alias: str, profile: TygSepsisCohortProfile) -> str:
    if profile.lab_anchor == "admission":
        return f"{alias}.admittime"
    return f"{alias}.intime"


def _lab_window_upper_bound_sql(alias: str, profile: TygSepsisCohortProfile) -> str:
    if profile.lab_anchor == "admission":
        return (
            f"LEAST(COALESCE({alias}.dischtime, {alias}.outtime), "
            f"{alias}.admittime + INTERVAL '{profile.baseline_lab_window_hours} hours')"
        )
    return (
        f"LEAST({alias}.outtime, "
        f"{alias}.intime + INTERVAL '{profile.baseline_lab_window_hours} hours')"
    )


def _diagnosis_flag_select_sql() -> str:
    lines: list[str] = []
    names = list(COMORBIDITY_CODE_PREFIXES)
    for index, name in enumerate(names):
        prefix_map = COMORBIDITY_CODE_PREFIXES[name]
        suffix = "," if index < len(names) - 1 else ""
        lines.append(
            f"        MAX(CASE WHEN {_diagnosis_match_sql('dx', prefix_map)} THEN 1 ELSE 0 END) AS {name}{suffix}"
        )
    return "\n".join(lines)


def _diagnosis_match_sql(alias: str, prefixes: DiagnosisCodePrefixes) -> str:
    clauses: list[str] = []
    if prefixes.icd9:
        icd9_clause = " OR ".join(f"{alias}.icd_code LIKE '{prefix}%'" for prefix in prefixes.icd9)
        clauses.append(f"({alias}.icd_version = 9 AND ({icd9_clause}))")
    if prefixes.icd10:
        icd10_clause = " OR ".join(f"{alias}.icd_code LIKE '{prefix}%'" for prefix in prefixes.icd10)
        clauses.append(f"({alias}.icd_version = 10 AND ({icd10_clause}))")
    return " OR ".join(clauses) if clauses else "FALSE"

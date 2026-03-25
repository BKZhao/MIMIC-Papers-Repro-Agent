# MIMIC-IV-explanation Integration Notes

Source repository analyzed:
- local `MIMIC-IV-explanation` checkout

## Why it matters for this project

The repository already contains practical cohort-building and reporting code against
the same PostgreSQL-backed MIMIC environment used by this project.
It is a strong reference for our cohort/statistics/report subagents.

It also shows the real table families we can rely on in your environment:

- `mimiciv_hosp`
- `mimiciv_icu`
- `mimiciv_note`

The most reused tables in the production-like codepath are:

- `mimiciv_icu.icustays`
- `mimiciv_icu.chartevents`
- `mimiciv_icu.inputevents`
- `mimiciv_icu.procedureevents`
- `mimiciv_hosp.labevents`
- `mimiciv_hosp.diagnoses_icd`
- `mimiciv_hosp.admissions`
- `mimiciv_hosp.patients`
- `mimiciv_note.discharge`

## High-value reusable components

1. Cohort selection SQL patterns
- File: `data_exploration/code/build_cohort.py`
- Relevance: end-to-end funnel with explicit steps and intermediate criteria.
- Action: adapt SQL style and funnel accounting to `cohort_agent`.

Concrete reusable patterns from `build_cohort.py`:

- Start from `first_icu` using `DISTINCT ON (subject_id, hadm_id)` over `mimiciv_icu.icustays`.
- Define clinical anchors such as `T0` from the earliest qualifying event.
- Use event windows relative to `T0` or ICU stay bounds instead of global admission-level windows.
- Keep separate SQL blocks for funnel counts and final cohort extraction.
- Store both screening counts and final patient rows in structured outputs.

2. Modular extraction/report architecture
- Files: `data_exploration/code/shock_lib/*`, `generate_report.py`
- Relevance: good split of fetch, chart, and markdown rendering concerns.
- Action: mirror this separation in `stats_agent` + `report_agent`.

Concrete reusable patterns:

- `fetch.py` demonstrates bulk extraction keyed by `stay_id`, `hadm_id`, and custom anchor times.
- `charts.py` keeps visualization logic downstream from data fetching.
- `render.py` keeps markdown/report assembly separate from query logic.

3. Pipeline execution strategy
- File: `data_exploration/code/README.md` and `run_batch.sh`
- Relevance: phase-based orchestration with post-validation and summary.
- Action: map to our orchestrator step graph and quality gates.

The phase split there is already close to our target workflow:

1. build cohort
2. generate patient or study outputs
3. validate with LLM / consistency checks
4. summarize

We should keep this style, but convert it to our MIMIC paper reproduction stages:

1. paper extraction
2. patient screening
3. dataset and modeling
4. figure generation
5. result comparison

4. MIMIC table documentation corpus
- Files: `docs/hosp/*`, `docs/icu/*`, `docs/note/*`, etc.
- Relevance: fast schema comprehension for SQL generation.
- Action: use as reference material for `paper_parser` and `cohort_agent`.

5. Useful dataset assumptions already proven in that repo
- `mimiciv_note.discharge` is available and queryable.
- ICU event time windows over `chartevents` / `inputevents` / `procedureevents` are a practical default.
- Hospital labs are pulled from `mimiciv_hosp.labevents` and linked by `hadm_id`.
- Comorbidity derivation is currently ICD-prefix-based and can be templated.
- Baseline extraction is often done in batches after the final cohort is known, not one patient at a time.

## Risks to avoid when reusing

- Credentials are hardcoded in some upstream scripts; keep env-based config only.
- Some scripts are tailored to septic shock tasks; extract reusable logic, do not copy assumptions blindly.
- VM-specific paths in upstream scripts need local adapter layers.
- The repo mixes concept-level logic and itemid-level logic in one script; our framework should separate those into reusable compiler pieces.
- `mimiciv_note` usage is helpful, but many papers will not need note-based filtering, so it should remain an optional capability.

## Recommended next technical step

Implement a production `cohort_agent` adapter that:
1. Reads DB settings from `.env` (`MIMIC_PG_*`),
2. Executes tested funnel SQL against `mimiciv_22`,
3. Emits `shared/cohort.csv` + `shared/cohort_funnel.json`,
4. Fails fast with detailed gate diagnostics.

Additional concrete follow-ups:

5. Capture reusable time-window templates such as `ICU stay window`, `T0 + 24h`, and `first measurement within ICU`.
6. Build a reusable MIMIC variable mapping layer around:
   - demographics from `patients` / `admissions`
   - ICU events from `chartevents` / `inputevents` / `procedureevents`
   - labs from `labevents`
   - diagnoses from `diagnoses_icd`
   - optional note evidence from `mimiciv_note.discharge`

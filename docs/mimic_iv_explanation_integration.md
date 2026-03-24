# MIMIC-IV-explanation Integration Notes

Source repository analyzed:
- `/home/bingkun_zhao/projects/MIMIC-IV-explanation`

## Why it matters for this project

The repository already contains practical cohort-building and reporting code against
the same PostgreSQL host (`172.22.0.2:5432`) and database (`mimiciv_22`).
It is a strong reference for our cohort/statistics/report subagents.

## High-value reusable components

1. Cohort selection SQL patterns
- File: `data_exploration/code/build_cohort.py`
- Relevance: end-to-end funnel with explicit steps and intermediate criteria.
- Action: adapt SQL style and funnel accounting to `cohort_agent`.

2. Modular extraction/report architecture
- Files: `data_exploration/code/shock_lib/*`, `generate_report.py`
- Relevance: good split of fetch, chart, and markdown rendering concerns.
- Action: mirror this separation in `stats_agent` + `report_agent`.

3. Pipeline execution strategy
- File: `data_exploration/code/README.md` and `run_batch.sh`
- Relevance: phase-based orchestration with post-validation and summary.
- Action: map to our orchestrator step graph and quality gates.

4. MIMIC table documentation corpus
- Files: `docs/hosp/*`, `docs/icu/*`, `docs/note/*`, etc.
- Relevance: fast schema comprehension for SQL generation.
- Action: use as reference material for `paper_parser` and `cohort_agent`.

## Risks to avoid when reusing

- Credentials are hardcoded in some upstream scripts; keep env-based config only.
- Some scripts are tailored to septic shock tasks; extract reusable logic, do not copy assumptions blindly.
- VM-specific paths in upstream scripts need local adapter layers.

## Recommended next technical step

Implement a production `cohort_agent` adapter that:
1. Reads DB settings from `.env` (`MIMIC_PG_*`),
2. Executes tested funnel SQL against `mimiciv_22`,
3. Emits `shared/cohort.csv` + `shared/cohort_funnel.json`,
4. Fails fast with detailed gate diagnostics.


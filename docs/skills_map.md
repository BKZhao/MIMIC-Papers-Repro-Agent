# Clinical Skills Map

## External skill targets

These are the primary external scientific skill families currently modeled in the project:

- `pyhealth` -> `.codex/skills/pyhealth`
- `clinicaltrials-database` -> `.codex/skills/clinicaltrials-database`
- `clinical-reports` -> `.codex/skills/clinical-reports`
- `statistical-analysis` -> `.codex/skills/statistical-analysis`

Source repository:

- `K-Dense-AI/claude-scientific-skills`

Install helper:

- `scripts/bootstrap_skills.sh`

## Local wrapper skills

The framework also models local wrapper skills to stabilize behavior behind project-owned interfaces:

- `task-spec-normalizer`
- `clinical-variable-role-parser`
- `mimic-cohort-builder`
- `mimic-variable-mapper`
- `clinical-survival-analysis`
- `table-figure-compiler`
- `paper-alignment-verifier`
- `git-github-update`

These wrappers are currently defined in code via `src/repro_agent/skills_registry.py`.

## Default agent-to-skill intent

- `paper_parser_agent`
  normalize task specs and summarize paper/report content
- `study_design_agent`
  parse variable roles and study design intent
- `cohort_agent`
  build or plan dataset-specific cohorts
- `feature_agent`
  map task variables into dataset features
- `stats_agent`
  run survival/statistical analyses
- `figure_agent`
  compile figures and output-ready tables
- `verify_agent`
  compare reproduced artifacts against alignment targets
- `report_agent`
  synthesize markdown outputs and result summaries
- `git_update_agent`
  own Git/GitHub update workflows

## Current status

- Skill routing and allowlists are implemented.
- Some local wrapper skills are still architectural placeholders and are not yet fully independent runtime toolchains.

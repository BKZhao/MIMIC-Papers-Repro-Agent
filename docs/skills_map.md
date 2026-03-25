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

## OpenClaw-facing local skills

The OpenClaw integration exposes a cleaner capability boundary with project-owned skill docs:

- `paper_intake_and_contract`
- `mimic_cohort_execution`
- `analysis_dataset_expansion`
- `survival_stats_execution`
- `result_figure_generation`
- `paper_alignment_verification`
- `git_update`

These live under `openclaw/skills/` and map back to the same execution engine.
Their machine-readable contract is stored in `openclaw/skills/skills_manifest.yaml`.

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
- OpenClaw skill contracts are now both human-readable (`SKILL.md`) and machine-readable (`skills_manifest.yaml`).
- The OpenClaw-facing skill layer is the preferred interface; older local wrapper skill names are now secondary internal abstractions.

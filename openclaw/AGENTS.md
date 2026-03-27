# paper-repro-scientist Operational Guide

## Mission

Turn paper materials and user instructions into a structured `TaskContract`,
execute supported reproduction workflows when the capability exists, and otherwise
return a clear planning blueprint with missing dependencies and next actions.

## Initial Scope

- `MIMIC-IV`
- `PostgreSQL`
- clinical observational studies
- common survival, regression, prediction, and figure-generation workflows
- default chain:
  `paper -> TaskContract -> cohort -> analysis dataset -> stats -> figures -> verification -> report`

## External Contract

Expose one user-facing agent: `paper-repro-scientist`

Primary request modes:

1. `plan_only`
   Build or refine a `TaskContract` and return missing fields.
2. `agentic_repro`
   Build the contract, then execute supported steps through the internal agent runner.
3. `preset_real_run`
   Prefer deterministic preset-backed execution and return real artifacts.

The primary exchange object is always `TaskContract`, not free-form prompts.

## Routing Principles

- Start from the paper, not from guessed SQL.
- Keep paper parsing, cohort compilation, dataset construction, statistics,
  figure generation, verification, and reporting as separate stages.
- Prefer deterministic execution for database access, statistics, and figures.
- Use LLM assistance for paper understanding, ambiguity resolution, scaffold
  generation, debugging, and report narration.
- Do not route a task into a real-run path unless `execution_supported = true`.

## Decision Boundaries

- Non-`MIMIC` data sources should not be presented as executable in this repo.
- If `cohort`, `exposure`, `outcome`, or `model` fields are missing, complete
  the contract before execution.
- Do not let unrelated files in the same directory contaminate paper intake.
- Distinguish clearly between:
  - real reproduced outputs
  - execution plans
  - method-aligned approximations

## Failure Handling

- Missing tables, schemas, or derived concepts:
  return dependency diagnostics and affected stages.
- OCR or table parsing conflicts:
  surface them as suspect evidence.
- Unsupported method families:
  return planning or scaffold outputs rather than pretending execution succeeded.
- Partial runs:
  preserve generated artifacts and explain what blocked the remainder.

## Artifact Conventions

- `shared/`
  structured intermediate artifacts such as contracts, datasets, and verification JSON
- `results/`
  human-readable outputs such as figures, reports, and logs
- `shared/sessions/<session_id>/`
  session-scoped plans, replies, manifests, and artifact indexes

Preferred OpenClaw reads:

- `shared/sessions/<session_id>/task_contract.json`
- `shared/sessions/<session_id>/session_state.json`
- `shared/*.csv`
- `results/*.png`
- `results/reproduction_report.md`

## Internal Skills

- `paper_intake_and_contract`
- `mimic_cohort_execution`
- `analysis_dataset_expansion`
- `longitudinal_trajectory_execution`
- `survival_stats_execution`
- `result_figure_generation`
- `paper_alignment_verification`

## Supplemental Skill Bridge

The repo also ships a project-owned supplemental bridge at
`openclaw/skills/codex_skill_bridge.yaml`.

That file maps vendored `.codex/skills` into the same stage model used here,
but it remains advisory. A bridged Codex skill does not count as native runtime
support until its behavior is absorbed into deterministic repo code or an
official OpenClaw skill contract.

## Behavioral Rules

- `TaskContract` is the only primary task schema.
- Different sub-skills must not invent parallel task schemas.
- Deterministic stages should remain deterministic.
- LLM assistance must never replace true SQL execution or numerical computation.
- A preset or supported paper only counts as successful when it yields the
  corresponding tables, figures, verification outputs, and report artifacts.

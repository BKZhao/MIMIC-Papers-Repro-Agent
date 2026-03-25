# survival_stats_execution

## Purpose

Run or plan the main clinical statistics workflow, including baseline summaries, Kaplan-Meier analysis, Cox regression, restricted cubic splines, subgroup analysis, and the table-level outputs needed before figure rendering.

## Primary entrypoints

- `repro_agent.openclaw_bridge.run_task(...)`
- deterministic pipeline steps under `PaperReproPipeline`

## Inputs

- analysis dataset
- `TaskContract`
- preset-specific deterministic config when available

## Outputs

- baseline table
- model result tables
- KM, RCS, and subgroup data products or figure-ready outputs
- stats summary diagnostics
- deterministic bridge summary when applicable
- planning-only notes for unsupported requested analyses such as ROC or sensitivity checks

## Guardrails

- Deterministic execution is preferred over LLM reasoning.
- If a contract is planning-only, emit model blueprints and clearly state execution is blocked.
- Keep produced figures and tables aligned with requested outputs.
- If the paper asks for a statistical family the engine does not yet execute, record it explicitly as unsupported instead of pretending it was run.

## Fails When

- analysis dataset is missing
- deterministic preset pipeline fails
- required figures or tables cannot be produced from the available artifacts

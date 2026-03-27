# survival_stats_execution

## Purpose

Run or plan the main clinical statistics workflow, including baseline summaries,
Kaplan-Meier analysis, Cox regression, restricted cubic splines, subgroup
analysis, ROC, and the table-level outputs needed before figure rendering.
For trajectory papers, this skill works after the repeated-measurement panel has
already been converted into trajectory labels.

For method families that are recognized but not yet natively wired, this skill
should emit a local execution scaffold request rather than inventing results.

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
- trajectory-to-survival tables once trajectory labels exist
- ROC outputs when supported
- scaffold-oriented planning artifacts for hybrid families such as logistic, calibration, DCA, nomogram, or SHAP
- stats summary diagnostics
- deterministic bridge summary when applicable
- planning-only notes for unsupported requested analyses such as ROC or sensitivity checks

## Guardrails

- Deterministic execution is preferred over LLM reasoning.
- Let the LLM decide the route and skill chain when method interpretation is ambiguous, but keep numerical estimation in local code.
- If a contract is planning-only, emit model blueprints and clearly state execution is blocked.
- Keep produced figures and tables aligned with requested outputs.
- If the paper asks for a statistical family the engine does not yet execute, record it explicitly as unsupported instead of pretending it was run.

## Fails When

- analysis dataset is missing
- deterministic preset pipeline fails
- required figures or tables cannot be produced from the available artifacts

# mimic_cohort_execution

## Purpose

Translate a supported MIMIC task contract or preset into patient screening and cohort extraction work. For preset contracts, route into the deterministic SQL bridge. For non-preset contracts, emit an executable blueprint and clearly report missing generic compiler pieces.

## Primary entrypoints

- `repro_agent.openclaw_bridge.run_task(...)`
- `paper-repro run-task`

## Inputs

- persisted `TaskContract`
- session context
- MIMIC PostgreSQL connectivity

## Outputs

- cohort blueprint
- cohort funnel artifacts when available
- diagnostics about execution support and missing dependencies
- session-scoped artifact references
- explicit inclusion and exclusion mapping notes

## Guardrails

- Do not pretend arbitrary `CohortSpec` is executable unless a real compiler exists.
- Surface `mimiciv_derived` dependencies explicitly.
- Preserve cohort screening steps in session artifacts.
- For non-preset papers, emit the extracted inclusion/exclusion logic as a blueprint even when executable SQL is not available yet.

## Fails When

- MIMIC PostgreSQL is unreachable
- required schemas or derived concepts are missing
- contract is non-preset and no generic SQL compiler exists

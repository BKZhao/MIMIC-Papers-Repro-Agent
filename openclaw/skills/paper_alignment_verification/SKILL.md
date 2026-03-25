# paper_alignment_verification

## Purpose

Compare reproduced outputs against paper-aligned targets, explain deviations at each workflow step, and mark suspicious or weakly parsed evidence instead of silently accepting it.

## Primary entrypoints

- `repro_agent.openclaw_bridge.run_task(...)`
- `paper-repro run-task`

## Inputs

- verification targets from `TaskContract`
- produced artifacts
- parsed table and supplement targets

## Outputs

- verification plan or diagnostics
- alignment summaries
- reproduction report support notes
- `deviation_table.json` or equivalent verification artifact
- step-level deviation notes for paper extraction, cohort screening, modeling tables, and figures

## Guardrails

- Distinguish real measured deviations from missing artifacts.
- Flag OCR or table parsing uncertainty as `suspect`.
- Never present planning-only outputs as validated reproduction.

## Fails When

- paper targets are missing and no verification baseline can be built
- required output artifacts are absent
- quality gates exceed configured tolerance

## Success Criteria

- return a machine-readable verification artifact
- explain whether deviations come from cohort mismatch, baseline mismatch, model mismatch, or missing artifacts
- identify the exact workflow stage where each important deviation first appears

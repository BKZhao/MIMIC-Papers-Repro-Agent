# analysis_dataset_expansion

## Purpose

Build or plan the analysis dataset required for downstream modeling. Use semantic variable mappings when available, keep exposure and outcome construction explicit, and record missingness outputs for verification and review.

## Primary entrypoints

- `repro_agent.openclaw_bridge.extract_analysis_dataset(...)`
- `paper-repro extract-analysis-dataset`

## Inputs

- `TaskContract`
- cohort output or preset bridge config
- `sepsis_source`

## Outputs

- analysis dataset artifact
- missingness report
- feature blueprint with semantic mapping summary
- unmapped-variable diagnostics
- modeling-ready dataset notes for table generation

## Guardrails

- Prefer deterministic local code for supported presets.
- Record unmapped variables instead of silently dropping them.
- Keep dataset field mapping visible in artifacts.

## Fails When

- required cohort artifacts are absent
- required variables cannot be sourced for a preset that claims full execution

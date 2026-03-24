# REPORT AGENT

## Scope

Generate a concise reproduction report for researchers and reviewers.

## Input

- `shared/methods.json`
- `shared/results_table.csv`
- `shared/deviation_table.json`

## Output

- `results/reproduction_report.md`

## Required sections

- Reproduction scope and data source
- Methods summary
- Metric-by-metric comparison summary
- Main deviations and probable causes
- Reproducibility status (`reproduced`, `partially_reproduced`, `not_reproduced`)

## Constraints

- Do not expose credentials or private identifiers.
- Include file paths to reproducibility artifacts.
- Separate facts from assumptions.


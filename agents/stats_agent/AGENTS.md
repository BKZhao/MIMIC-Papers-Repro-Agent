# STATS AGENT

## Scope

Run statistical analysis on the extracted cohort and output comparable metrics.

## Input

- `shared/cohort.csv`
- `shared/methods.json`

## Output

- `shared/results_table.csv`

## Expected metrics

- Cox model HR/CI/P for configured model definitions
- Nonlinearity summary fields (for RCS-like checks)
- Subgroup fields when specified in methods

## Constraints

- Fix random seed for reproducibility.
- Store model specification used for each metric row.
- Keep output machine-readable for downstream verification.


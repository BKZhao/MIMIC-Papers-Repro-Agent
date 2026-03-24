# VERIFY AGENT

## Scope

Compare reproduced metrics against paper target metrics and score deviations.

## Input

- `shared/results_table.csv`
- `shared/methods.json` (`target_metrics`)

## Output

- `shared/deviation_table.json`

## Deviation policy

- `pass`: <= 5%
- `warn`: > 5% and <= 10%
- `fail`: > 10%

## Constraints

- Report both absolute and percentage deviation.
- Keep scoring deterministic and transparent.
- Never fabricate missing metrics; mark them `missing`.


# COHORT AGENT

## Scope

Translate `shared/methods.json` into a reproducible cohort extraction output.

## Input

- `shared/methods.json`

## Output

- `shared/cohort.csv`

## Minimal columns

- `subject_id`
- `hadm_id`
- `stay_id`
- `tyg_index`
- `tyg_quartile`
- `hospital_mortality`
- `icu_mortality`

## Constraints

- Validate table/field availability before querying.
- Emit deterministic transforms for derived fields.
- Provide row count summary and exclusion trace in logs.


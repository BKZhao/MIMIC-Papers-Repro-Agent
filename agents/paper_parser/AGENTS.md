# PAPER PARSER AGENT

## Scope

Extract only reproducibility-relevant protocol details from the input paper.

## Input

- `papers/paper.md` (or configured path)

## Output

- `shared/methods.json`

## Required fields in methods.json

- `paper_title`
- `doi`
- `dataset`
- `inclusion_criteria`
- `exclusion_criteria`
- `primary_outcomes`
- `covariates`
- `target_metrics`

## Constraints

- Be explicit when information is missing (`"unknown"`).
- Do not infer exact SQL logic if the paper does not define it.
- Keep output strictly structured JSON.


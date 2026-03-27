# paper_intake_and_contract

## Purpose

Read paper materials and user instructions, extract structured `paper_evidence`,
then produce a normalized `TaskContract` with cohort logic, variable roles,
models, outputs, tables, figures, result targets, preset detection, and
study-template inference.

## Primary entrypoints

- `repro_agent.openclaw_bridge.plan_task(...)`
- `paper-repro plan-task`

## Inputs

- `paper_path` or `paper_content`
- primary paper may be `pdf`, `md`, or plain text
- `instructions`
- `config_path`
- optional `session_id`
- optional `use_llm`

## Outputs

- `paper_evidence.json` when structured paper evidence is available
- persisted `TaskContract`
- extracted study design summary
- extracted table and figure target summary
- `missing_high_impact_fields`
- `execution_backend`
- `preset`
- `study_template`
- `execution_supported`
- `task_contract_path`

## Reads

- primary paper file
- same-stem companion markdown or docx when present
- sibling `table.md`
- sibling `si.docx`
- semantic registry config when available

## Writes

- `shared/sessions/<session_id>/paper_evidence.json`
- `shared/sessions/<session_id>/task_contract.json`
- `shared/sessions/<session_id>/session_state.json`

## Fails When

- missing paper input
- invalid config path
- LLM unavailable when explicitly requested
- incomplete paper causing missing high-impact fields

## Guardrails

- Prefer explicit paper facts over inference.
- Prefer hybrid behavior:
  use LLM reasoning for paper understanding when configured, but keep contract normalization deterministic.
- Avoid cross-paper contamination from unrelated markdown files in the same directory.
- If key fields are missing, return gaps instead of inventing them.
- Keep `TaskContract` as the only task schema.
- Never claim a non-preset paper is executable unless downstream support says so.
- If the paper requests analyses that are not currently executable, preserve them as planning facts instead of silently dropping them.
- Capture requested tables, figures, and reported results even when they cannot yet be fully verified.

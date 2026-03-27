# result_figure_generation

## Purpose

Generate reproduction figures from the reproduced data and model outputs, rather than copying or visually imitating the original paper images.

## Primary entrypoints

- `repro_agent.openclaw_bridge.run_task(...)`
- figure-related artifact generation under `AgentRunner`

## Inputs

- analysis dataset
- model outputs and diagnostics
- `TaskContract`
- paper figure targets when available

## Outputs

- reproduced figures
- figure metadata
- trajectory profile figures when a trajectory backend exists
- blocked figure notes when the requested plot family is not yet executable

## Guardrails

- Figures must be generated from reproduced data, not from OCR or screenshot manipulation.
- For trajectory papers, the figure must come from the extracted hourly panel and derived class summaries, not from paper screenshots.
- Keep figure file names and captions aligned with the requested outputs in the contract.
- If a requested plot family is unsupported, emit a blocked or planning-only note instead of pretending the figure was generated.

## Fails When

- required model outputs are absent
- figure rendering dependencies fail
- the requested figure type is unsupported in the current engine

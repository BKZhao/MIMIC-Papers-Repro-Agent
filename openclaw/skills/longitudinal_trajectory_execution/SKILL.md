# longitudinal_trajectory_execution

## Purpose

Handle repeated-measurement trajectory studies where the paper requires a time-series panel first, then derives trajectory classes before downstream survival or regression analysis.

## Primary entrypoints

- `repro_agent.openclaw_bridge.run_task(...)`
- trajectory-oriented profile scripts under `scripts/profiles/`

## Inputs

- `TaskContract`
- repeated-measurement extraction requirements from the paper
- analysis dataset columns or raw hourly panel definitions
- trajectory backend configuration such as class count and time window

## Outputs

- hourly panel extraction plan or artifact
- trajectory class assignments
- trajectory summary table
- trajectory figure
- downstream-ready exposure labels for KM / Cox / baseline tables

## Guardrails

- Never claim the backend is paper-identical LGMM unless it truly is.
- Keep the method gap explicit when the engine uses a Python-only approximation route.
- Preserve the exact measurement window, sampling interval, and class-count assumptions in the artifacts.
- If the repeated-measurement panel cannot be built, block cleanly instead of silently downgrading to a baseline-only analysis.

## Fails When

- required repeated measurements are missing
- the trajectory class backend cannot fit or converge
- the class labels cannot be bridged into downstream analysis artifacts

# Architecture

## Summary

The framework now uses a dual-track architecture:

- `deterministic`
  Repeatable paper reproduction for stable presets, currently strongest for the MIMIC-IV TyG sepsis workflow
- `agentic`
  Generic task planning and multi-subagent execution driven by a structured `TaskContract`

The key design rule is: paper-specific logic should move into presets, contracts, and verification targets, while the engine remains reusable.

## Main layers

### 1. Interface and contract layer

Primary module:

- `src/repro_agent/contracts.py`

This layer defines the system-wide schemas for:

- `TaskContract`
- `DatasetSpec`
- `CohortSpec`
- `VariableSpec`
- `ModelSpec`
- `OutputSpec`
- `SessionState`
- `AgentRun`
- `ArtifactRecord`

Everything above and below this layer should consume the same contract instead of inventing paper-specific interfaces.

### 2. Configuration and routing layer

Primary modules:

- `src/repro_agent/config.py`
- `src/repro_agent/skills_registry.py`
- `src/repro_agent/llm.py`

This layer controls:

- execution mode and interaction mode
- agent-level model routing
- skill allowlists and defaults
- dataset adapter selection
- artifact and verification policy

Current default LLM route is OpenAI-compatible SiliconFlow configuration driven by environment variables.

### 3. Orchestration layer

Primary modules:

- `src/repro_agent/pipeline.py`
- `src/repro_agent/agent_runner.py`
- `src/repro_agent/cli.py`

This layer has two executors:

- `PaperReproPipeline`
  deterministic orchestrator with fixed step ordering for stable batch runs
- `AgentRunner`
  multi-subagent executor for planning and interactive study execution

### 4. Dataset and execution layer

Primary modules:

- `src/repro_agent/dataset_adapters.py`
- `src/repro_agent/db/connectors.py`
- `scripts/build_tyg_sepsis_cohort.py`
- `scripts/build_tyg_analysis_dataset.py`
- `src/repro_agent/stats_analysis.py`

This layer handles:

- dataset-specific semantics
- SQL-backed extraction
- cohort generation
- wide-table feature expansion
- stats execution and figure generation

At the moment, `MIMIC-IV` is the first adapter and the TyG sepsis paper is the most complete preset.

### 5. Artifact and runtime layer

Primary module:

- `src/repro_agent/runtime.py`

This layer is responsible for explicit artifact writes and logging:

- `shared/`
- `results/`
- `shared/sessions/<session_id>/`
- `results/run_events.jsonl`
- `results/agent_runs.jsonl`
- `results/artifacts.jsonl`

This artifact-first design is central to reproducibility and debugging.

## Execution flows

### Deterministic flow

Current stable step order:

1. `paper_parser`
2. `cohort_agent`
3. `stats_agent`
4. `verify_agent`
5. `report_agent`

This flow is best used for paper-aligned reruns where the expected outputs and quality gates are already known.

### Agentic flow

Current high-level step order:

1. parse paper materials and user instructions
2. build a `TaskContract`
3. create session artifacts
4. run subagents:
   - `paper_parser_agent`
   - `study_design_agent`
   - `cohort_agent`
   - `feature_agent`
   - `stats_agent`
   - `figure_agent`
   - `verify_agent`
   - `report_agent`
   - `git_update_agent`

If the task matches a built-in preset, `AgentRunner` may bridge into the deterministic backend instead of treating the task as planning-only.

## Current maturity and boundaries

Already mature:

- deterministic MIMIC TyG sepsis paper workflow
- real cohort extraction and analysis dataset expansion
- Python-based baseline, KM, Cox, RCS, subgroup, and alignment diagnostics
- session-based task planning and artifact recording

Partially mature:

- generic agentic execution for non-preset MIMIC studies
- general dataset adapter execution beyond planning blueprints
- skill wrappers as independent runtime tools

Known gap:

- `papers/table.md` and `papers/si.docx` are already ingested as paper materials, but they are not yet fully promoted into structured verification truth across all diagnostics

## Security stance

- No hardcoded credentials in repo-tracked files
- Database and API secrets come from environment variables only
- `src/repro_agent/db/connectors.py` exposes masked DSN diagnostics only
- Reports and logs should never contain raw credentials

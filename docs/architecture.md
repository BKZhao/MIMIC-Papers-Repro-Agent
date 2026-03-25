# Architecture

## Scope

The repository is being actively aligned around one concrete goal:

- automate reproduction of `MIMIC-IV` clinical observational papers
- use `PostgreSQL` as the execution backend
- read the paper first, then derive cohort logic, variables, models, tables, and figures
- export reproducible artifacts that can be consumed by OpenClaw or Lobster

This is not yet a general paper reproduction platform. The active design target is a focused `MIMIC clinical paper reproduction engine`.

## Current source of truth

The current architecture is no longer driven by the older directory-based prompt-file design.

The active source of truth is now:

- [`openclaw/SOUL.MD`](../openclaw/SOUL.MD)
- [`openclaw/skills/`](../openclaw/skills)
- [`src/repro_agent/task_builder.py`](../src/repro_agent/task_builder.py)
- [`src/repro_agent/openclaw_bridge.py`](../src/repro_agent/openclaw_bridge.py)
- [`src/repro_agent/paper_profiles.py`](../src/repro_agent/paper_profiles.py)
- [`scripts/profiles/`](../scripts/profiles)
- [`src/repro_agent/profile_stats.py`](../src/repro_agent/profile_stats.py)

`docs/reference/` is historical context only.

## End-to-end workflow

### 1. Read the paper

Paper intake starts from `papers/` and currently supports:

- PDF
- Markdown
- DOCX sidecars

Implementation:

- [`src/repro_agent/paper_materials.py`](../src/repro_agent/paper_materials.py)

Key behavior:

- same-stem sidecars are preferred
- generic `table.md` and `si.docx` are attached only in the legacy single-paper layout
- this avoids cross-paper contamination when the `papers/` directory contains multiple studies

### 2. Extract study structure

The paper text and user instructions are converted into a normalized `TaskContract`.

Implementation:

- [`src/repro_agent/task_builder.py`](../src/repro_agent/task_builder.py)

Current behavior:

- use LLM extraction when available
- fall back to heuristic extraction if LLM is unavailable
- infer exposures, outcomes, covariates, models, outputs, and cohort logic
- detect supported presets
- infer study templates
- apply MIMIC semantic variable mapping

This is the layer that should learn from the paper itself instead of guessing.

### 3. Route the task

After contract construction, the system decides whether to:

- bridge into deterministic execution for a supported paper profile
- stay in planning-first mode for an unsupported non-preset paper

Implementation:

- [`src/repro_agent/openclaw_bridge.py`](../src/repro_agent/openclaw_bridge.py)
- [`src/repro_agent/preset_registry.py`](../src/repro_agent/preset_registry.py)
- [`src/repro_agent/study_templates.py`](../src/repro_agent/study_templates.py)
- [`src/repro_agent/semantic_registry.py`](../src/repro_agent/semantic_registry.py)

The `TaskContract` is the only stable exchange object between planning and execution.

### 4. Build cohort and analysis dataset

For supported papers, execution is profile-driven.

Profile definition:

- [`src/repro_agent/paper_profiles.py`](../src/repro_agent/paper_profiles.py)

Current deterministic scripts:

- [`scripts/profiles/build_profile_cohort.py`](../scripts/profiles/build_profile_cohort.py)
- [`scripts/profiles/build_profile_analysis_dataset.py`](../scripts/profiles/build_profile_analysis_dataset.py)
- [`scripts/profiles/run_profile_stats.py`](../scripts/profiles/run_profile_stats.py)

What the profile carries today:

- paper key and title
- dataset version expectation
- predictor and outcome columns
- quartile boundaries
- model adjustment sets
- subgroup definitions
- expected outputs

This is the active execution path for supported MIMIC papers.

### 5. Generate paper-like tables and figures

The active stats layer already exports outputs that look much closer to the paper than a generic "run succeeded" status.

Implementation:

- [`src/repro_agent/profile_stats.py`](../src/repro_agent/profile_stats.py)

Current structured table outputs:

- baseline table in CSV and Markdown
- Cox model table in CSV and Markdown
- subgroup analysis table in CSV and Markdown

Current figure outputs:

- Kaplan-Meier curve
- restricted cubic spline plot
- ROC curve
- subgroup forest plot

Current summary outputs:

- cohort funnel JSON
- missingness JSON
- KM summary JSON
- RCS summary JSON
- ROC summary JSON
- stats summary JSON
- model-ready analysis dataset CSV

This is the layer that should keep expanding until it can emit the same table and figure families used by the target paper.

### 6. Verify and report

The repository follows an artifact-first design.

Primary runtime modules:

- [`src/repro_agent/runtime.py`](../src/repro_agent/runtime.py)
- [`src/repro_agent/agent_runner.py`](../src/repro_agent/agent_runner.py)
- [`src/repro_agent/pipeline.py`](../src/repro_agent/pipeline.py)

Artifact locations:

- `shared/`
  structured intermediate outputs
- `results/`
  human-facing figures and reports
- `shared/sessions/<session_id>/`
  planning and execution artifacts for one agentic session

This artifact contract is what OpenClaw and Lobster should read instead of inferring state from prompts.

## System layers

### Contract layer

Primary file:

- [`src/repro_agent/contracts.py`](../src/repro_agent/contracts.py)

Core objects:

- `TaskContract`
- `DatasetSpec`
- `CohortSpec`
- `VariableSpec`
- `ModelSpec`
- `OutputSpec`
- `SessionState`

### Configuration and LLM layer

Primary files:

- [`src/repro_agent/config.py`](../src/repro_agent/config.py)
- [`src/repro_agent/llm.py`](../src/repro_agent/llm.py)
- [`src/repro_agent/skills_registry.py`](../src/repro_agent/skills_registry.py)

Responsibilities:

- environment and database wiring
- OpenAI-compatible LLM client setup
- model routing
- skill allowlists
- execution and verification policy

### Execution layer

Primary files:

- [`src/repro_agent/paper_materials.py`](../src/repro_agent/paper_materials.py)
- [`src/repro_agent/task_builder.py`](../src/repro_agent/task_builder.py)
- [`src/repro_agent/paper_profiles.py`](../src/repro_agent/paper_profiles.py)
- [`src/repro_agent/profile_stats.py`](../src/repro_agent/profile_stats.py)
- [`src/repro_agent/dataset_adapters.py`](../src/repro_agent/dataset_adapters.py)
- [`src/repro_agent/db/connectors.py`](../src/repro_agent/db/connectors.py)

### OpenClaw bridge layer

Primary files:

- [`openclaw/SOUL.MD`](../openclaw/SOUL.MD)
- [`openclaw/skills/skills_manifest.yaml`](../openclaw/skills/skills_manifest.yaml)
- [`src/repro_agent/openclaw_bridge.py`](../src/repro_agent/openclaw_bridge.py)

External contract:

- `plan_task`
- `run_task`
- `export_contract`
- `run_preset_pipeline`
- `extract_analysis_dataset`

## Deterministic vs agentic

### Deterministic

Best for:

- supported paper presets
- stable reruns
- reproducible table and figure generation

Current strongest path:

- profile-driven MIMIC execution

### Agentic

Best for:

- reading a new paper
- extracting study design
- deciding whether the task is already executable
- producing a `TaskContract`, missing fields, and execution blueprint

Current limitation:

- unsupported non-preset papers are still planning-first in many cases

## What is mature now

- paper intake from PDF / Markdown / DOCX
- LLM-backed contract extraction with heuristic fallback
- MIMIC semantic mapping scaffold
- deterministic profile execution for supported papers
- baseline, Cox, subgroup, KM, RCS, and ROC outputs
- artifact-first session and run tracking
- one-agent OpenClaw integration scaffold

## Main gaps still open

- arbitrary new papers do not yet compile automatically into executable SQL
- supplement tables and paper targets still need stronger normalization into verification truth
- broader study families still need deterministic templates
- paper-specific variable engineering still requires more reusable mapping

## Security stance

- no hardcoded credentials in tracked source files
- database and API secrets should come from environment variables
- DSN diagnostics should stay masked
- reports and logs must not leak secrets

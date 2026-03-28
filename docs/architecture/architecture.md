# Architecture

## Positioning

This repository has one explicit target:

- reproduce `MIMIC-IV` clinical observational papers
- keep the runtime scope `MIMIC-only`; non-MIMIC dataset requests are out of scope
- start from the paper, not from guessed SQL
- use `TaskContract` as the single primary task schema
- keep LLM reasoning in the control plane
- keep SQL, statistics, and figures deterministic

The correct description today is:

`paper-first, hybrid, artifact-first MIMIC clinical paper reproduction engine v1`

## Source Of Truth

The active source of truth is intentionally limited to:

- [`../../README.md`](../../README.md)
- [`clinical-analysis-capability-map.md`](clinical-analysis-capability-map.md)
- [`../../openclaw/SOUL.MD`](../../openclaw/SOUL.MD)
- [`../../openclaw/AGENTS.md`](../../openclaw/AGENTS.md)
- [`../../openclaw/skills/skills_manifest.yaml`](../../openclaw/skills/skills_manifest.yaml)

Legacy design notes and duplicate integration docs are not part of the runtime contract.

## Supplemental Reference Layer

There is also a vendored reference layer built from Codex skills under:

- `.codex/skills/*`

These are useful for expanding methods and improving implementation quality, but
they are not part of the product runtime contract. We should treat them as a
capability reservoir: borrow ideas, then re-express them through
`TaskContract`, `ClinicalAnalysisFamily`, deterministic runners, or official
project-owned OpenClaw skills.

Reference map:

- [`../../openclaw/skills/codex_skill_bridge.yaml`](../../openclaw/skills/codex_skill_bridge.yaml)
- [`supplemental-codex-skill-map.md`](supplemental-codex-skill-map.md)

## Runtime Layers

### 1. Paper understanding and normalization

This layer reads paper materials, extracts evidence, and produces the normalized contract.

Key modules:

- [`../../src/repro_agent/paper/materials.py`](../../src/repro_agent/paper/materials.py)
- [`../../src/repro_agent/paper/builder.py`](../../src/repro_agent/paper/builder.py)
- [`../../src/repro_agent/paper/presets.py`](../../src/repro_agent/paper/presets.py)
- [`../../src/repro_agent/paper/profiles.py`](../../src/repro_agent/paper/profiles.py)
- [`../../src/repro_agent/paper/templates.py`](../../src/repro_agent/paper/templates.py)

Responsibilities:

- read PDF / Markdown / text paper inputs
- build `paper_evidence`
- normalize into `TaskContract`
- infer preset, profile, and study-template context
- preserve paper dataset version and execution-environment version separately

### 2. Agentic control plane

This layer decides whether a task is executable, planning-only, or preset-backed.

Key modules:

- [`../../src/repro_agent/agentic/decision.py`](../../src/repro_agent/agentic/decision.py)
- [`../../src/repro_agent/agentic/skill_planner.py`](../../src/repro_agent/agentic/skill_planner.py)
- [`../../src/repro_agent/agentic/runner.py`](../../src/repro_agent/agentic/runner.py)
- [`../../src/repro_agent/openclaw_bridge.py`](../../src/repro_agent/openclaw_bridge.py)

Responsibilities:

- build `agent_decision`
- expose `analysis_family_route`
- write `llm_execution_plan`
- persist session artifacts
- distinguish real execution from planning-only output

### 3. Deterministic execution plane

This layer handles SQL extraction, wide-table building, statistics, and figures.

Key modules:

- [`../../src/repro_agent/sql/cohort.py`](../../src/repro_agent/sql/cohort.py)
- [`../../src/repro_agent/sql/analysis_dataset.py`](../../src/repro_agent/sql/analysis_dataset.py)
- [`../../src/repro_agent/analysis/profile_stats.py`](../../src/repro_agent/analysis/profile_stats.py)
- [`../../src/repro_agent/analysis/stats.py`](../../src/repro_agent/analysis/stats.py)
- [`../../src/repro_agent/analysis/trajectory.py`](../../src/repro_agent/analysis/trajectory.py)
- [`../../src/repro_agent/analysis/trajectory_stats.py`](../../src/repro_agent/analysis/trajectory_stats.py)
- [`../../scripts/profiles/build_profile_cohort.py`](../../scripts/profiles/build_profile_cohort.py)
- [`../../scripts/profiles/build_profile_analysis_dataset.py`](../../scripts/profiles/build_profile_analysis_dataset.py)
- [`../../scripts/profiles/run_profile_stats.py`](../../scripts/profiles/run_profile_stats.py)

Responsibilities:

- run profile-backed cohort extraction
- build analysis datasets and missingness artifacts
- generate baseline / KM / Cox / spline / subgroup outputs
- render only the tables and figures requested by the paper-facing contract or profile outputs, instead of always emitting a fixed figure bundle
- support the experimental trajectory backend for the heart-rate profile

### 4. Registry and integration surface

This layer defines what the outside world can ask the system to do.

Key modules:

- [`../../src/repro_agent/registry/analysis.py`](../../src/repro_agent/registry/analysis.py)
- [`../../src/repro_agent/registry/semantic.py`](../../src/repro_agent/registry/semantic.py)
- [`../../src/repro_agent/registry/skill_contracts.py`](../../src/repro_agent/registry/skill_contracts.py)
- [`../../src/repro_agent/registry/skills.py`](../../src/repro_agent/registry/skills.py)
- [`../../openclaw/skills`](../../openclaw/skills)

Responsibilities:

- keep one project-owned skill surface
- describe supported clinical analysis families
- load semantic mappings and skill contracts
- avoid parallel schema inventions outside `TaskContract`

### Legacy compatibility layer

This layer exists only to preserve deprecated preset-style execution surfaces.

Key modules:

- [`../../src/repro_agent/legacy/pipeline.py`](../../src/repro_agent/legacy/pipeline.py)
- [`../../src/repro_agent/pipeline.py`](../../src/repro_agent/pipeline.py)

Responsibilities:

- preserve deprecated `dry-run`, `run`, and preset bridge behavior
- keep historical imports working through thin facades
- avoid receiving new primary execution logic

## Primary Workflow

The primary workflow is:

1. read the paper
2. extract `paper_evidence`
3. normalize to `TaskContract`
4. compute `agent_decision` and `analysis_family_route`
5. run supported execution or return planning output
6. persist artifacts under `shared/`, `results/`, and `shared/sessions/<session_id>/`

The figure policy is:

- do not treat every supported plot type as mandatory output
- prefer paper-aligned figure intents captured in `TaskContract.outputs` or profile-owned output declarations
- use deterministic renderers to reproduce only the figure families that the paper actually reports

## Interface Posture

Recommended public entrypoints:

- `paper-repro plan-task`
- `paper-repro continue-session`
- `paper-repro run-task`
- `paper-repro extract-analysis-dataset`
- `paper-repro describe-openclaw`

Deprecated compatibility entrypoints:

- `paper-repro dry-run`
- `paper-repro run`
- `paper-repro run-preset-pipeline`

These legacy commands remain for compatibility, but they are not the preferred architecture path.

## Skill Surface

The official project-owned skills are:

- `paper_intake_and_contract`
- `mimic_cohort_execution`
- `analysis_dataset_expansion`
- `longitudinal_trajectory_execution`
- `survival_stats_execution`
- `result_figure_generation`
- `paper_alignment_verification`

Developer-local `.codex/skills` content may still exist in a working tree, but
it is not part of the default runtime contract. It should be treated as a
reference layer only. The project-owned bridge that records how those vendored
skills map onto OpenClaw stages lives in
[`../../openclaw/skills/codex_skill_bridge.yaml`](../../openclaw/skills/codex_skill_bridge.yaml).

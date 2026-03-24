# paper-repro-agent

Multi-subagent framework for automated clinical paper reproduction.

The project now has two execution modes in the same codebase:

- `deterministic`
  Stable, paper-aligned pipeline for repeatable runs. The current strongest implementation is the MIMIC-IV TyG sepsis paper workflow.
- `agentic`
  Interactive task-definition and multi-subagent execution flow. It turns paper text plus user instructions into a structured task contract, then runs planning or execution against the active dataset adapter.

The long-term goal is to keep the engine generic while treating individual papers as task presets or verification targets instead of hardcoded pipelines.

## Core concepts

- `TaskContract`
  The unified task definition for cohort logic, variables, models, outputs, and verification targets.
- `AgentRunner`
  The true multi-subagent executor for `paper_parser_agent`, `study_design_agent`, `cohort_agent`, `feature_agent`, `stats_agent`, `figure_agent`, `verify_agent`, `report_agent`, and `git_update_agent`.
- `DatasetAdapter`
  The boundary where generic study intent is translated into dataset-specific semantics. `MIMIC-IV` is the first implemented adapter.
- Artifact-first workflow
  Intermediate and final outputs are written explicitly to `shared/`, `results/`, and `shared/sessions/<session_id>/`.

## Current status

What already works well:

- Real `deterministic` execution for the MIMIC-IV TyG sepsis reproduction workflow
- SQL-backed cohort extraction and analysis dataset expansion for the current paper preset
- Python-based baseline table, KM, Cox, RCS, subgroup analysis, and alignment diagnostics
- `agentic` task planning via `TaskContract`
- Session persistence, artifact tracking, and agent-run logging
- SiliconFlow-compatible LLM routing configuration via environment variables

What is still in progress:

- Non-preset MIMIC studies are already plannable, but not yet fully executable end to end
- `papers/table.md` and `papers/si.docx` are read as paper materials, but they are not yet fully converted into machine-verifiable alignment truth
- Skills are modeled and routed, but some wrapper skills still act as architectural placeholders rather than fully independent runtime tools

## Execution modes

### 1. Deterministic mode

Use this for the current stable paper reproduction flow.

Key config:

- [`configs/pipeline.example.yaml`](configs/pipeline.example.yaml)

Main path:

1. parse paper metadata and build a paper alignment contract
2. build the cohort
3. expand the analysis dataset
4. run statistics and figure generation
5. verify against paper targets
6. write the reproduction report

Typical command:

```bash
paper-repro dry-run --config configs/pipeline.example.yaml
```

### 2. Agentic mode

Use this for interactive study design and future generic paper workflows.

Key config:

- [`configs/agentic.example.yaml`](configs/agentic.example.yaml)

Main path:

1. read paper materials and free-form instructions
2. build a `TaskContract`
3. persist session artifacts under `shared/sessions/<session_id>/`
4. run the multi-subagent executor
5. if the contract matches a built-in preset, bridge into the deterministic backend
6. otherwise emit planning blueprints for cohort, features, models, figures, and verification

Typical commands:

```bash
paper-repro plan-task \
  --config configs/agentic.example.yaml \
  --paper-path papers/MIMIC.md \
  --instructions "自变量: TyG index; 因变量: in-hospital mortality, ICU mortality; 模型: Cox, Kaplan-Meier, RCS"
```

```bash
paper-repro run-task \
  --config configs/agentic.example.yaml \
  --session-id <session_id>
```

## Project layout

```text
paper-repro-agent/
├── configs/
│   ├── pipeline.example.yaml
│   └── agentic.example.yaml
├── docs/
│   ├── architecture.md
│   ├── security_notes.md
│   └── skills_map.md
├── papers/
│   ├── MIMIC.md
│   ├── paper.md
│   ├── table.md
│   └── si.docx
├── scripts/
│   ├── build_tyg_sepsis_cohort.py
│   ├── build_tyg_analysis_dataset.py
│   ├── bootstrap_skills.sh
│   └── git_update.sh
├── shared/
│   ├── *.csv / *.json intermediate artifacts
│   └── sessions/<session_id>/ agentic task/session artifacts
├── results/
│   ├── reproduction_report.md
│   ├── *.png figures
│   └── *.jsonl run logs
└── src/repro_agent/
    ├── cli.py
    ├── contracts.py
    ├── config.py
    ├── runtime.py
    ├── pipeline.py
    ├── task_builder.py
    ├── agent_runner.py
    ├── dataset_adapters.py
    ├── stats_analysis.py
    ├── paper_contract.py
    ├── paper_materials.py
    ├── skills_registry.py
    ├── llm.py
    └── db/connectors.py
```

Artifact path conventions:

- `shared/`
  Structured intermediate artifacts used for downstream steps
- `results/`
  Final human-facing outputs and run logs
- `shared/sessions/<session_id>/`
  Agentic planning and execution artifacts tied to one task session

## Quick start

1. Copy the env template:

```bash
cp .env.example .env
```

`paper-repro` automatically loads `.env` from the project root.

2. Install the package:

```bash
pip install -e .
```

3. Optional: install project-scoped scientific skills:

```bash
bash scripts/bootstrap_skills.sh
```

4. Validate database env wiring:

```bash
paper-repro validate-env
paper-repro probe-db
```

5. Run a deterministic dry run:

```bash
paper-repro dry-run --config configs/pipeline.example.yaml
```

6. Build an agentic task contract:

```bash
paper-repro plan-task \
  --config configs/agentic.example.yaml \
  --paper-path papers/paper.md \
  --instructions "Describe the cohort, variables, models, and outputs you want"
```

7. Publish updates with the fixed git helper:

```bash
bash scripts/git_update.sh "feat: your change summary"
```

## Credentials and security

- Store DB and API keys in environment variables only
- Keep sample values only in `.env.example`
- Never commit raw secrets
- Logs and reports should expose only masked connection details

Current env inputs include:

- `MIMIC_PG_*`
- `SILICONFLOW_API_KEY`
- optional `LLM_BASE_URL`
- optional `LLM_DEFAULT_MODEL`

## Skills

External scientific skills are tracked in [`docs/skills_map.md`](docs/skills_map.md).

Current external skill targets:

- `pyhealth`
- `clinicaltrials-database`
- `clinical-reports`
- `statistical-analysis`

## Contributors

See [CONTRIBUTORS.md](CONTRIBUTORS.md).

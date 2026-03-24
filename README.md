# paper-repro-agent

Multi-subagent framework for automated clinical paper reproduction.

This project is designed to reproduce clinical studies (starting with MIMIC-IV style cohorts) using:
- an orchestrator + specialized subagents,
- strict artifact contracts in `shared/`,
- pluggable database connectors,
- project-scoped scientific skills.

## Why this structure

The framework borrows proven patterns from existing agent systems in this workspace:
- shared runtime + task adapters (instead of hardcoding per-task logic),
- strict `ACTION`/`FINAL_ANSWER` style handoff contracts,
- tool boundary between orchestration and data systems,
- config/env-based credential injection (no hardcoded secrets).

## Folder layout

```text
paper-repro-agent/
├── AGENTS.md
├── agents/
│   ├── paper_parser/AGENTS.md
│   ├── cohort_agent/AGENTS.md
│   ├── stats_agent/AGENTS.md
│   ├── verify_agent/AGENTS.md
│   └── report_agent/AGENTS.md
├── configs/
│   └── pipeline.example.yaml
├── docs/
│   └── architecture.md
├── papers/
├── shared/
├── results/
├── scripts/
│   └── bootstrap_skills.sh
└── src/repro_agent/
    ├── cli.py
    ├── config.py
    ├── contracts.py
    ├── pipeline.py
    ├── runtime.py
    └── db/connectors.py
```

## Quick start

1. Copy env template:
```bash
cp .env.example .env
```
`paper-repro` will auto-load `.env` from the project root.

2. Install Python deps:
```bash
pip install -e .
```

3. Optional: install project-scoped scientific skills:
```bash
bash scripts/bootstrap_skills.sh
```

4. Run a dry pipeline:
```bash
paper-repro dry-run --config configs/pipeline.example.yaml
```

5. Probe database connectivity:
```bash
paper-repro probe-db
```

The dry run creates placeholder outputs in `shared/` and `results/` so you can verify orchestration before wiring real databases and model calls.

## Target skills (clinical research)

- `scientific-skills/pyhealth`
- `scientific-skills/clinicaltrials-database`
- `scientific-skills/clinical-reports`
- `scientific-skills/statistical-analysis`

These are installed into `./.codex/skills` to keep this framework self-contained.

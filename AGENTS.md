# Repository Guide

This repository no longer uses this file as the primary orchestration contract.

Current source of truth:

- `README.md`
- `docs/architecture.md`
- `docs/clinical-analysis-capability-map.md`
- `openclaw/SOUL.MD`
- `openclaw/AGENTS.md`
- `openclaw/skills/skills_manifest.yaml`

The active workflow is:

`paper -> paper_evidence -> TaskContract -> agent_decision -> cohort/dataset/stats -> verification/report`

Do not treat older `shared/methods.json -> shared/cohort.csv -> results/reproduction_report.md` examples as the primary architecture anymore.

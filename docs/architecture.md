# Architecture

## Core design

The framework follows a reusable 3-layer split:

1. Orchestration layer
- step ordering
- quality gate evaluation
- run metadata and status

2. Runtime layer
- shared interfaces for subagents
- deterministic artifact writes
- standardized event logs

3. Tool/data layer
- database connectors
- external services and skills
- optional MCP boundaries

## Why this matters

- New paper domains should only require adapter changes, not engine rewrites.
- Reproducibility improves when artifacts are explicit and versioned.
- Security improves when credentials are injected at runtime.

## Security stance

- No hardcoded credentials in repository files.
- `src/repro_agent/db/connectors.py` builds masked connection specs from env.
- Reports/logs should only include masked secrets.

## Current status

This scaffold provides an executable dry pipeline for end-to-end orchestration tests.
Real SQL extraction and statistical execution are intentionally adapter points for the next step.


# Historical Reference: Early Agent Design Notes

This document is retained only as historical context.

The project no longer uses the older layout based on multiple prompt files in nested directories. That design has been retired in favor of the current OpenClaw and profile-driven architecture.

## Current active design

The active pipeline is:

1. read the paper from `papers/`
2. extract study structure into a `TaskContract`
3. detect a supported preset or stay in planning-first mode
4. build the cohort and analysis dataset
5. run statistics and export table and figure artifacts
6. verify against paper targets and write a report

## Current active files

- [`src/repro_agent/paper_materials.py`](../src/repro_agent/paper_materials.py)
- [`src/repro_agent/task_builder.py`](../src/repro_agent/task_builder.py)
- [`src/repro_agent/openclaw_bridge.py`](../src/repro_agent/openclaw_bridge.py)
- [`src/repro_agent/paper_profiles.py`](../src/repro_agent/paper_profiles.py)
- [`scripts/profiles/build_profile_cohort.py`](../scripts/profiles/build_profile_cohort.py)
- [`scripts/profiles/build_profile_analysis_dataset.py`](../scripts/profiles/build_profile_analysis_dataset.py)
- [`scripts/profiles/run_profile_stats.py`](../scripts/profiles/run_profile_stats.py)
- [`src/repro_agent/profile_stats.py`](../src/repro_agent/profile_stats.py)

## Current external interface

- one external agent: `paper-repro-scientist`
- soul file: [`openclaw/SOUL.MD`](../openclaw/SOUL.MD)
- skills: [`openclaw/skills/`](../openclaw/skills)
- contract bridge: [`src/repro_agent/openclaw_bridge.py`](../src/repro_agent/openclaw_bridge.py)

## Read these instead

- [`docs/architecture.md`](../architecture.md)
- [`docs/openclaw_integration.md`](../openclaw_integration.md)
- [`docs/skills_map.md`](../skills_map.md)

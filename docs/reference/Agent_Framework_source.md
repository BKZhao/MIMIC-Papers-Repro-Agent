# Historical Reference: Early Agent Framework

This file used to mirror an earlier directory-based prompt-file design.

That older layout has been removed from the active project because the current implementation no longer loads agent instructions from per-directory prompt files.

## What replaced it

The current source of truth is now:

- [`openclaw/SOUL.MD`](../openclaw/SOUL.MD)
- [`openclaw/skills/`](../openclaw/skills)
- [`src/repro_agent/task_builder.py`](../src/repro_agent/task_builder.py)
- [`src/repro_agent/openclaw_bridge.py`](../src/repro_agent/openclaw_bridge.py)
- [`src/repro_agent/paper_profiles.py`](../src/repro_agent/paper_profiles.py)
- [`scripts/profiles/`](../scripts/profiles)
- [`src/repro_agent/profile_stats.py`](../src/repro_agent/profile_stats.py)

## Why the old design was retired

- the repository now exposes one external OpenClaw agent instead of a directory of prompt files
- skill contracts and `TaskContract` objects are the real integration boundary
- deterministic execution now lives in profile-driven scripts, not in hand-maintained prompt files

## Read these instead

- [`docs/architecture.md`](../architecture.md)
- [`docs/openclaw_integration.md`](../openclaw_integration.md)
- [`docs/lobster_agent_contract.md`](../lobster_agent_contract.md)

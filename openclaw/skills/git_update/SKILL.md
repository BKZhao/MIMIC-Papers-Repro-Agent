# git_update

## Purpose

Provide a stable project-owned Git/GitHub update workflow so that OpenClaw can request repository sync behavior without embedding Git command logic into every agent prompt.

## Primary entrypoints

- `scripts/git_update.sh`
- `paper-repro` session artifacts for `git_update_agent`

## Inputs

- repo working tree
- commit message or update intent

## Outputs

- git update plan artifact
- repository sync side effects only when explicitly invoked

## Fails When

- git working tree is dirty in a conflicting way
- credentials or remote access are not available

## Guardrails

- Do not push automatically just because a session ran.
- Preserve user changes and avoid destructive git commands.
- Keep repository update behavior behind the fixed helper skill.

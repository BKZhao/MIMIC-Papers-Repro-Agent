---
name: git-github-update
description: Standardized GitHub update workflow for code changes. Use when users ask to commit, sync, or push repository updates, especially after making code or documentation edits that must be published to a remote GitHub branch with consistent safety checks.
---

# Git Github Update

## Overview

Use this skill to keep repository updates consistent and repeatable:
- preflight checks,
- staging + commit,
- pull with rebase,
- push to a target remote/branch.

This skill avoids force-push and other destructive operations by default.

## Workflow

1. Confirm repository path and remote target.
2. Run preflight check:
   - verify inside a git repo,
   - print current branch,
   - print concise `git status`.
3. Run standardized publish script with a commit message.
4. Report commit hash and push destination.

Use `scripts/git_update.sh` for the publish step.
If remote is not configured, use `scripts/set_remote.sh`.
If SSH identity needs verification, use `scripts/check_ssh_identity.sh`.
If repository creation is needed and a token is available, use `scripts/create_repo_with_token.sh`.

## Commands

Set remote once:
```bash
bash scripts/set_remote.sh /path/to/repo git@github.com:BKZhao/<repo>.git
```

Publish updates:
```bash
bash scripts/git_update.sh /path/to/repo "feat: message" origin main
```

Check SSH identity:
```bash
bash scripts/check_ssh_identity.sh
```

Create repo with API token:
```bash
bash scripts/create_repo_with_token.sh BKZhao paper-repro-agent private
```

## Rules

- Use clear commit messages with conventional style (`feat:`, `fix:`, `docs:`).
- Do not run `git push --force` unless user explicitly requests it.
- If no changes exist, stop and report `nothing to commit`.
- If `pull --rebase` conflicts, stop and ask user before continuing.

## References

For detailed behavior and edge-case handling, read:
- `references/workflow.md`

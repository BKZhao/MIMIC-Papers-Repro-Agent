# Workflow Reference

## Standard publish sequence

1. Ensure the working directory is a git repository.
2. Inspect current change set (`git status --short`).
3. Stage all intended updates (`git add -A`).
4. Commit with a clear message.
5. Pull latest remote changes with rebase.
6. Push current branch to remote.

## Failure handling

- `nothing to commit`: stop without error.
- `pull --rebase` conflict: stop and ask user for manual conflict resolution preference.
- `remote not found`: configure with `scripts/set_remote.sh`.
- `permission denied`: run `scripts/check_ssh_identity.sh` and verify repo access.

## Safety constraints

- Avoid force push by default.
- Avoid history rewrite commands unless explicitly requested.
- Keep commit messages scoped and descriptive.


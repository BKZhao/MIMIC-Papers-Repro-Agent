#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <commit_message> [remote] [branch]"
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMIT_MSG="$1"
REMOTE="${2:-origin}"
BRANCH="${3:-main}"

bash "$REPO_ROOT/.codex/skills/git-github-update/scripts/git_update.sh" \
  "$REPO_ROOT" "$COMMIT_MSG" "$REMOTE" "$BRANCH"

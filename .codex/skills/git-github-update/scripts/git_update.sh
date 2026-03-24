#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <repo_path> <commit_message> [remote] [branch]"
  exit 2
fi

REPO_PATH="$1"
COMMIT_MSG="$2"
REMOTE="${3:-origin}"
BRANCH="${4:-}"

if [[ ! -d "$REPO_PATH" ]]; then
  echo "Repository path not found: $REPO_PATH"
  exit 2
fi

if ! git -C "$REPO_PATH" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not a git repository: $REPO_PATH"
  exit 2
fi

if [[ -z "$BRANCH" ]]; then
  BRANCH="$(git -C "$REPO_PATH" rev-parse --abbrev-ref HEAD)"
fi

echo "[1/5] Preflight"
git -C "$REPO_PATH" status --short

echo "[2/5] Stage changes"
git -C "$REPO_PATH" add -A

if git -C "$REPO_PATH" diff --cached --quiet; then
  echo "No staged changes. Nothing to commit."
  exit 0
fi

echo "[3/5] Commit"
git -C "$REPO_PATH" commit -m "$COMMIT_MSG"

echo "[4/5] Pull with rebase from $REMOTE/$BRANCH"
if git -C "$REPO_PATH" ls-remote --exit-code --heads "$REMOTE" "$BRANCH" >/dev/null 2>&1; then
  git -C "$REPO_PATH" pull --rebase "$REMOTE" "$BRANCH"
else
  echo "Remote branch $REMOTE/$BRANCH not found yet. Skip pull --rebase."
fi

echo "[5/5] Push to $REMOTE/$BRANCH"
git -C "$REPO_PATH" push "$REMOTE" "$BRANCH"

echo "Done."

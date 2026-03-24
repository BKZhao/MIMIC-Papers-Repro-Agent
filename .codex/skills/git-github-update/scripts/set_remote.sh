#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <repo_path> <remote_url> [remote_name]"
  exit 2
fi

REPO_PATH="$1"
REMOTE_URL="$2"
REMOTE_NAME="${3:-origin}"

if [[ ! -d "$REPO_PATH" ]]; then
  echo "Repository path not found: $REPO_PATH"
  exit 2
fi

if ! git -C "$REPO_PATH" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not a git repository: $REPO_PATH"
  exit 2
fi

if git -C "$REPO_PATH" remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
  git -C "$REPO_PATH" remote set-url "$REMOTE_NAME" "$REMOTE_URL"
  echo "Updated $REMOTE_NAME -> $REMOTE_URL"
else
  git -C "$REPO_PATH" remote add "$REMOTE_NAME" "$REMOTE_URL"
  echo "Added $REMOTE_NAME -> $REMOTE_URL"
fi

git -C "$REPO_PATH" remote -v

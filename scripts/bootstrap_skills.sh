#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="$REPO_ROOT/.codex/skills"
INSTALLER="/home/bingkun_zhao/.codex/skills/.system/skill-installer/scripts/install-skill-from-github.py"

mkdir -p "$DEST_DIR"

echo "Installing clinical research skills into: $DEST_DIR"
python3 "$INSTALLER" \
  --repo K-Dense-AI/claude-scientific-skills \
  --path scientific-skills/pyhealth \
  scientific-skills/clinicaltrials-database \
  scientific-skills/clinical-reports \
  scientific-skills/statistical-analysis \
  --dest "$DEST_DIR"

echo ""
echo "Done. Restart Codex to pick up new skills."


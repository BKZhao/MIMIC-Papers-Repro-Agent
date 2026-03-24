#!/usr/bin/env bash
set -euo pipefail

PUB_KEY="${HOME}/.ssh/id_ed25519.pub"

if [[ -f "$PUB_KEY" ]]; then
  echo "Local key fingerprint:"
  ssh-keygen -lf "$PUB_KEY" | awk '{print $2}'
else
  echo "No ${PUB_KEY} found."
fi

echo ""
echo "GitHub SSH identity check:"
set +e
ssh -T -o StrictHostKeyChecking=accept-new git@github.com
RC=$?
set -e

# GitHub returns exit code 1 for successful auth without shell access.
if [[ $RC -eq 1 || $RC -eq 0 ]]; then
  exit 0
fi
exit "$RC"

#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <owner> <repo_name> [visibility: private|public]"
  exit 2
fi

OWNER="$1"
REPO="$2"
VISIBILITY="${3:-private}"
TOKEN="${GITHUB_TOKEN:-${GH_TOKEN:-}}"

if [[ -z "$TOKEN" ]]; then
  echo "Missing GITHUB_TOKEN or GH_TOKEN."
  exit 2
fi

if [[ "$VISIBILITY" != "private" && "$VISIBILITY" != "public" ]]; then
  echo "Visibility must be 'private' or 'public'."
  exit 2
fi

IS_PRIVATE=true
if [[ "$VISIBILITY" == "public" ]]; then
  IS_PRIVATE=false
fi

PAYLOAD=$(cat <<EOF
{
  "name": "${REPO}",
  "private": ${IS_PRIVATE},
  "auto_init": false
}
EOF
)

HTTP_CODE=$(curl -sS -o /tmp/github_create_repo_response.json -w "%{http_code}" \
  -X POST "https://api.github.com/user/repos" \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -d "$PAYLOAD")

if [[ "$HTTP_CODE" != "201" ]]; then
  echo "GitHub API returned HTTP $HTTP_CODE"
  cat /tmp/github_create_repo_response.json
  exit 1
fi

echo "Repository created: ${OWNER}/${REPO}"
cat /tmp/github_create_repo_response.json

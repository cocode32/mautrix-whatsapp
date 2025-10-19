#!/bin/bash
#
# mirror-to-github.sh
# Mirrors the current Codeberg repository to a GitHub remote via SSH.
# Requires: an existing SSH key configured for GitHub.
#

set -euo pipefail

# --- Config ---
GITHUB_USER="cocode32"
GITHUB_REPO="watgbridge"
REMOTE_NAME="github"

# --- Script start ---
echo "🔁 Starting mirror to GitHub..."

# Check if we're in a git repo
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: Not inside a git repository."
  exit 1
fi

# Remove any stale github remote
if git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
  echo "Removing existing '$REMOTE_NAME' remote..."
  git remote remove "$REMOTE_NAME"
fi

# Add github remote via SSH
GIT_URL="git@github.com:${GITHUB_USER}/${GITHUB_REPO}.git"
echo "Adding GitHub remote: $GIT_URL"
git remote add "$REMOTE_NAME" "$GIT_URL"

# Push all refs and tags
echo "Pushing all refs and tags to GitHub..."
git push --mirror "$REMOTE_NAME"

# Remove github remote for a clean workspace
echo "Cleaning up..."
git remote remove "$REMOTE_NAME"

echo "✅ Mirror complete: Codeberg → GitHub"

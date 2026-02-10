#!/usr/bin/env bash
#
# Install Git hooks for the project.
# Run once after cloning: ./scripts/install-hooks.sh
#
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"

echo "Configuring Git to use .githooks directory..."
git config core.hooksPath "$REPO_ROOT/.githooks"

echo "Making hooks executable..."
chmod +x "$REPO_ROOT/.githooks/"*

echo "✓ Git hooks installed. Pre-commit checks are now active."

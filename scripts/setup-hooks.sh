#!/bin/bash
set -e

echo "Installing pre-commit with uv..."
uvx --from pre-commit pre-commit install

echo "Running pre-commit on all files to verify setup..."
uvx --from pre-commit pre-commit run --all-files || true

echo ""
echo "✅ Pre-commit hooks installed successfully!"
echo "Hooks will now run automatically on git commit."
echo ""
echo "Useful commands:"
echo "  uvx --from pre-commit pre-commit run --all-files  # Run on all files"
echo "  uvx --from pre-commit pre-commit run               # Run on staged files"
echo "  git commit --no-verify                             # Skip hooks for one commit"

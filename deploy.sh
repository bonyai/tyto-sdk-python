#!/bin/bash
set -e

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

SDK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! python3 -c "import twine"; then
  echo "❌ Twine is not installed for $(python3 --version)."
  echo "   Install deployment dependencies with: python3 -m pip install -e '.[dev]'"
  exit 1
fi

echo "📦 Building tyto (Python)..."
rm -rf "$SDK_DIR/dist"
python3 -m build "$SDK_DIR"

echo "🔍 Checking package metadata..."
python3 -m twine check "$SDK_DIR"/dist/*

echo "✅ Publishing tyto to PyPI..."
python3 -m twine upload "$SDK_DIR"/dist/*

echo "🎉 tyto deployed successfully!"

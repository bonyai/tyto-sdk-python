#!/bin/bash
set -e

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

SDK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "📦 Building tyto (Python)..."
python3 -m build

echo "✅ Publishing tyto to PyPI..."
twine upload dist/*

echo "🎉 tyto deployed successfully!"

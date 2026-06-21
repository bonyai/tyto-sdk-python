#!/bin/bash
set -e

SDK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "📦 Building tyto (Python)..."
python -m build

echo "✅ Publishing tyto to PyPI..."
twine upload dist/*

echo "🎉 tyto deployed successfully!"

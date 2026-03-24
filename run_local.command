#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")"

if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

export OPEN_BROWSER=1
export FLASK_DEBUG=1
python3 app.py

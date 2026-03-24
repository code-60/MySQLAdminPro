#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v pyinstaller >/dev/null 2>&1; then
  echo "pyinstaller not found. Install it with: pip install pyinstaller"
  exit 1
fi

pyinstaller \
  --noconfirm \
  --clean \
  --windowed \
  --name "MySQLAdminPro" \
  --add-data "templates:templates" \
  desktop_launcher.py

echo "Done. App bundle: dist/MySQLAdminPro.app"

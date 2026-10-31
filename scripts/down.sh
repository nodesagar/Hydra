#!/usr/bin/env bash
# Stop every broker started by scripts/up.sh. Data in data/ is kept.
cd "$(dirname "$0")/.."
for f in data/*.pid; do
  [ -e "$f" ] || continue
  kill "$(cat "$f")" 2>/dev/null || true
  rm -f "$f"
done
echo "brokers stopped"

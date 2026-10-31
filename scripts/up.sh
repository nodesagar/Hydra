#!/usr/bin/env bash
# Build hydra and start one broker per site in topology.json.
# Logs: data/logs/<site>.log. Pids: data/<site>.pid. Stop with scripts/down.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/hydra ./cmd/hydra
mkdir -p data/logs
for site in $(bin/hydra topo -names); do
  if [ -e "data/$site.pid" ] && kill -0 "$(cat "data/$site.pid")" 2>/dev/null; then
    echo "$site already running"; continue
  fi
  bin/hydra broker -site "$site" >"data/logs/$site.log" 2>&1 &
  echo $! >"data/$site.pid"
done
echo "brokers started; logs in data/logs/, stop with scripts/down.sh"

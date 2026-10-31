# Hydra Stream

Hackathon build of a Kafka-style broker that keeps every record inside its legal jurisdiction, routes around slow links, and survives a site outage without freezing.

## Start every session here

1. `docs/PLAN.md`: current phase, next action, session log. Update it before ending a session.
2. `docs/CONTRACTS.md`: every interface between components. Code must match it; cite sections as "CONTRACTS §n".
3. `docs/DECISIONS.md`: where and why the build differs from the blueprint.

Blueprint (architecture rationale, roadmap, demo script, backlog with status dropdowns): https://claude.ai/artifact/RFsaKAAaW5gUKXLTyNWRvw, a Claude Docs doc read through the docs connector.

## Rules

- Change an interface only together with CONTRACTS.md and a DECISIONS.md entry.
- The residency predicate exists once: `topo.Topology.Allowed(label, site)`. Never re-implement it.
- Never log, expose or gossip record keys or values (invariant R5).
- Stdlib first. Allowed dependencies: `hashicorp/raft`, `hashicorp/raft-boltdb/v2`, `hashicorp/go-hclog` (raft's logger type). Anything else needs a DECISIONS.md entry first.
- Every non-trivial piece of logic leaves one runnable check; `go test ./...` stays green.
- Mark deliberate shortcuts with `// ponytail:` plus the ceiling and the upgrade path.
- Build in PLAN.md step order; finish a step, with its check, before starting the next.

## Commands

- `go vet ./... && go test -race ./...`: unit tests plus in-process clusters that kill leaders
- `go run ./cmd/hydra topo` validates and prints `topology.json`
- `scripts/up.sh` builds `bin/hydra` and starts one broker per site (logs in `data/logs/`); `scripts/down.sh` stops them; `rm -rf data` resets all state
- `bin/hydra produce -site eu-par -label EU -key k -value v` and `bin/hydra fetch -site eu-ams -label EU -partition 0`
- `curl -s 127.0.0.1:7101/v1/state | jq` shows one broker's links, groups and counters

## Layout

- `cmd/hydra`: the single binary and its subcommands
- `internal/topo`: topology, placement, residency predicate, simulated RTT
- `internal/api`: edge HTTP API wire types (CONTRACTS §4, §7, §8)
- `internal/mesh`: broker-to-broker streams, M1–M3, relays, probes, simulated latency, faults (§5)
- `internal/broker`: one site; `fsm.go` state machine (§6.3), `group.go` Raft per partition (§6.1), `broker.go` edge API (§4)
- `internal/sdk`: client used by the CLI, load generator and checker
- `internal/testkit`: throwaway topologies on free ports for tests
- `scripts/`: `up.sh`, `down.sh`
- `topology.json`: demo deployment, EU, IN and US × 3 sites; ports 7100 observer, 7101–7109 API, 7201–7209 mesh

## Glossary

- **Jurisdiction**: a legal boundary (EU). Its name is the residency **label**.
- **Site**: a failure domain inside one jurisdiction (eu-fra). One broker per site.
- **Partition id**: `topic.JURISDICTION.index`, for example `payments.EU.2`. One Raft group per partition, voters on every site of its jurisdiction.

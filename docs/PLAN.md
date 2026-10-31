# Plan and status

Blueprint (why, architecture, demo script): https://claude.ai/artifact/RFsaKAAaW5gUKXLTyNWRvw

This file is the single source of build status. Update the checkboxes, **Now** and the session log at the end of every work session.

## Now

- **Phase:** 1 is done (gate G1 passed 2026-10-31). Phase 2 is next.
- **Next action:** step 2.1, routing in `internal/mesh`: choose a relay from probe rows with the blueprint's routing formula (blueprint §3.3, `path_J`: direct vs best in-jurisdiction relay + 1 ms), hysteresis 20% for 3 rounds, report `via` in `/v1/state`; add the rule to CONTRACTS as §5.6.
- **Open questions for the user:** none blocking. Assumed: 36 h, Go, demo on one laptop.

Legend: `[x]` done · `[~]` in progress · `[ ]` not started. "G1 check" lines are the exact commands that prove a gate.

## Phase 0 · Contracts · gate G0: contracts frozen

- [x] 0.1 Repo scaffold: `go.mod` (module `hydra`), `cmd/hydra`, `internal/topo`, `internal/api`
- [x] 0.2 `topology.json`: EU, IN, US × 3 sites; simulated RTT matrix; topic `payments` with 4 partitions per jurisdiction
- [x] 0.3 `docs/CONTRACTS.md`: identifiers, topology, edge API, mesh, Raft commands, state, faults, invariants
- [x] 0.4 `hydra topo` prints the topology; `topo` tests cover lookup, fallback, residency predicate, placement, rejections

G0 check: `go vet ./... && go test ./... && go run ./cmd/hydra topo`

## Phase 1 · Skeleton · gate G1: three brokers produce, commit and fetch; a leader kill is survived

- [x] 1.1 Mesh v0: header line, dispatch by kind and group, `Dial`/`Listen`, M1–M3 checks, direct links only (CONTRACTS §5.1–5.3)
- [x] 1.2 Mesh probes and faults: ping/pong, EWMA RTT, suspicion, rows; simulated latency, loss, down (§5.4–5.5)
- [x] 1.3 Partition group: hashicorp/raft over a mesh stream layer, boltdb, state machine with offsets, idempotence, group offsets, retention (§6)
- [x] 1.4 Edge API: produce (forward to leader), fetch, commit, offsets, metadata, state, faults; R3/R4 checks (§4)
- [x] 1.5 SDK v0 (`internal/sdk`): partitioner, entry broker choice, idempotent producer, retries with backoff, fetch
- [x] 1.6 `hydra broker`, `scripts/up.sh` for nine brokers, and an integration test: 3 EU brokers, produce and fetch, kill the leader, produce again

G1 check: `go test -race ./...` (broker and SDK integration tests kill a leader), then `scripts/up.sh`, `bin/hydra produce -site eu-par -label EU -key k -value v`, `bin/hydra fetch -site eu-ams -label EU -partition 0`, `scripts/down.sh`.

G1 result (2026-10-31): in-process leader kill, writes resume in 1.16 s; real `kill -9` of the EU.0 leader process, the next CLI write lands after 1.93 s including CLI start; the restarted broker caught up from its boltdb log; measured RTTs match the matrix (eu-fra to eu-ams 8.7 ms vs 8 simulated).

## Phase 2 · Three challenges · gate G2: all three challenges work from the CLI

- [ ] 2.1 Mesh routing: one-relay detours with hysteresis (20% better for 3 rounds), `via` in state. Relay splice already exists (1.1). When a route changes, close the pooled streams on the old path (Raft and HTTP pools redial).
- [ ] 2.2 Nine brokers; site isolation through faults; pause measurement
- [ ] 2.3 `hydra load`: load generator with per-site producers through the SDK
- [ ] 2.4 Residency paths shown from the CLI: wrong-label produce, foreign fetch, forced foreign hop, each refused and counted
- [ ] 2.5 `hydra observer` v1: live map, residency matrix, pause meter

## Phase 3 · Brains and proof · gate G3: checker passes three runs in a row; backup video recorded

- [ ] 3.1 Leadership optimizer: score, hysteresis, 10 s rate limit, evacuation of suspect sites
- [ ] 3.2 `hydra check`: loss, duplicates, order, residency matrix, pauses
- [ ] 3.3 Scenario scripts for demo steps 1–7; detours on/off toggle
- [ ] 3.4 Backup video

## Phase 4 · Harden and stretch · gate G4: code freeze

- [ ] 4.1 P1: hedged produce, nearest-replica reads, residency-aware spool, unrestricted topic
- [ ] 4.2 P2: Kafka gateway via kmsg, real multi-region run, design slides

## Blueprint backlog sync

The blueprint's Backlog table has a Status dropdown per row. Keep it in step with this file at the end of each step.
Ids: doc `c46ee87e-62ed-4aea-b818-ca61f74c0b70`, body node `508b20ee-e2e2`, table `my8p62d66y5.22225`, Status column 4, enum `28b6bb39-bc85` (index 0 Not started, 1 In progress, 2 Done). Rows count from 1 below the header, in backlog order.
How: `read` the body with `{"kind":"view","sinceRev":<last rev>}`, then `update` with one `replace` per cell, target `{"kind":"cell","table":…,"row":n,"col":4}`, `ifRev` = that read's rev, content `<?claude block x?>` with `x` a dropdown chip.
Last sync: doc rev 21 (2026-10-31). Rows 1–5 Done; rows 6–7 In progress; the rest Not started.

## Session log (newest first)

- **2026-10-31 · Phase 1 done.** Built `internal/mesh` (header, dispatch, M1–M3, relay splice, probes, receiver-side delay, faults), `internal/broker` (Raft per partition over the mesh, gob FSM with idempotence and retention, edge API with R3/R4, follower forwarding over mesh HTTP, down-site management plane), `internal/sdk` (nearest-first brokers, one batch in flight per partition, jittered backoff, fail-closed reads), `internal/testkit`, `hydra broker|produce|fetch`, `scripts/up.sh` and `down.sh`. Tests: FSM unit, mesh residency and faults, 3-broker failover, SDK exactly-once through failover. Next: 2.1.
- **2026-10-31 · Phase 0 done.** Installed Go 1.27.3 (Homebrew). Wrote `topology.json`, `internal/topo` (parse, validate, RTT lookup, residency predicate, placement) with tests, `internal/api` wire types, `hydra topo`, and the CONTRACTS, DECISIONS and PLAN docs. Decisions D1–D8 recorded. Next: 1.1.

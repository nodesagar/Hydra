# Decisions

Where the build differs from, or pins down, the blueprint (https://claude.ai/artifact/RFsaKAAaW5gUKXLTyNWRvw). Newest first. Each entry: decision, why, and what it affects.

## D8 · 2026-10-31 · HTTP forwarding rides the mesh

A follower forwards produce and commit to the leader as plain HTTP over a mesh stream (`kind: http`). Routing, residency checks and fault injection then apply to forwarding with no extra protocol. Affects CONTRACTS §4.3, §5.2.

## D7 · 2026-10-31 · Latency is simulated at the receiver of each hop

Each mesh hop delays received data by `RTT/2` plus the injected delay for that link. Client requests pay one simulated RTT at the broker. One laptop then behaves like a WAN, deterministically. The delay sits on the receiving side so a sender's Close can never drop queued bytes; a first draft that delayed writes would have lost data on close. Affects CONTRACTS §4.1, §5.5.

## D6 · 2026-10-31 · JSON outside, gob inside

The edge API and mesh headers use JSON (debuggable with curl). Raft log entries and snapshots use gob (binary, stdlib). Affects CONTRACTS §4, §6.2.

## D5 · 2026-10-31 · Client location comes from a declared header

`Hydra-Client-Site` is trusted in the MVP. Production would bind it to an mTLS identity. This is stated openly in the pitch. Affects CONTRACTS §4.1.

## D4 · 2026-10-31 · Raft uses the mesh from day one

The blueprint started Raft on hashicorp's stock TCP transport so two people could work in parallel. With one builder that split buys nothing and costs a port per group. A minimal mesh (header plus dispatch) comes first, and Raft uses it through `raft.NetworkTransport` with a mesh stream layer. Affects PLAN Phase 1 order and CONTRACTS §5, §6.1.

## D3 · 2026-10-31 · Voters are all sites of the home jurisdiction

There is no replica-count knob. The demo has exactly 3 sites per jurisdiction. Add a count plus rotation when a jurisdiction has more sites than replicas. Affects CONTRACTS §2.5.

## D2 · 2026-10-31 · Topology file is JSON

The blueprint sketched YAML. JSON needs no dependency (stdlib `encoding/json`) and matches the API. Affects `topology.json`, CONTRACTS §2.

## D1 · 2026-10-31 · Stack from the blueprint

Go 1.27 (Homebrew), `hashicorp/raft` with `raft-boltdb/v2`, stdlib for everything else. Module path `hydra`. A Kafka wire gateway via franz-go `kmsg` stays a P2 stretch goal.

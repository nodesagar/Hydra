# Contracts

Every interface between Hydra components. Code must match this file. Change both together and add a `docs/DECISIONS.md` entry. Section numbers are stable: cite them as "CONTRACTS §5.3".

Go mirror of §4, §7, §8: `internal/api/api.go`. Go mirror of §1–2: `internal/topo/topo.go`.

## 1. Identifiers

| Thing | Format | Example |
| --- | --- | --- |
| Jurisdiction = residency label | `^[A-Z]{2,8}$` | `EU` |
| Site (one broker per site) | `^[a-z0-9][a-z0-9-]{0,31}$`, unique across jurisdictions | `eu-fra` |
| Topic | `^[a-z0-9_-]{1,64}$` | `payments` |
| Partition id | `<topic>.<jurisdiction>.<index>`, index `0..partitions-1` | `payments.EU.2` |
| Offset | int64, contiguous from 0 per partition | `1200` |
| Producer id | 16 lowercase hex chars, random per producer instance | `9f1c2a7e5b3d4c11` |
| Raft server id and address | the site name | `eu-par` |

A topic with `"partitions": 4` has 4 partitions in **each** jurisdiction: 12 in total for EU, IN, US.

## 2. Topology file (`topology.json`)

```json
{
  "jurisdictions": [
    {"name": "EU", "sites": [
      {"name": "eu-fra", "city": "Frankfurt", "api": "127.0.0.1:7101", "mesh": "127.0.0.1:7201"}
    ]}
  ],
  "rtt_ms": {"eu-fra/eu-par": 10, "EU/IN": 120},
  "topics": [{"name": "payments", "partitions": 4}],
  "observer": "127.0.0.1:7100"
}
```

- **2.1** Unknown fields are rejected.
- **2.2** Names follow §1 and are unique. Every jurisdiction has at least one site; it needs three to survive a site loss.
- **2.3** `api` and `mesh` addresses are required and unique.
- **2.4** `rtt_ms` holds simulated round-trip times. For sites a≠b the lookup tries `a/b`, `b/a`, then the jurisdictions `J1/J2`, `J2/J1` (`EU/EU` is a default inside EU). Every site pair must resolve to a value above 0, and every key must name known sites or jurisdictions.
- **2.5** Placement: the voters of a partition are all sites of its jurisdiction, in file order (`topo.Voters`).
- **2.6** Residency predicate: `Allowed(label, site)` is true only if the site belongs to jurisdiction `label` (`topo.Allowed`). Every residency check calls this one function.

Demo ports: observer 7100, edge APIs 7101–7109, mesh 7201–7209.

## 3. Processes

| Command | Listens on | State | Phase |
| --- | --- | --- | --- |
| `hydra topo [-topology f]` | – | none; validates and prints | 0 |
| `hydra broker -site <site> [-topology f] [-data dir]` | site `api` (HTTP), site `mesh` (TCP) | `<data>/<site>/<partition id>/` | 1 |
| `hydra observer [-topology f]` | `observer` (HTTP: dashboard, fault fan-out) | none | 2 |
| `hydra produce -site s -label L [-key k] -value v [-n count]` | – | none; SDK client at site s | 1 |
| `hydra fetch -site s -label L [-partition p] [-offset o] [-max m]` | – | none; SDK client at site s | 1 |
| `hydra load …`, `hydra check …` | – | – | 2, 3 |

Defaults: `-topology topology.json`, `-data data`.

## 4. Edge HTTP API (each broker, JSON over HTTP/1.1)

### 4.1 Headers

- `Hydra-Client-Site: <site>` is required on produce, fetch, commit and offsets. It declares where the caller runs. An unknown site gets `BAD_REQUEST`. The MVP trusts it; production would derive it from an mTLS identity.
- `Hydra-Forwarded-By: <site>` is set by a broker that forwards a request to the leader over the mesh (§5.2, kind `http`).
- Simulated client latency: a broker sleeps `RTT(client site, broker site)` before handling a request that has `Hydra-Client-Site` and no `Hydra-Forwarded-By`.

### 4.2 Errors

Every non-2xx response has the body `{"error":"<CODE>","message":"<text>"}`.

| Code | HTTP | Retry? | When |
| --- | --- | --- | --- |
| `BAD_REQUEST` | 400 | no | malformed body, unknown site, topic or label, index out of range, 0 or more than 1000 records |
| `RESIDENCY_DENIED` | 403 | no | label is not the broker's jurisdiction (R3), or the client is outside the label's jurisdiction on a read (R4) |
| `UNKNOWN_PARTITION` | 404 | no | partition not hosted on this broker |
| `UNAVAILABLE` | 503 | yes | no leader yet, site down, timeout, leader moved during a forward |
| `INTERNAL` | 500 | no | bug |

Clients retry `UNAVAILABLE` and network errors with jittered exponential backoff (50 ms start, 1 s cap). Producer idempotence (§6.3) makes produce retries safe.

### 4.3 Endpoints

**4.3.1 `POST /v1/produce`**, any broker in the label's jurisdiction.

```json
{"topic":"payments","label":"EU","partition":2,"producer_id":"9f1c2a7e5b3d4c11","base_seq":17,
 "records":[{"key":"dXNlci00Mg==","value":"eyJhbW91bnQiOjEyfQ=="}]}
```
→ `{"partition_id":"payments.EU.2","base_offset":1200,"count":1,"duplicate":false,"leader":"eu-fra"}`

- Keys and values are base64. A batch holds 1–1000 records, all with the batch's label.
- R3: the broker refuses unless `Allowed(label, self)`.
- A follower forwards the request once to the leader over the mesh; it never forwards across a border. If the target is no longer leader, the answer is `UNAVAILABLE`.
- The response is sent after the batch is committed on a majority of voters and applied (A1).
- `producer_id` is optional; without it there is no deduplication.

**4.3.2 `POST /v1/fetch`**, any broker in the label's jurisdiction; served from the local replica.

```json
{"topic":"payments","label":"EU","partition":2,"offset":1200,"max":500}
```
→ `{"partition_id":"payments.EU.2","records":[{"offset":1200,"key":"…","value":"…"}],"log_start_offset":0,"high_watermark":1201,"served_by":"eu-par"}`

- R4: refused unless both the client site and this broker are allowed for the label (home-only reads).
- Returns committed records only, up to this replica's applied state. `max` defaults to 500 and is capped at 1000. An offset below `log_start_offset` reads from `log_start_offset`.

**4.3.3 `POST /v1/commit`**: `{"group":"billing","topic":"payments","label":"EU","partition":2,"offset":1201}` → `{}`. Same R4 rule as fetch. It is forwarded to the leader and stored through Raft, so offsets fail over with the data.

**4.3.4 `POST /v1/offsets`**: `{"group":"billing","topic":"payments","label":"EU","partition":2}` → `{"offset":1201}`, or `-1` if nothing is committed. Same R4 rule; it reads the local replica.

**4.3.5 `GET /v1/metadata`** → `{"site":"eu-par","jurisdiction":"EU","partitions":[{"id":"payments.EU.0","leader":"eu-fra","voters":["eu-fra","eu-par","eu-ams"]}]}`. It lists only the partitions this broker hosts; `leader` is `""` while unknown.

**4.3.6 `GET /v1/state`**: §7. **4.3.7 `GET` and `POST /v1/faults`**: §8.

## 5. Mesh (broker-to-broker TCP)

### 5.1 Connection header

Every mesh TCP connection starts with one JSON line, at most 1 KiB, ending in `\n`:

```json
{"v":1,"kind":"raft","src":"eu-fra","dst":"eu-par","label":"EU","group":"payments.EU.2","via":""}
```

After the header the stream belongs to the kind's protocol. `via` names the single relay site, or is `""` for a direct link.

### 5.2 Kinds

| Kind | Label | Group | After the header | Relays |
| --- | --- | --- | --- | --- |
| `probe` | `""` (metadata only) | `""` | newline-delimited JSON (§5.4) | never |
| `raft` | the partition's jurisdiction | partition id | hashicorp/raft NetworkTransport protocol | allowed |
| `http` | the jurisdiction of both ends | `""` | HTTP/1.1 carrying the §4 API (leader forwarding) | allowed |

### 5.3 Residency checks on the mesh

- **M1, sender:** a stream with a non-empty label may only be dialled when `Allowed(label, dst)` and, if relayed, `Allowed(label, via)`.
- **M2, relay:** a relay forwards only when `Allowed(label, self)` and `Allowed(label, dst)`. It dials `dst` with the same header (keeping `via`) and splices both directions.
- **M3, receiver:** it accepts only when `dst == self` and, for a non-empty label, `Allowed(label, self)`.
- Each refusal closes the connection and increments `firewall_blocked`. Kind `probe` with an empty label may cross borders: it carries latency data only.

### 5.4 Probes

- Every broker dials one `probe` stream to every other site and sends `{"t":"ping","seq":n,"ns":<sender clock>}` every 250 ms. The receiver echoes `{"t":"pong","seq":n,"ns":<same>}`.
- RTT is smoothed as an EWMA with α = 0.2. A peer is suspect when no pong arrives for max(1 s, 4 × smoothed RTT).
- Every 1 s the sender also writes its row, `{"t":"row","site":"eu-fra","rtt":{"eu-par":10.4},"suspect":["in-mum"]}`. Receivers keep the latest row per site; Phase 2 routing reads them.

### 5.5 Simulated latency and faults

- Every hop delays received data by `RTT(peer, self)/2 + delay_ms(peer, self)`, preserving order. Here `peer` is the immediate TCP peer: `src`, or `via` on a relayed stream at `dst`. Senders write straight to TCP, so closing a stream never drops data in flight. The header line is not delayed.
- With probability `loss_pct/100` a received chunk also pays a 200 ms retransmit penalty, and later chunks queue behind it.
- Read deadlines apply to the delayed stream, so Raft and HTTP timeouts see simulated latency.
- A site listed as down closes all its mesh connections, refuses new ones, and makes its API drop connections, except `GET /v1/state` and `GET`/`POST /v1/faults`: that management plane stays up so the observer can show and heal the site.

## 6. Replication (Raft per partition)

### 6.1 Setup

- There is one `hashicorp/raft` instance per partition on each voter site. Server id and address are the site name, and the transport is `raft.NetworkTransport` over the mesh (`kind: raft`).
- Storage: `raft-boltdb/v2` (`raft.db`, log plus stable) and a file snapshot store keeping 2 snapshots, both under `<data>/<site>/<partition id>/`.
- Bootstrap: on first start (no existing state) every voter calls `BootstrapCluster` with the identical voter list from §2.5.
- Timing: heartbeat and election timeout 500 ms, leader lease 250 ms, commit timeout 20 ms, pre-vote on (library default). Apply timeout 1 s, forward timeout 1.5 s, client request timeout 2 s.

### 6.2 Commands (gob-encoded Raft log entries)

| Command | Fields | Result |
| --- | --- | --- |
| `produce` | label, producer id, base seq, records (key, value) | base offset, count, duplicate |
| `commit` | group, offset | – |

### 6.3 State machine semantics

- Offsets are assigned at apply time, contiguous per partition.
- Defense in depth: a `produce` whose label differs from the partition's jurisdiction is rejected at apply time.
- Idempotence: per producer id the state machine keeps the last batch's (base seq, base offset, count). The same base seq returns that base offset with `duplicate: true`. A lower base seq returns `duplicate: true` with base offset -1. A higher one is appended. The SDK keeps at most one batch in flight per partition.
- Retention: the 50,000 most recent records per partition stay in memory. `log_start_offset` advances past dropped records.
- Snapshots hold the retained records, producer table and group offsets.

## 7. Observer state (`GET /v1/state`)

```json
{"site":"eu-fra","jurisdiction":"EU","down":false,"now_ms":1793434000000,
 "links":{"eu-par":{"rtt_ms":10.4,"suspect":false,"via":""}},
 "groups":[{"id":"payments.EU.0","role":"leader","leader":"eu-fra","term":3,"commit_index":1042,
            "applied_index":1042,"log_end_offset":997,"last_write_ms":1793433999000}],
 "counters":{"produced_records":997,"residency_denied":2,"firewall_blocked":0,"relayed_streams":0,
             "stored_bytes_by_label":{"EU":123456}}}
```

`stored_bytes_by_label` sums key and value bytes held by this broker's replicas, keyed by each record's label. The observer stacks these rows into the residency matrix (sites × labels); any off-diagonal byte is a violation. The state never contains keys or values (R5).

## 8. Faults (`GET` and `POST /v1/faults`)

```json
{"links":[{"a":"eu-fra","b":"eu-par","delay_ms":300,"loss_pct":5}],"down":["in-mum"]}
```

A POST replaces the whole fault state; GET returns it. Links are symmetric. Each broker applies the entries that involve itself. The observer posts the same body to every broker.

## 9. Invariants (what tests and the checker verify)

| Id | Invariant |
| --- | --- |
| R1 | Placement: every voter of a partition is allowed for its jurisdiction. |
| R2 | Transport: no mesh stream with label L is sent to, relayed by, or accepted by a site outside L (M1–M3). |
| R3 | Produce: accepted only by a broker allowed for the batch label. |
| R4 | Read: fetch, commit and offsets are served only when both client site and broker are allowed for the label. |
| R5 | Side channels: logs, metrics, state and gossip never contain record keys or values. |
| A1 | Durability: an acknowledged produce is committed on a majority of the partition's voters. |
| A2 | Isolation: no Raft group, lock or quorum spans two jurisdictions. |
| A3 | Pause: writes to partitions led by a failed site resume within 3 s. |

// Package api holds the edge HTTP API wire types (docs/CONTRACTS.md §4, §7, §8).
// Broker, SDK and observer all import these; change them only together with
// CONTRACTS.md and a DECISIONS.md entry.
package api

// Headers (CONTRACTS §4.1).
const (
	HeaderClientSite  = "Hydra-Client-Site"  // caller's site; drives read checks and simulated latency
	HeaderForwardedBy = "Hydra-Forwarded-By" // set by a broker forwarding to the leader over the mesh
)

// Error codes (CONTRACTS §4.2).
const (
	ErrBadRequest       = "BAD_REQUEST"
	ErrResidencyDenied  = "RESIDENCY_DENIED"
	ErrUnknownPartition = "UNKNOWN_PARTITION"
	ErrUnavailable      = "UNAVAILABLE"
	ErrInternal         = "INTERNAL"
)

var statusOf = map[string]int{
	ErrBadRequest:       400,
	ErrResidencyDenied:  403,
	ErrUnknownPartition: 404,
	ErrUnavailable:      503,
	ErrInternal:         500,
}

// Status is the HTTP status for an error code.
func Status(code string) int {
	if s, ok := statusOf[code]; ok {
		return s
	}
	return 500
}

// Retriable reports whether a client should retry after this error.
func Retriable(code string) bool { return code == ErrUnavailable }

// Error is the body of every non-2xx response.
type Error struct {
	Code    string `json:"error"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Record is one message. Offset is ignored on produce and set on fetch.
// Key and Value are base64 in JSON (Go []byte). They are never logged (invariant R5).
type Record struct {
	Offset int64  `json:"offset"`
	Key    []byte `json:"key,omitempty"`
	Value  []byte `json:"value"`
}

// ProduceRequest: POST /v1/produce (CONTRACTS §4.3.1).
type ProduceRequest struct {
	Topic      string   `json:"topic"`
	Label      string   `json:"label"`     // residency label = home jurisdiction of the partition
	Partition  int      `json:"partition"` // index within the label's partitions
	ProducerID string   `json:"producer_id,omitempty"`
	BaseSeq    int64    `json:"base_seq"`
	Records    []Record `json:"records"`
}

type ProduceResponse struct {
	PartitionID string `json:"partition_id"`
	BaseOffset  int64  `json:"base_offset"`
	Count       int    `json:"count"`
	Duplicate   bool   `json:"duplicate"`
	Leader      string `json:"leader"`
}

// FetchRequest: POST /v1/fetch (CONTRACTS §4.3.2).
type FetchRequest struct {
	Topic     string `json:"topic"`
	Label     string `json:"label"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Max       int    `json:"max"`
}

type FetchResponse struct {
	PartitionID    string   `json:"partition_id"`
	Records        []Record `json:"records"`
	LogStartOffset int64    `json:"log_start_offset"`
	HighWatermark  int64    `json:"high_watermark"` // next offset after the last committed record
	ServedBy       string   `json:"served_by"`
}

// CommitRequest: POST /v1/commit (CONTRACTS §4.3.3).
type CommitRequest struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Label     string `json:"label"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// OffsetsRequest: POST /v1/offsets (CONTRACTS §4.3.4).
type OffsetsRequest struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Label     string `json:"label"`
	Partition int    `json:"partition"`
}

type OffsetsResponse struct {
	Offset int64 `json:"offset"` // -1 when the group has committed nothing
}

// MetadataResponse: GET /v1/metadata (CONTRACTS §4.3.5).
type MetadataResponse struct {
	Site         string          `json:"site"`
	Jurisdiction string          `json:"jurisdiction"`
	Partitions   []PartitionInfo `json:"partitions"`
}

type PartitionInfo struct {
	ID     string   `json:"id"`
	Leader string   `json:"leader"` // "" while no leader is known
	Voters []string `json:"voters"`
}

// Faults: GET/POST /v1/faults (CONTRACTS §8). POST replaces the whole fault state.
type Faults struct {
	Links []LinkFault `json:"links"`
	Down  []string    `json:"down"` // sites that behave as dead
}

// LinkFault degrades the link between sites A and B in both directions.
type LinkFault struct {
	A       string  `json:"a"`
	B       string  `json:"b"`
	DelayMS float64 `json:"delay_ms"` // added one-way delay
	LossPct float64 `json:"loss_pct"` // chance a write pays a retransmit penalty
}

// State: GET /v1/state (CONTRACTS §7). Never carries keys or values (R5).
type State struct {
	Site         string               `json:"site"`
	Jurisdiction string               `json:"jurisdiction"`
	Down         bool                 `json:"down"`
	NowMS        int64                `json:"now_ms"`
	Links        map[string]LinkState `json:"links"` // peer site -> what this broker measures
	Groups       []GroupState         `json:"groups"`
	Counters     Counters             `json:"counters"`
}

type LinkState struct {
	RTTms   float64 `json:"rtt_ms"`        // smoothed measured RTT; -1 before the first pong
	Suspect bool    `json:"suspect"`       // no pong within the suspicion window
	Via     string  `json:"via,omitempty"` // relay used for this peer ("" = direct); Phase 2
}

type GroupState struct {
	ID           string `json:"id"`
	Role         string `json:"role"` // leader | follower | candidate | shutdown
	Leader       string `json:"leader"`
	Term         uint64 `json:"term"`
	CommitIndex  uint64 `json:"commit_index"`
	AppliedIndex uint64 `json:"applied_index"`
	LogEndOffset int64  `json:"log_end_offset"` // next offset to assign
	LastWriteMS  int64  `json:"last_write_ms"`  // wall time of the last applied produce
}

type Counters struct {
	ProducedRecords    int64            `json:"produced_records"`
	ResidencyDenied    int64            `json:"residency_denied"`
	FirewallBlocked    int64            `json:"firewall_blocked"`
	RelayedStreams     int64            `json:"relayed_streams"`
	StoredBytesByLabel map[string]int64 `json:"stored_bytes_by_label"`
}

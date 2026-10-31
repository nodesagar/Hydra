package broker

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/hashicorp/raft"

	"hydra/internal/api"
	"hydra/internal/topo"
)

// retain is how many records each replica keeps in memory per partition (CONTRACTS §6.3).
// ponytail: memory-only retention; add segment files when demos outgrow 50k records.
const retain = 50_000

var errLabel = errors.New("label does not match the partition's jurisdiction")

// command is one Raft log entry (CONTRACTS §6.2), gob-encoded.
type command struct {
	Op         string // "produce" | "commit"
	Label      string
	ProducerID string
	BaseSeq    int64
	Records    []api.Record // Offset ignored
	Group      string
	Offset     int64
	NowMS      int64 // leader wall clock at proposal, so every replica stores the same value
}

func (c command) encode() []byte {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(c) // encoding a plain struct cannot fail
	return b.Bytes()
}

type produceResult struct {
	BaseOffset int64
	Count      int
	Duplicate  bool
	Err        error
}

type stored struct {
	Label      string
	Key, Value []byte
}

type batchRef struct {
	BaseSeq, BaseOffset int64
	Count               int
}

// fsm is one partition's replicated state machine (CONTRACTS §6.3).
type fsm struct {
	label string // the partition's jurisdiction

	mu        sync.RWMutex
	start     int64 // offset of recs[0]
	recs      []stored
	producers map[string]batchRef
	groups    map[string]int64
	bytes     map[string]int64 // stored key+value bytes per record label
	lastWrite int64
}

func newFSM(p topo.PartitionID) *fsm {
	return &fsm{label: p.Jurisdiction, producers: map[string]batchRef{}, groups: map[string]int64{}, bytes: map[string]int64{}}
}

func (f *fsm) Apply(l *raft.Log) any {
	var c command
	if err := gob.NewDecoder(bytes.NewReader(l.Data)).Decode(&c); err != nil {
		return produceResult{Err: err}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch c.Op {
	case "produce":
		return f.produce(c)
	case "commit":
		f.groups[c.Group] = c.Offset
		return produceResult{}
	}
	return produceResult{Err: fmt.Errorf("unknown op %q", c.Op)}
}

func (f *fsm) produce(c command) produceResult {
	if c.Label != f.label {
		return produceResult{Err: errLabel}
	}
	if last, ok := f.producers[c.ProducerID]; ok && c.ProducerID != "" {
		switch {
		case c.BaseSeq == last.BaseSeq:
			return produceResult{BaseOffset: last.BaseOffset, Count: last.Count, Duplicate: true}
		case c.BaseSeq < last.BaseSeq:
			return produceResult{BaseOffset: -1, Count: len(c.Records), Duplicate: true}
		}
	}
	base := f.start + int64(len(f.recs))
	for _, r := range c.Records {
		f.recs = append(f.recs, stored{c.Label, r.Key, r.Value})
		f.bytes[c.Label] += int64(len(r.Key) + len(r.Value))
	}
	if c.ProducerID != "" {
		// ponytail: producer table never expires; add a TTL when producer ids churn.
		f.producers[c.ProducerID] = batchRef{c.BaseSeq, base, len(c.Records)}
	}
	f.lastWrite = c.NowMS
	if drop := len(f.recs) - retain; drop > 0 {
		for _, r := range f.recs[:drop] {
			f.bytes[r.Label] -= int64(len(r.Key) + len(r.Value))
		}
		f.recs = slices.Clone(f.recs[drop:])
		f.start += int64(drop)
	}
	return produceResult{BaseOffset: base, Count: len(c.Records)}
}

// read returns committed records from offset (clamped to the log start), at most max.
func (f *fsm) read(offset int64, max int) (recs []api.Record, logStart, highWatermark int64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	logStart, highWatermark = f.start, f.start+int64(len(f.recs))
	for o := max64(offset, logStart); o < highWatermark && len(recs) < max; o++ {
		r := f.recs[o-f.start]
		recs = append(recs, api.Record{Offset: o, Key: r.Key, Value: r.Value})
	}
	return recs, logStart, highWatermark
}

// committed returns a group's committed offset, -1 if none.
func (f *fsm) committed(group string) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if o, ok := f.groups[group]; ok {
		return o
	}
	return -1
}

func (f *fsm) stats() (logEnd, lastWrite int64, bytesByLabel map[string]int64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.start + int64(len(f.recs)), f.lastWrite, maps.Clone(f.bytes)
}

// snapState is what a snapshot holds (CONTRACTS §6.3).
type snapState struct {
	Start     int64
	Recs      []stored
	Producers map[string]batchRef
	Groups    map[string]int64
	LastWrite int64
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &snapState{f.start, slices.Clone(f.recs), maps.Clone(f.producers), maps.Clone(f.groups), f.lastWrite}, nil
}

func (s *snapState) Persist(sink raft.SnapshotSink) error {
	if err := gob.NewEncoder(sink).Encode(s); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapState) Release() {}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s snapState
	if err := gob.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.start, f.recs, f.lastWrite = s.Start, s.Recs, s.LastWrite
	f.producers, f.groups = orEmpty(s.Producers), orEmpty(s.Groups)
	f.bytes = map[string]int64{}
	for _, r := range f.recs {
		f.bytes[r.Label] += int64(len(r.Key) + len(r.Value))
	}
	return nil
}

func orEmpty[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

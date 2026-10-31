package broker

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"

	"hydra/internal/api"
	"hydra/internal/topo"
)

func apply(f *fsm, c command) produceResult {
	return f.Apply(&raft.Log{Data: c.encode()}).(produceResult)
}

func recs(n int) []api.Record {
	out := make([]api.Record, n)
	for i := range out {
		out[i] = api.Record{Key: []byte("k"), Value: []byte("vv")}
	}
	return out
}

func TestFSMOffsetsIdempotenceAndLabel(t *testing.T) {
	f := newFSM(topo.PartitionID{Topic: "payments", Jurisdiction: "EU"})

	if r := apply(f, command{Op: "produce", Label: "EU", ProducerID: "p1", BaseSeq: 0, Records: recs(3)}); r.BaseOffset != 0 || r.Duplicate {
		t.Fatalf("first batch: %+v", r)
	}
	if r := apply(f, command{Op: "produce", Label: "EU", ProducerID: "p1", BaseSeq: 3, Records: recs(2)}); r.BaseOffset != 3 {
		t.Fatalf("second batch: %+v", r)
	}
	// A retry of the last batch returns its original offset and appends nothing.
	if r := apply(f, command{Op: "produce", Label: "EU", ProducerID: "p1", BaseSeq: 3, Records: recs(2)}); !r.Duplicate || r.BaseOffset != 3 {
		t.Fatalf("retry: %+v", r)
	}
	if r := apply(f, command{Op: "produce", Label: "EU", ProducerID: "p1", BaseSeq: 0, Records: recs(3)}); !r.Duplicate || r.BaseOffset != -1 {
		t.Fatalf("older retry: %+v", r)
	}
	// Defense in depth: a batch with a foreign label never lands (CONTRACTS §6.3).
	if r := apply(f, command{Op: "produce", Label: "US", Records: recs(1)}); r.Err != errLabel {
		t.Fatalf("foreign label: %+v", r)
	}
	got, start, hw := f.read(1, 10)
	if start != 0 || hw != 5 || len(got) != 4 || got[0].Offset != 1 {
		t.Fatalf("read: start=%d hw=%d n=%d", start, hw, len(got))
	}
	if _, _, b := f.stats(); b["EU"] != 15 || b["US"] != 0 {
		t.Fatalf("bytes by label: %v", b)
	}

	apply(f, command{Op: "commit", Group: "billing", Offset: 4})
	if o := f.committed("billing"); o != 4 {
		t.Fatalf("committed = %d", o)
	}
	if o := f.committed("nobody"); o != -1 {
		t.Fatalf("committed for unknown group = %d", o)
	}

	// Snapshot and restore round-trip.
	snap, _ := f.Snapshot()
	var buf bytes.Buffer
	snap.Persist(&sink{&buf})
	g := newFSM(topo.PartitionID{Topic: "payments", Jurisdiction: "EU"})
	if err := g.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if end, _, b := g.stats(); end != 5 || b["EU"] != 15 || g.committed("billing") != 4 {
		t.Fatalf("restored: end=%d bytes=%v", end, b)
	}
}

func TestFSMRetention(t *testing.T) {
	f := newFSM(topo.PartitionID{Topic: "payments", Jurisdiction: "EU"})
	apply(f, command{Op: "produce", Label: "EU", Records: recs(retain)})
	apply(f, command{Op: "produce", Label: "EU", Records: recs(10)})
	got, start, hw := f.read(0, 5)
	if start != 10 || hw != retain+10 || got[0].Offset != 10 {
		t.Fatalf("after retention: start=%d hw=%d first=%d", start, hw, got[0].Offset)
	}
	if _, _, b := f.stats(); b["EU"] != int64(retain*3) {
		t.Fatalf("bytes after retention = %d", b["EU"])
	}
}

type sink struct{ *bytes.Buffer }

func (s *sink) ID() string    { return "test" }
func (s *sink) Cancel() error { return nil }
func (s *sink) Close() error  { return nil }

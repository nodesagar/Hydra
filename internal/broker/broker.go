// Package broker is one Hydra site: the mesh endpoint, one Raft group per
// partition of its jurisdiction, and the edge HTTP API (docs/CONTRACTS.md §4, §6).
package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"hydra/internal/api"
	"hydra/internal/mesh"
	"hydra/internal/topo"
)

const maxBatch = 1000

type Broker struct {
	t      *topo.Topology
	site   *topo.Site
	data   string
	mesh   *mesh.Mesh
	groups map[string]*group // partition id -> replica
	srv    *http.Server
	fwd    *http.Client // leader forwarding over the mesh (kind http)
	log    hclog.Logger

	produced        atomic.Int64
	residencyDenied atomic.Int64
	closeOnce       sync.Once
}

// New prepares the broker for site; Start opens its groups and listeners.
func New(t *topo.Topology, site, dataDir string, log hclog.Logger) (*Broker, error) {
	s := t.Site(site)
	if s == nil {
		return nil, fmt.Errorf("unknown site %q", site)
	}
	b := &Broker{t: t, site: s, data: filepath.Join(dataDir, site), mesh: mesh.New(t, site), groups: map[string]*group{}, log: log}
	b.fwd = &http.Client{Timeout: forwardTimeout, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			host, _, _ := net.SplitHostPort(addr)
			return b.mesh.Dial(host, mesh.KindHTTP, "", s.Jurisdiction, time.Second)
		},
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     30 * time.Second,
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/produce", b.handleProduce)
	mux.HandleFunc("POST /v1/fetch", b.handleFetch)
	mux.HandleFunc("POST /v1/commit", b.handleCommit)
	mux.HandleFunc("POST /v1/offsets", b.handleOffsets)
	mux.HandleFunc("GET /v1/metadata", b.handleMetadata)
	mux.HandleFunc("GET /v1/state", b.handleState)
	mux.HandleFunc("GET /v1/faults", b.handleGetFaults)
	mux.HandleFunc("POST /v1/faults", b.handleSetFaults)
	b.srv = &http.Server{Handler: b.middleware(mux), ReadHeaderTimeout: 5 * time.Second}
	return b, nil
}

func (b *Broker) Start() error {
	if err := b.mesh.Start(); err != nil {
		return err
	}
	for _, pid := range b.t.Partitions(b.site.Jurisdiction) {
		g, err := openGroup(b.data, b.t, b.mesh, b.site.Name, pid, b.log.Named(pid.String()))
		if err != nil {
			b.Close()
			return fmt.Errorf("partition %s: %w", pid, err)
		}
		b.groups[g.id] = g
	}
	ln, err := net.Listen("tcp", b.site.API)
	if err != nil {
		b.Close()
		return err
	}
	go b.srv.Serve(ln)
	go b.srv.Serve(b.mesh.Listen(mesh.KindHTTP, ""))
	return nil
}

func (b *Broker) Close() {
	b.closeOnce.Do(func() {
		b.srv.Close()
		for _, g := range b.groups {
			g.close()
		}
		b.mesh.Close()
	})
}

// middleware applies the down fault and simulated client latency (CONTRACTS §4.1, §5.5).
func (b *Broker) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A down site keeps only its management plane, so the observer can show and heal it.
		mgmt := r.URL.Path == "/v1/state" || r.URL.Path == "/v1/faults"
		if !mgmt && b.mesh.Down() {
			if hj, ok := w.(http.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					c.Close()
					return
				}
			}
			fail(w, api.ErrUnavailable, "site down")
			return
		}
		if cs := r.Header.Get(api.HeaderClientSite); cs != "" {
			if b.t.Site(cs) == nil {
				fail(w, api.ErrBadRequest, "unknown client site %q", cs)
				return
			}
			if r.Header.Get(api.HeaderForwardedBy) == "" {
				select {
				case <-time.After(time.Duration(b.t.RTT(cs, b.site.Name) * float64(time.Millisecond))):
				case <-r.Context().Done():
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// --- handlers (CONTRACTS §4.3) ---

func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req api.ProduceRequest
	body, ok := decode(w, r, &req)
	if !ok {
		return
	}
	g, ok := b.partition(w, req.Topic, req.Label, req.Partition)
	if !ok {
		return
	}
	if n := len(req.Records); n == 0 || n > maxBatch {
		fail(w, api.ErrBadRequest, "a batch holds 1..%d records, got %d", maxBatch, n)
		return
	}
	if b.forwardIfFollower(w, r, g, body) {
		return
	}
	res, err := g.apply(command{Op: "produce", Label: req.Label, ProducerID: req.ProducerID, BaseSeq: req.BaseSeq,
		Records: req.Records, NowMS: time.Now().UnixMilli()})
	if err != nil {
		fail(w, api.ErrUnavailable, "partition %s: %v", g.id, err)
		return
	}
	if res.Err != nil {
		b.residencyDenied.Add(1)
		fail(w, api.ErrResidencyDenied, "partition %s: %v", g.id, res.Err)
		return
	}
	if !res.Duplicate {
		b.produced.Add(int64(res.Count))
	}
	reply(w, api.ProduceResponse{PartitionID: g.id, BaseOffset: res.BaseOffset, Count: res.Count, Duplicate: res.Duplicate, Leader: b.site.Name})
}

func (b *Broker) handleFetch(w http.ResponseWriter, r *http.Request) {
	var req api.FetchRequest
	if _, ok := decode(w, r, &req); !ok {
		return
	}
	g, ok := b.partition(w, req.Topic, req.Label, req.Partition)
	if !ok || !b.readAllowed(w, r, req.Label) {
		return
	}
	max := req.Max
	if max <= 0 {
		max = 500
	}
	recs, start, hw := g.fsm.read(req.Offset, min(max, maxBatch))
	reply(w, api.FetchResponse{PartitionID: g.id, Records: recs, LogStartOffset: start, HighWatermark: hw, ServedBy: b.site.Name})
}

func (b *Broker) handleCommit(w http.ResponseWriter, r *http.Request) {
	var req api.CommitRequest
	body, ok := decode(w, r, &req)
	if !ok {
		return
	}
	g, ok := b.partition(w, req.Topic, req.Label, req.Partition)
	if !ok || !b.readAllowed(w, r, req.Label) {
		return
	}
	if req.Group == "" {
		fail(w, api.ErrBadRequest, "group is required")
		return
	}
	if b.forwardIfFollower(w, r, g, body) {
		return
	}
	if _, err := g.apply(command{Op: "commit", Label: req.Label, Group: req.Group, Offset: req.Offset}); err != nil {
		fail(w, api.ErrUnavailable, "partition %s: %v", g.id, err)
		return
	}
	reply(w, struct{}{})
}

func (b *Broker) handleOffsets(w http.ResponseWriter, r *http.Request) {
	var req api.OffsetsRequest
	if _, ok := decode(w, r, &req); !ok {
		return
	}
	g, ok := b.partition(w, req.Topic, req.Label, req.Partition)
	if !ok || !b.readAllowed(w, r, req.Label) {
		return
	}
	reply(w, api.OffsetsResponse{Offset: g.fsm.committed(req.Group)})
}

func (b *Broker) handleMetadata(w http.ResponseWriter, r *http.Request) {
	resp := api.MetadataResponse{Site: b.site.Name, Jurisdiction: b.site.Jurisdiction}
	for _, pid := range b.t.Partitions(b.site.Jurisdiction) {
		resp.Partitions = append(resp.Partitions, api.PartitionInfo{ID: pid.String(), Leader: b.groups[pid.String()].leader(), Voters: b.t.Voters(pid)})
	}
	reply(w, resp)
}

func (b *Broker) handleState(w http.ResponseWriter, r *http.Request) {
	st := api.State{
		Site: b.site.Name, Jurisdiction: b.site.Jurisdiction, Down: b.mesh.Down(), NowMS: time.Now().UnixMilli(),
		Links: b.mesh.Links(),
		Counters: api.Counters{
			ProducedRecords: b.produced.Load(), ResidencyDenied: b.residencyDenied.Load(),
			FirewallBlocked: b.mesh.FirewallBlocked.Load(), RelayedStreams: b.mesh.RelayedStreams.Load(),
			StoredBytesByLabel: map[string]int64{},
		},
	}
	for _, pid := range b.t.Partitions(b.site.Jurisdiction) {
		g := b.groups[pid.String()]
		st.Groups = append(st.Groups, g.state())
		_, _, bytesByLabel := g.fsm.stats()
		for label, n := range bytesByLabel {
			st.Counters.StoredBytesByLabel[label] += n
		}
	}
	reply(w, st)
}

func (b *Broker) handleGetFaults(w http.ResponseWriter, r *http.Request) { reply(w, b.mesh.Faults()) }

func (b *Broker) handleSetFaults(w http.ResponseWriter, r *http.Request) {
	var f api.Faults
	if _, ok := decode(w, r, &f); !ok {
		return
	}
	for _, s := range f.Down {
		if b.t.Site(s) == nil {
			fail(w, api.ErrBadRequest, "unknown site %q", s)
			return
		}
	}
	for _, l := range f.Links {
		if b.t.Site(l.A) == nil || b.t.Site(l.B) == nil || l.DelayMS < 0 || l.LossPct < 0 || l.LossPct > 100 {
			fail(w, api.ErrBadRequest, "bad link fault %s/%s", l.A, l.B)
			return
		}
	}
	b.mesh.SetFaults(f)
	reply(w, struct{}{})
}

// --- helpers ---

// partition validates the address of a partition, applies R3 (the label must be
// this broker's jurisdiction) and returns the local replica.
func (b *Broker) partition(w http.ResponseWriter, topic, label string, index int) (*group, bool) {
	tp := b.t.Topic(topic)
	if tp == nil || b.t.Sites(label) == nil || index < 0 || index >= tp.Partitions {
		fail(w, api.ErrBadRequest, "no partition %s.%s.%d", topic, label, index)
		return nil, false
	}
	if !b.t.Allowed(label, b.site.Name) {
		b.residencyDenied.Add(1)
		fail(w, api.ErrResidencyDenied, "%s data is never handled at %s (%s)", label, b.site.Name, b.site.Jurisdiction)
		return nil, false
	}
	g := b.groups[topo.PartitionID{Topic: topic, Jurisdiction: label, Index: index}.String()]
	if g == nil {
		fail(w, api.ErrUnknownPartition, "partition %s.%s.%d not hosted at %s", topic, label, index, b.site.Name)
		return nil, false
	}
	return g, true
}

// readAllowed applies R4: the reader must sit inside the label's jurisdiction.
func (b *Broker) readAllowed(w http.ResponseWriter, r *http.Request, label string) bool {
	cs := r.Header.Get(api.HeaderClientSite)
	if cs == "" {
		fail(w, api.ErrBadRequest, "header %s is required", api.HeaderClientSite)
		return false
	}
	if !b.t.Allowed(label, cs) {
		b.residencyDenied.Add(1)
		fail(w, api.ErrResidencyDenied, "%s data may not be read from %s", label, cs)
		return false
	}
	return true
}

// forwardIfFollower sends the request to the leader over the mesh, once (CONTRACTS §4.3.1).
// It reports true when it has written the response.
func (b *Broker) forwardIfFollower(w http.ResponseWriter, r *http.Request, g *group, body []byte) bool {
	if g.raft.State() == raft.Leader {
		return false
	}
	leader := g.leader()
	if leader == "" || leader == b.site.Name || r.Header.Get(api.HeaderForwardedBy) != "" {
		fail(w, api.ErrUnavailable, "partition %s: no leader reachable from %s", g.id, b.site.Name)
		return true
	}
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "http://"+leader+r.URL.Path, bytes.NewReader(body))
	req.Header.Set(api.HeaderForwardedBy, b.site.Name)
	req.Header.Set(api.HeaderClientSite, r.Header.Get(api.HeaderClientSite))
	resp, err := b.fwd.Do(req)
	if err != nil {
		fail(w, api.ErrUnavailable, "partition %s: forward to %s failed", g.id, leader)
		return true
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}

func decode(w http.ResponseWriter, r *http.Request, v any) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err == nil {
		err = json.Unmarshal(body, v)
	}
	if err != nil {
		fail(w, api.ErrBadRequest, "malformed body: %v", err)
		return nil, false
	}
	return body, true
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// fail writes a CONTRACTS §4.2 error. Messages carry ids and labels only, never keys or values (R5).
func fail(w http.ResponseWriter, code, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(api.Status(code))
	json.NewEncoder(w).Encode(api.Error{Code: code, Message: fmt.Sprintf(format, args...)})
}

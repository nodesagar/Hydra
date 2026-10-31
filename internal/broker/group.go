package broker

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"hydra/internal/api"
	"hydra/internal/mesh"
	"hydra/internal/topo"
)

// Raft timing (CONTRACTS §6.1).
const (
	applyTimeout   = time.Second
	forwardTimeout = 1500 * time.Millisecond
)

// group is this site's replica of one partition: a Raft instance plus its state machine.
type group struct {
	pid   topo.PartitionID
	id    string
	raft  *raft.Raft
	fsm   *fsm
	trans *raft.NetworkTransport
	store *raftboltdb.BoltStore
}

// streamLayer carries one group's Raft traffic over the mesh (CONTRACTS §5.2, kind raft).
type streamLayer struct {
	net.Listener
	m            *mesh.Mesh
	group, label string
}

func (s *streamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return s.m.Dial(string(addr), mesh.KindRaft, s.group, s.label, timeout)
}

func openGroup(dataDir string, t *topo.Topology, m *mesh.Mesh, self string, pid topo.PartitionID, log hclog.Logger) (*group, error) {
	id := pid.String()
	dir := filepath.Join(dataDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(dir, "raft.db")})
	if err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(dir, 2, log)
	if err != nil {
		store.Close()
		return nil, err
	}
	trans := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  &streamLayer{Listener: m.Listen(mesh.KindRaft, id), m: m, group: id, label: pid.Jurisdiction},
		MaxPool: 3,
		Timeout: 2 * time.Second,
		Logger:  log,
	})

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(self)
	c.HeartbeatTimeout = 500 * time.Millisecond
	c.ElectionTimeout = 500 * time.Millisecond
	c.LeaderLeaseTimeout = 250 * time.Millisecond
	c.CommitTimeout = 20 * time.Millisecond
	c.SnapshotThreshold = 8192
	c.TrailingLogs = 4096
	c.Logger = log

	has, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		trans.Close()
		store.Close()
		return nil, err
	}
	f := newFSM(pid)
	r, err := raft.NewRaft(c, f, store, store, snaps, trans)
	if err != nil {
		trans.Close()
		store.Close()
		return nil, err
	}
	if !has { // CONTRACTS §6.1: every voter bootstraps with the identical voter list
		var servers []raft.Server
		for _, v := range t.Voters(pid) {
			servers = append(servers, raft.Server{ID: raft.ServerID(v), Address: raft.ServerAddress(v)})
		}
		r.BootstrapCluster(raft.Configuration{Servers: servers})
	}
	return &group{pid: pid, id: id, raft: r, fsm: f, trans: trans, store: store}, nil
}

func (g *group) leader() string {
	addr, _ := g.raft.LeaderWithID()
	return string(addr)
}

// apply proposes a command and waits until it is committed and applied here (A1).
func (g *group) apply(c command) (produceResult, error) {
	fut := g.raft.Apply(c.encode(), applyTimeout)
	if err := fut.Error(); err != nil {
		return produceResult{}, err
	}
	return fut.Response().(produceResult), nil
}

func (g *group) state() api.GroupState {
	logEnd, lastWrite, _ := g.fsm.stats()
	return api.GroupState{
		ID: g.id, Role: strings.ToLower(g.raft.State().String()), Leader: g.leader(),
		Term: g.raft.CurrentTerm(), CommitIndex: g.raft.CommitIndex(), AppliedIndex: g.raft.AppliedIndex(),
		LogEndOffset: logEnd, LastWriteMS: lastWrite,
	}
}

func (g *group) close() {
	g.raft.Shutdown().Error()
	g.trans.Close()
	g.store.Close()
}

// Package mesh is the broker-to-broker network (docs/CONTRACTS.md §5): one JSON
// header line per TCP stream, dispatch by kind and group, residency checks
// M1–M3, single-relay forwarding, probes, simulated latency and faults.
package mesh

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"hydra/internal/api"
	"hydra/internal/topo"
)

// Stream kinds (CONTRACTS §5.2).
const (
	KindProbe = "probe"
	KindRaft  = "raft"
	KindHTTP  = "http"
)

const (
	maxHeader   = 1024
	pingEvery   = 250 * time.Millisecond
	rowEvery    = time.Second
	minSuspect  = time.Second
	lossPenalty = 200 // ms, CONTRACTS §5.5
	ewmaAlpha   = 0.2
)

var (
	ErrResidency = errors.New("mesh: residency check failed")
	ErrDown      = errors.New("mesh: site down")
)

// Header opens every mesh stream (CONTRACTS §5.1).
type Header struct {
	V     int    `json:"v"`
	Kind  string `json:"kind"`
	Src   string `json:"src"`
	Dst   string `json:"dst"`
	Label string `json:"label"`
	Group string `json:"group"`
	Via   string `json:"via"`
}

// Row is one site's measured view of its links (CONTRACTS §5.4).
type Row struct {
	Site    string
	RTT     map[string]float64
	Suspect []string
}

type link struct {
	rtt      float64 // EWMA ms, -1 before the first pong
	lastPong time.Time
}

// Mesh is one broker's endpoint on the mesh.
type Mesh struct {
	t     *topo.Topology
	self  string
	start time.Time
	ln    net.Listener
	done  chan struct{}

	mu        sync.Mutex
	listeners map[string]*listener // kind + "/" + group
	conns     map[*conn]struct{}
	faults    api.Faults
	links     map[string]*link // peer -> what this site measures
	rows      map[string]Row   // peer -> latest row it sent us

	FirewallBlocked atomic.Int64
	RelayedStreams  atomic.Int64
}

func New(t *topo.Topology, self string) *Mesh {
	m := &Mesh{
		t: t, self: self, start: time.Now(), done: make(chan struct{}),
		listeners: map[string]*listener{}, conns: map[*conn]struct{}{},
		links: map[string]*link{}, rows: map[string]Row{},
	}
	for _, s := range t.AllSites() {
		if s.Name != self {
			m.links[s.Name] = &link{rtt: -1, lastPong: m.start}
		}
	}
	return m
}

// Start listens on the site's mesh address and starts probing every peer.
func (m *Mesh) Start() error {
	ln, err := net.Listen("tcp", m.t.Site(m.self).Mesh)
	if err != nil {
		return err
	}
	m.ln = ln
	go m.acceptLoop()
	for peer := range m.links {
		go m.probe(peer)
	}
	return nil
}

func (m *Mesh) Close() {
	m.mu.Lock()
	select {
	case <-m.done: // already closed
		m.mu.Unlock()
		return
	default:
	}
	close(m.done)
	if m.ln != nil {
		m.ln.Close()
	}
	conns := make([]*conn, 0, len(m.conns))
	for c := range m.conns {
		conns = append(conns, c)
	}
	for _, l := range m.listeners {
		l.Close()
	}
	m.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// Listen returns the listener for streams of one kind and group, e.g. ("raft", "payments.EU.2").
// Its Addr is the site name, which is also the Raft server address.
func (m *Mesh) Listen(kind, group string) net.Listener {
	l := &listener{c: make(chan net.Conn), done: make(chan struct{}), addr: siteAddr(m.self)}
	m.mu.Lock()
	m.listeners[kind+"/"+group] = l
	m.mu.Unlock()
	return l
}

// Dial opens a stream to dst carrying data labelled label (CONTRACTS §5).
// ponytail: direct only until Phase 2.1 picks a relay from the probe rows.
func (m *Mesh) Dial(dst, kind, group, label string, timeout time.Duration) (net.Conn, error) {
	return m.dialVia(dst, "", kind, group, label, timeout)
}

func (m *Mesh) dialVia(dst, via, kind, group, label string, timeout time.Duration) (net.Conn, error) {
	if m.t.Site(dst) == nil || via != "" && m.t.Site(via) == nil {
		return nil, fmt.Errorf("mesh: unknown site %q/%q", dst, via)
	}
	// M1: data only goes to, and through, sites allowed for its label; only probes go unlabelled.
	if label == "" && kind != KindProbe || label != "" && (!m.t.Allowed(label, dst) || via != "" && !m.t.Allowed(label, via)) {
		m.FirewallBlocked.Add(1)
		return nil, ErrResidency
	}
	next := dst
	if via != "" {
		next = via
	}
	return m.dialRaw(next, Header{V: 1, Kind: kind, Src: m.self, Dst: dst, Label: label, Group: group, Via: via}, timeout)
}

func (m *Mesh) dialRaw(next string, h Header, timeout time.Duration) (*conn, error) {
	if m.isDown(m.self) || m.isDown(next) {
		return nil, ErrDown
	}
	raw, err := net.DialTimeout("tcp", m.t.Site(next).Mesh, timeout)
	if err != nil {
		return nil, err
	}
	b, _ := json.Marshal(h)
	if _, err := raw.Write(append(b, '\n')); err != nil {
		raw.Close()
		return nil, err
	}
	return m.wrap(raw, next), nil
}

func (m *Mesh) acceptLoop() {
	for {
		raw, err := m.ln.Accept()
		if err != nil {
			return
		}
		go m.handle(raw)
	}
}

func (m *Mesh) handle(raw net.Conn) {
	raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	h, err := readHeader(raw)
	raw.SetReadDeadline(time.Time{})
	if err != nil || h.V != 1 || m.isDown(m.self) || m.t.Site(h.Src) == nil {
		raw.Close()
		return
	}
	if h.Dst != m.self {
		m.relay(raw, h)
		return
	}
	// M3: accept data only if this site is allowed for its label; only probes go unlabelled.
	if h.Label == "" && h.Kind != KindProbe || h.Label != "" && !m.t.Allowed(h.Label, m.self) {
		m.block(raw)
		return
	}
	peer := h.Src
	if h.Via != "" {
		peer = h.Via
	}
	if m.t.Site(peer) == nil {
		raw.Close()
		return
	}
	c := m.wrap(raw, peer)
	if h.Kind == KindProbe {
		m.serveProbe(c, h.Src)
		return
	}
	m.mu.Lock()
	l := m.listeners[h.Kind+"/"+h.Group]
	m.mu.Unlock()
	if l == nil {
		c.Close()
		return
	}
	l.deliver(c)
}

// relay forwards a stream for which this site is the named relay (M2).
func (m *Mesh) relay(raw net.Conn, h Header) {
	if h.Via != m.self || h.Kind == KindProbe || m.t.Site(h.Dst) == nil {
		raw.Close()
		return
	}
	if !m.t.Allowed(h.Label, m.self) || !m.t.Allowed(h.Label, h.Dst) {
		m.block(raw)
		return
	}
	in := m.wrap(raw, h.Src)
	out, err := m.dialRaw(h.Dst, h, 2*time.Second)
	if err != nil {
		in.Close()
		return
	}
	m.RelayedStreams.Add(1)
	go func() {
		io.Copy(out, in)
		out.Close()
		in.Close()
	}()
	io.Copy(in, out)
	in.Close()
	out.Close()
}

func (m *Mesh) block(c net.Conn) {
	m.FirewallBlocked.Add(1)
	c.Close()
}

func readHeader(c net.Conn) (Header, error) {
	var h Header
	buf := make([]byte, 0, 256)
	one := make([]byte, 1)
	for len(buf) < maxHeader {
		if _, err := c.Read(one); err != nil {
			return h, err
		}
		if one[0] == '\n' {
			return h, json.Unmarshal(buf, &h)
		}
		buf = append(buf, one[0])
	}
	return h, errors.New("mesh: header too long")
}

// --- probes (CONTRACTS §5.4) ---

type probeMsg struct {
	T       string             `json:"t"`
	Seq     int                `json:"seq,omitempty"`
	NS      int64              `json:"ns,omitempty"`
	Site    string             `json:"site,omitempty"`
	RTT     map[string]float64 `json:"rtt,omitempty"`
	Suspect []string           `json:"suspect,omitempty"`
}

func (m *Mesh) probe(peer string) {
	for {
		select {
		case <-m.done:
			return
		default:
		}
		c, err := m.dialRaw(peer, Header{V: 1, Kind: KindProbe, Src: m.self, Dst: peer}, time.Second)
		if err == nil {
			m.runProbe(c, peer)
			c.Close()
		}
		select {
		case <-m.done:
			return
		case <-time.After(pingEvery):
		}
	}
}

func (m *Mesh) runProbe(c *conn, peer string) {
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		sc := bufio.NewScanner(c)
		for sc.Scan() {
			var msg probeMsg
			if json.Unmarshal(sc.Bytes(), &msg) == nil && msg.T == "pong" {
				m.observe(peer, time.Duration(m.now()-msg.NS))
			}
		}
	}()
	enc := json.NewEncoder(c)
	ping, row := time.NewTicker(pingEvery), time.NewTicker(rowEvery)
	defer ping.Stop()
	defer row.Stop()
	for seq := 1; ; seq++ {
		var err error
		select {
		case <-ping.C:
			err = enc.Encode(probeMsg{T: "ping", Seq: seq, NS: m.now()})
		case <-row.C:
			r := m.ownRow()
			err = enc.Encode(probeMsg{T: "row", Site: m.self, RTT: r.RTT, Suspect: r.Suspect})
		case <-ended:
			return
		case <-m.done:
			return
		}
		if err != nil {
			return
		}
	}
}

func (m *Mesh) serveProbe(c *conn, src string) {
	defer c.Close()
	sc := bufio.NewScanner(c)
	enc := json.NewEncoder(c)
	for sc.Scan() {
		var msg probeMsg
		if json.Unmarshal(sc.Bytes(), &msg) != nil {
			return
		}
		switch msg.T {
		case "ping":
			msg.T = "pong"
			if enc.Encode(msg) != nil {
				return
			}
		case "row":
			m.mu.Lock()
			m.rows[src] = Row{Site: src, RTT: msg.RTT, Suspect: msg.Suspect}
			m.mu.Unlock()
		}
	}
}

func (m *Mesh) now() int64 { return int64(time.Since(m.start)) }

func (m *Mesh) observe(peer string, d time.Duration) {
	ms := float64(d.Microseconds()) / 1000
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.links[peer]
	if l.rtt < 0 {
		l.rtt = ms
	} else {
		l.rtt = (1-ewmaAlpha)*l.rtt + ewmaAlpha*ms
	}
	l.lastPong = time.Now()
}

func suspect(l *link) bool {
	window := max(minSuspect, time.Duration(4*l.rtt*float64(time.Millisecond)))
	return time.Since(l.lastPong) > window
}

// Links reports what this site measures to every peer (for GET /v1/state).
func (m *Mesh) Links() map[string]api.LinkState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]api.LinkState, len(m.links))
	for p, l := range m.links {
		out[p] = api.LinkState{RTTms: math.Round(l.rtt*10) / 10, Suspect: suspect(l)}
	}
	return out
}

func (m *Mesh) ownRow() Row {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := Row{Site: m.self, RTT: map[string]float64{}}
	for p, l := range m.links {
		if suspect(l) {
			r.Suspect = append(r.Suspect, p)
		} else if l.rtt >= 0 {
			r.RTT[p] = math.Round(l.rtt*10) / 10
		}
	}
	return r
}

// Rows returns the latest row each peer sent (Phase 2 routing input).
func (m *Mesh) Rows() map[string]Row {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Row, len(m.rows))
	for k, v := range m.rows {
		out[k] = v
	}
	return out
}

// --- faults (CONTRACTS §5.5, §8) ---

// SetFaults replaces the fault state and cuts connections that a down site must lose.
func (m *Mesh) SetFaults(f api.Faults) {
	m.mu.Lock()
	m.faults = f
	selfDown := slices.Contains(f.Down, m.self)
	var cut []*conn
	for c := range m.conns {
		if selfDown || slices.Contains(f.Down, c.peer) {
			cut = append(cut, c)
		}
	}
	m.mu.Unlock()
	for _, c := range cut {
		c.Close()
	}
}

func (m *Mesh) Faults() api.Faults {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.faults
}

// Down reports whether this site is down.
func (m *Mesh) Down() bool { return m.isDown(m.self) }

func (m *Mesh) isDown(site string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Contains(m.faults.Down, site)
}

// linkDelay is the simulated one-way delay from peer to this site; false if either is down.
func (m *Mesh) linkDelay(peer string) (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slices.Contains(m.faults.Down, m.self) || slices.Contains(m.faults.Down, peer) {
		return 0, false
	}
	ms := m.t.RTT(m.self, peer) / 2
	for _, lf := range m.faults.Links {
		if lf.A == m.self && lf.B == peer || lf.B == m.self && lf.A == peer {
			ms += lf.DelayMS
			if lf.LossPct > 0 && rand.Float64()*100 < lf.LossPct {
				ms += lossPenalty
			}
		}
	}
	return time.Duration(ms * float64(time.Millisecond)), true
}

// --- listener: a net.Listener fed by the mesh dispatcher ---

type siteAddr string

func (a siteAddr) Network() string { return "hydra-mesh" }
func (a siteAddr) String() string  { return string(a) }

type listener struct {
	c    chan net.Conn
	done chan struct{}
	once sync.Once
	addr siteAddr
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.c:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *listener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *listener) Addr() net.Addr { return l.addr }

func (l *listener) deliver(c net.Conn) {
	select {
	case l.c <- c:
	case <-l.done:
		c.Close()
	case <-time.After(5 * time.Second):
		c.Close()
	}
}

// --- conn: simulated link delay on the receiving side of each hop ---

type chunk struct {
	b   []byte
	at  time.Time
	err error
}

// conn delays every chunk it receives by the link's simulated one-way delay.
// Writes go straight to TCP, so a sender's Close never drops data in flight.
type conn struct {
	net.Conn
	m    *Mesh
	peer string // immediate TCP peer
	ch   chan chunk
	done chan struct{}
	once sync.Once

	// reader state: one reader at a time, as with any net.Conn
	cur     []byte
	err     error
	pending *chunk

	dmu      sync.Mutex
	deadline time.Time
}

func (m *Mesh) wrap(raw net.Conn, peer string) *conn {
	c := &conn{Conn: raw, m: m, peer: peer, ch: make(chan chunk, 64), done: make(chan struct{})}
	m.mu.Lock()
	m.conns[c] = struct{}{}
	m.mu.Unlock()
	go c.fill()
	return c
}

func (c *conn) fill() {
	var last time.Time
	for {
		buf := make([]byte, 32<<10)
		n, err := c.Conn.Read(buf)
		d, ok := c.m.linkDelay(c.peer)
		if !ok {
			err = ErrDown
		}
		at := time.Now().Add(d)
		if at.Before(last) {
			at = last // keep order when a fault shortens the delay
		}
		last = at
		select {
		case c.ch <- chunk{buf[:n], at, err}:
		case <-c.done:
			return
		}
		if err != nil {
			return
		}
	}
}

func (c *conn) Read(p []byte) (int, error) {
	if len(c.cur) == 0 && c.err == nil {
		var timeout <-chan time.Time
		if dl := c.readDeadline(); !dl.IsZero() {
			t := time.NewTimer(time.Until(dl))
			defer t.Stop()
			timeout = t.C
		}
		for len(c.cur) == 0 && c.err == nil {
			var ch chunk
			if c.pending != nil {
				ch, c.pending = *c.pending, nil
			} else {
				select {
				case ch = <-c.ch:
				case <-c.done:
					return 0, net.ErrClosed
				case <-timeout:
					return 0, os.ErrDeadlineExceeded
				}
			}
			if wait := time.Until(ch.at); wait > 0 {
				t := time.NewTimer(wait)
				select {
				case <-t.C:
				case <-c.done:
					t.Stop()
					return 0, net.ErrClosed
				case <-timeout:
					t.Stop()
					c.pending = &ch
					return 0, os.ErrDeadlineExceeded
				}
			}
			c.cur, c.err = ch.b, ch.err
		}
	}
	if len(c.cur) == 0 {
		return 0, c.err
	}
	n := copy(p, c.cur)
	c.cur = c.cur[n:]
	return n, nil
}

func (c *conn) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.m.mu.Lock()
		delete(c.m.conns, c)
		c.m.mu.Unlock()
		c.Conn.Close()
	})
	return nil
}

// Read deadlines apply to the delayed stream, not the raw socket.
func (c *conn) SetReadDeadline(t time.Time) error {
	c.dmu.Lock()
	c.deadline = t
	c.dmu.Unlock()
	return nil
}

func (c *conn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.Conn.SetWriteDeadline(t)
}

func (c *conn) readDeadline() time.Time {
	c.dmu.Lock()
	defer c.dmu.Unlock()
	return c.deadline
}

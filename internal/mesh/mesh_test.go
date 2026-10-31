package mesh

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"hydra/internal/api"
	"hydra/internal/testkit"
	"hydra/internal/topo"
)

func startAll(t *testing.T, tp *topo.Topology) map[string]*Mesh {
	ms := map[string]*Mesh{}
	for _, s := range tp.AllSites() {
		m := New(tp, s.Name)
		if err := m.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		ms[s.Name] = m
	}
	return ms
}

func echo(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() { io.Copy(c, c); c.Close() }()
	}
}

func roundTrip(t *testing.T, c net.Conn, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if line, err := bufio.NewReader(c).ReadString('\n'); err != nil || line != "hello\n" {
		t.Fatalf("echo: %q %v", line, err)
	}
}

// rawHeader opens a stream that skips the sender-side check M1, as a buggy or hostile sender would.
func rawHeader(t *testing.T, addr string, h Header) net.Conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(h)
	c.Write(append(b, '\n'))
	return c
}

func closedByPeer(c net.Conn) bool {
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := c.Read(make([]byte, 1))
	return errors.Is(err, io.EOF)
}

func TestStreamsAndResidencyChecks(t *testing.T) {
	tp := testkit.Topology(t, `{"EU/EU":4,"US/US":4,"EU/US":20}`, "",
		testkit.Juris{Name: "EU", Sites: []string{"eu-a", "eu-b", "eu-c"}}, testkit.Juris{Name: "US", Sites: []string{"us-a"}})
	ms := startAll(t, tp)
	go echo(ms["eu-b"].Listen(KindHTTP, ""))
	go echo(ms["us-a"].Listen(KindHTTP, ""))
	a := ms["eu-a"]
	rt := func(c net.Conn, err error) { t.Helper(); roundTrip(t, c, err) }

	rt(a.Dial("eu-b", KindHTTP, "", "EU", time.Second))
	rt(a.dialVia("eu-b", "eu-c", KindHTTP, "", "EU", time.Second))
	if got := ms["eu-c"].RelayedStreams.Load(); got != 1 {
		t.Errorf("eu-c relayed %d streams, want 1", got)
	}
	rt(a.Dial("us-a", KindHTTP, "", "US", time.Second)) // US data into the US is fine

	// M1: the sender refuses EU data to or through a US site, and unlabelled data.
	if _, err := a.Dial("us-a", KindHTTP, "", "EU", time.Second); !errors.Is(err, ErrResidency) {
		t.Errorf("EU data to us-a: err=%v, want ErrResidency", err)
	}
	if _, err := a.dialVia("eu-b", "us-a", KindHTTP, "", "EU", time.Second); !errors.Is(err, ErrResidency) {
		t.Errorf("EU data via us-a: err=%v, want ErrResidency", err)
	}
	if _, err := a.Dial("eu-b", KindHTTP, "", "", time.Second); !errors.Is(err, ErrResidency) {
		t.Errorf("unlabelled data: err=%v, want ErrResidency", err)
	}
	if got := a.FirewallBlocked.Load(); got != 3 {
		t.Errorf("eu-a firewall_blocked = %d, want 3", got)
	}

	// M2: a relay refuses to carry EU data to a US site even if the sender skipped M1.
	c := rawHeader(t, tp.Site("eu-c").Mesh, Header{V: 1, Kind: KindHTTP, Src: "eu-a", Dst: "us-a", Label: "EU", Via: "eu-c"})
	if !closedByPeer(c) {
		t.Error("relay did not close a cross-border stream")
	}
	testkit.Eventually(t, "eu-c to count the refusal", 5*time.Second, func() bool { return ms["eu-c"].FirewallBlocked.Load() == 1 })

	// M3: a US site refuses EU data addressed to it.
	c = rawHeader(t, tp.Site("us-a").Mesh, Header{V: 1, Kind: KindHTTP, Src: "eu-a", Dst: "us-a", Label: "EU"})
	if !closedByPeer(c) {
		t.Error("us-a accepted EU data")
	}
	testkit.Eventually(t, "us-a to count the refusal", 5*time.Second, func() bool { return ms["us-a"].FirewallBlocked.Load() == 1 })
}

func TestProbesFaultsAndRecovery(t *testing.T) {
	tp := testkit.Topology(t, `{"EU/EU":20}`, "", testkit.Juris{Name: "EU", Sites: []string{"eu-a", "eu-b"}})
	ms := startAll(t, tp)
	a := ms["eu-a"]
	setAll := func(f api.Faults) {
		for _, m := range ms {
			m.SetFaults(f)
		}
	}

	testkit.Eventually(t, "baseline RTT near 20 ms", 5*time.Second, func() bool { r := a.Links()["eu-b"].RTTms; return r >= 18 && r <= 40 })
	testkit.Eventually(t, "a row from eu-b", 5*time.Second, func() bool { return a.Rows()["eu-b"].RTT["eu-a"] > 0 })

	setAll(api.Faults{Links: []api.LinkFault{{A: "eu-a", B: "eu-b", DelayMS: 50}}})
	testkit.Eventually(t, "RTT to rise by 2 x 50 ms", 5*time.Second, func() bool { return a.Links()["eu-b"].RTTms > 90 })

	setAll(api.Faults{Down: []string{"eu-b"}})
	testkit.Eventually(t, "eu-b to turn suspect", 5*time.Second, func() bool { return a.Links()["eu-b"].Suspect })
	if _, err := a.Dial("eu-b", KindHTTP, "", "EU", time.Second); !errors.Is(err, ErrDown) {
		t.Errorf("dial to a down site: err=%v, want ErrDown", err)
	}

	setAll(api.Faults{})
	testkit.Eventually(t, "eu-b to recover", 5*time.Second, func() bool { return !a.Links()["eu-b"].Suspect })
}

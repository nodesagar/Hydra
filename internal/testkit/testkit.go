// Package testkit builds throwaway topologies on free localhost ports for tests.
package testkit

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"hydra/internal/topo"
)

// Juris is one jurisdiction and its site names.
type Juris struct {
	Name  string
	Sites []string
}

// Topology returns a valid topology whose sites listen on free 127.0.0.1 ports.
// rtt is the rtt_ms JSON object, e.g. `{"EU/EU":4}`; topics is the topics JSON array or "".
func Topology(t testing.TB, rtt, topics string, js ...Juris) *topo.Topology {
	t.Helper()
	var parts []string
	for _, j := range js {
		var sites []string
		for _, s := range j.Sites {
			sites = append(sites, fmt.Sprintf(`{"name":%q,"api":%q,"mesh":%q}`, s, freeAddr(t), freeAddr(t)))
		}
		parts = append(parts, fmt.Sprintf(`{"name":%q,"sites":[%s]}`, j.Name, strings.Join(sites, ",")))
	}
	if topics == "" {
		topics = "[]"
	}
	doc := fmt.Sprintf(`{"jurisdictions":[%s],"rtt_ms":%s,"topics":%s}`, strings.Join(parts, ","), rtt, topics)
	tp, err := topo.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return tp
}

func freeAddr(t testing.TB) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// Eventually polls cond every 50 ms until it holds or timeout passes.
func Eventually(t testing.TB, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); !cond(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
	}
}

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"hydra/internal/api"
	"hydra/internal/broker"
	"hydra/internal/testkit"
	"hydra/internal/topo"
)

func startEU(t *testing.T) (*topo.Topology, map[string]*broker.Broker) {
	tp := testkit.Topology(t, `{"EU/EU":4,"US/US":4,"EU/US":30}`, `[{"name":"payments","partitions":2}]`,
		testkit.Juris{Name: "EU", Sites: []string{"eu-a", "eu-b", "eu-c"}}, testkit.Juris{Name: "US", Sites: []string{"us-a"}})
	dir := t.TempDir()
	bs := map[string]*broker.Broker{}
	for _, s := range tp.Sites("EU") {
		b, err := broker.New(tp, s, dir, hclog.NewNullLogger())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		bs[s] = b
	}
	return tp, bs
}

func leaderOf(t *testing.T, tp *topo.Topology, site, id string) string {
	resp, err := http.Get("http://" + tp.Site(site).API + "/v1/metadata")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var md api.MetadataResponse
	json.NewDecoder(resp.Body).Decode(&md)
	for _, p := range md.Partitions {
		if p.ID == id {
			return p.Leader
		}
	}
	return ""
}

func TestProduceRidesThroughFailoverExactlyOnce(t *testing.T) {
	tp, bs := startEU(t)
	c, err := New(tp, "eu-b")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	produce := func(from, to int) {
		for i := from; i < to; i++ {
			if _, err := c.Produce(ctx, "payments", "EU", []byte(fmt.Sprintf("user-%d", i)), []byte(fmt.Sprint(i))); err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
		}
	}

	produce(0, 20)
	var victim string
	testkit.Eventually(t, "a leader for payments.EU.0", 5*time.Second, func() bool {
		victim = leaderOf(t, tp, "eu-c", "payments.EU.0")
		return victim != ""
	})
	bs[victim].Close()
	produce(20, 40) // the SDK must hide the failover: no error reaches the application

	// Exactly once: 40 acknowledged records, 40 stored, no duplicates, contiguous offsets.
	testkit.Eventually(t, "all 40 records readable", 10*time.Second, func() bool {
		seen := map[string]bool{}
		for p := range 2 {
			fr, err := c.Fetch(ctx, "payments", "EU", p, 0, 1000)
			if err != nil {
				return false
			}
			for i, r := range fr.Records {
				if r.Offset != int64(i) || seen[string(r.Value)] {
					t.Fatalf("partition %d: offset %d at position %d, duplicate=%v", p, r.Offset, i, seen[string(r.Value)])
				}
				seen[string(r.Value)] = true
			}
		}
		return len(seen) == 40
	})

	// Commit and read back a consumer offset.
	if err := c.Commit(ctx, "billing", "payments", "EU", 0, 5); err != nil {
		t.Fatal(err)
	}
	testkit.Eventually(t, "committed offset 5", 5*time.Second, func() bool {
		o, err := c.Committed(ctx, "billing", "payments", "EU", 0)
		return err == nil && o == 5
	})

	// R4 fails closed in the SDK: a US client cannot even ask for EU data.
	us, _ := New(tp, "us-a")
	var e *api.Error
	if _, err := us.Fetch(ctx, "payments", "EU", 0, 0, 10); !errors.As(err, &e) || e.Code != api.ErrResidencyDenied {
		t.Errorf("EU read from us-a: %v", err)
	}
}

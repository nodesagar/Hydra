package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"hydra/internal/api"
	"hydra/internal/testkit"
	"hydra/internal/topo"
)

// cluster starts one broker per EU site. The US site exists only as a foreign place for residency checks.
func cluster(t *testing.T) (*topo.Topology, map[string]*Broker) {
	tp := testkit.Topology(t, `{"EU/EU":4,"US/US":4,"EU/US":30}`, `[{"name":"payments","partitions":2}]`,
		testkit.Juris{Name: "EU", Sites: []string{"eu-a", "eu-b", "eu-c"}}, testkit.Juris{Name: "US", Sites: []string{"us-a"}})
	dir := t.TempDir()
	bs := map[string]*Broker{}
	for _, s := range tp.Sites("EU") {
		b, err := New(tp, s, dir, hclog.NewNullLogger())
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

// call posts body to site's edge API as a client located at clientSite; 0 means a network error.
func call(t *testing.T, tp *topo.Topology, site, clientSite, path string, body, out any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, "http://"+tp.Site(site).API+path, bytes.NewReader(b))
	req.Header.Set(api.HeaderClientSite, clientSite)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if out != nil {
		json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func TestClusterProduceFetchCommitFailover(t *testing.T) {
	tp, bs := cluster(t)
	const pid = "payments.EU.0"
	testkit.Eventually(t, "a leader for "+pid, 5*time.Second, func() bool { return bs["eu-a"].groups[pid].leader() != "" })
	leader := bs["eu-a"].groups[pid].leader()
	var others []string
	for _, s := range tp.Sites("EU") {
		if s != leader {
			others = append(others, s)
		}
	}
	follower, third := others[0], others[1]

	produce := api.ProduceRequest{Topic: "payments", Label: "EU", Partition: 0, ProducerID: "p1", BaseSeq: 0,
		Records: []api.Record{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}}}
	var pr api.ProduceResponse
	if code := call(t, tp, follower, follower, "/v1/produce", produce, &pr); code != 200 || pr.BaseOffset != 0 || pr.Leader != leader {
		t.Fatalf("produce via a follower: code=%d %+v", code, pr)
	}
	// A retried batch is a duplicate carrying its original offset (CONTRACTS §6.3).
	if code := call(t, tp, third, third, "/v1/produce", produce, &pr); code != 200 || !pr.Duplicate || pr.BaseOffset != 0 {
		t.Fatalf("retry: code=%d %+v", code, pr)
	}

	// Any replica serves committed records.
	fetch := api.FetchRequest{Topic: "payments", Label: "EU", Partition: 0}
	var fr api.FetchResponse
	testkit.Eventually(t, "the third replica to serve 3 records", 3*time.Second, func() bool {
		return call(t, tp, third, third, "/v1/fetch", fetch, &fr) == 200 && fr.HighWatermark == 3 && len(fr.Records) == 3
	})
	if fr.ServedBy != third || string(fr.Records[2].Value) != "c" {
		t.Fatalf("fetch: %+v", fr)
	}

	// Consumer offsets go through Raft and can be read anywhere in the jurisdiction.
	commit := api.CommitRequest{Group: "billing", Topic: "payments", Label: "EU", Partition: 0, Offset: 3}
	if code := call(t, tp, follower, follower, "/v1/commit", commit, nil); code != 200 {
		t.Fatalf("commit: code=%d", code)
	}
	var or api.OffsetsResponse
	testkit.Eventually(t, "the committed offset on the third replica", 3*time.Second, func() bool {
		req := api.OffsetsRequest{Group: "billing", Topic: "payments", Label: "EU", Partition: 0}
		return call(t, tp, third, third, "/v1/offsets", req, &or) == 200 && or.Offset == 3
	})

	// R3: an EU broker never takes US data. R4: EU data is never served to a US client.
	var e api.Error
	usData := api.ProduceRequest{Topic: "payments", Label: "US", Partition: 0, Records: produce.Records}
	if code := call(t, tp, follower, follower, "/v1/produce", usData, &e); code != 403 || e.Code != api.ErrResidencyDenied {
		t.Errorf("US data to an EU broker: code=%d %+v", code, e)
	}
	if code := call(t, tp, follower, "us-a", "/v1/fetch", fetch, &e); code != 403 || e.Code != api.ErrResidencyDenied {
		t.Errorf("EU data read from us-a: code=%d %+v", code, e)
	}

	// Kill the leader's broker; writes resume through a survivor (A3: within 3 s).
	bs[leader].Close()
	killed := time.Now()
	next := api.ProduceRequest{Topic: "payments", Label: "EU", Partition: 0, ProducerID: "p1", BaseSeq: 3,
		Records: []api.Record{{Value: []byte("d")}}}
	testkit.Eventually(t, "writes to resume after the leader died", 10*time.Second, func() bool {
		return call(t, tp, follower, follower, "/v1/produce", next, &pr) == 200
	})
	pause := time.Since(killed)
	t.Logf("write pause after leader kill: %v (new leader %s)", pause.Round(time.Millisecond), pr.Leader)
	if pr.BaseOffset != 3 || pr.Leader == leader {
		t.Fatalf("after failover: %+v", pr)
	}
	if pause > 3*time.Second {
		t.Errorf("pause %v exceeds the 3 s target (A3)", pause)
	}
}

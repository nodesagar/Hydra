// Package sdk is the Hydra client (docs/CONTRACTS.md §4). It only ever talks to
// brokers of a record's own jurisdiction, keeps one batch in flight per
// partition so retries stay idempotent, and retries UNAVAILABLE with backoff.
package sdk

import (
	"bytes"
	"cmp"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"hydra/internal/api"
	"hydra/internal/topo"
)

const (
	requestTimeout = 2 * time.Second // CONTRACTS §6.1
	backoffStart   = 50 * time.Millisecond
	backoffCap     = time.Second
)

// Client acts for one application running at one site.
type Client struct {
	t          *topo.Topology
	site       string
	producerID string
	http       *http.Client

	mu   sync.Mutex
	next map[string]int64       // partition id -> next base sequence
	lock map[string]*sync.Mutex // partition id -> one batch in flight
}

// New returns a client located at site (sent as Hydra-Client-Site).
func New(t *topo.Topology, site string) (*Client, error) {
	if t.Site(site) == nil {
		return nil, fmt.Errorf("sdk: unknown site %q", site)
	}
	id := make([]byte, 8)
	crand.Read(id)
	return &Client{
		t: t, site: site, producerID: hex.EncodeToString(id),
		http: &http.Client{Timeout: requestTimeout, Transport: &http.Transport{MaxIdleConnsPerHost: 16}},
		next: map[string]int64{}, lock: map[string]*sync.Mutex{},
	}, nil
}

// Partition picks the partition for a key inside the label's jurisdiction: FNV-1a of the key, random if no key.
func (c *Client) Partition(topic string, key []byte) (int, error) {
	tp := c.t.Topic(topic)
	if tp == nil {
		return 0, fmt.Errorf("sdk: unknown topic %q", topic)
	}
	if len(key) == 0 {
		return rand.IntN(tp.Partitions), nil
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(tp.Partitions)), nil
}

// Produce writes one record; the key picks the partition.
func (c *Client) Produce(ctx context.Context, topic, label string, key, value []byte) (api.ProduceResponse, error) {
	p, err := c.Partition(topic, key)
	if err != nil {
		return api.ProduceResponse{}, err
	}
	return c.Send(ctx, topic, label, p, []api.Record{{Key: key, Value: value}})
}

// Send writes a batch to one partition and retries until ctx ends. Retries reuse the
// batch's sequence number, so the broker stores it once (CONTRACTS §6.3). After an
// error the outcome is unknown: the batch may or may not have been stored.
func (c *Client) Send(ctx context.Context, topic, label string, partition int, recs []api.Record) (api.ProduceResponse, error) {
	id := topo.PartitionID{Topic: topic, Jurisdiction: label, Index: partition}.String()
	c.mu.Lock()
	l := c.lock[id]
	if l == nil {
		l = &sync.Mutex{}
		c.lock[id] = l
	}
	c.mu.Unlock()

	l.Lock()
	defer l.Unlock()
	c.mu.Lock()
	seq := c.next[id]
	c.next[id] = seq + int64(len(recs)) // advance even on failure, so a new batch never reuses this sequence
	c.mu.Unlock()

	req := api.ProduceRequest{Topic: topic, Label: label, Partition: partition, ProducerID: c.producerID, BaseSeq: seq, Records: recs}
	var resp api.ProduceResponse
	err := c.do(ctx, label, "/v1/produce", req, &resp)
	return resp, err
}

// Fetch reads committed records from the nearest replica of the label's jurisdiction.
func (c *Client) Fetch(ctx context.Context, topic, label string, partition int, offset int64, max int) (api.FetchResponse, error) {
	var resp api.FetchResponse
	if err := c.readCheck(label); err != nil {
		return resp, err
	}
	req := api.FetchRequest{Topic: topic, Label: label, Partition: partition, Offset: offset, Max: max}
	return resp, c.do(ctx, label, "/v1/fetch", req, &resp)
}

// Commit stores a consumer group's next offset for a partition.
func (c *Client) Commit(ctx context.Context, group, topic, label string, partition int, offset int64) error {
	if err := c.readCheck(label); err != nil {
		return err
	}
	req := api.CommitRequest{Group: group, Topic: topic, Label: label, Partition: partition, Offset: offset}
	return c.do(ctx, label, "/v1/commit", req, nil)
}

// Committed returns a group's committed offset for a partition, -1 if none.
func (c *Client) Committed(ctx context.Context, group, topic, label string, partition int) (int64, error) {
	var resp api.OffsetsResponse
	if err := c.readCheck(label); err != nil {
		return 0, err
	}
	req := api.OffsetsRequest{Group: group, Topic: topic, Label: label, Partition: partition}
	return resp.Offset, c.do(ctx, label, "/v1/offsets", req, &resp)
}

// readCheck fails closed before any network call when this client may not read the label (R4).
func (c *Client) readCheck(label string) error {
	if !c.t.Allowed(label, c.site) {
		return &api.Error{Code: api.ErrResidencyDenied, Message: fmt.Sprintf("%s data may not be read from %s", label, c.site)}
	}
	return nil
}

// brokers lists the label's sites, nearest to this client first. The SDK never contacts any other site.
func (c *Client) brokers(label string) []string {
	sites := slices.Clone(c.t.Sites(label))
	slices.SortStableFunc(sites, func(a, b string) int {
		return cmp.Compare(c.t.RTT(c.site, a), c.t.RTT(c.site, b))
	})
	return sites
}

// do posts to the label's brokers, nearest first, rotating on failure, with jittered
// exponential backoff on network errors and UNAVAILABLE, until ctx ends.
func (c *Client) do(ctx context.Context, label, path string, body, out any) error {
	sites := c.brokers(label)
	if len(sites) == 0 {
		return &api.Error{Code: api.ErrBadRequest, Message: fmt.Sprintf("unknown label %q", label)}
	}
	payload, _ := json.Marshal(body)
	backoff := backoffStart
	for attempt := 0; ; attempt++ {
		site := sites[attempt%len(sites)]
		err := c.post(ctx, site, path, payload, out)
		var apiErr *api.Error
		if err == nil || errors.As(err, &apiErr) && !api.Retriable(apiErr.Code) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("sdk: %s: %w (last error: %v)", path, ctx.Err(), err)
		case <-time.After(backoff/2 + rand.N(backoff/2+1)):
		}
		backoff = min(2*backoff, backoffCap)
	}
}

func (c *Client) post(ctx context.Context, site, path string, payload []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+c.t.Site(site).API+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set(api.HeaderClientSite, c.site)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		apiErr := &api.Error{}
		if json.NewDecoder(resp.Body).Decode(apiErr) != nil || apiErr.Code == "" {
			apiErr = &api.Error{Code: api.ErrUnavailable, Message: resp.Status}
		}
		return apiErr
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

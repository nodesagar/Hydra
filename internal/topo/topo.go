// Package topo loads the deployment topology (docs/CONTRACTS.md §2) and owns
// the residency predicate: every residency check in Hydra calls Allowed.
package topo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Site is one failure domain inside a jurisdiction. MVP: one broker per site.
type Site struct {
	Name string `json:"name"`
	City string `json:"city"`
	API  string `json:"api"`  // edge HTTP API, host:port
	Mesh string `json:"mesh"` // broker-to-broker mesh, host:port

	Jurisdiction string `json:"-"` // set by Parse
}

// Jurisdiction is a legal boundary; its name is the residency label.
type Jurisdiction struct {
	Name  string  `json:"name"`
	Sites []*Site `json:"sites"`
}

// Topic is geo-partitioned: Partitions is the count per jurisdiction.
type Topic struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

type Topology struct {
	Jurisdictions []*Jurisdiction    `json:"jurisdictions"`
	RTTms         map[string]float64 `json:"rtt_ms"` // simulated round trips, CONTRACTS §2.4
	Topics        []*Topic           `json:"topics"`
	Observer      string             `json:"observer"`

	sites  map[string]*Site
	juris  map[string]*Jurisdiction
	topics map[string]*Topic
}

// PartitionID names one partition (CONTRACTS §1), e.g. payments.EU.2.
type PartitionID struct {
	Topic        string
	Jurisdiction string
	Index        int
}

func (p PartitionID) String() string {
	return fmt.Sprintf("%s.%s.%d", p.Topic, p.Jurisdiction, p.Index)
}

var (
	reJuris = regexp.MustCompile(`^[A-Z]{2,8}$`)
	reSite  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	reTopic = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)
)

func Load(path string) (*Topology, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	t, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

// Parse decodes and validates a topology (rules CONTRACTS §2.1–2.4).
func Parse(b []byte) (*Topology, error) {
	var t Topology
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&t); err != nil {
		return nil, err
	}
	if err := t.index(); err != nil {
		return nil, err
	}
	return &t, nil
}

func (t *Topology) index() error {
	t.sites, t.juris, t.topics = map[string]*Site{}, map[string]*Jurisdiction{}, map[string]*Topic{}
	addrs := map[string]bool{}
	for _, j := range t.Jurisdictions {
		switch {
		case !reJuris.MatchString(j.Name):
			return fmt.Errorf("jurisdiction %q: name must match %s", j.Name, reJuris)
		case t.juris[j.Name] != nil:
			return fmt.Errorf("jurisdiction %q: duplicate", j.Name)
		case len(j.Sites) == 0:
			return fmt.Errorf("jurisdiction %q: no sites", j.Name)
		}
		t.juris[j.Name] = j
		for _, s := range j.Sites {
			switch {
			case !reSite.MatchString(s.Name):
				return fmt.Errorf("site %q: name must match %s", s.Name, reSite)
			case t.sites[s.Name] != nil:
				return fmt.Errorf("site %q: duplicate", s.Name)
			case s.API == "" || s.Mesh == "":
				return fmt.Errorf("site %q: api and mesh addresses are required", s.Name)
			}
			for _, a := range []string{s.API, s.Mesh} {
				if addrs[a] {
					return fmt.Errorf("site %q: address %s already used", s.Name, a)
				}
				addrs[a] = true
			}
			s.Jurisdiction = j.Name
			t.sites[s.Name] = s
		}
	}
	for _, tp := range t.Topics {
		switch {
		case !reTopic.MatchString(tp.Name):
			return fmt.Errorf("topic %q: name must match %s", tp.Name, reTopic)
		case t.topics[tp.Name] != nil:
			return fmt.Errorf("topic %q: duplicate", tp.Name)
		case tp.Partitions < 1 || tp.Partitions > 64:
			return fmt.Errorf("topic %q: partitions must be 1..64", tp.Name)
		}
		t.topics[tp.Name] = tp
	}
	for k, v := range t.RTTms {
		a, b, ok := strings.Cut(k, "/")
		if !ok || v <= 0 || !t.known(a) || !t.known(b) {
			return fmt.Errorf("rtt_ms %q: want <site|jurisdiction>/<site|jurisdiction> with a positive value", k)
		}
	}
	for a := range t.sites {
		for b := range t.sites {
			if a < b && t.RTT(a, b) == 0 {
				return fmt.Errorf("rtt_ms: no value for %s/%s or for their jurisdictions", a, b)
			}
		}
	}
	return nil
}

func (t *Topology) known(name string) bool { return t.sites[name] != nil || t.juris[name] != nil }

// Allowed is THE residency predicate (CONTRACTS §2.6): data labelled label may
// touch site only if site belongs to that jurisdiction. Unknown site: false.
func (t *Topology) Allowed(label, site string) bool {
	s := t.sites[site]
	return s != nil && s.Jurisdiction == label
}

// Site returns the named site, or nil.
func (t *Topology) Site(name string) *Site { return t.sites[name] }

// Topic returns the named topic, or nil.
func (t *Topology) Topic(name string) *Topic { return t.topics[name] }

// AllSites lists every site in file order.
func (t *Topology) AllSites() []*Site {
	var out []*Site
	for _, j := range t.Jurisdictions {
		out = append(out, j.Sites...)
	}
	return out
}

// Sites lists the site names of jurisdiction j in file order (nil if unknown).
func (t *Topology) Sites(j string) []string {
	jj := t.juris[j]
	if jj == nil {
		return nil
	}
	out := make([]string, len(jj.Sites))
	for i, s := range jj.Sites {
		out[i] = s.Name
	}
	return out
}

// Partitions lists every partition homed in jurisdiction j: topic order, then index.
func (t *Topology) Partitions(j string) []PartitionID {
	if t.juris[j] == nil {
		return nil
	}
	var out []PartitionID
	for _, tp := range t.Topics {
		for i := 0; i < tp.Partitions; i++ {
			out = append(out, PartitionID{tp.Name, j, i})
		}
	}
	return out
}

// Voters is the placement rule (CONTRACTS §2.5): every site of the home jurisdiction.
// ponytail: no replica-count knob; add one with rotation when a jurisdiction has more sites than replicas.
func (t *Topology) Voters(p PartitionID) []string { return t.Sites(p.Jurisdiction) }

// RTT is the simulated round trip between two known sites in ms (CONTRACTS §2.4).
// Same site: 0. Unknown site: 0, so callers validate names at trust boundaries.
func (t *Topology) RTT(a, b string) float64 {
	sa, sb := t.sites[a], t.sites[b]
	if a == b || sa == nil || sb == nil {
		return 0
	}
	if v, ok := t.lookup(a, b); ok {
		return v
	}
	v, _ := t.lookup(sa.Jurisdiction, sb.Jurisdiction)
	return v
}

func (t *Topology) lookup(a, b string) (float64, bool) {
	if v, ok := t.RTTms[a+"/"+b]; ok {
		return v, true
	}
	v, ok := t.RTTms[b+"/"+a]
	return v, ok
}

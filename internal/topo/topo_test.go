package topo

import (
	"slices"
	"testing"
)

func TestDemoTopology(t *testing.T) {
	tp, err := Load("../../topology.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := tp.RTT("eu-par", "eu-fra"); got != 10 {
		t.Errorf("RTT eu-par/eu-fra = %v, want 10 (reversed key)", got)
	}
	if got := tp.RTT("eu-fra", "in-mum"); got != 120 {
		t.Errorf("RTT eu-fra/in-mum = %v, want 120 (jurisdiction fallback)", got)
	}
	if got := tp.RTT("eu-fra", "eu-fra"); got != 0 {
		t.Errorf("RTT same site = %v, want 0", got)
	}
	if !tp.Allowed("EU", "eu-ams") || tp.Allowed("EU", "us-east") || tp.Allowed("EU", "nowhere") {
		t.Error("Allowed: EU data must be allowed on eu-ams only, never on us-east or unknown sites")
	}
	ps := tp.Partitions("EU")
	if len(ps) != 4 || ps[2].String() != "payments.EU.2" {
		t.Fatalf("Partitions(EU) = %v", ps)
	}
	if v := tp.Voters(ps[2]); !slices.Equal(v, []string{"eu-fra", "eu-par", "eu-ams"}) {
		t.Errorf("Voters = %v", v)
	}
	if len(tp.AllSites()) != 9 {
		t.Errorf("AllSites = %d, want 9", len(tp.AllSites()))
	}
}

func TestParseRejects(t *testing.T) {
	site := func(name, port string) string {
		return `{"name":"` + name + `","api":"h:1` + port + `","mesh":"h:2` + port + `"}`
	}
	cases := map[string]string{
		"lowercase jurisdiction": `{"jurisdictions":[{"name":"eu","sites":[` + site("a", "1") + `]}]}`,
		"duplicate site":         `{"jurisdictions":[{"name":"EU","sites":[` + site("a", "1") + `,` + site("a", "2") + `]}],"rtt_ms":{"EU/EU":5}}`,
		"missing rtt":            `{"jurisdictions":[{"name":"EU","sites":[` + site("a", "1") + `,` + site("b", "2") + `]}]}`,
		"rtt typo":               `{"jurisdictions":[{"name":"EU","sites":[` + site("a", "1") + `]}],"rtt_ms":{"a/zz":5}}`,
		"unknown field":          `{"jurisdictions":[],"replicas":3}`,
		"zero partitions":        `{"jurisdictions":[],"topics":[{"name":"t","partitions":0}]}`,
	}
	for name, js := range cases {
		if _, err := Parse([]byte(js)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	ok := `{"jurisdictions":[{"name":"EU","sites":[` + site("a", "1") + `,` + site("b", "2") + `]}],"rtt_ms":{"EU/EU":5}}`
	tp, err := Parse([]byte(ok))
	if err != nil || tp.RTT("a", "b") != 5 {
		t.Errorf("jurisdiction-wide default: err=%v", err)
	}
}

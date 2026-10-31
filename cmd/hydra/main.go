// Command hydra is the single Hydra Stream binary (docs/CONTRACTS.md §3).
//
//	hydra topo    [-topology f] [-names]                          validate and print the topology
//	hydra broker  -site s [-topology f] [-data dir]               run the broker for one site
//	hydra produce -site s -label L [-topic t] [-key k] -value v [-n count]
//	hydra fetch   -site s -label L [-topic t] [-partition p] [-offset o] [-max m]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/hashicorp/go-hclog"

	"hydra/internal/broker"
	"hydra/internal/sdk"
	"hydra/internal/topo"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	path := fs.String("topology", "topology.json", "topology file")
	site := fs.String("site", "", "site this process runs at")
	switch os.Args[1] {
	case "topo":
		names := fs.Bool("names", false, "print only site names, one per line")
		fs.Parse(os.Args[2:])
		t := load(*path)
		if *names {
			for _, s := range t.AllSites() {
				fmt.Println(s.Name)
			}
			return
		}
		printTopo(t)
	case "broker":
		data := fs.String("data", "data", "data directory")
		fs.Parse(os.Args[2:])
		runBroker(load(*path), *site, *data)
	case "produce", "fetch":
		topic := fs.String("topic", "payments", "topic")
		label := fs.String("label", "", "residency label (jurisdiction)")
		key := fs.String("key", "", "record key (produce)")
		value := fs.String("value", "", "record value (produce)")
		n := fs.Int("n", 1, "records to produce; values get a -i suffix when n > 1")
		partition := fs.Int("partition", 0, "partition index (fetch)")
		offset := fs.Int64("offset", 0, "first offset (fetch)")
		max := fs.Int("max", 20, "records to fetch")
		fs.Parse(os.Args[2:])
		c, err := sdk.New(load(*path), *site)
		if err != nil {
			fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if os.Args[1] == "produce" {
			for i := range *n {
				v := *value
				if *n > 1 {
					v = fmt.Sprintf("%s-%d", v, i)
				}
				r, err := c.Produce(ctx, *topic, *label, []byte(*key), []byte(v))
				if err != nil {
					fatal(err)
				}
				fmt.Printf("%s offset %d (leader %s, duplicate %v)\n", r.PartitionID, r.BaseOffset, r.Leader, r.Duplicate)
			}
			return
		}
		r, err := c.Fetch(ctx, *topic, *label, *partition, *offset, *max)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("%s served by %s, log %d..%d\n", r.PartitionID, r.ServedBy, r.LogStartOffset, r.HighWatermark)
		for _, rec := range r.Records {
			fmt.Printf("%d\t%s\t%s\n", rec.Offset, rec.Key, rec.Value)
		}
	default:
		usage()
	}
}

func runBroker(t *topo.Topology, site, data string) {
	log := hclog.New(&hclog.LoggerOptions{Name: site, Level: hclog.Warn})
	b, err := broker.New(t, site, data, log)
	if err != nil {
		fatal(err)
	}
	if err := b.Start(); err != nil {
		fatal(err)
	}
	s := t.Site(site)
	log.Warn("broker up", "jurisdiction", s.Jurisdiction, "api", s.API, "mesh", s.Mesh)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	b.Close()
}

func load(path string) *topo.Topology {
	t, err := topo.Load(path)
	if err != nil {
		fatal(err)
	}
	return t
}

func printTopo(t *topo.Topology) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "JURISDICTION\tSITE\tCITY\tAPI\tMESH\tPARTITIONS HOMED")
	for _, j := range t.Jurisdictions {
		for _, s := range j.Sites {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n", j.Name, s.Name, s.City, s.API, s.Mesh, len(t.Partitions(j.Name)))
		}
	}
	w.Flush()
	fmt.Println("\nSimulated RTT (ms)")
	sites := t.AllSites()
	fmt.Fprint(w, "\t")
	for _, s := range sites {
		fmt.Fprintf(w, "%s\t", s.Name)
	}
	fmt.Fprintln(w)
	for _, a := range sites {
		fmt.Fprintf(w, "%s\t", a.Name)
		for _, b := range sites {
			fmt.Fprintf(w, "%g\t", t.RTT(a.Name, b.Name))
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  hydra topo    [-topology f] [-names]
  hydra broker  -site s [-topology f] [-data dir]
  hydra produce -site s -label L [-topic t] [-key k] -value v [-n count]
  hydra fetch   -site s -label L [-topic t] [-partition p] [-offset o] [-max m]`)
	os.Exit(2)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "hydra:", err)
	os.Exit(1)
}

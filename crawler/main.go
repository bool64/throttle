package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bool64/logz"
	throttle "github.com/bool64/throttle"
	"github.com/valyala/fasthttp"
	"github.com/vearutop/dynhist-go"
	"golang.org/x/time/rate"
)

func main() {
	if len(os.Args) == 1 {
		println("usage: crawler <file with urls to get>")
		return
	}

	println("fetching through", os.Args[1])

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	cr := &CountingReader{
		Reader: f,
	}

	var r io.Reader = cr

	if strings.HasSuffix(os.Args[1], ".gz") {
		r, err = gzip.NewReader(cr)
		if err != nil {
			println("gzip reader:", err.Error())
			return
		}
	}

	fi, err := f.Stat()
	if err != nil {
		println("file stats:", err.Error())
		return
	}

	total := fi.Size()

	p := Progress{
		Interval: 10 * time.Second,
	}

	p.Start(total, cr, "sending")

	scanner := bufio.NewScanner(r)

	buf := make([]byte, 1e7)
	scanner.Buffer(buf, len(buf))

	maxConcurrency := 1000
	latencyPercentile := 90.0
	t := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.MaxConcurrency = int64(maxConcurrency)
		cfg.InitialConcurrency = 100
		cfg.MinConcurrency = 20

		cfg.LimitLatency = 300 * time.Millisecond
		cfg.LimitLatencyPercentile = latencyPercentile
		cfg.LimitFailedPercent = 5

		cfg.Interval = 20 * time.Second
		cfg.ReceiveUpdate = func(grown bool, newRate rate.Limit, newConcurrency int64) {
			println("concurrency updated:", newConcurrency)
		}
	})

	statuses := dynhist.Collector{}
	errs := logz.Observer{}
	errs.MaxSamples = 2
	errs.MaxCardinality = 10

	printStat := func() {
		println(fmt.Sprintf(
			"latency %.0f%%: %.1f ms, in progress: %d, failed: %.2f%%",
			latencyPercentile, t.Latency(latencyPercentile)*1000, t.InProgress(), t.FailedPercent(),
		))

		println("statuses:")
		println(statuses.String())

		entries := errs.GetEntries()
		if len(entries) > 0 {
			// Order by count desc.
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].Count > entries[j].Count
			})

			println("top errors:")
			for _, e := range entries {
				println(e.Count, e.Message)
			}
			println()
		}
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)

			printStat()
		}
	}()

	sp := sync.Pool{
		New: func() any {
			return make([]byte, 0, 100)
		},
	}

	c := fasthttp.Client{}
	c.MaxConnsPerHost = maxConcurrency
	c.Dial = func(addr string) (net.Conn, error) {
		return net.Dial("tcp", addr)
	}

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		line := string(scanner.Bytes())

		if len(line) == 0 {
			statuses.Add(601)
			continue
		}

		if strings.HasPrefix(line, "FAILED: ") {
			p := strings.LastIndex(line, " http")
			if p == -1 {
				statuses.Add(603)
				continue
			}

			line = line[p+1:]
		}

		if _, err := url.Parse(line); err != nil {
			statuses.Add(602)
			errs.ObserveMessage(err.Error(), nil)
			continue
		}

		t.Acquire()
		s := time.Now()

		go func() {
			failed := false
			defer func() {
				p.CountLine()
				t.Release(time.Since(s), failed)
			}()

			body := sp.Get().([]byte)

			statusCode, body, err := c.Get(body, line)
			sp.Put(body[:0])

			if err != nil {
				failed = true

				errs.ObserveMessage(err.Error(), line)

				fmt.Println("FAILED:", err.Error(), line)

				statuses.Add(600)

				return
			}

			if statusCode >= 500 {
				failed = true
			}

			// mu.Lock()
			// fmt.Println(statusCode, line)
			// mu.Unlock()

			statuses.Add(float64(statusCode))
		}()
	}

	// Wait for remaining requests in progress.
	t.WaitInProgress()
	p.Stop()

	printStat()
}

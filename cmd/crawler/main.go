// Package main provides crawler tool.
package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bool64/logz"
	"github.com/bool64/progress"
	throttle "github.com/bool64/throttle"
	"github.com/klauspost/compress/zstd"
	"github.com/valyala/fasthttp"
	"github.com/vearutop/dynhist-go"
	"golang.org/x/time/rate"
)

func main() {
	skipLines := flag.Int("skip-lines", 0, "Skip first lines.")
	minConcurrency := flag.Int("min-concurrency", 1, "Min concurrent requests.")
	startConcurrency := flag.Int("start-concurrency", 20, "Start concurrent requests.")
	maxConcurrency := flag.Int("max-concurrency", 200, "Max concurrent requests.")

	minRate := flag.Int("min-rate", 0, "Min rate limit, rps (0 = unlimited).")
	startRate := flag.Int("start-rate", 0, "Start rate limit, rps (0 = unlimited).")
	maxRate := flag.Int("max-rate", 0, "Max rate limit, rps (0 = unlimited).")

	latencyPercentile := flag.Float64("latency-percentile", 90.0, "Percentile latency of requests.")
	throttleLatency := flag.Int("throttle-latency", 500, "Min latency of requests (at percentile) to start throttling, ms.")
	throttleErrors := flag.Float64("throttle-errors", 5.0, "Min fraction of failed requests to start throttling, %.")

	adaptInterval := flag.Int("adapt-interval", 5, "Interval to reevaluate concurrency and rate limit, s.")
	statusInterval := flag.Int("status-interval", 1, "Interval to print status, s.")

	flag.Parse()

	if len(flag.Args()) == 0 {
		println("usage: crawler <file with urls to call, one at a line>")
		flag.Usage()
		return
	}

	fn := flag.Arg(0)

	println("fetching through", fn)

	f, err := os.Open(fn) //nolint:gosec // File is opened by user.
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			println("file close:", err.Error())
		}
	}()

	var r io.Reader = f

	if strings.HasSuffix(fn, ".gz") {
		r, err = gzip.NewReader(r)
		if err != nil {
			println("gzip reader:", err.Error())
			return
		}
	}

	if strings.HasSuffix(fn, ".zst") {
		r, err = zstd.NewReader(r)
		if err != nil {
			println("zstd reader:", err.Error())
			return
		}
	}

	fi, err := f.Stat()
	if err != nil {
		println("file stats:", err.Error())
		return
	}

	total := fi.Size()

	cr := progress.NewCountingReader(r)

	p := progress.Progress{
		Interval: time.Duration(*statusInterval) * time.Second,
	}

	scanner := bufio.NewScanner(cr)

	buf := make([]byte, 1e7) // 10MB buffer.
	scanner.Buffer(buf, len(buf))
	currentConcurrency := int64(*startConcurrency)

	t := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.MaxConcurrency = int64(*maxConcurrency)
		cfg.InitialConcurrency = int64(*startConcurrency)
		cfg.MinConcurrency = int64(*minConcurrency)

		cfg.MinRate = rate.Limit(*minRate)
		cfg.InitialRate = rate.Limit(*startRate)
		cfg.MaxRate = rate.Limit(*maxRate)

		cfg.LimitLatency = time.Duration(*throttleLatency) * time.Millisecond
		cfg.LimitLatencyPercentile = *latencyPercentile
		cfg.LimitFailedPercent = *throttleErrors

		cfg.Interval = time.Duration(*adaptInterval) * time.Second
		cfg.ReceiveUpdate = func(grown bool, newRate rate.Limit, newConcurrency int64) {
			atomic.StoreInt64(&currentConcurrency, newConcurrency)
			update := "faster"
			if !grown {
				update = "slower"
			}

			rateLimit := "unlimited"
			if newRate > 0 {
				rateLimit = fmt.Sprintf("%.1f rps", newRate)
			}

			println(fmt.Sprintf("limits updated: %s, rate limit: %s, concurrency limit: %d", update, rateLimit, newConcurrency))
		}
	})

	var (
		requestsFinished int64
		totalLines       int64
	)

	statuses := dynhist.Collector{}
	errs := logz.Observer{}
	errs.MaxSamples = 2
	errs.MaxCardinality = 10

	printStat := func() {
		println(fmt.Sprintf(
			"latency %.0f%%: %.1f ms, concurrency: %d, in progress: %d, failed: %.2f%%",
			*latencyPercentile, t.Latency(*latencyPercentile)*1000, atomic.LoadInt64(&currentConcurrency), t.InProgress(), t.FailedPercent(),
		))

		println("statuses:")
		println(StatusesString(&statuses))

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

	p.IncrementalSpeed = true
	p.Print = func(s progress.Status) {
		if tl := atomic.LoadInt64(&totalLines); tl > 0 {
			s.DonePercent = 100 * float64(s.LinesCompleted) / float64(tl)
			s.Remaining = time.Duration((100.0 - s.DonePercent) * float64(s.Elapsed) / (s.DonePercent)).Round(time.Second)
		}

		println(fmt.Sprintf(s.Task+": %.1f%% bytes read, %d requests processed, %.1f rps, elapsed %s, remaining %s",
			s.DonePercent, s.LinesCompleted, s.SpeedLPS,
			s.Elapsed.Round(10*time.Millisecond).String(), s.Remaining.String()))

		printStat()
	}

	p.Start(func(t *progress.Task) {
		t.TotalBytes = func() int64 {
			return total
		}
		t.CurrentBytes = func() int64 {
			cb := cr.Bytes()
			if cb == total {
				atomic.StoreInt64(&totalLines, cr.Lines())
			}

			return cb
		}
		t.CurrentLines = func() int64 {
			//	return cr.Lines()
			return atomic.LoadInt64(&requestsFinished)
		}
		t.Task = "fetching"
	})

	sp := sync.Pool{
		New: func() any {
			b := make([]byte, 0, 100)

			return &b
		},
	}

	c := fasthttp.Client{}
	c.MaxConnsPerHost = *maxConcurrency
	c.Dial = func(addr string) (net.Conn, error) {
		return net.Dial("tcp", addr)
	}

	lines := 0
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			break
		}

		lines++

		if *skipLines > 0 && lines < *skipLines {
			continue
		}

		line := string(scanner.Bytes())

		if len(line) == 0 {
			statuses.Add(601)
			continue
		}

		if strings.HasPrefix(line, "FAILED: ") {
			p := strings.LastIndex(line, "URL: http")
			if p == -1 {
				statuses.Add(603)
				continue
			}

			line = line[p+5:]
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
				atomic.AddInt64(&requestsFinished, 1)
				t.Release(time.Since(s), !failed)
			}()

			b := sp.Get().(*[]byte) //nolint:errcheck
			body := (*b)[:0]

			statusCode, body, err := c.Get(body, line)
			sp.Put(&body)

			if err != nil {
				statuses.Add(600)

				failed = true

				errs.ObserveMessage(err.Error(), line)

				fmt.Println("FAILED: ", err.Error(), ", URL: ", line)

				return
			}

			if statusCode >= 500 {
				failed = true
			}

			statuses.Add(float64(statusCode))
		}()
	}

	// Wait for remaining requests in progress.

	println("waiting in progress to finish")
	t.WaitInProgress()
	p.Stop()

	printStat()

	if err := scanner.Err(); err != nil {
		println(err.Error())
	}
}

// StatusesString renders buckets value.
func StatusesString(c *dynhist.Collector) string {
	c.Lock()
	defer c.Unlock()

	if len(c.Buckets) == 0 {
		return ""
	}

	cLen := printfLen("%d", c.Count)

	var res strings.Builder

	fmt.Fprintf(&res, "[%s] %*s total%%", "sta", cLen, "cnt")

	fmt.Fprintf(&res, " (%d requests)\n", c.Count)

	for _, b := range c.Buckets {
		percent := float64(100*b.Count) / float64(c.Count)

		fmt.Fprintf(&res, "[%d] %*d %5.2f%%", int(b.Min), cLen, b.Count, percent)

		if dots := strings.Repeat(".", int(percent)); len(dots) > 0 {
			fmt.Fprint(&res, " ", dots)
		}

		fmt.Fprintln(&res)
	}

	return res.String()
}

func printfLen(format string, val interface{}) int {
	s := fmt.Sprintf(format, val)

	return len(s)
}

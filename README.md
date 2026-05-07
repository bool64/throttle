# throttle

[![Build Status](https://github.com/bool64/throttle/workflows/test-unit/badge.svg)](https://github.com/bool64/throttle/actions?query=branch%3Amaster+workflow%3Atest-unit)
[![Coverage Status](https://codecov.io/gh/bool64/throttle/branch/master/graph/badge.svg)](https://codecov.io/gh/bool64/throttle)
[![GoDevDoc](https://img.shields.io/badge/dev-doc-00ADD8?logo=go)](https://pkg.go.dev/github.com/bool64/throttle)
[![Time Tracker](https://wakatime.com/badge/github/bool64/throttle.svg)](https://wakatime.com/badge/github/bool64/throttle)
![Code lines](https://sloc.xyz/github/bool64/throttle/?category=code)
![Comments](https://sloc.xyz/github/bool64/throttle/?category=comments)

`throttle` is an adaptive rate and concurrency limiter for Go. It adjusts throughput based on observed latency, failure rate, memory pressure, or a custom health signal so callers can keep a system near a healthy operating point instead of relying on a fixed limit.

## Features

- Controls concurrency with a semaphore-like permit pool.
- Controls request rate with `golang.org/x/time/rate`.
- Reduces throughput when latency, failures, heap usage, or a custom health check cross configured limits.
- Grows throughput gradually after a configurable number of healthy feedback cycles.
- Exposes live metrics such as current concurrency, in-flight operations, latency histogram, and failure percentage.

## Installation

```bash
go get github.com/bool64/throttle
```

## Quick Start

```go
package main

import (
	"time"

	"github.com/bool64/throttle"
	"golang.org/x/time/rate"
)

func main() {
	t := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.Interval = 5 * time.Second
		cfg.MaxConcurrency = 100
		cfg.MinConcurrency = 10
		cfg.InitialConcurrency = 50

		cfg.MaxRate = rate.Limit(500)
		cfg.MinRate = rate.Limit(50)
		cfg.InitialRate = rate.Limit(200)

		cfg.LimitLatency = 250 * time.Millisecond
		cfg.LimitLatencyPercentile = 95
		cfg.LimitFailedPercent = 2
	})

	t.Go(func() bool {
		start := time.Now()
		_ = start

		// Perform work here.
		time.Sleep(20 * time.Millisecond)

		// Return true on success, false on failure.
		return true
	})

	t.WaitInProgress()
}
```

## How It Works

Every `Config.Interval`, the limiter evaluates configured health signals:

- latency percentile from operation durations passed to `Release`
- failed operation ratio
- heap-in-use from `runtime.ReadMemStats`
- a custom `LimitReached` callback

If any limit is exceeded, the limiter reduces concurrency and rate by configured fractions. If the system stays healthy for `GrowthSkipCycles` consecutive feedback cycles, it increases them again until `MaxConcurrency` and `MaxRate` are reached.

## Usage Patterns

### `Go`

Use `Go` when the limiter should manage permit acquisition, panic accounting, latency measurement, and release automatically.

```go
t.Go(func() bool {
	// work
	return true
})
```

### `Acquire` / `Release`

Use manual acquisition when work is not naturally wrapped in a single function or when you already have your own goroutine lifecycle.

```go
t.Acquire()

start := time.Now()
ok := doWork()
t.Release(time.Since(start), ok)
```

## Configuration Notes

- `MaxConcurrency == 0` disables concurrency limiting.
- `MaxRate == 0` disables rate limiting.
- `LimitLatency == 0` disables latency tracking.
- `InitialConcurrency` defaults to `MaxConcurrency`.
- `InitialRate` defaults to `MaxRate`.
- `MinConcurrency` defaults to `runtime.NumCPU()`.
- `GrowthSkipCycles` defaults to `3`.
- `StepFracConcurrency` and `StepFracRate` default to `0.1`.

## Observability

The limiter exposes lightweight runtime information:

- `InProgress()` returns currently running operations.
- `Concurrency()` returns the current allowed concurrency.
- `Latency(percentile)` returns latency percentile in seconds.
- `LatencyHistogram()` returns the underlying histogram collector.
- `FailedPercent()` returns the current failed operation percentage.
- `ReceiveUpdate` can be used to observe limit changes as they happen.

## Stability Notes

- `Release` must be called exactly once for every successful `Acquire`.
- `Go` recovers panics, marks the operation as failed, and still releases the permit.
- `WaitInProgress` waits for currently running operations to finish; it does not stop new callers from entering.

## License

MIT, see [LICENSE](LICENSE).

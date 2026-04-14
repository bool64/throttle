// Package throttle implements a feedback-driven rate and concurrency limiter
// to keep application in bounds of healthy state.
package throttle

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vearutop/dynhist-go"
	"golang.org/x/time/rate"
)

// Config controls Throttle behavior.
type Config struct {
	// Interval is a duration between two consecutive feedback checks, default 15s.
	Interval time.Duration

	// GrowthSkipCycles is a number of feedback cycles to skip before growing rate and concurrency, default 3.
	// The more the value, less often Throttle will be adjusted in a healthy state.
	GrowthSkipCycles int

	// LimitHeapInUse is maximum heap in use size (bytes) of a healthy system.
	LimitHeapInUse uint64

	// LimitLatency is a percentile latency of a healthy system.
	LimitLatency time.Duration

	// LimitLatencyPercentile is the percentile of latency to use for LimitLatency, default 90.0 (90%).
	LimitLatencyPercentile float64

	// LimitFailedPercent is the fraction of failed operations of a healthy system, in percents, example: 2.0 (2%).
	LimitFailedPercent float64

	// LimitReached is a user-defined function to indicate unhealthy state.
	LimitReached func() bool

	// StepFracConcurrency is a fraction of current concurrency value to add or remove during performance tweak cycle,
	// default 0.1 (10%).
	StepFracConcurrency float64

	// StepFracRate is a fraction of current rate value to add or remove during performance tweak cycle,
	// default 0.1 (10%).
	StepFracRate float64

	// MinConcurrency is the lower bound of concurrency limiting, default runtime.NumCPU().
	MinConcurrency int64

	// InitialConcurrency is the starting concurrency value, must be between MinConcurrency and MaxConcurrency,
	// default MaxConcurrency.
	InitialConcurrency int64

	// MaxConcurrency is the maximum number of operation that can be in progress at the same time, default 1000.
	MaxConcurrency int64

	// MinRate is the lower bound of rate limiting.
	MinRate rate.Limit

	// InitialRate is the starting rate limit value, must be between MinRate and MaxRate, default MaxRate.
	InitialRate rate.Limit

	// MaxRate is the maximum allowed rate of operations.
	MaxRate rate.Limit

	// ReceiveUpdate collects new values of rate and concurrency limits when they are updated.
	ReceiveUpdate func(grown bool, newRate rate.Limit, newConcurrency int64)
}

// Throttle is an adaptive rate and concurrency limiter.
//
// Rate of operations is maintained to keep the system in healthy state based on feedback cycles.
// Feedback is collected once in Config.Interval, if any of health checks are failing,
// Throttle would reduce concurrency by a fraction Config.StepFracConcurrency once and
// rate by a fraction Config.StepFracRate once. Target values are bounded below with Config.MinConcurrency
// and Config.MinRate.
type Throttle struct {
	*throttle
}

type throttle struct {
	sem     chan struct{} // Semaphore channel for concurrency limit.
	blocked int64         // Number of blocked slots in semaphore.

	latency    *dynhist.Collector // Latency histogram collector for percentile calculation.
	hCycles    int64              // Number of healthy cycles passed since last perf adjustment.
	inProgress int64              // Current number of operations in progress.
	failed     int64              // Number of failed operations.
	total      int64              // Total number of performed operations.
	lastCycle  int64              // Timestamp of last feedback cycle.

	cfg Config

	rlExists bool
	mu       sync.RWMutex
	rl       *rate.Limiter
}

// Option is applied to Config.
type Option func(cfg *Config)

// NewThrottle creates a configured Throttle instance.
func NewThrottle(options ...Option) *Throttle {
	t := &throttle{}

	// T is wrapping t to employ runtime.SetFinalizer to finish feedback goroutine.
	T := &Throttle{throttle: t}

	t.cfg.Interval = 15 * time.Second
	t.cfg.GrowthSkipCycles = 3
	t.cfg.LimitLatency = 10 * time.Second
	t.cfg.LimitLatencyPercentile = 90.0
	t.cfg.MaxConcurrency = 1000
	t.cfg.MinConcurrency = int64(runtime.NumCPU())
	t.cfg.StepFracConcurrency = 0.1
	t.cfg.StepFracRate = 0.1

	for _, o := range options {
		o(&t.cfg)
	}

	if t.cfg.MaxConcurrency != 0 {
		t.sem = make(chan struct{}, t.cfg.MaxConcurrency)

		if t.cfg.InitialConcurrency != 0 {
			t.blocked = t.cfg.MaxConcurrency - t.cfg.InitialConcurrency
			for i := 0; i < int(t.blocked); i++ {
				t.sem <- struct{}{}
			}
		}
	}

	if t.cfg.MaxRate != 0 {
		t.rlExists = true

		if t.cfg.InitialRate == 0 {
			t.cfg.InitialRate = t.cfg.MaxRate
		}

		t.rl = rate.NewLimiter(t.cfg.InitialRate, 1)
	}

	if t.cfg.LimitLatency != 0 {
		t.latency = &dynhist.Collector{BucketsLimit: 300, WeightFunc: dynhist.ExpWidth(1.5, 1)}
	}

	done := make(chan struct{})

	go func() {
		for {
			interval := t.cfg.Interval

			select {
			case <-time.After(interval):
				t.runFeedbackCycle()

			case <-done:
				return
			}
		}
	}()

	runtime.SetFinalizer(T, func(_ *Throttle) {
		close(done)
	})

	return T
}

// Go runs a concurrent job.
func (t *Throttle) Go(f func() (passed bool)) {
	t.Acquire()
	go func() {
		var (
			s      = time.Now()
			passed bool
		)

		defer func() {
			if r := recover(); r != nil {
				atomic.AddInt64(&t.failed, 1)
			}
			t.Release(time.Since(s), passed)
		}()
		passed = f()
	}()
}

// Acquire waits until a permit is available and blocks one in the pool.
func (t *Throttle) Acquire() {
	if t.rlExists {
		t.mu.RLock()
		rl := t.rl
		t.mu.RUnlock()

		_ = rl.Wait(context.Background()) //nolint:errcheck // Does not fail with background context.
	}

	if t.sem != nil {
		t.sem <- struct{}{}
	}

	atomic.AddInt64(&t.inProgress, 1)
}

// Release returns permit back to the pool.
func (t *Throttle) Release(elapsed time.Duration, passed bool) {
	if t.sem != nil {
		<-t.sem
	}

	t.latency.Add(elapsed.Seconds())
	atomic.AddInt64(&t.inProgress, -1)
	atomic.AddInt64(&t.total, 1)
	if !passed {
		atomic.AddInt64(&t.failed, 1)
	}
}

func (t *throttle) runFeedbackCycle() {
	atomic.StoreInt64(&t.lastCycle, time.Now().UnixNano())

	if t.cfg.LimitHeapInUse != 0 {
		m := runtime.MemStats{}
		runtime.ReadMemStats(&m)

		if m.HeapInuse >= t.cfg.LimitHeapInUse {
			t.slower()

			return
		}
	}

	if t.cfg.LimitLatency != 0 {
		lat := t.latency.Percentile(t.cfg.LimitLatencyPercentile)
		if lat > t.cfg.LimitLatency.Seconds() {
			t.slower()

			return
		}
	}

	if t.cfg.LimitFailedPercent != 0 {
		failed := atomic.LoadInt64(&t.failed)
		total := atomic.LoadInt64(&t.total)

		if 100*float64(failed)/float64(total) > t.cfg.LimitFailedPercent {
			t.slower()

			return
		}
	}

	if t.cfg.LimitReached != nil {
		if t.cfg.LimitReached() {
			t.slower()

			return
		}
	}

	t.faster()
}

// slower tightens limits to reduce throughput and allow the system to cool down into a healthy state.
func (t *throttle) slower() {
	atomic.StoreInt64(&t.hCycles, 0)

	newRate := rate.Limit(0)
	newConcurrency := int64(0)

	if t.sem != nil {
		blocked := atomic.LoadInt64(&t.blocked)
		active := t.cfg.MaxConcurrency - blocked
		delta := int64(float64(active) * t.cfg.StepFracConcurrency)

		if active-delta <= t.cfg.MinConcurrency {
			delta = active - t.cfg.MinConcurrency
		}

		newConcurrency = active - delta

		if delta > 0 {
			for i := 0; i < int(delta); i++ {
				t.sem <- struct{}{}
			}

			atomic.AddInt64(&t.blocked, delta)
		}
	}

	if t.rlExists {
		newRate = t.updateRateLimiter(true)
	}

	if t.cfg.ReceiveUpdate != nil {
		t.cfg.ReceiveUpdate(false, newRate, newConcurrency)
	}
}

// faster loosens the limits to allow for more throughput.
func (t *throttle) faster() {
	vv := atomic.AddInt64(&t.hCycles, 1)
	if vv <= int64(t.cfg.GrowthSkipCycles) {
		return
	}

	newRate := rate.Limit(0)
	newConcurrency := int64(0)

	if t.sem != nil {
		blocked := atomic.LoadInt64(&t.blocked)
		if blocked == 0 {
			return
		}

		active := t.cfg.MaxConcurrency - blocked
		delta := int64(float64(active) * t.cfg.StepFracConcurrency)

		if delta < 1 {
			delta = 1
		}

		if active+delta >= t.cfg.MaxConcurrency {
			delta = t.cfg.MaxConcurrency - active
		}

		newConcurrency = active + delta

		if delta != 0 {
			for i := 0; i < int(delta); i++ {
				<-t.sem
			}

			atomic.AddInt64(&t.blocked, -delta)
		}
	}

	if t.rlExists {
		newRate = t.updateRateLimiter(false)
	}

	if t.cfg.ReceiveUpdate != nil {
		t.cfg.ReceiveUpdate(true, newRate, newConcurrency)
	}
}

func (t *throttle) updateRateLimiter(slower bool) rate.Limit {
	t.mu.Lock()
	defer t.mu.Unlock()

	lim := t.rl.Limit()
	delta := float64(lim) * t.cfg.StepFracRate

	if slower {
		delta = -delta
	}

	newLim := lim + rate.Limit(delta)
	if newLim > t.cfg.MaxRate {
		newLim = t.cfg.MaxRate
	} else if newLim < t.cfg.MinRate {
		newLim = t.cfg.MinRate
	}

	if newLim != lim {
		t.rl = rate.NewLimiter(newLim, 1)
	}

	return newLim
}

// InProgress returns the number of operations that are currently in progress.
func (t *Throttle) InProgress() int64 {
	return atomic.LoadInt64(&t.inProgress)
}

// Latency returns percentile latency in seconds, percent value of 10.0 means 10%.
func (t *Throttle) Latency(percent float64) float64 {
	if t.latency == nil {
		return 0
	}

	return t.latency.Percentile(percent)
}

// LatencyHistogram returns latency histogram collector.
func (t *Throttle) LatencyHistogram() *dynhist.Collector {
	return t.latency
}

// Concurrency returns the number of available slots in the semaphore.
func (t *Throttle) Concurrency() int64 {
	return t.cfg.MaxConcurrency - atomic.LoadInt64(&t.blocked)
}

// FailedPercent returns the fraction of failed operations of a healthy system, in percents.
func (t *Throttle) FailedPercent() float64 {
	return 100 * float64(atomic.LoadInt64(&t.failed)) / float64(atomic.LoadInt64(&t.total))
}

// WaitInProgress blocks until all operations in progress are finished.
func (t *Throttle) WaitInProgress() {
	if t.sem == nil {
		return
	}

	inProgress := t.InProgress()

	// Fill the semaphore to wait for operations in progress.
	for i := 0; i < int(inProgress); i++ {
		t.sem <- struct{}{}
	}

	// Release temporary filling.
	for i := 0; i < int(inProgress); i++ {
		<-t.sem
	}
}

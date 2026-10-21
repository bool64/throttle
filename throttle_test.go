package throttle_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	throttle "github.com/bool64/throttle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestSemaphor_Acquire(t *testing.T) {
	var concurrency int64

	s := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.Interval = 100 * time.Millisecond
		cfg.StepFracConcurrency = 0.9
		cfg.LimitLatencyPercentile = 93
		cfg.MaxConcurrency = 100
		cfg.MinConcurrency = 20
		cfg.MaxRate = 1000
		cfg.MinRate = 20
		cfg.LimitLatency = 5 * time.Millisecond
		cfg.Interval = 100 * time.Millisecond
		cfg.ReceiveUpdate = func(grown bool, r rate.Limit, c int64) {
			atomic.StoreInt64(&concurrency, c)
		}
	})

	wg := sync.WaitGroup{}

	for i := 0; i < 1000; i++ {
		s.Acquire()

		wg.Add(1)
		go func() {
			defer func() { wg.Done() }()
			time.Sleep(time.Millisecond)
			s.Release(time.Millisecond, true)
		}()
	}

	for i := 0; i < 100; i++ {
		s.Acquire()

		wg.Add(1)
		go func() {
			defer func() { wg.Done() }()
			time.Sleep(10 * time.Millisecond)
			s.Release(10*time.Millisecond, true)
		}()
	}

	time.Sleep(200 * time.Millisecond)
	require.GreaterOrEqual(t, 10*time.Millisecond.Seconds(), s.Latency(90))
	assert.Equal(t, int64(20), atomic.LoadInt64(&concurrency)) // Min concurrency.
	println("now healthy")

	for i := 0; i < 2000; i++ {
		if i%100 == 0 {
			println(i, s.Latency(90))
		}

		s.Acquire()

		wg.Add(1)
		go func() {
			defer func() { wg.Done() }()
			time.Sleep(1 * time.Microsecond)
			s.Release(1*time.Microsecond, true)
		}()
	}

	wg.Wait()

	runtime.GC()
	runtime.GC()
}

func TestThrottle_ReleaseWithoutLatencyLimit(t *testing.T) {
	s := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.MaxConcurrency = 1
		cfg.MinConcurrency = 1
		cfg.LimitLatency = 0
	})

	s.Acquire()
	s.Release(time.Millisecond, true)

	assert.Equal(t, int64(0), s.InProgress())
	assert.Equal(t, 0.0, s.Latency(90))
}

func TestThrottle_WaitInProgressWithoutSemaphore(t *testing.T) {
	s := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.MaxConcurrency = 0
	})

	s.Go(func() bool {
		time.Sleep(20 * time.Millisecond)
		return true
	})

	start := time.Now()
	s.WaitInProgress()

	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	assert.Equal(t, int64(0), s.InProgress())
}

func TestThrottle_RateCanGrowAtMaxConcurrency(t *testing.T) {
	updates := make(chan rate.Limit, 4)

	throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.Interval = 10 * time.Millisecond
		cfg.GrowthSkipCycles = 0
		cfg.MaxConcurrency = 10
		cfg.MinConcurrency = 10
		cfg.InitialRate = 10
		cfg.MinRate = 10
		cfg.MaxRate = 20
		cfg.StepFracRate = 0.5
		cfg.ReceiveUpdate = func(grown bool, r rate.Limit, _ int64) {
			if grown && r > 0 {
				updates <- r
			}
		}
	})

	require.Eventually(t, func() bool {
		select {
		case v := <-updates:
			return v > 10
		default:
			return false
		}
	}, 200*time.Millisecond, 10*time.Millisecond)
}

func TestThrottle_FailedPercentZeroTotal(t *testing.T) {
	s := throttle.NewThrottle()

	assert.Equal(t, 0.0, s.FailedPercent())
}

func TestThrottle_SetLimits(t *testing.T) {
	th := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.Interval = time.Hour
		cfg.MinConcurrency = 2
		cfg.MaxConcurrency = 20
		cfg.InitialConcurrency = 10
		cfg.MinRate = 10
		cfg.MaxRate = 100
	})

	assert.Equal(t, int64(10), th.Concurrency())

	// Out of bounds.
	require.Error(t, th.SetLimits(throttle.Limits{MinRate: 10, MaxRate: 100, MinConcurrency: 2, MaxConcurrency: 21}))
	require.Error(t, th.SetLimits(throttle.Limits{MinRate: 200, MaxRate: 100, MinConcurrency: 2, MaxConcurrency: 10}))

	// Lower the max, concurrency follows in background.
	l := throttle.Limits{MinRate: 10, MaxRate: 100, MinConcurrency: 2, MaxConcurrency: 4}
	require.NoError(t, th.SetLimits(l))
	assert.Equal(t, l, th.Limits())
	assert.Eventually(t, func() bool { return th.Concurrency() == 4 }, time.Second, time.Millisecond)

	// Raise the min above current.
	l.MinConcurrency = 6
	l.MaxConcurrency = 20
	require.NoError(t, th.SetLimits(l))
	assert.Eventually(t, func() bool { return th.Concurrency() == 6 }, time.Second, time.Millisecond)
}

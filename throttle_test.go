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

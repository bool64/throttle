package throttle_test

import (
	"testing"
	"time"

	throttle "github.com/bool64/throttle"
)

func BenchmarkThrottle_Acquire(b *testing.B) {
	s := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.MaxConcurrency = 1000
		cfg.MinConcurrency = 20
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Acquire()
		go s.Release(time.Millisecond, false)
	}
}

func BenchmarkThrottle_Acquire_rateLimit(b *testing.B) {
	s := throttle.NewThrottle(func(cfg *throttle.Config) {
		cfg.MaxConcurrency = 1000
		cfg.MinConcurrency = 20
		cfg.MaxRate = 10000000 // Rate limit is unrealistically high to exhibit baseline performance impact.
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Acquire()
		go s.Release(time.Millisecond, false)
	}
}

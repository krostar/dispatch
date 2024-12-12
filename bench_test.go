package dispatch_test

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/krostar/dispatch"
)

func Benchmark_Service_OneProcessor(b *testing.B) {
	type myEvent struct{ ID int }

	runner := dispatch.New([]dispatch.EventProcessorFunc[myEvent]{
		func(context.Context, myEvent) error { return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	cerr := dispatch.RunRunner(ctx, runner)
	defer close(cerr)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := runner.ProcessEvent(ctx, myEvent{ID: b.N}); err != nil {
				b.Fail()
			}
		}
	})

	cancel()
	assert.NilError(b, <-cerr)
}

func Benchmark_Service_TwoProcessor(b *testing.B) {
	type myEvent struct{ ID int }

	runner := dispatch.New([]dispatch.EventProcessorFunc[myEvent]{
		func(context.Context, myEvent) error { return nil },
		func(context.Context, myEvent) error { return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	cerr := make(chan error)
	go func() { cerr <- runner.Run(ctx) }()
	defer close(cerr)

	time.Sleep(time.Second)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := runner.ProcessEvent(ctx, myEvent{ID: b.N}); err != nil {
				b.Fail()
			}
		}
	})

	cancel()
	assert.NilError(b, <-cerr)
}

func Benchmark_Service_ThreeProcessor(b *testing.B) {
	type myEvent struct{ ID int }

	runner := dispatch.New([]dispatch.EventProcessorFunc[myEvent]{
		func(context.Context, myEvent) error { return nil },
		func(context.Context, myEvent) error { return nil },
		func(context.Context, myEvent) error { return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	cerr := make(chan error)
	go func() { cerr <- runner.Run(ctx) }()
	defer close(cerr)

	time.Sleep(time.Second)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := runner.ProcessEvent(ctx, myEvent{ID: b.N}); err != nil {
				b.Fail()
			}
		}
	})

	cancel()
	assert.NilError(b, <-cerr)
}

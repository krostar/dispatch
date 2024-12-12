package dispatch

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eapache/go-resiliency/breaker"
	"github.com/eapache/go-resiliency/retrier"
	"gotest.tools/v3/assert"
)

func Test_MiddlewareCircuitBreaker(t *testing.T) {
	t.Parallel()

	type myEvent struct{}
	ctx := context.Background()
	expectedErr := errors.New("boom")

	var called int

	processor := MiddlewareCircuitBreaker[myEvent](2, 2, time.Millisecond*100)()(
		func(context.Context, myEvent) error {
			called++

			switch called {
			case 1:
				return nil
			case 2:
				return expectedErr
			case 3:
				return nil
			case 4:
				return expectedErr
			case 5:
				return expectedErr
			case 6:
				return nil
			case 7:
				return nil
			case 8:
				return expectedErr
			case 9:
				return nil
			default:
				panic("unhandled")
			}
		},
	)

	assert.NilError(t, processor(ctx, myEvent{}))
	assert.ErrorIs(t, processor(ctx, myEvent{}), expectedErr)
	time.Sleep(time.Millisecond * 100)
	assert.NilError(t, processor(ctx, myEvent{}))
	assert.ErrorIs(t, processor(ctx, myEvent{}), expectedErr)
	assert.ErrorIs(t, processor(ctx, myEvent{}), expectedErr)
	assert.ErrorIs(t, processor(ctx, myEvent{}), breaker.ErrBreakerOpen)
	assert.Equal(t, called, 5, "should not be 6, breaker is opened")
	time.Sleep(time.Millisecond * 100 * 2)
	assert.NilError(t, processor(ctx, myEvent{}))
	assert.NilError(t, processor(ctx, myEvent{}))
	assert.ErrorIs(t, processor(ctx, myEvent{}), expectedErr)
	assert.NilError(t, processor(ctx, myEvent{}))
}

func Test_MiddlewareRateLimiter(t *testing.T) {
	t.Parallel()

	type myEvent struct{}
	ctx, cancel := context.WithCancel(context.Background())

	var callingTimes []time.Duration

	last := time.Now()

	processor := MiddlewareRateLimiter[myEvent](1, 2)()(
		func(context.Context, myEvent) error {
			callingTimes = append(callingTimes, time.Since(last))
			return nil
		},
	)

	assert.NilError(t, processor(ctx, myEvent{}))
	assert.NilError(t, processor(ctx, myEvent{}))
	assert.NilError(t, processor(ctx, myEvent{}))
	assert.NilError(t, processor(ctx, myEvent{}))
	cancel()
	assert.ErrorContains(t, processor(ctx, myEvent{}), "unable to wait limiter")

	assert.Assert(t, len(callingTimes) == 4)
	assert.Check(t, callingTimes[0] < time.Second)
	assert.Check(t, callingTimes[1] < time.Second)
	assert.Check(t, callingTimes[2] >= time.Second)
	assert.Check(t, callingTimes[3] >= time.Second)
}

func Test_MiddlewareRecover(t *testing.T) {
	type myEvent struct{}
	ctx := context.Background()

	processor := func(context.Context, myEvent) error {
		panic("panic reason")
	}

	defer func() {
		reason := recover()
		assert.Equal(t, nil, reason)
	}()

	expectedErr := errors.New("boom")

	err := MiddlewareRecover[myEvent](func(reason any) error {
		return fmt.Errorf("%v: %w", reason, expectedErr)
	})()(processor)(ctx, myEvent{})

	assert.ErrorContains(t, err, "panic reason")
	assert.ErrorIs(t, err, expectedErr)
}

func Test_MiddlewareRetrier(t *testing.T) {
	t.Parallel()

	type myEvent struct {
		nbFailure int
	}
	ctx := context.Background()
	expectedErr := errors.New("boom")

	for name, test := range map[string]struct {
		failures  int
		assertion func(t *testing.T, called int, err error)
	}{
		"no error": {
			failures: 0,
			assertion: func(t *testing.T, called int, err error) {
				assert.Equal(t, called, 1)
				assert.NilError(t, err)
			},
		},
		"failed then succeeded": {
			failures: 1,
			assertion: func(t *testing.T, called int, err error) {
				assert.Equal(t, called, 2)
				assert.NilError(t, err)
			},
		},
		"failed after all retries": {
			failures: 4,
			assertion: func(t *testing.T, called int, err error) {
				assert.Equal(t, called, 4)
				assert.ErrorIs(t, err, expectedErr)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var called int
			err := MiddlewareRetrier[myEvent](retrier.ConstantBackoff(3, time.Millisecond*100), nil)()(
				func(_ context.Context, event myEvent) error {
					called++
					if event.nbFailure >= called {
						return expectedErr
					}
					return nil
				},
			)(ctx, myEvent{nbFailure: test.failures})
			test.assertion(t, called, err)
		})
	}
}

func Test_MiddlewareTimeout(t *testing.T) {
	t.Parallel()

	type myEvent struct {
		sleep bool
	}

	ctx := context.Background()

	for name, test := range map[string]struct {
		sleep     bool
		assertion func(t *testing.T, err error)
	}{
		"before deadline": {
			sleep: false,
			assertion: func(t *testing.T, err error) {
				assert.NilError(t, err)
			},
		},
		"after deadline": {
			sleep: true,
			assertion: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, context.DeadlineExceeded)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := MiddlewareTimeout[myEvent](time.Millisecond*100)()(
				func(ctx context.Context, event myEvent) error {
					if event.sleep {
						<-ctx.Done()
					}
					return ctx.Err()
				},
			)(ctx, myEvent{sleep: test.sleep})
			test.assertion(t, err)
		})
	}
}

func Test_ProcessorMiddlewaresFunc_Chain(t *testing.T) {
	type myEvent struct {
		ID string
	}

	var order []string

	middlewares := ProcessorMiddlewaresFunc[myEvent]{
		func(next EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
			return func(ctx context.Context, event myEvent) error {
				order = append(order, "A1")
				defer func() { order = append(order, "A2") }()
				return next(ctx, event)
			}
		},
		func(next EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
			return func(ctx context.Context, event myEvent) error {
				order = append(order, "B1")
				defer func() { order = append(order, "B2") }()
				return next(ctx, event)
			}
		},
		func(next EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
			return func(ctx context.Context, event myEvent) error {
				order = append(order, "C1")
				defer func() { order = append(order, "C2") }()
				return next(ctx, event)
			}
		},
		func(next EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
			return func(ctx context.Context, event myEvent) error {
				order = append(order, "D1")
				defer func() { order = append(order, "D2") }()
				return next(ctx, event)
			}
		},
	}

	processor := middlewares.Chain(func(context.Context, myEvent) error {
		order = append(order, "P")
		return nil
	})

	assert.NilError(t, processor(context.Background(), myEvent{ID: "hello"}))
	assert.DeepEqual(t, []string{"A1", "B1", "C1", "D1", "P", "D2", "C2", "B2", "A2"}, order)
}

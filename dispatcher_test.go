package dispatch

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/eapache/go-resiliency/retrier"
	"go.uber.org/goleak"
	"gotest.tools/v3/assert"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func RunRunner[Event any](ctx context.Context, dispatcher *Dispatcher[Event]) chan error {
	cerr := make(chan error)
	go func() { cerr <- dispatcher.Run(ctx) }()

	for run := true; run; {
		dispatcher.m.RLock()
		run = dispatcher.events == nil
		dispatcher.m.RUnlock()
	}

	return cerr
}

func Test_Service_Run(t *testing.T) {
	t.Parallel()

	type myEvent struct {
		Value string
	}

	t.Run("processor not started", func(t *testing.T) {
		runner := New([]EventProcessorFunc[myEvent]{func(context.Context, myEvent) error { return nil }})
		assert.ErrorContains(t, runner.ProcessEvent(context.Background(), myEvent{}), "event not processed: processors are stopped")
	})

	t.Run("runner error", func(t *testing.T) {
		runner := New([]EventProcessorFunc[myEvent]{func(context.Context, myEvent) error { return nil }})

		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()

		assert.ErrorIs(t, runner.Run(ctx), context.DeadlineExceeded)
	})

	t.Run("ok limited", func(t *testing.T) {
		processed := make([][]myEvent, 3)
		expectedErr := errors.New("boom")

		processor1 := func(_ context.Context, event myEvent) error {
			processed[0] = append(processed[0], event)
			return nil
		}

		processor2 := func(_ context.Context, event myEvent) error {
			processed[1] = append(processed[1], event)
			return nil
		}

		processor3 := func(ctx context.Context, event myEvent) error {
			processed[2] = append(processed[2], event)
			switch event.Value {
			case "boom":
				return expectedErr
			case "sleepy":
				<-ctx.Done()
				return ctx.Err()
			default:
				return nil
			}
		}

		runner := New([]EventProcessorFunc[myEvent]{processor1, processor2, processor3},
			WithMultipleInstances[myEvent](2),
			WithTimeout[myEvent](time.Millisecond*500),
			WithPerInstanceMiddleware(MiddlewareRetrier[myEvent](retrier.ConstantBackoff(1, time.Millisecond*100), nil)),
		)

		ctx, cancel := context.WithCancel(context.Background())
		cerr := RunRunner(ctx, runner)
		defer close(cerr)

		assert.NilError(t, runner.ProcessEvent(ctx, myEvent{Value: "hello"}))
		assert.NilError(t, runner.ProcessEvent(ctx, myEvent{Value: "world"}))
		{
			err := runner.ProcessEvent(ctx, myEvent{Value: "boom"})
			assert.Error(t, err, "unable to process event by processor with pid 3: boom")
			assert.ErrorIs(t, err, expectedErr)
		}
		{
			err := runner.ProcessEvent(ctx, myEvent{Value: "sleepy"})
			assert.ErrorContains(t, err, "unable to process event by processor with pid 3")
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		}
		assert.NilError(t, runner.ProcessEvent(ctx, myEvent{Value: "notboom"}))

		cancel()
		assert.NilError(t, <-cerr)
		assert.DeepEqual(t, [][]myEvent{
			0: {{Value: "hello"}, {Value: "world"}, {Value: "boom"}, {Value: "sleepy"}, {Value: "notboom"}},
			1: {{Value: "hello"}, {Value: "world"}, {Value: "boom"}, {Value: "sleepy"}, {Value: "notboom"}},
			2: {{Value: "hello"}, {Value: "world"}, {Value: "boom"}, {Value: "boom"}, {Value: "sleepy"}, {Value: "notboom"}},
		}, processed)
	})

	t.Run("ok unlimited", func(t *testing.T) {
		processed := make([][]myEvent, 2)

		processor1 := func(ctx context.Context, event myEvent) error {
			processed[0] = append(processed[0], event)
			<-ctx.Done()
			return nil
		}

		processor2 := func(ctx context.Context, event myEvent) error {
			processed[1] = append(processed[1], event)
			<-ctx.Done()
			return nil
		}

		runner := New([]EventProcessorFunc[myEvent]{processor1, processor2},
			WithUnlimitedInstances[myEvent](),
			WithTimeout[myEvent](time.Millisecond*300),
			WithPerInstanceMiddleware(MiddlewareRetrier[myEvent](retrier.ConstantBackoff(1, time.Millisecond*100), nil)),
		)

		ctx, cancel := context.WithCancel(context.Background())
		cerr := RunRunner(ctx, runner)
		defer close(cerr)

		var wg sync.WaitGroup
		wg.Add(3)
		for i := range 3 {
			go func() {
				defer wg.Done()
				assert.NilError(t, runner.ProcessEvent(ctx, myEvent{Value: strconv.FormatInt(int64(i), 10)}))
			}()
		}
		wg.Wait()

		cancel()
		assert.NilError(t, <-cerr)
		assert.Check(t, len(processed[0]) == 3)
		assert.Check(t, len(processed[1]) == 3)
	})
}

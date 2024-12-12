package dispatch

import (
	"context"
	"fmt"
	"time"

	"github.com/eapache/go-resiliency/breaker"
	"github.com/eapache/go-resiliency/retrier"
	"golang.org/x/time/rate"
)

type (
	// ProcessorMiddlewareFunc defines the signature a of single EventProcessorFunc middleware.
	ProcessorMiddlewareFunc[Event any] func(EventProcessorFunc[Event]) EventProcessorFunc[Event]
	// ProcessorMiddlewaresFunc defines the signature a of a list of EventProcessorFunc middlewares.
	ProcessorMiddlewaresFunc[Event any] []ProcessorMiddlewareFunc[Event]
	// CreateProcessorMiddlewareFunc defines the signature to build a middleware.
	CreateProcessorMiddlewareFunc[Event any] func() ProcessorMiddlewareFunc[Event]
)

// MiddlewareCircuitBreaker is a circuit breaker processor middleware that starts closed.
// While in the closed state, it calls the next processor in the chain, as normally expected.
//
// It opens ("breaking" the middlewares call chain) if $errorThreshold errors are seen without
// an error-free period of at least $timeout. In that case, it stops calling the next processor
// in the chain and immediately returns an error.
//
// Once open, the breaker half-closes after $timeout, "restoring" the middleware wall chain.
//
// From half-open, the breaker closes back to original state after $successThreshold consecutive
// successes, or re-opens after a single error.
func MiddlewareCircuitBreaker[Event any](errorThreshold, successThreshold int, timeout time.Duration) CreateProcessorMiddlewareFunc[Event] {
	return func() ProcessorMiddlewareFunc[Event] {
		b := breaker.New(errorThreshold, successThreshold, timeout)

		return func(next EventProcessorFunc[Event]) EventProcessorFunc[Event] {
			return func(ctx context.Context, event Event) error {
				return b.Run(func() error {
					return next(ctx, event)
				})
			}
		}
	}
}

// MiddlewareRateLimiter rates limit the processor to maxRatePerSecond, and permits a bursts of at most maxBurst.
//
//	runner := New[myEvent](
//		[]EventProcessorFunc[myEvent]{
//			processor1,
//			processor2,
//		},
//		WithMultipleInstances[myEvent](4),
//		WithGlobalMiddleware[myEvent](
//			MiddlewareRateLimiter[myEvent](10, 1),
//		),
//	)
//
// here all total 8 instances are sharing the same global rate limiter instance.
// Be aware that since all processors must process all events, rate limiting one processor will effectively limit all processors.
func MiddlewareRateLimiter[Event any](maxRatePerSecond float64, maxBurst int) CreateProcessorMiddlewareFunc[Event] {
	return func() ProcessorMiddlewareFunc[Event] {
		r := rate.NewLimiter(rate.Limit(maxRatePerSecond), maxBurst)

		return func(next EventProcessorFunc[Event]) EventProcessorFunc[Event] {
			return func(ctx context.Context, event Event) error {
				if err := r.Wait(ctx); err != nil {
					return fmt.Errorf("middleware ratelimiter: unable to wait limiter: %w", err)
				}
				return next(ctx, event)
			}
		}
	}
}

// MiddlewareRecover stops any panic propagation and recover from it to avoid runner to crash.
func MiddlewareRecover[Event any](onPanic func(reason any) error) CreateProcessorMiddlewareFunc[Event] {
	return func() ProcessorMiddlewareFunc[Event] {
		return func(next EventProcessorFunc[Event]) EventProcessorFunc[Event] {
			return func(ctx context.Context, event Event) (err error) {
				defer func() {
					if reason := recover(); reason != nil {
						err = fmt.Errorf("panic recovered: %w", onPanic(reason))
					}
				}()
				return next(ctx, event)
			}
		}
	}
}

// MiddlewareRetrier retries errored execution of the next processor in the middleware chain.
// Depending on the error classification it retries or return the processor error.
// If the classifier asks to retry, then the handler sleeps (context cancellation is still respected) according
// to the provided backoff policy before retrying. After the total number of retries is exceeded then the last
// return value of the processor is returned regardless of the classifier. See retrier.Classifier for more details.
// To build the $backoff parameter the following functions can help: retrier.ConstantBackoff, retrier.ExponentialBackoff, retrier.LimitedExponentialBackoff.
func MiddlewareRetrier[Event any](backoff []time.Duration, classifier retrier.Classifier) CreateProcessorMiddlewareFunc[Event] {
	if classifier == nil {
		classifier = retrier.BlacklistClassifier{context.DeadlineExceeded, context.Canceled}
	}

	return func() ProcessorMiddlewareFunc[Event] {
		return func(next EventProcessorFunc[Event]) EventProcessorFunc[Event] {
			r := retrier.New(backoff, classifier)

			return func(ctx context.Context, event Event) error {
				return r.RunCtx(ctx, func(ctx context.Context) error {
					return next(ctx, event)
				})
			}
		}
	}
}

// MiddlewareTimeout sets the maximum duration for processing one event.
// If you want to have a global timeout, you can do something like this:
//
//	runner := New[myEvent](
//		[]EventProcessorFunc[myEvent]{
//			processor1,
//			processor2,
//		},
//		WithMultipleInstances[myEvent](4),
//		WithGlobalMiddleware[myEvent](
//			MiddlewareTimeout[myEvent](time.Minute),
//		),
//	)
//
// with this setup, all instances will time out after processing an event for longer than a minute,
// If you want to have a custom timeout for a specific processor, you can do something like this:
//
//	runner := New[myEvent](
//		[]EventProcessorFunc[myEvent]{
//			MiddlewareTimeout[myEvent](time.Second*10)(processor1),
//			processor2,
//		},
//		WithMultipleInstances[myEvent](4),
//		WithGlobalMiddleware[myEvent](
//			MiddlewareTimeout[myEvent](time.Minute),
//		),
//	)
//
// with this setup, all instances of processor1 will time out after processing an event for longer
// than 10 second, while processor2 instances will time out after a minute.
func MiddlewareTimeout[Event any](timeout time.Duration) CreateProcessorMiddlewareFunc[Event] {
	return func() ProcessorMiddlewareFunc[Event] {
		return func(next EventProcessorFunc[Event]) EventProcessorFunc[Event] {
			return func(ctx context.Context, event Event) error {
				ctx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				return next(ctx, event)
			}
		}
	}
}

// Chain creates a chain of middlewares linked together that act as a single EventProcessorFunc.
func (middlewares ProcessorMiddlewaresFunc[Event]) Chain(processor EventProcessorFunc[Event]) EventProcessorFunc[Event] {
	reversed := append([]ProcessorMiddlewareFunc[Event]{}, middlewares...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}

	chain := processor
	for _, middleware := range reversed {
		chain = middleware(chain)
	}
	return chain
}

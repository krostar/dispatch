package dispatch

import (
	"context"

	"go.uber.org/multierr"
)

type (
	// EventProcessor expose the provided event to all configured processors.
	// See Dispatcher.ProcessEvent for more information.
	EventProcessor[Event any] interface {
		ProcessEvent(ctx context.Context, event Event) error
	}

	// EventProcessorFunc defines the function signature to process an event.
	EventProcessorFunc[Event any] func(ctx context.Context, event Event) error
)

// EventProcessorChain creates a single EventProcessorFunc based on a list of EventProcessorFunc.
func EventProcessorChain[Event any](processor EventProcessorFunc[Event], processors ...EventProcessorFunc[Event]) EventProcessorFunc[Event] {
	return func(ctx context.Context, event Event) error {
		var errs []error
		for _, processor := range append([]EventProcessorFunc[Event]{processor}, processors...) {
			errs = append(errs, processor(ctx, event))
		}
		return multierr.Combine(errs...)
	}
}

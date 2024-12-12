package dispatch

import (
	"context"
	"fmt"
)

// EventTransformerFunc defines how to map an event type to another type.
type EventTransformerFunc[RawEvent, TransformedEvent any] func(in RawEvent) (out TransformedEvent, err error)

// NewEventTransformer takes a dispatcher compatible with TransformedEvent and create a dispatcher based on an event transformer that deal with RawEvent.
func NewEventTransformer[RawEvent, TransformedEvent any](transformer EventTransformerFunc[RawEvent, TransformedEvent], processor EventProcessor[TransformedEvent], opts ...Option[RawEvent]) *Dispatcher[RawEvent] {
	return New([]EventProcessorFunc[RawEvent]{
		func(ctx context.Context, in RawEvent) error {
			out, err := transformer(in)
			if err != nil {
				return fmt.Errorf("unable to transform event: %w", err)
			}
			return processor.ProcessEvent(ctx, out)
		},
	}, opts...)
}

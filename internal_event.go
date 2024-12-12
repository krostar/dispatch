package dispatch

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/multierr"
)

type processorInternalEvent[Event any] struct {
	m          sync.Mutex      // processor may push results concurrently, mutex is used to avoid race conditions
	ctx        context.Context //nolint:containedctx // ProcessEvent's context must be shared with processors
	underlying Event           // raw event provided to ProcessEvent

	remaining int        // number of results expected before final push to result
	errors    []error    // results of all processors
	result    chan error // once all processors are done, the overall result is shared through this channel
}

func createProcessorInternalEvent[Event any](ctx context.Context, event Event, nbProcessors int) (*processorInternalEvent[Event], chan error) {
	result := make(chan error, 1)
	return &processorInternalEvent[Event]{
		ctx:        ctx,
		underlying: event,
		remaining:  nbProcessors,
		errors:     make([]error, nbProcessors),
		result:     result,
	}, result
}

func (evt *processorInternalEvent[Event]) Context() context.Context { return evt.ctx }

func (evt *processorInternalEvent[Event]) Event() Event { return evt.underlying }

func (evt *processorInternalEvent[Event]) PushResult(pid int, err error) {
	evt.m.Lock()
	defer evt.m.Unlock()

	evt.errors[pid] = err
	evt.remaining--

	if evt.remaining == 0 {
		evt.result <- evt.error()
	}
}

func (evt *processorInternalEvent[Event]) error() error {
	var final []error

	for pid, err := range evt.errors {
		if err != nil {
			msg := "unable to process event"
			if len(evt.errors) > 1 {
				msg = fmt.Sprintf("%s by processor with pid %d", msg, pid+1)
			}
			final = append(final, fmt.Errorf("%s: %w", msg, err))
		}
	}

	return multierr.Combine(final...)
}

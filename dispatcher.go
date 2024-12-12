package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type (
	// Dispatcher exposes a way to run multiple pools of different processors
	// and a way to push event to those processors.
	Dispatcher[Event any] struct {
		m sync.RWMutex // used to prevent editing events while new events are pushed
		o options[Event]

		processors       []EventProcessorFunc[Event]
		events           chan *processorInternalEvent[Event]
		processorsEvents []chan *processorInternalEvent[Event]
	}

	// EventProcessor expose the provided event to all configured processors.
	// See Dispatcher.ProcessEvent for more information.
	EventProcessor[Event any] interface {
		ProcessEvent(ctx context.Context, event Event) error
	}

	// EventProcessorFunc defines the function signature to process an event.
	EventProcessorFunc[Event any] func(ctx context.Context, event Event) error
)

// New initializes a Dispatcher with provided processors and options.
func New[Event any](processors []EventProcessorFunc[Event], opts ...Option[Event]) *Dispatcher[Event] {
	o := options[Event]{instances: 1}
	for _, opt := range opts {
		opt(&o)
	}
	return &Dispatcher[Event]{o: o, processors: processors}
}

// Run starts processors and blocks until the provided context is errored. Because of that
// a context.Canceled error from ctx.Err() is considered a normal behavior and Run returns nil.
// While it is possible to run the service multiple times, it must only be executed once at a time.
func (s *Dispatcher[Event]) Run(ctx context.Context) error {
	{ // setup main and processor event channels
		s.m.Lock() // the lock is used to prevent race condition in the ProcessEvent func
		s.events = make(chan *processorInternalEvent[Event])

		s.processorsEvents = make([]chan *processorInternalEvent[Event], len(s.processors))
		for i := range len(s.processors) {
			s.processorsEvents[i] = make(chan *processorInternalEvent[Event])
		}
		s.m.Unlock()
	}

	broadcastStopped := s.startEventBroadcaster()
	processorStopped := s.startEventProcessors()

	// wait until context is done
	<-ctx.Done()
	// the context is done, we must quit

	{
		// here come some nice cleanup orchestration:
		// 		the lock is used to prevent "writing on closed channel" error in the ProcessEvent func
		s.m.Lock()
		// 		if we have the lock we can safely close the main event channel
		close(s.events)
		// 		setting this value to nil make future calls to the ProcessEvent func fail quickly
		s.events = nil
		// 		we can now let the ProcessEvent func be called again, we safely won't have any more events
		s.m.Unlock()
		//		by closing the main event channel, we closed the broadcaster main loop, so it should quit
		<-broadcastStopped
		close(broadcastStopped)
		//		since it returned we for sure know that no new processor events can be written
		for i, events := range s.processorsEvents {
			//	we can safely close all processor event channels
			close(events)
			s.processorsEvents[i] = nil
		}
		//		by closing each processor event channels, we closed the processors main loops, so they should quit
		<-processorStopped
		close(processorStopped)
		// we are done, everything is stopped, no goroutines should subsist
	}

	// it is expected that the context is canceled, if it's not the error then its unexpected
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// ProcessEvent pushes the provided event to each processor, wait for execution and return the execution result.
// If multiple processors (not multiple instances, but multiple functions) are provided, each provider will
// be executing the provided event, and the execution result will be the merge of all processor executions results.
// Run must be called prior to this function, otherwise processors won't be started and ProcessEvent will fail.
// The provided context is passed to underlying processors. In case the context is canceled before any processors
// are ready to process the task, the ProcessEvent function fails fast without calling processors. Otherwise,
// once at least one processor received the event the ProcessEvent function block until they all returns.
func (s *Dispatcher[Event]) ProcessEvent(ctx context.Context, event Event) error {
	// we can have as many caller in parallel
	// we need to prevent writes to s.events during this function lifecycle
	s.m.RLock()
	defer s.m.RUnlock()

	// if we call ProcessEvent before Run() is called, or after it has returned
	// either way, we cannot process that event
	if s.events == nil {
		return errors.New("event not processed: processors are stopped")
	}

	if s.o.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.o.timeout)
		defer cancel()
	}

	evt, result := createProcessorInternalEvent(ctx, event, len(s.processors))
	defer close(result)

	select {
	case <-ctx.Done():
		// s.events is an unbuffered channel, it is possible that it take time to be writeable
		return fmt.Errorf("event not processed: %w", ctx.Err())
	case s.events <- evt:
		return <-result
	}
}

// startEventBroadcaster broadcasts the main even channel to all processors sub channels.
func (s *Dispatcher[Event]) startEventBroadcaster() chan struct{} {
	stopped := make(chan struct{})

	go func() {
		if s.events != nil {
			// for each new message, propagate it to all event processor channels
			for event := range s.events {
				for _, events := range s.processorsEvents {
					events <- event
				}
			}
		}
		stopped <- struct{}{}
	}()

	return stopped
}

// startEventProcessors starts all processors handlers.
func (s *Dispatcher[Event]) startEventProcessors() chan struct{} {
	stopped := make(chan struct{})

	go func() {
		var wg sync.WaitGroup

		wg.Add(len(s.processors) * s.o.instances)
		getProcessorMiddlewares := s.o.getGlobalMiddlewares()

		for i := range s.processors {
			pid, events, getProcessorInstanceMiddleware := i, s.processorsEvents[i], getProcessorMiddlewares(s.processors[i])

			// run as many instances of the same processors as asked
			for range s.o.instances {
				processor := getProcessorInstanceMiddleware()
				go func() {
					defer wg.Done()
					for event := range events {
						event.PushResult(pid, processor(event.Context(), event.Event()))
					}
				}()
			}
		}

		wg.Wait()
		stopped <- struct{}{}
	}()

	return stopped
}

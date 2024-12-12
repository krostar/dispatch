package dispatch

import "time"

type (
	// Option defines the function signature of an option applier.
	Option[Event any] func(o *options[Event])

	options[Event any] struct {
		instances int
		timeout   time.Duration

		middlewaresOrder        []string
		middlewaresGlobal       []CreateProcessorMiddlewareFunc[Event]
		middlewaresPerProcessor []CreateProcessorMiddlewareFunc[Event]
		middlewaresPerInstances []CreateProcessorMiddlewareFunc[Event]
	}
)

// WithMultipleInstances specifies how many instances of the same processor will be started.
func WithMultipleInstances[Event any](instances int) Option[Event] {
	if instances <= 0 {
		instances = 1
	}
	return func(o *options[Event]) {
		o.instances = instances
	}
}

// WithTimeout specifies the maximum of time a given event can be processed before canceling processors.
// The valid is directly provided to context.WithTimeout.
func WithTimeout[Event any](timeout time.Duration) Option[Event] {
	return func(o *options[Event]) {
		o.timeout = timeout
	}
}

// WithGlobalMiddleware specifies a list of middleware to be chained together, initialized once.
// Meaning the created middleware instance is shared across all instances of all processors.
// Middleware order is preserved across all With*Middleware calls.
func WithGlobalMiddleware[Event any](middleware CreateProcessorMiddlewareFunc[Event], middlewares ...CreateProcessorMiddlewareFunc[Event]) Option[Event] {
	middlewares = append([]CreateProcessorMiddlewareFunc[Event]{middleware}, middlewares...)
	return func(o *options[Event]) {
		o.middlewaresGlobal = append(o.middlewaresGlobal, middlewares...)
		for range middlewares {
			o.middlewaresOrder = append(o.middlewaresOrder, "global")
		}
	}
}

// WithPerProcessorMiddleware specifies a list of middleware to be chained together, initialized once per processor type.
// Meaning the created middleware is shared across all instances of a same processor.
// Middleware order is preserved across all With*Middleware calls.
func WithPerProcessorMiddleware[Event any](middleware CreateProcessorMiddlewareFunc[Event], middlewares ...CreateProcessorMiddlewareFunc[Event]) Option[Event] {
	middlewares = append([]CreateProcessorMiddlewareFunc[Event]{middleware}, middlewares...)
	return func(o *options[Event]) {
		o.middlewaresPerProcessor = append(o.middlewaresPerProcessor, middlewares...)
		for range middlewares {
			o.middlewaresOrder = append(o.middlewaresOrder, "processor")
		}
	}
}

// WithPerInstanceMiddleware specifies a list of middleware to be chained together, initialized for each instance.
// Meaning the provided middleware is created for each processor instance.
// Middleware order is preserved across all With*Middleware calls.
func WithPerInstanceMiddleware[Event any](middleware CreateProcessorMiddlewareFunc[Event], middlewares ...CreateProcessorMiddlewareFunc[Event]) Option[Event] {
	middlewares = append([]CreateProcessorMiddlewareFunc[Event]{middleware}, middlewares...)
	return func(o *options[Event]) {
		o.middlewaresPerInstances = append(o.middlewaresPerInstances, middlewares...)
		for range middlewares {
			o.middlewaresOrder = append(o.middlewaresOrder, "instance")
		}
	}
}

func (o *options[Event]) getGlobalMiddlewares() func(processor EventProcessorFunc[Event]) func() EventProcessorFunc[Event] {
	var global ProcessorMiddlewaresFunc[Event]
	for _, middleware := range o.middlewaresGlobal {
		global = append(global, middleware())
	}

	return func(processor EventProcessorFunc[Event]) func() EventProcessorFunc[Event] {
		var perProcessor ProcessorMiddlewaresFunc[Event]
		for _, middleware := range o.middlewaresPerProcessor {
			perProcessor = append(perProcessor, middleware())
		}

		return func() EventProcessorFunc[Event] {
			var perInstance ProcessorMiddlewaresFunc[Event]
			for _, middleware := range o.middlewaresPerInstances {
				perInstance = append(perInstance, middleware())
			}

			var (
				chain           ProcessorMiddlewaresFunc[Event]
				globalIdx       int
				perProcessorIdx int
				perInstanceIdx  int
			)

			for _, order := range o.middlewaresOrder {
				switch order {
				case "global":
					chain = append(chain, global[globalIdx])
					globalIdx++
				case "processor":
					chain = append(chain, perProcessor[perProcessorIdx])
					perProcessorIdx++
				case "instance":
					chain = append(chain, perInstance[perInstanceIdx])
					perInstanceIdx++
				}
			}

			return chain.Chain(processor)
		}
	}
}

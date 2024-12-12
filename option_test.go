package dispatch

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func Test_WithMultipleInstances(t *testing.T) {
	type myEvent struct{}

	var o options[myEvent]

	WithMultipleInstances[myEvent](0)(&o)
	assert.Equal(t, o.instances, 1)

	WithMultipleInstances[myEvent](3)(&o)
	assert.Equal(t, o.instances, 3)
}

func Test_WithTimeout(t *testing.T) {
	type myEvent struct{}

	var o options[myEvent]

	WithTimeout[myEvent](time.Second)(&o)
	assert.Equal(t, o.timeout, time.Second)

	WithTimeout[myEvent](time.Minute)(&o)
	assert.Equal(t, o.timeout, time.Minute)
}

func Test_WithGlobalMiddleware(t *testing.T) {
	type myEvent struct{}

	var o options[myEvent]

	mid := func() ProcessorMiddlewareFunc[myEvent] {
		return func(EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
			return nil
		}
	}

	WithGlobalMiddleware(mid, mid)(&o)
	assert.DeepEqual(t, o.middlewaresOrder, []string{"global", "global"})
	assert.Equal(t, len(o.middlewaresGlobal), 2)
	WithGlobalMiddleware(mid)(&o)
	assert.DeepEqual(t, o.middlewaresOrder, []string{"global", "global", "global"})
	assert.Equal(t, len(o.middlewaresGlobal), 3)
}

func Test_WithPerProcessorMiddleware(t *testing.T) {
	type myEvent struct{}

	var o options[myEvent]

	mid := func() ProcessorMiddlewareFunc[myEvent] {
		return func(EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
			return nil
		}
	}

	WithPerProcessorMiddleware(mid, mid)(&o)
	assert.DeepEqual(t, o.middlewaresOrder, []string{"processor", "processor"})
	assert.Equal(t, len(o.middlewaresPerProcessor), 2)
	WithPerProcessorMiddleware(mid)(&o)
	assert.DeepEqual(t, o.middlewaresOrder, []string{"processor", "processor", "processor"})
	assert.Equal(t, len(o.middlewaresPerProcessor), 3)
}

func Test_WithPerInstanceMiddleware(t *testing.T) {
	type myEvent struct{}

	var o options[myEvent]

	mid := func() ProcessorMiddlewareFunc[myEvent] {
		return func(EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
			return nil
		}
	}

	WithPerInstanceMiddleware(mid, mid)(&o)
	assert.DeepEqual(t, o.middlewaresOrder, []string{"instance", "instance"})
	assert.Equal(t, len(o.middlewaresPerInstances), 2)
	WithPerInstanceMiddleware(mid)(&o)
	assert.DeepEqual(t, o.middlewaresOrder, []string{"instance", "instance", "instance"})
	assert.Equal(t, len(o.middlewaresPerInstances), 3)
}

func Test_options_middlewares(t *testing.T) {
	type myEvent struct{}

	var (
		o     options[myEvent]
		order []int
	)
	created := make(map[int]int)

	createMiddleware := func(id int) CreateProcessorMiddlewareFunc[myEvent] {
		return func() ProcessorMiddlewareFunc[myEvent] {
			created[id]++
			return func(p EventProcessorFunc[myEvent]) EventProcessorFunc[myEvent] {
				return func(ctx context.Context, event myEvent) error {
					order = append(order, id)
					return p(ctx, event)
				}
			}
		}
	}

	WithGlobalMiddleware(createMiddleware(1), createMiddleware(2))(&o)
	WithGlobalMiddleware(createMiddleware(3))(&o)
	WithPerProcessorMiddleware(createMiddleware(4))(&o)
	WithPerInstanceMiddleware(createMiddleware(5), createMiddleware(6))(&o)
	WithGlobalMiddleware(createMiddleware(7))(&o)
	WithPerProcessorMiddleware(createMiddleware(8))(&o)
	WithPerInstanceMiddleware(createMiddleware(9))(&o)

	getProcessorChain := o.getGlobalMiddlewares()
	processor1Chain := getProcessorChain(func(context.Context, myEvent) error {
		order = append(order, 101)
		return nil
	})
	processor2Chain := getProcessorChain(func(context.Context, myEvent) error {
		order = append(order, 102)
		return nil
	})

	assert.Check(t, len(order) == 0)
	assert.NilError(t, processor1Chain()(context.Background(), myEvent{}))
	assert.DeepEqual(t, order, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 101})
	assert.DeepEqual(t, created, map[int]int{1: 1, 2: 1, 3: 1, 4: 2, 5: 1, 6: 1, 7: 1, 8: 2, 9: 1})

	order = nil
	assert.NilError(t, processor2Chain()(context.Background(), myEvent{}))
	assert.DeepEqual(t, order, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 102})
	assert.DeepEqual(t, created, map[int]int{1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 1, 8: 2, 9: 2})

	order = nil
	assert.NilError(t, processor1Chain()(context.Background(), myEvent{}))
	assert.DeepEqual(t, order, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 101})
	assert.DeepEqual(t, created, map[int]int{1: 1, 2: 1, 3: 1, 4: 2, 5: 3, 6: 3, 7: 1, 8: 2, 9: 3})

	order = nil
	assert.NilError(t, processor2Chain()(context.Background(), myEvent{}))
	assert.DeepEqual(t, order, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 102})
	assert.DeepEqual(t, created, map[int]int{1: 1, 2: 1, 3: 1, 4: 2, 5: 4, 6: 4, 7: 1, 8: 2, 9: 4})
}

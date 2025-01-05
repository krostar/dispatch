package dispatch

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func Test_EventProcessorChain(t *testing.T) {
	p1 := EventProcessorFunc[*int](func(_ context.Context, event *int) error {
		*event++
		return nil
	})
	p2 := EventProcessorFunc[*int](func(_ context.Context, event *int) error {
		*event += 10
		return nil
	})
	p3 := EventProcessorFunc[*int](func(_ context.Context, event *int) error {
		*event += 100
		return nil
	})

	i := 0
	assert.NilError(t, EventProcessorChain(p1, p2, p3)(context.Background(), &i))
	assert.Equal(t, 111, i)
}

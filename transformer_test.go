package dispatch

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"gotest.tools/v3/assert"
)

func Test_NewEventTransformer(t *testing.T) {
	type (
		myEventA struct {
			Value int
		}
		myEventB struct {
			Value string
		}
	)

	var eventReceived []myEventB
	expectedErr := errors.New("boom")

	runnerB := New([]EventProcessorFunc[myEventB]{func(_ context.Context, event myEventB) error {
		eventReceived = append(eventReceived, event)
		return nil
	}})

	runnerA := NewEventTransformer[myEventA, myEventB](func(in myEventA) (myEventB, error) {
		if in.Value == -1 {
			return myEventB{}, expectedErr
		}
		return myEventB{Value: strconv.Itoa(in.Value)}, nil
	}, runnerB)

	ctx, cancel := context.WithCancel(context.Background())
	cerrA := RunRunner(ctx, runnerA)
	defer close(cerrA)
	cerrB := RunRunner(ctx, runnerB)
	defer close(cerrB)

	assert.NilError(t, runnerA.ProcessEvent(ctx, myEventA{Value: 1}))
	assert.NilError(t, runnerA.ProcessEvent(ctx, myEventA{Value: 2}))
	assert.NilError(t, runnerA.ProcessEvent(ctx, myEventA{Value: 3}))
	assert.ErrorIs(t, runnerA.ProcessEvent(ctx, myEventA{Value: -1}), expectedErr)
	cancel()
	assert.NilError(t, <-cerrA)
	assert.NilError(t, <-cerrB)
	assert.DeepEqual(t, []myEventB{{"1"}, {"2"}, {"3"}}, eventReceived)
}

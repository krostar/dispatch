package dispatch

import (
	"context"
	"errors"
	"testing"

	"gotest.tools/v3/assert"
)

func Test_processorInternalEvent(t *testing.T) {
	type myEvent struct{ ID int }

	ctx := context.WithValue(context.Background(), "key", "value")
	rawEvent := myEvent{ID: 4}

	event, result := createProcessorInternalEvent(ctx, rawEvent, 2)
	assert.Equal(t, event.Context().Value("key").(string), "value")
	assert.Equal(t, event.Event(), rawEvent)

	var ready bool
	select {
	case <-result:
		ready = true
	default:
		ready = false
	}
	assert.Check(t, !ready)
	event.PushResult(0, nil)
	select {
	case <-result:
		ready = true
	default:
		ready = false
	}
	assert.Check(t, !ready)
	event.PushResult(1, nil)
	assert.NilError(t, <-result)
	close(result)
}

func Test_processorInternalEvent_error(t *testing.T) {
	type myEvent struct{}

	expectedError := errors.New("boom")

	for name, test := range map[string]struct {
		err1   error
		err2   error
		assert func(t *testing.T, err error)
	}{
		"both nil": {
			assert: func(t *testing.T, err error) {
				assert.NilError(t, err)
			},
		},
		"one nil": {
			err2: expectedError,
			assert: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, expectedError)
				assert.ErrorContains(t, err, "unable to process event")
				assert.ErrorContains(t, err, "by processor with pid 2")
			},
		},
		"both err": {
			err1: errors.New("bim"),
			err2: expectedError,
			assert: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, expectedError)
				assert.ErrorContains(t, err, "unable to process event")
				assert.ErrorContains(t, err, "by processor with pid 1")
				assert.ErrorContains(t, err, "by processor with pid 2")
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			event, result := createProcessorInternalEvent(context.Background(), myEvent{}, 2)
			event.PushResult(0, test.err1)
			event.PushResult(1, test.err2)
			test.assert(t, <-result)
		})
	}
}

# dispatch

`dispatch` provides a generic, flexible event processing system. It allows you to define multiple event processors, distribute events to them concurrently, and manage their execution with various middleware options like circuit breakers, rate limiters, retries, and timeouts.

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/krostar/dispatch"
)

type MyEvent struct {
    Message string
}

func processor1(ctx context.Context, event MyEvent) error {
    fmt.Println("Processor 1:", event.Message)
    return nil
}

func processor2(ctx context.Context, event MyEvent) error {
    fmt.Println("Processor 2:", event.Message)
    time.Sleep(200 * time.Millisecond) // Simulate some work
    return nil
}

func main() {
    // Create a dispatcher with two processors
    dispatcher := dispatch.New[MyEvent](
        []dispatch.EventProcessorFunc[MyEvent]{processor1, processor2},
        dispatch.WithTimeout[MyEvent](time.Second), // Global timeout of 1 second
    )

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Start the dispatcher in a separate goroutine
    cerr := make(chan error)
    go func() { cerr <- dispatcher.Run(ctx) }()
    defer close(cerr)
    time.Sleep(100 * time.Millisecond)


    // Process some events
    err := dispatcher.ProcessEvent(ctx, MyEvent{Message: "Event 1"})
    if err != nil {
        fmt.Println("Error processing event:", err)
    }

    err = dispatcher.ProcessEvent(ctx, MyEvent{Message: "Event 2"})
    if err != nil {
        fmt.Println("Error processing event:", err)
    }

  cancel()
  fmt.Println("Dispatcher stopped with:", <- cerr)
}
```

package throttle_test

import (
	"context"
	"fmt"
	"time"

	"github.com/ivannguyendev/throttle-go/throttle"
)

func Example() {
	w, err := throttle.New[string]("sync:room:", throttle.Config{Window: 100 * time.Millisecond})
	if err != nil {
		panic(err)
	}
	defer w.Close()
	ctx := context.Background()
	syncRoom := func(_ context.Context, phase throttle.Phase) (string, error) {
		return "synced (" + phase.String() + ")", nil
	}

	ran, _ := w.Exec(ctx, "42", syncRoom)
	fmt.Println("first call ran now:", ran)

	ran, _ = w.Exec(ctx, "42", syncRoom)
	fmt.Println("second call ran now:", ran)

	room, _ := w.Wait(ctx, "42", syncRoom)
	fmt.Println("waited for:", room)
	// Output:
	// first call ran now: true
	// second call ran now: false
	// waited for: synced (trailing)
}

func ExampleWindow_Wait_timeout() {
	w, _ := throttle.New[string]("report:", throttle.Config{Window: time.Second})
	// Shutdown also waits for the run that outlived the caller's deadline.
	defer func() { _ = w.Shutdown(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := w.Wait(ctx, "daily", func(context.Context, throttle.Phase) (string, error) {
		time.Sleep(50 * time.Millisecond)
		return "report", nil
	})
	fmt.Println(err)
	// Output:
	// throttle: key "daily": context deadline exceeded
}

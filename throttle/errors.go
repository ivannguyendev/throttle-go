package throttle

import (
	"errors"
	"fmt"
	"strconv"
)

var (
	// ErrClosed reports that the Window was closed before the call could run,
	// or that Exec or Wait was called after Close.
	ErrClosed = errors.New("throttle: window closed")
	// ErrDropped reports that a pending call gave up after losing the window
	// race to other instances more than 10 times in a row.
	ErrDropped = errors.New("throttle: dropped after losing the window race")
	// ErrPanicked reports that the run covering the call panicked on a
	// goroutine owned by the Window. The panic value is in the message.
	ErrPanicked = errors.New("throttle: run panicked")
	// ErrInvalidArgument reports an invalid key prefix, Config, key or fn.
	ErrInvalidArgument = errors.New("throttle: invalid argument")
)

// KeyError attaches the throttle key to a runtime outcome: ErrClosed,
// ErrDropped, ErrPanicked, or the caller's context error.
// Match it with errors.Is(err, ErrDropped) or errors.AsType[*KeyError](err).
type KeyError struct {
	Key string
	Err error
}

// Error implements error.
func (e *KeyError) Error() string {
	return "throttle: key " + strconv.Quote(e.Key) + ": " + e.Err.Error()
}

// Unwrap returns the underlying cause.
func (e *KeyError) Unwrap() error { return e.Err }

func keyError(key string, err error) error {
	return &KeyError{Key: key, Err: err}
}

func invalidArgument(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, reason)
}

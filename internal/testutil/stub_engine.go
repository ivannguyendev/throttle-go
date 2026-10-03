// Package testutil holds test fakes shared by this module's tests.
package testutil

import (
	"context"
	"sync"

	"github.com/ivannguyendev/throttle-go/throttle"
)

// Op is the scheduler intent behind an engine call.
type Op string

// Engine call intents, as the Window maps them onto Engine methods.
const (
	OpAcquire Op = "acquire" // Set with NX
	OpExtend  Op = "extend"  // Set without NX
	OpGet     Op = "get"
	OpDel     Op = "del"
)

// Call records one engine call.
type Call struct {
	Op    Op
	Key   string
	Value string
	Opt   throttle.SetOptions
}

// Reply is a scripted answer. OK answers Set; Value/Found answer Get.
type Reply struct {
	OK    bool
	Value string
	Found bool
	Err   error
}

// StubEngine answers every call through Respond and records it first. A
// Respond that should hang must block on ctx.Done() (durably blocking for
// synctest) and return ctx.Err().
type StubEngine struct {
	Respond func(ctx context.Context, call Call) Reply

	mu    sync.Mutex
	calls []Call
}

// Calls returns a copy of the recorded calls.
func (e *StubEngine) Calls() []Call {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Call(nil), e.calls...)
}

// Count returns how many calls had op.
func (e *StubEngine) Count(op Op) int {
	n := 0
	for _, c := range e.Calls() {
		if c.Op == op {
			n++
		}
	}
	return n
}

func (e *StubEngine) do(ctx context.Context, call Call) Reply {
	e.mu.Lock()
	e.calls = append(e.calls, call)
	e.mu.Unlock()
	return e.Respond(ctx, call)
}

// Get implements throttle.Engine.
func (e *StubEngine) Get(ctx context.Context, key string) (string, bool, error) {
	r := e.do(ctx, Call{Op: OpGet, Key: key})
	return r.Value, r.Found, r.Err
}

// Set implements throttle.Engine.
func (e *StubEngine) Set(ctx context.Context, key, value string, opt throttle.SetOptions) (bool, error) {
	op := OpExtend
	if opt.NX {
		op = OpAcquire
	}
	r := e.do(ctx, Call{Op: op, Key: key, Value: value, Opt: opt})
	return r.OK, r.Err
}

// Del implements throttle.Engine.
func (e *StubEngine) Del(ctx context.Context, key string) error {
	return e.do(ctx, Call{Op: OpDel, Key: key}).Err
}

// Hang blocks until ctx ends, like an engine whose client never answers.
func Hang(ctx context.Context) Reply {
	<-ctx.Done()
	return Reply{Err: ctx.Err()}
}

package testutil

import (
	"context"
	"log/slog"
	"sync"
)

// Record is one captured log record, with attributes flattened to strings.
type Record struct {
	Level   slog.Level
	Message string
	Attrs   map[string]string
}

// Records collects what a recording logger wrote.
type Records struct {
	mu      sync.Mutex
	records []Record
}

// All returns a copy of every record.
func (r *Records) All() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Record(nil), r.records...)
}

// Find returns the records with message msg.
func (r *Records) Find(msg string) []Record {
	var out []Record
	for _, rec := range r.All() {
		if rec.Message == msg {
			out = append(out, rec)
		}
	}
	return out
}

// NewRecordingLogger returns a debug-level logger that captures records.
func NewRecordingLogger() (*slog.Logger, *Records) {
	records := &Records{}
	return slog.New(&recordingHandler{records: records}), records
}

type recordingHandler struct {
	records *Records
	attrs   []slog.Attr
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]string, r.NumAttrs()+len(h.attrs))
	for _, a := range h.attrs {
		attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.records.mu.Lock()
	h.records.records = append(h.records.records, Record{Level: r.Level, Message: r.Message, Attrs: attrs})
	h.records.mu.Unlock()
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &recordingHandler{records: h.records, attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...)}
}

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

// Package joblogs captures bounded command output separately from operational
// logs. Payloads are raw bytes and must never be written to the Runner logger.
package joblogs

import (
	"context"
	"io"
	"sync"
	"time"
)

const (
	MaxChunkBytes  = 4096
	MaxOutputBytes = 1 << 20
	MaxChunks      = 256
)

// Stream identifies the command output pipe.
type Stream int

const (
	Stdout Stream = 1
	Stderr Stream = 2
)

// Record carries one observed chunk or an empty final lifecycle marker.
// Content belongs to the record and is never reused by the capture writer.
type Record struct {
	JobID       string
	Sequence    uint64
	ObservedAt  time.Time
	Stream      Stream
	Content     []byte
	EndOfOutput bool
	Truncated   bool
	Incomplete  bool
}

// Sink delivers records synchronously. Implementations must honor context and
// apply a deadline, including to a final record sent during shutdown.
type Sink interface {
	Append(context.Context, Record) error
}

// Capture serializes stdout and stderr observations with bounded memory and
// no background queue. Slow delivery applies backpressure to the Docker CLI.
// A failed delivery suppresses further data calls; Close still attempts a final
// incomplete marker. Cross-pipe ordering reflects observation, not write time.
type Capture struct {
	mu             sync.Mutex
	ctx            context.Context
	sink           Sink
	jobID          string
	now            func() time.Time
	sequence       uint64
	bytes          int
	chunks         int
	truncated      bool
	incomplete     bool
	deliveryFailed bool
	closed         bool
}

// NewCapture creates the bounded capture for one dispatched Job.
func NewCapture(ctx context.Context, jobID string, sink Sink) *Capture {
	return &Capture{ctx: ctx, jobID: jobID, sink: sink, now: time.Now}
}

// Writer returns a pipe writer safe to use concurrently with the other stream.
// Excess output is drained without retention so the command can finish.
func (c *Capture) Writer(stream Stream) io.Writer {
	return streamWriter{capture: c, stream: stream}
}

type streamWriter struct {
	capture *Capture
	stream  Stream
}

func (w streamWriter) Write(p []byte) (int, error) {
	c := w.capture
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(p)
	for len(p) > 0 && !c.closed && !c.deliveryFailed {
		if c.chunks == MaxChunks || c.bytes == MaxOutputBytes {
			c.truncated = true
			break
		}
		size := min(len(p), MaxChunkBytes, MaxOutputBytes-c.bytes)
		c.sequence++
		record := Record{JobID: c.jobID, Sequence: c.sequence, ObservedAt: c.now().UTC(), Stream: w.stream, Content: append([]byte(nil), p[:size]...)}
		c.bytes += size
		c.chunks++
		if c.sink != nil && c.sink.Append(c.ctx, record) != nil {
			c.deliveryFailed = true
			c.incomplete = true
		}
		p = p[size:]
	}
	return n, nil
}

// MarkIncomplete records a failure in the output source without changing the
// command outcome. It is safe to call while a pipe writer is active.
func (c *Capture) MarkIncomplete() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.incomplete = true
}

// Close attempts exactly one final marker. interrupted also covers a Docker
// output-stream failure; it does not alter the command's execution outcome.
// ctx may be detached from a cancelled execution; the Sink bounds delivery.
// A returned error means the final marker was not acknowledged.
func (c *Capture) Close(ctx context.Context, interrupted bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.sequence++
	if c.sink != nil {
		return c.sink.Append(ctx, Record{JobID: c.jobID, Sequence: c.sequence, ObservedAt: c.now().UTC(), EndOfOutput: true, Truncated: c.truncated, Incomplete: c.incomplete || interrupted})
	}
	return nil
}

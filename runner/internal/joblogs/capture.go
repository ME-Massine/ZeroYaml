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
// Failed chunks are not retried; delivery stops after two failures so a stalled
// ingress cannot consume the Job deadline one chunk at a time.
// One pending chunk coalesces writes until full, a stream switch, or Close.
// Cross-pipe ordering reflects observation, not write time.
type Capture struct {
	mu               sync.Mutex
	delivery         chan struct{}
	cancel           context.CancelFunc
	ctx              context.Context
	sink             Sink
	jobID            string
	now              func() time.Time
	sequence         uint64
	bytes            int
	chunks           int
	pending          []byte
	pendingStream    Stream
	pendingAt        time.Time
	deliveryFailures int
	truncated        bool
	incomplete       bool
	closed           bool
}

// NewCapture creates the bounded capture for one dispatched Job.
func NewCapture(ctx context.Context, jobID string, sink Sink) *Capture {
	deliveryCtx, cancel := context.WithCancel(ctx)
	capture := &Capture{ctx: deliveryCtx, cancel: cancel, jobID: jobID, sink: sink, now: time.Now, delivery: make(chan struct{}, 1)}
	capture.delivery <- struct{}{}
	return capture
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
	n := len(p)
	// Serialize delivery without holding the state lock over network I/O.
	select {
	case <-c.delivery:
		defer func() { c.delivery <- struct{}{} }()
	case <-c.ctx.Done():
		c.mu.Lock()
		c.incomplete = true
		c.truncated = c.truncated || n > 0
		c.mu.Unlock()
		return n, nil
	}
	if c.ctx.Err() != nil {
		c.mu.Lock()
		c.incomplete = true
		c.truncated = c.truncated || n > 0
		c.mu.Unlock()
		return n, nil
	}
	for len(p) > 0 {
		c.mu.Lock()
		if c.closed || c.ctx.Err() != nil {
			c.incomplete = true
			c.truncated = true
			c.mu.Unlock()
			break
		}
		if c.deliveryFailures >= 2 || c.chunks == MaxChunks || c.bytes == MaxOutputBytes {
			c.truncated = true
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()
		if len(c.pending) > 0 && c.pendingStream != w.stream {
			c.flush(c.ctx)
			continue
		}
		if len(c.pending) == 0 {
			c.pending = make([]byte, 0, MaxChunkBytes)
			c.pendingStream = w.stream
			c.pendingAt = c.now().UTC()
		}
		size := min(len(p), MaxChunkBytes-len(c.pending), MaxOutputBytes-c.bytes)
		c.pending = append(c.pending, p[:size]...)
		c.bytes += size
		p = p[size:]
		if len(c.pending) == MaxChunkBytes {
			c.flush(c.ctx)
		}
	}
	return n, nil
}

// flush requires the delivery token. A record owns its bytes after delivery;
// the next pending buffer never reuses them. No state lock spans a Sink call.
func (c *Capture) flush(ctx context.Context) {
	if len(c.pending) == 0 {
		return
	}
	c.mu.Lock()
	c.sequence++
	c.chunks++
	record := Record{JobID: c.jobID, Sequence: c.sequence, ObservedAt: c.pendingAt, Stream: c.pendingStream, Content: c.pending}
	c.pending = nil
	c.mu.Unlock()
	if c.sink != nil && c.sink.Append(ctx, record) != nil {
		c.mu.Lock()
		c.deliveryFailures++
		c.incomplete = true
		c.truncated = true
		c.mu.Unlock()
	}
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
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	// Cancel in-flight delivery before waiting for ordering. State changes do
	// not wait for a network deadline; ctx bounds the delivery drain.
	c.cancel()
	c.mu.Unlock()
	select {
	case <-c.delivery:
		defer func() { c.delivery <- struct{}{} }()
	case <-ctx.Done():
		return ctx.Err()
	}
	c.flush(ctx)
	c.mu.Lock()
	c.sequence++
	record := Record{JobID: c.jobID, Sequence: c.sequence, ObservedAt: c.now().UTC(), EndOfOutput: true, Truncated: c.truncated, Incomplete: c.incomplete || interrupted}
	c.mu.Unlock()
	if c.sink != nil {
		return c.sink.Append(ctx, record)
	}
	return nil
}

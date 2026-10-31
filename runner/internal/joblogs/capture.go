// Package joblogs captures bounded command output separately from operational
// logs. Payloads are raw bytes and must never be written to the Runner logger.
package joblogs

import (
	"context"
	"io"
	"sync"
	"time"
)

// Per-Job capture limits are shared with the Control Plane log contract.
const (
	MaxChunkBytes  = 4096
	MaxOutputBytes = 1 << 20
	MaxChunks      = 256
)

// maxPendingAge bounds how long pending output waits for a full chunk once more
// output arrives. Aged output is sent on the next write to either stream; no
// timer flushes it, so a Job silent for longer than the Control Plane's
// five-minute retention idle limit may still lose its retained prefix under
// capacity pressure. The age is high enough to protect the chunk budget: 256
// chunks still cover more than two hours of slow output on both streams.
const maxPendingAge = time.Minute

// maxConsecutiveFailures pauses data delivery once the ingress has failed this
// many chunks in a row. Any acknowledgment resets the count; a send cancelled
// by the capture's own context is not an ingress failure.
const maxConsecutiveFailures = 2

// deliveryCooldown is how long data delivery stays paused after
// maxConsecutiveFailures. Output observed meanwhile, and any pending chunk
// flushed meanwhile, is dropped without a network call; the first chunk sent
// after the cooldown is a probe whose success resumes delivery and whose
// failure pauses it again. A stalled ingress therefore costs at most one call
// bounded by ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT (5 seconds by default) per
// 30 seconds, while transient refusals such as RESOURCE_EXHAUSTED during a
// retention burst, or a short network interruption, lose only the output
// observed during one cooldown.
const deliveryCooldown = 30 * time.Second

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
// Failed chunks are not retried. After two consecutive ingress failures,
// delivery pauses for deliveryCooldown and then probes with the next chunk, so
// a stalled ingress cannot consume the Job deadline one chunk at a time and a
// recovered one is used again. Sends cancelled locally do not count.
//
// Each stream coalesces writes into its own pending chunk, which is sent when
// full, when a write arrives after it has waited maxPendingAge, or on Close.
// A stream switch does not send a partial chunk: interleaved line output, such
// as shell traces on stderr between stdout lines, would otherwise spend one
// chunk per line and exhaust the chunk budget long before the byte budget.
// Cross-stream order is therefore kept at chunk granularity only. No timer
// flushes idle output.
type Capture struct {
	mu                  sync.Mutex
	delivery            chan struct{}
	cancel              context.CancelFunc
	ctx                 context.Context
	sink                Sink
	jobID               string
	now                 func() time.Time
	sequence            uint64
	bytes               int
	chunks              int
	pending             [2]pendingChunk
	consecutiveFailures int
	retryAfter          time.Time
	dropped             int
	truncated           bool
	incomplete          bool
	closed              bool
}

// pendingChunk is the not yet sent output of one stream. observedAt is the
// observation time of its first byte.
type pendingChunk struct {
	content    []byte
	observedAt time.Time
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
		return c.drainStopped(n)
	}
	if c.ctx.Err() != nil {
		return c.drainStopped(n)
	}
	c.flushAged(c.ctx)
	pending := &c.pending[w.stream-1]
	for len(p) > 0 {
		c.mu.Lock()
		if c.closed || c.ctx.Err() != nil {
			c.incomplete = true
			c.truncated = true
			c.mu.Unlock()
			break
		}
		// A chunk is counted when it starts, so the pending chunks of both
		// streams can never exceed the chunk budget once they are sent.
		if c.coolingDown() || c.bytes == MaxOutputBytes ||
			(len(pending.content) == 0 && c.chunks == MaxChunks) {
			c.truncated = true
			c.mu.Unlock()
			break
		}
		if len(pending.content) == 0 {
			c.chunks++
			pending.content = make([]byte, 0, MaxChunkBytes)
			pending.observedAt = c.now().UTC()
		}
		size := min(len(p), MaxChunkBytes-len(pending.content), MaxOutputBytes-c.bytes)
		c.bytes += size
		c.mu.Unlock()
		pending.content = append(pending.content, p[:size]...)
		p = p[size:]
		if len(pending.content) == MaxChunkBytes {
			c.flush(c.ctx, w.stream)
		}
	}
	return n, nil
}

// drainStopped accepts n bytes written after delivery stopped without retaining
// them, so the command can still finish.
func (c *Capture) drainStopped(n int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.incomplete = true
	c.truncated = c.truncated || n > 0
	return n, nil
}

// flushAged sends every pending chunk that has waited maxPendingAge, oldest
// first. It requires the delivery token.
func (c *Capture) flushAged(ctx context.Context) {
	for _, stream := range c.pendingByAge() {
		if c.now().Sub(c.pending[stream-1].observedAt) >= maxPendingAge {
			c.flush(ctx, stream)
		}
	}
}

// pendingByAge lists streams with pending output, oldest first byte first.
// It requires the delivery token.
func (c *Capture) pendingByAge() []Stream {
	streams := make([]Stream, 0, len(c.pending))
	for _, stream := range []Stream{Stdout, Stderr} {
		if len(c.pending[stream-1].content) > 0 {
			streams = append(streams, stream)
		}
	}
	if len(streams) == 2 && c.pending[Stderr-1].observedAt.Before(c.pending[Stdout-1].observedAt) {
		streams[0], streams[1] = streams[1], streams[0]
	}
	return streams
}

// flush requires the delivery token. A record owns its bytes after delivery;
// the next pending chunk never reuses them. No state lock spans a Sink call.
// During a delivery cooldown, the chunk is dropped without a network call and
// the output is marked truncated. Sequence numbers are assigned only to sent
// chunks, so they stay contiguous across drops.
//
// A send that fails because ctx itself ended, through StopDelivery, the drain
// bound, the Job deadline, or the pending-chunk share of Close, is lost but does not count as an ingress
// failure: it says nothing about the ingress, and counting it could start a
// cooldown that makes Close drop its pending chunks unsent. A timeout inside
// the Sink's own per-call deadline leaves ctx intact and still counts.
func (c *Capture) flush(ctx context.Context, stream Stream) {
	pending := &c.pending[stream-1]
	if len(pending.content) == 0 {
		return
	}
	record := Record{JobID: c.jobID, ObservedAt: pending.observedAt, Stream: stream, Content: pending.content}
	pending.content = nil
	c.mu.Lock()
	if c.coolingDown() {
		c.dropped++
		c.truncated = true
		c.mu.Unlock()
		return
	}
	c.sequence++
	record.Sequence = c.sequence
	c.mu.Unlock()
	if c.sink == nil {
		return
	}
	err := c.sink.Append(ctx, record)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.incomplete = true
		c.truncated = true
		if ctx.Err() != nil {
			return
		}
		c.consecutiveFailures++
		if c.consecutiveFailures >= maxConsecutiveFailures {
			c.retryAfter = c.now().Add(deliveryCooldown)
		}
		return
	}
	c.consecutiveFailures = 0
}

// coolingDown reports whether data delivery is paused. It requires mu.
func (c *Capture) coolingDown() bool {
	return c.consecutiveFailures >= maxConsecutiveFailures && c.now().Before(c.retryAfter)
}

// RemainingChunks reports how many data chunks the capture may still send,
// including pending ones. It bounds the delivery work left after command exit.
// Every started chunk is eventually sent, dropped, or still pending.
func (c *Capture) RemainingChunks() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return MaxChunks - int(c.sequence) - c.dropped
}

// MarkIncomplete records a failure in the output source without changing the
// command outcome. It is safe to call while a pipe writer is active.
func (c *Capture) MarkIncomplete() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.incomplete = true
}

// StopDelivery cancels in-flight and later chunk delivery and marks the output
// incomplete. Later writes are drained without retention; Close still flushes
// the pending chunks and sends the final marker under its own context.
func (c *Capture) StopDelivery() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.incomplete = true
	c.cancel()
}

// Close attempts exactly one final marker. interrupted also covers a Docker
// output-stream failure; it does not alter the command's execution outcome.
// ctx may be detached from a cancelled execution; the Sink bounds delivery.
// A returned error means the final marker was not acknowledged.
//
// Pending chunks are sent before the marker because the Control Plane refuses
// records after a final marker, and the marker's flags must reflect their
// delivery. They share only half of the time left in ctx, so a slow chunk
// send cannot leave the marker without a window.
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
	flushCtx, cancelFlush := pendingFlushContext(ctx)
	for _, stream := range c.pendingByAge() {
		c.flush(flushCtx, stream)
	}
	cancelFlush()
	c.mu.Lock()
	c.sequence++
	record := Record{JobID: c.jobID, Sequence: c.sequence, ObservedAt: c.now().UTC(), EndOfOutput: true, Truncated: c.truncated, Incomplete: c.incomplete || interrupted}
	c.mu.Unlock()
	if c.sink != nil {
		return c.sink.Append(ctx, record)
	}
	return nil
}

// pendingFlushContext bounds the pending-chunk sends in Close to the first half
// of the time left in ctx, reserving the rest for the final marker. Without a
// deadline on ctx, the Sink's per-call deadline bounds each send instead.
func pendingFlushContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, deadline.Add(-time.Until(deadline)/2))
}

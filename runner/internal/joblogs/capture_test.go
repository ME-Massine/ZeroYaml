package joblogs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type recordingSink struct {
	records []Record
	failAt  uint64
	fail    map[uint64]bool
}

func (s *recordingSink) Append(ctx context.Context, record Record) error {
	s.records = append(s.records, record)
	if record.Sequence == s.failAt || s.fail[record.Sequence] {
		return errors.New("delivery failed")
	}
	return nil
}

func TestCapturePreservesStreamsRawBytesAndObservationSequence(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "job-1", sink)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	capture.now = func() time.Time { return now }
	input := []byte{'a', 0xff, '\n'}
	capture.Writer(Stdout).Write(input)
	input[0] = 'x'
	capture.Writer(Stderr).Write([]byte("error without newline"))
	capture.Close(context.Background(), false)
	capture.Close(context.Background(), false)
	if len(sink.records) != 3 {
		t.Fatalf("records = %d, want 3", len(sink.records))
	}
	for i, record := range sink.records {
		if record.JobID != "job-1" || record.Sequence != uint64(i+1) || !record.ObservedAt.Equal(now) {
			t.Fatalf("record metadata: %+v", record)
		}
	}
	if !bytes.Equal(sink.records[0].Content, []byte{'a', 0xff, '\n'}) || sink.records[0].Stream != Stdout || sink.records[1].Stream != Stderr {
		t.Fatal("streams or raw bytes lost")
	}
	if last := sink.records[2]; !last.EndOfOutput || last.Truncated || last.Incomplete || len(last.Content) != 0 {
		t.Fatalf("final marker: %+v", last)
	}
}

func TestCaptureBoundsLargeOutputAndContinuesDraining(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "large-job", sink)
	input := bytes.Repeat([]byte("x"), MaxOutputBytes*3)
	n, err := capture.Writer(Stdout).Write(input)
	if n != len(input) || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	capture.Close(context.Background(), false)
	if len(sink.records) != MaxChunks+1 {
		t.Fatalf("retained %d chunks", len(sink.records))
	}
	total := 0
	for _, record := range sink.records {
		if len(record.Content) > MaxChunkBytes {
			t.Fatal("oversized chunk")
		}
		total += len(record.Content)
	}
	if total != MaxOutputBytes || !sink.records[MaxChunks].Truncated {
		t.Fatal("output budget or truncation lost")
	}
}

func TestCaptureBoundsChunkCountAcrossBothStreams(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "tiny-writes", sink)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	capture.now = func() time.Time { return now }
	// Every write ages the pending chunks, so each one forces partial chunks.
	for i := 0; i < MaxChunks*2; i++ {
		capture.Writer(Stream(i%2 + 1)).Write([]byte("x"))
		now = now.Add(maxPendingAge)
	}
	capture.Close(context.Background(), false)
	if len(sink.records) != MaxChunks+1 || !sink.records[MaxChunks].Truncated {
		t.Fatalf("chunk count not bounded: %d records", len(sink.records))
	}
	for i, record := range sink.records {
		if record.Sequence != uint64(i+1) {
			t.Fatal("concurrent sequence not contiguous")
		}
	}
}

func TestDeliveryFailureAllowsLaterDataAndMarksFinalIncomplete(t *testing.T) {
	sink := &recordingSink{failAt: 1}
	capture := NewCapture(context.Background(), "job", sink)
	capture.Writer(Stdout).Write(make([]byte, MaxOutputBytes*2))
	capture.Writer(Stderr).Write([]byte("drained"))
	capture.Close(context.Background(), false)
	if len(sink.records) != MaxChunks+1 || !sink.records[MaxChunks].EndOfOutput || !sink.records[MaxChunks].Incomplete || !sink.records[MaxChunks].Truncated || sink.records[MaxChunks].Sequence != MaxChunks+1 {
		t.Fatal("delivery failure lifecycle lost")
	}
}

func TestEmptyAndInterruptedCapturesHaveFinalMarkers(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		sink := &recordingSink{}
		capture := NewCapture(context.Background(), "job", sink)
		capture.Close(context.Background(), interrupted)
		if len(sink.records) != 1 || !sink.records[0].EndOfOutput || sink.records[0].Incomplete != interrupted {
			t.Fatal("empty capture marker lost")
		}
	}
}

func TestCaptureRecoversAfterSingleFailedChunk(t *testing.T) {
	sink := &recordingSink{failAt: 1}
	capture := NewCapture(context.Background(), "job", sink)
	capture.Writer(Stdout).Write([]byte("lost"))
	capture.Writer(Stderr).Write([]byte("delivered"))
	capture.Close(context.Background(), false)
	if len(sink.records) != 3 || string(sink.records[1].Content) != "delivered" || !sink.records[2].Incomplete || !sink.records[2].Truncated {
		t.Fatal("delivery did not recover with explicit loss metadata")
	}
}

type sinkFunc func(context.Context, Record) error

func (f sinkFunc) Append(ctx context.Context, record Record) error { return f(ctx, record) }

func TestCloseCancelsBlockedDeliveryAndMarkIncompleteDoesNotWait(t *testing.T) {
	entered := make(chan struct{})
	finished := make(chan Record, 1)
	capture := NewCapture(context.Background(), "job", sinkFunc(func(ctx context.Context, record Record) error {
		if record.EndOfOutput {
			finished <- record
			return nil
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}))
	written := make(chan struct{})
	go func() { capture.Writer(Stdout).Write(make([]byte, MaxChunkBytes)); close(written) }()
	<-entered
	marked := make(chan struct{})
	go func() { capture.MarkIncomplete(); close(marked) }()
	select {
	case <-marked:
	case <-time.After(time.Second):
		t.Fatal("state update blocked on network")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := capture.Close(ctx, false); err != nil {
		t.Fatal(err)
	}
	<-written
	if final := <-finished; !final.Incomplete || !final.Truncated || final.Sequence != 2 {
		t.Fatalf("final = %+v", final)
	}
}

func TestCaptureCoalescesShortLines(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "lines", sink)
	var input bytes.Buffer
	for i := 0; i < 3000; i++ {
		line := []byte(fmt.Sprintf("line %04d\n", i))
		input.Write(line)
		capture.Writer(Stdout).Write(line)
	}
	capture.Close(context.Background(), false)
	var output bytes.Buffer
	for _, record := range sink.records {
		output.Write(record.Content)
	}
	if !bytes.Equal(output.Bytes(), input.Bytes()) || len(sink.records)-1 != (input.Len()+MaxChunkBytes-1)/MaxChunkBytes || sink.records[len(sink.records)-1].Truncated {
		t.Fatal("short lines lost or not coalesced")
	}
}

func TestCaptureFlushesSlowOutputOnceItReachesPendingAge(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "slow", sink)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	capture.now = func() time.Time { return now }
	capture.Writer(Stdout).Write([]byte("first\n"))
	now = start.Add(maxPendingAge - time.Second)
	capture.Writer(Stdout).Write([]byte("second\n"))
	if len(sink.records) != 0 {
		t.Fatalf("writes within the pending age were not coalesced: %+v", sink.records)
	}
	now = start.Add(maxPendingAge)
	capture.Writer(Stdout).Write([]byte("third\n"))
	if len(sink.records) != 1 || string(sink.records[0].Content) != "first\nsecond\n" || !sink.records[0].ObservedAt.Equal(start) {
		t.Fatalf("aged pending output was not delivered as its own record: %+v", sink.records)
	}
	capture.Close(context.Background(), false)
	if len(sink.records) != 3 || string(sink.records[1].Content) != "third\n" || !sink.records[1].ObservedAt.Equal(now) || sink.records[2].Incomplete {
		t.Fatalf("records after the aged flush = %+v", sink.records)
	}
}

func TestStopDeliveryCancelsBlockedChunkAndDrainsLaterOutput(t *testing.T) {
	entered := make(chan struct{})
	var records []Record
	capture := NewCapture(context.Background(), "stopped", sinkFunc(func(ctx context.Context, record Record) error {
		records = append(records, record)
		if record.EndOfOutput || record.Sequence > 1 {
			return nil
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}))
	written := make(chan struct{})
	go func() { capture.Writer(Stdout).Write(make([]byte, MaxChunkBytes)); close(written) }()
	<-entered
	capture.StopDelivery()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("stopped delivery left the writer blocked")
	}
	if n, err := capture.Writer(Stderr).Write([]byte("late")); n != 4 || err != nil {
		t.Fatalf("late write = %d, %v", n, err)
	}
	if err := capture.Close(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || !records[1].EndOfOutput || !records[1].Incomplete || !records[1].Truncated {
		t.Fatalf("records = %+v", records)
	}
}

func TestCaptureInterleavedLinesDoNotExhaustChunkBudget(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "interleaved", sink)
	inputs := map[Stream]*bytes.Buffer{Stdout: {}, Stderr: {}}
	// Shell traces on stderr between stdout lines switch streams on every line.
	for i := 0; i < 3000; i++ {
		stream := Stream(i%2 + 1)
		line := []byte(fmt.Sprintf("line %04d\n", i))
		inputs[stream].Write(line)
		capture.Writer(stream).Write(line)
	}
	capture.Close(context.Background(), false)
	outputs := map[Stream]*bytes.Buffer{Stdout: {}, Stderr: {}}
	for _, record := range sink.records[:len(sink.records)-1] {
		outputs[record.Stream].Write(record.Content)
	}
	want := 0
	for _, stream := range []Stream{Stdout, Stderr} {
		if !bytes.Equal(outputs[stream].Bytes(), inputs[stream].Bytes()) {
			t.Fatalf("stream %d bytes changed", stream)
		}
		want += (inputs[stream].Len() + MaxChunkBytes - 1) / MaxChunkBytes
	}
	final := sink.records[len(sink.records)-1]
	if len(sink.records)-1 != want || final.Truncated || final.Incomplete {
		t.Fatalf("data records = %d, want %d; final = %+v", len(sink.records)-1, want, final)
	}
}

func TestCloseSendsPendingStreamsInFirstObservationOrder(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "ordered", sink)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	capture.now = func() time.Time { return now }
	capture.Writer(Stderr).Write([]byte("+ mvn verify\n"))
	now = start.Add(time.Second)
	capture.Writer(Stdout).Write([]byte("[INFO] BUILD\n"))
	capture.Close(context.Background(), false)
	if len(sink.records) != 3 || sink.records[0].Stream != Stderr || !sink.records[0].ObservedAt.Equal(start) ||
		sink.records[1].Stream != Stdout || !sink.records[1].ObservedAt.Equal(now) {
		t.Fatalf("records = %+v", sink.records)
	}
}

func TestCaptureResetsDeliveryFailuresAfterAcknowledgment(t *testing.T) {
	sink := &recordingSink{fail: map[uint64]bool{1: true, 3: true, 5: true}}
	capture := NewCapture(context.Background(), "recovering", sink)
	capture.Writer(Stdout).Write(make([]byte, 10*MaxChunkBytes))
	capture.Close(context.Background(), false)
	if len(sink.records) != 11 {
		t.Fatalf("isolated failures stopped delivery: %d records", len(sink.records))
	}
	if final := sink.records[10]; !final.EndOfOutput || !final.Incomplete || !final.Truncated {
		t.Fatalf("final = %+v", final)
	}
}

func TestCaptureStopsDeliveryAfterConsecutiveFailures(t *testing.T) {
	sink := &recordingSink{fail: map[uint64]bool{1: true, 2: true}}
	capture := NewCapture(context.Background(), "stalled", sink)
	capture.Writer(Stderr).Write([]byte("pending on the other stream"))
	capture.Writer(Stdout).Write(make([]byte, 10*MaxChunkBytes))
	capture.Close(context.Background(), false)
	// The pending stderr chunk is dropped without another network call.
	if len(sink.records) != 3 || sink.records[2].Sequence != 3 || !sink.records[2].EndOfOutput || !sink.records[2].Truncated {
		t.Fatalf("records = %+v", sink.records)
	}
}

func TestRemainingChunksCountsUnsentChunks(t *testing.T) {
	capture := NewCapture(context.Background(), "remaining", &recordingSink{})
	capture.Writer(Stdout).Write(make([]byte, 3*MaxChunkBytes+1))
	if got := capture.RemainingChunks(); got != MaxChunks-3 {
		t.Fatalf("remaining = %d, want %d", got, MaxChunks-3)
	}
}

func TestCloseFlushesPendingBytesAfterExecutionCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sink := &recordingSink{}
	capture := NewCapture(ctx, "cancel", sink)
	capture.Writer(Stderr).Write([]byte("diagnostics"))
	cancel()
	capture.Close(context.Background(), true)
	if len(sink.records) != 2 || string(sink.records[0].Content) != "diagnostics" || !sink.records[1].EndOfOutput || !sink.records[1].Incomplete {
		t.Fatal("pending output lost on cancellation")
	}
}

func TestConcurrentCapturePreservesEachStreamsBytes(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "concurrent", sink)
	var writers sync.WaitGroup
	for _, stream := range []Stream{Stdout, Stderr} {
		writers.Go(func() {
			for i := 0; i < 100; i++ {
				capture.Writer(stream).Write([]byte{byte(stream)})
			}
		})
	}
	writers.Wait()
	capture.Close(context.Background(), false)
	counts := map[Stream]int{}
	for i, record := range sink.records {
		if record.Sequence != uint64(i+1) {
			t.Fatal("sequence changed")
		}
		for _, b := range record.Content {
			if b != byte(record.Stream) {
				t.Fatal("stream bytes changed")
			}
			counts[record.Stream]++
		}
	}
	if counts[Stdout] != 100 || counts[Stderr] != 100 {
		t.Fatal("concurrent output lost")
	}
}

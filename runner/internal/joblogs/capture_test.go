package joblogs

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordingSink struct {
	records []Record
	failAt  uint64
}

func (s *recordingSink) Append(ctx context.Context, record Record) error {
	s.records = append(s.records, record)
	if record.Sequence == s.failAt {
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

func TestCaptureBoundsTinyWritesAcrossBothStreams(t *testing.T) {
	sink := &recordingSink{}
	capture := NewCapture(context.Background(), "tiny-writes", sink)
	var writers sync.WaitGroup
	for _, stream := range []Stream{Stdout, Stderr} {
		writers.Go(func() {
			for i := 0; i < MaxChunks; i++ {
				capture.Writer(stream).Write([]byte("x"))
			}
		})
	}
	writers.Wait()
	capture.Close(context.Background(), false)
	if len(sink.records) != MaxChunks+1 || !sink.records[MaxChunks].Truncated {
		t.Fatal("chunk count not bounded")
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
	go func() { capture.Writer(Stdout).Write([]byte("pending")); close(written) }()
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

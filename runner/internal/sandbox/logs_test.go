package sandbox

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
)

type logSink struct {
	records  []joblogs.Record
	onAppend func(joblogs.Record)
}

func (s *logSink) Append(_ context.Context, r joblogs.Record) error {
	s.records = append(s.records, r)
	if s.onAppend != nil {
		s.onAppend(r)
	}
	return nil
}

func TestRunCapturesBothStreamsForSucceededAndFailedCommands(t *testing.T) {
	for _, code := range []string{"0", "42"} {
		t.Run(code, func(t *testing.T) {
			docker := newFakeDocker()
			docker.exitCodes["job-id"] = code
			sink := &logSink{}
			s := newTestSandbox(docker)
			s.logs = sink
			calls := 0
			s.stream = func(ctx context.Context, stdout, stderr io.Writer, args ...string) error {
				calls++
				if !slices.Equal(args, []string{"logs", "--follow", "job-id"}) {
					t.Fatalf("stream args: %v", args)
				}
				stdout.Write([]byte("stdout\n"))
				stderr.Write([]byte("stderr\n"))
				return nil
			}
			result := s.Run(context.Background(), testSpec())
			if result.ExitCode == nil || (code == "42" && result.Outcome != OutcomeFailed) || (code == "0" && result.Outcome != OutcomeSucceeded) {
				t.Fatalf("result: %+v", result)
			}
			if calls != 1 || len(sink.records) != 3 {
				t.Fatalf("stream calls = %d, records = %d", calls, len(sink.records))
			}
			if sink.records[0].Stream != joblogs.Stdout || sink.records[1].Stream != joblogs.Stderr || !sink.records[2].EndOfOutput || sink.records[2].Incomplete {
				t.Fatal("output lifecycle lost")
			}
			docker.assertCleanedUp(t, "checkout-id", "job-id")
		})
	}
}

func TestRunOutputFailureKeepsCommandOutcomeAndMarksIncomplete(t *testing.T) {
	docker := newFakeDocker()
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	s.stream = func(_ context.Context, stdout, stderr io.Writer, _ ...string) error {
		stdout.Write([]byte("partial"))
		return errors.New("stream connection lost")
	}
	result := s.Run(context.Background(), testSpec())
	if result.Outcome != OutcomeSucceeded || !sink.records[len(sink.records)-1].Incomplete {
		t.Fatalf("result = %+v, marker = %+v", result, sink.records)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunCancellationDuringCaptureCleansResourcesAndMarksIncomplete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	docker := newFakeDocker()
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	s.stream = func(streamCtx context.Context, stdout, stderr io.Writer, _ ...string) error {
		stdout.Write([]byte("before cancellation"))
		cancel()
		<-streamCtx.Done()
		return streamCtx.Err()
	}
	// The scripted wait honors cancellation just like the real Docker CLI.
	docker.blockOnWait["job-id"] = true
	result := s.Run(ctx, testSpec())
	if result.Outcome != OutcomeCancelled {
		t.Fatalf("outcome = %s", result.Outcome)
	}
	if !sink.records[len(sink.records)-1].Incomplete {
		t.Fatal("cancellation not marked")
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunTimeoutDuringCaptureCleansResourcesAndMarksIncomplete(t *testing.T) {
	docker := newFakeDocker()
	docker.blockOnWait["job-id"] = true
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	s.stream = func(ctx context.Context, stdout, stderr io.Writer, _ ...string) error {
		<-ctx.Done()
		return ctx.Err()
	}
	spec := testSpec()
	spec.Timeout = -time.Nanosecond
	result := s.Run(context.Background(), spec)
	if result.Outcome != OutcomeTimedOut || !sink.records[len(sink.records)-1].Incomplete {
		t.Fatalf("result = %+v", result)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

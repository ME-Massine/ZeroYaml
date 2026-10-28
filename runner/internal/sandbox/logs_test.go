package sandbox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
)

type logSink struct {
	records  []joblogs.Record
	failData bool
}

func (s *logSink) Append(_ context.Context, r joblogs.Record) error {
	s.records = append(s.records, r)
	if s.failData && !r.EndOfOutput {
		return context.DeadlineExceeded
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
			s.stream = func(ctx context.Context, stdout, stderr io.Writer, containerID string) error {
				calls++
				if containerID != "job-id" {
					t.Fatalf("stream container: %s", containerID)
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
	s.stream = func(_ context.Context, stdout, stderr io.Writer, _ string) error {
		stdout.Write([]byte("partial"))
		return errors.New("stream connection lost")
	}
	result := s.Run(context.Background(), testSpec())
	if result.Outcome != OutcomeSucceeded || !sink.records[len(sink.records)-1].Incomplete {
		t.Fatalf("result = %+v, marker = %+v", result, sink.records)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunStalledLogDeliveryDoesNotConsumeJobDeadline(t *testing.T) {
	docker := newFakeDocker()
	sink := &logSink{failData: true}
	s := newTestSandbox(docker)
	s.logs = sink
	s.stream = func(_ context.Context, stdout, stderr io.Writer, _ string) error {
		_, err := stdout.Write(make([]byte, joblogs.MaxOutputBytes))
		return err
	}
	result := s.Run(context.Background(), testSpec())
	if result.Outcome != OutcomeSucceeded || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("result = %+v", result)
	}
	if len(sink.records) != 3 || !sink.records[2].EndOfOutput || !sink.records[2].Incomplete || !sink.records[2].Truncated {
		t.Fatalf("stalled delivery records = %+v", sink.records)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestCommandExitWinsWhenLogDeliveryReachesExecutionDeadline(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	docker := newFakeDocker()
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	waitFinished := make(chan struct{})
	originalDocker := s.docker
	s.docker = func(ctx context.Context, args ...string) (string, error) {
		output, err := originalDocker(ctx, args...)
		if args[0] == "wait" {
			close(waitFinished)
		}
		return output, err
	}
	s.stream = func(ctx context.Context, stdout, stderr io.Writer, _ string) error {
		<-waitFinished
		cancel()
		return ctx.Err()
	}
	capture := joblogs.NewCapture(runCtx, "job", sink)
	id := newResourceNames(testExecutionID)
	code, err := s.runContainer(context.Background(), runCtx, testExecutionID, id.job, jobCreateArgs(testSpec(), id, true), capture)
	if err != nil || code != 0 {
		t.Fatalf("command exit = %d, %v", code, err)
	}
	if err := capture.Close(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 1 || !sink.records[0].EndOfOutput || !sink.records[0].Incomplete {
		t.Fatalf("final marker = %+v", sink.records)
	}
	if !slices.Contains(docker.commandSummary(), "rm job-id") {
		t.Fatal("job container was not removed")
	}
}

func TestRunCancellationDuringCaptureCleansResourcesAndMarksIncomplete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	docker := newFakeDocker()
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	s.stream = func(streamCtx context.Context, stdout, stderr io.Writer, _ string) error {
		stderr.Write([]byte("failure diagnostics before cancellation"))
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
	if len(sink.records) < 2 || sink.records[0].Stream != joblogs.Stderr || string(sink.records[0].Content) != "failure diagnostics before cancellation" {
		t.Fatalf("stderr before cancellation was lost: %+v", sink.records)
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
	s.stream = func(ctx context.Context, stdout, stderr io.Writer, _ string) error {
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

type blockingLogSink struct{}

func (blockingLogSink) Append(ctx context.Context, _ joblogs.Record) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRunBoundsLogCloseAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	docker := newFakeDocker()
	docker.blockOnWait["job-id"] = true
	s := newTestSandbox(docker)
	s.logs = blockingLogSink{}
	s.logCloseTimeout = 20 * time.Millisecond
	s.stream = func(streamCtx context.Context, stdout, stderr io.Writer, _ string) error {
		stderr.Write([]byte("pending diagnostic"))
		cancel()
		<-streamCtx.Done()
		return streamCtx.Err()
	}
	result := make(chan Result, 1)
	go func() { result <- s.Run(ctx, testSpec()) }()
	select {
	case got := <-result:
		if got.Outcome != OutcomeCancelled {
			t.Fatalf("result = %+v", got)
		}
	case <-time.After(s.logCloseTimeout + time.Second):
		t.Fatal("log close delayed terminal status beyond cancellation bound")
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunWithoutLogDeliveryDoesNotFollowOrWarn(t *testing.T) {
	docker := newFakeDocker()
	s := newTestSandbox(docker)
	var output bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&output, nil))
	s.stream = func(context.Context, io.Writer, io.Writer, string) error {
		t.Error("disabled log follower invoked")
		return errors.New("unsupported")
	}
	if result := s.Run(context.Background(), testSpec()); result.Outcome != OutcomeSucceeded {
		t.Fatalf("result = %+v", result)
	}
	if output.Len() != 0 {
		t.Fatalf("unexpected operational logs: %s", output.String())
	}
}

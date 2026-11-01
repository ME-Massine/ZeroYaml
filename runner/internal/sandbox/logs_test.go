package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
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
	code, err := s.runContainer(context.Background(), runCtx, testExecutionID, id.job, jobCreateArgs(testSpec(), id, true), &jobOutput{capture: capture})
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

func TestRunBoundsLogCloseWithoutCancellation(t *testing.T) {
	for name, timeout := range map[string]time.Duration{"completed": time.Minute, "timed out": 20 * time.Millisecond} {
		t.Run(name, func(t *testing.T) {
			docker := newFakeDocker()
			docker.blockOnWait["job-id"] = name == "timed out"
			s := newTestSandbox(docker)
			s.logs = blockingLogSink{}
			s.logFinalTimeout = 20 * time.Millisecond
			s.logCloseTimeout = time.Hour
			s.stream = func(_ context.Context, stdout, stderr io.Writer, _ string) error {
				stdout.Write([]byte("pending output"))
				stderr.Write([]byte("pending diagnostic"))
				return nil
			}
			spec := testSpec()
			spec.Timeout = timeout
			result := make(chan Result, 1)
			go func() { result <- s.Run(context.Background(), spec) }()
			select {
			case got := <-result:
				if got.Outcome == OutcomeCancelled {
					t.Fatalf("result = %+v", got)
				}
			case <-time.After(5 * time.Second):
				// The blocking sink never answers, so only the close bound ends Run.
				t.Fatal("log close delayed terminal status beyond its bound")
			}
			docker.assertCleanedUp(t, "checkout-id", "job-id")
		})
	}
}

type closeLogSink func(context.Context, joblogs.Record) error

func (s closeLogSink) Append(ctx context.Context, record joblogs.Record) error { return s(ctx, record) }

func TestRunBoundsLogCloseWhenShutdownStartsDuringDelivery(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			var records []joblogs.Record
			s := newTestSandbox(newFakeDocker())
			s.logFinalTimeout = time.Minute
			s.logCloseTimeout = 20 * time.Millisecond
			s.logs = closeLogSink(func(callCtx context.Context, record joblogs.Record) error {
				records = append(records, record)
				if pending && record.EndOfOutput {
					if callCtx.Err() != nil {
						t.Error("pending send consumed the final marker's shutdown window")
					}
					return callCtx.Err()
				}
				once.Do(func() { close(entered) })
				select {
				case <-callCtx.Done():
					return callCtx.Err()
				case <-release:
					return nil
				}
			})
			s.stream = func(_ context.Context, stdout, _ io.Writer, _ string) error {
				if pending {
					_, err := stdout.Write([]byte("pending output"))
					return err
				}
				return nil
			}
			result := make(chan Result, 1)
			done := make(chan struct{})
			go func() {
				result <- s.Run(ctx, testSpec())
				close(done)
			}()
			defer func() { close(release); <-done }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("log close did not start")
			}
			cancel()
			select {
			case got := <-result:
				if got.Outcome != OutcomeSucceeded {
					t.Fatalf("log cancellation changed command result: %+v", got)
				}
				last := records[len(records)-1]
				if !last.EndOfOutput || (pending && (!last.Incomplete || !last.Truncated)) {
					t.Fatalf("final marker = %+v", last)
				}
			case <-time.After(time.Second):
				t.Fatal("shutdown during close retained the normal close timeout")
			}
		})
	}
}

// slowLogSink acknowledges every record after a delay, like a healthy but
// slow ingress.
type slowLogSink struct {
	mu      sync.Mutex
	delay   time.Duration
	records []joblogs.Record
}

func (s *slowLogSink) Append(ctx context.Context, r joblogs.Record) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r)
	return nil
}

func TestRunBoundsLogDrainAfterCommandExit(t *testing.T) {
	docker := newFakeDocker()
	docker.exitCodes["job-id"] = "3"
	sink := &slowLogSink{delay: 20 * time.Millisecond}
	s := newTestSandbox(docker)
	s.logs = sink
	s.logDrainTimeout = 50 * time.Millisecond
	s.stream = func(ctx context.Context, stdout, _ io.Writer, _ string) error {
		for ctx.Err() == nil {
			stdout.Write(make([]byte, joblogs.MaxChunkBytes))
		}
		return ctx.Err()
	}
	started := time.Now()
	result := s.Run(context.Background(), testSpec())
	if elapsed := time.Since(started); elapsed > s.logDrainTimeout+time.Second {
		t.Fatalf("Run took %s; slow log delivery was not bounded after command exit", elapsed)
	}
	if result.Outcome != OutcomeFailed || result.ExitCode == nil || *result.ExitCode != 3 {
		t.Fatalf("result = %+v", result)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	final := sink.records[len(sink.records)-1]
	if !final.EndOfOutput || !final.Incomplete || len(sink.records) > joblogs.MaxChunks {
		t.Fatalf("final marker = %+v after %d records", final, len(sink.records))
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestLogDeliveryAfterCommandExitIsBoundedToFortySeconds(t *testing.T) {
	s := newTestSandbox(newFakeDocker())
	// The documented post-exit bound is the drain plus the final flush.
	if got := s.logDrainTimeout + s.logFinalTimeout; got != 40*time.Second {
		t.Fatalf("post-exit log delivery bound = %s, want 40s", got)
	}
}

func TestRunWaitFailureStopsFollowerWithoutWaitingForDrainBound(t *testing.T) {
	docker := newFakeDocker()
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	s.logDrainTimeout = time.Hour
	var output bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&output, nil))
	written := make(chan struct{})
	s.docker = func(ctx context.Context, args ...string) (string, error) {
		if args[0] == "wait" && args[1] == "job-id" {
			<-written
			return "", errors.New("docker wait: exit status 1: error during connect")
		}
		return docker.run(ctx, args...)
	}
	// The container is still running, so its follower only ends when stopped.
	s.stream = func(ctx context.Context, stdout, _ io.Writer, _ string) error {
		stdout.Write([]byte("still running\n"))
		close(written)
		<-ctx.Done()
		return fmt.Errorf("docker output stream stopped: %w", ctx.Err())
	}
	result := make(chan Result, 1)
	go func() { result <- s.Run(context.Background(), testSpec()) }()
	select {
	case got := <-result:
		if got.Outcome != OutcomeExecutionError || !strings.Contains(got.Message, "error during connect") {
			t.Fatalf("result = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a failed wait waited for the log drain bound")
	}
	if strings.Contains(output.String(), "job log drain exceeded its bound") || strings.Contains(output.String(), "job log capture interrupted") {
		t.Fatalf("expected follower stop was logged as a log failure: %s", output.String())
	}
	if len(sink.records) != 2 || string(sink.records[0].Content) != "still running\n" || !sink.records[1].EndOfOutput || !sink.records[1].Incomplete {
		t.Fatalf("records = %+v", sink.records)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunSendsNoFinalMarkerBeforeTheJobContainerStarts(t *testing.T) {
	for name, configure := range map[string]func(*fakeDocker){
		"workspace failure": func(d *fakeDocker) { d.failures["volume create"] = errors.New("docker volume: exit status 1") },
		"checkout failure":  func(d *fakeDocker) { d.exitCodes["checkout-id"] = "128" },
		"job create failure": func(d *fakeDocker) {
			d.failures["create zeroyaml-"+testExecutionID+"-job"] = errors.New("docker create: exit status 1")
		},
	} {
		t.Run(name, func(t *testing.T) {
			docker := newFakeDocker()
			configure(docker)
			sink := &logSink{}
			s := newTestSandbox(docker)
			s.logs = sink
			s.stream = func(context.Context, io.Writer, io.Writer, string) error {
				t.Error("follower started without a Job container")
				return nil
			}
			if result := s.Run(context.Background(), testSpec()); result.Outcome != OutcomeExecutionError {
				t.Fatalf("result = %+v", result)
			}
			if len(sink.records) != 0 {
				t.Fatalf("records for a command that never ran = %+v", sink.records)
			}
		})
	}
}

func TestRunMarksOutputIncompleteWhenTheJobContainerStartFails(t *testing.T) {
	docker := newFakeDocker()
	docker.failures["start job-id"] = errors.New("docker start: exit status 1")
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	if result := s.Run(context.Background(), testSpec()); result.Outcome != OutcomeExecutionError {
		t.Fatalf("result = %+v", result)
	}
	// A failed start may still have run the command, so completeness is reported.
	if len(sink.records) != 1 || !sink.records[0].EndOfOutput || !sink.records[0].Incomplete {
		t.Fatalf("records = %+v", sink.records)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestLogCloseTimeoutStaysWithinShutdownBudget(t *testing.T) {
	for shutdown, want := range map[time.Duration]time.Duration{
		5 * time.Second:        time.Second,
		time.Minute:            time.Second,
		time.Second:            200 * time.Millisecond,
		100 * time.Millisecond: 20 * time.Millisecond,
	} {
		if got := New("docker", slog.New(slog.DiscardHandler), nil, shutdown).logCloseTimeout; got != want {
			t.Errorf("log close timeout for %s shutdown = %s, want %s", shutdown, got, want)
		}
	}
}

func TestRunTimeoutStopsFollowerWithoutCaptureWarning(t *testing.T) {
	docker := newFakeDocker()
	docker.blockOnWait["job-id"] = true
	sink := &logSink{}
	s := newTestSandbox(docker)
	s.logs = sink
	var output bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&output, nil))
	s.stream = func(ctx context.Context, stdout, _ io.Writer, _ string) error {
		stdout.Write([]byte("partial\n"))
		<-ctx.Done()
		return fmt.Errorf("docker output stream stopped: %w", ctx.Err())
	}
	spec := testSpec()
	spec.Timeout = 50 * time.Millisecond
	if result := s.Run(context.Background(), spec); result.Outcome != OutcomeTimedOut {
		t.Fatalf("result = %+v", result)
	}
	if strings.Contains(output.String(), "job log capture interrupted") {
		t.Fatalf("expected deadline stop was logged as a capture failure: %s", output.String())
	}
	if final := sink.records[len(sink.records)-1]; !final.EndOfOutput || !final.Incomplete {
		t.Fatalf("final = %+v", final)
	}
}

func TestRunStreamFailureStillWarns(t *testing.T) {
	docker := newFakeDocker()
	s := newTestSandbox(docker)
	s.logs = &logSink{}
	var output bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&output, nil))
	s.stream = func(context.Context, io.Writer, io.Writer, string) error {
		return errors.New("docker output stream failed")
	}
	if result := s.Run(context.Background(), testSpec()); result.Outcome != OutcomeSucceeded {
		t.Fatalf("result = %+v", result)
	}
	if !strings.Contains(output.String(), "job log capture interrupted") {
		t.Fatalf("stream failure was not logged: %s", output.String())
	}
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

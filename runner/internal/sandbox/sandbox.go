// Package sandbox executes one Job inside isolated Docker containers.
//
// Every execution owns three Docker resources, all named after a random
// execution identifier and labelled io.zeroyaml.managed=true:
//
//  1. a workspace volume, mounted at /workspace;
//  2. a checkout container that fetches the repository revision into the
//     volume with git;
//  3. the Job container, which runs the dispatched command with the dispatched
//     working directory.
//
// The Runner never runs the Job command on its host. It only drives the Docker
// CLI with explicit argument vectors, so no shell on the host interprets
// request content.
//
// Every resource is removed before Run returns, whether the command succeeded,
// failed, timed out, or was cancelled. Removal uses its own bounded context, so
// a cancelled execution still cleans up.
package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
)

// defaultCleanupTimeout bounds each removal command. Removal runs after the
// execution context may already be done, so it needs a budget of its own.
const defaultCleanupTimeout = 30 * time.Second

// defaultLogDrainTimeout bounds log delivery that is still running after the
// Job command has exited. It leaves room for a few slow chunk acknowledgments
// without letting delivery hold the container until the execution deadline.
const defaultLogDrainTimeout = 10 * time.Second

// maxLogCloseTimeout caps the final log flush after a cancellation. The bound is
// also limited to a fifth of the Runner shutdown budget, so the log close never
// consumes the time reserved for container cleanup and the terminal status
// report, even with a short ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT.
const maxLogCloseTimeout = time.Second

// Outcome categorizes how an execution ended.
type Outcome string

const (
	// OutcomeSucceeded is a command that exited with code 0.
	OutcomeSucceeded Outcome = "succeeded"

	// OutcomeFailed is a command that ran to completion with a non-zero code.
	OutcomeFailed Outcome = "failed"

	// OutcomeTimedOut is an execution stopped because it exceeded Spec.Timeout.
	OutcomeTimedOut Outcome = "timed_out"

	// OutcomeCancelled is an execution stopped because its caller cancelled it,
	// for example during Runner shutdown.
	OutcomeCancelled Outcome = "cancelled"

	// OutcomeExecutionError is an execution the Runner could not carry out, for
	// example because Docker was unreachable or the checkout failed.
	OutcomeExecutionError Outcome = "execution_error"
)

// Result describes how one execution ended.
type Result struct {
	Outcome Outcome

	// ExitCode is the Job command's exit code. It is nil when the command did
	// not run to completion, so an absent code is never mistaken for success.
	ExitCode *int32

	// Message is an operator-facing diagnostic. It never contains command
	// output or the repository location.
	Message string

	// ExecutionID names the Docker resources of this execution, so operators
	// can correlate Runner logs with leftover resources.
	ExecutionID string
}

// dockerFunc runs one Docker CLI command and returns its standard output.
type dockerFunc func(ctx context.Context, args ...string) (string, error)

// Sandbox runs Jobs in Docker containers. It holds no per-execution state, so
// one instance may run concurrent executions.
type Sandbox struct {
	docker          dockerFunc
	stream          dockerStreamFunc
	logs            joblogs.Sink
	newID           func() (string, error)
	cleanupTimeout  time.Duration
	logCloseTimeout time.Duration
	logDrainTimeout time.Duration
	logger          *slog.Logger
}

// New creates a Sandbox that drives the Docker CLI found at dockerBinary and
// delivers bounded Job output to logs. A nil sink disables log following.
// shutdownTimeout is the Runner's shutdown budget; it bounds the final log
// flush after a cancellation.
func New(dockerBinary string, logger *slog.Logger, logs joblogs.Sink, shutdownTimeout time.Duration) *Sandbox {
	s := newSandbox(cliDocker(dockerBinary), newExecutionID, defaultCleanupTimeout, logger)
	s.stream = cliDockerStream(dockerBinary)
	s.logs = logs
	s.logCloseTimeout = logCloseTimeout(shutdownTimeout)
	return s
}

func logCloseTimeout(shutdownTimeout time.Duration) time.Duration {
	return min(maxLogCloseTimeout, shutdownTimeout/5)
}

func newSandbox(
	docker dockerFunc,
	newID func() (string, error),
	cleanupTimeout time.Duration,
	logger *slog.Logger,
) *Sandbox {
	return &Sandbox{
		docker:          docker,
		newID:           newID,
		cleanupTimeout:  cleanupTimeout,
		logCloseTimeout: maxLogCloseTimeout,
		logDrainTimeout: defaultLogDrainTimeout,
		logger:          logger,
	}
}

// Run executes spec and reports how it ended. It never returns before every
// Docker resource it created has been removed or its removal has failed and
// been logged.
//
// Cancelling ctx stops the execution and yields OutcomeCancelled; exceeding
// spec.Timeout yields OutcomeTimedOut.
func (s *Sandbox) Run(ctx context.Context, spec Spec) Result {
	runCtx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()
	capture := joblogs.NewCapture(runCtx, spec.JobID, s.logs)
	interrupted := false
	defer func() {
		closeCtx := context.WithoutCancel(ctx)
		if ctx.Err() != nil {
			var closeCancel context.CancelFunc
			closeCtx, closeCancel = context.WithTimeout(closeCtx, s.logCloseTimeout)
			defer closeCancel()
		}
		if err := capture.Close(closeCtx, interrupted); err != nil {
			s.logger.WarnContext(ctx, "job output final marker was not acknowledged")
		}
	}()
	executionID, err := s.newID()
	if err != nil {
		interrupted = true
		return Result{Outcome: OutcomeExecutionError, Message: fmt.Sprintf("generate execution identifier: %v", err)}
	}

	result := s.run(ctx, runCtx, executionID, spec, capture)
	result.ExecutionID = executionID
	interrupted = interrupted || result.Outcome == OutcomeCancelled || result.Outcome == OutcomeTimedOut || result.Outcome == OutcomeExecutionError

	return result
}

func (s *Sandbox) run(ctx context.Context, runCtx context.Context, executionID string, spec Spec, capture *joblogs.Capture) Result {
	names := newResourceNames(executionID)

	if _, err := s.docker(runCtx, volumeCreateArgs(names)...); err != nil {
		// A failed create never proves that nothing was created: the daemon may
		// have created the volume and then lost the response, for example when
		// the CLI was stopped or the connection broke. Removal by name is
		// therefore attempted for every create failure.
		s.removeAfterFailedCreate(ctx, executionID, "workspace volume", "volume", "rm", "--force", names.volume)

		return s.interrupted(ctx, runCtx, spec, "workspace creation", err)
	}
	defer s.remove(ctx, executionID, "workspace volume", "volume", "rm", "--force", names.volume)

	checkoutExitCode, err := s.runContainer(ctx, runCtx, executionID, names.checkout, checkoutCreateArgs(spec, names), nil)
	if err != nil {
		return s.interrupted(ctx, runCtx, spec, "source checkout", err)
	}
	if checkoutExitCode != 0 {
		return Result{
			Outcome: OutcomeExecutionError,
			Message: fmt.Sprintf(
				"source checkout exited with code %d; verify that the repository location and revision are reachable from the runner",
				checkoutExitCode,
			),
		}
	}

	exitCode, err := s.runContainer(ctx, runCtx, executionID, names.job, jobCreateArgs(spec, names, s.logs != nil), capture)
	if err != nil {
		return s.interrupted(ctx, runCtx, spec, "job command", err)
	}

	if exitCode != 0 {
		return Result{
			Outcome:  OutcomeFailed,
			ExitCode: &exitCode,
			Message:  fmt.Sprintf("job command exited with code %d", exitCode),
		}
	}

	return Result{Outcome: OutcomeSucceeded, ExitCode: &exitCode, Message: "job command exited with code 0"}
}

// runContainer creates, starts, and waits for one container, then removes it.
// Containers are removed explicitly instead of with --rm so that docker wait
// can always read the exit code before the container disappears.
func (s *Sandbox) runContainer(
	ctx context.Context,
	runCtx context.Context,
	executionID string,
	name string,
	createArgs []string,
	capture *joblogs.Capture,
) (int32, error) {
	output, err := s.docker(runCtx, createArgs...)
	if err != nil {
		// The container may exist even though the create call failed: a create
		// stopped by the deadline or a cancellation during a slow image pull, or
		// one whose response was lost, still leaves the named container behind.
		// It is therefore removed by name after every create failure.
		s.removeAfterFailedCreate(ctx, executionID, "container", "rm", "--force", "--volumes", name)

		return 0, err
	}

	containerID := strings.TrimSpace(output)
	if containerID == "" {
		return 0, errors.New("docker create returned no container ID")
	}
	defer s.remove(ctx, executionID, "container", "rm", "--force", "--volumes", containerID)

	if _, err := s.docker(runCtx, "start", containerID); err != nil {
		return 0, err
	}
	// Observe the command exit independently of log delivery, which can still
	// be stalled when a successful command reaches its execution deadline.
	type waitResult struct {
		output string
		err    error
	}
	wait := make(chan waitResult, 1)
	go func() {
		output, err := s.docker(runCtx, "wait", containerID)
		wait <- waitResult{output, err}
	}()
	var followed chan error
	followCtx, stopFollowing := context.WithCancel(runCtx)
	defer stopFollowing()
	if s.logs != nil && capture != nil && s.stream != nil {
		// Following after start also reads output from a command that already
		// exited. Checkout output is excluded because it can contain credentials.
		followed = make(chan error, 1)
		go func() {
			followed <- s.stream(followCtx, capture.Writer(joblogs.Stdout), capture.Writer(joblogs.Stderr), containerID)
		}()
	}

	result := <-wait
	if followed != nil {
		s.drainLogs(ctx, executionID, followed, stopFollowing, capture)
	}
	output, err = result.output, result.err
	if err != nil {
		return 0, err
	}

	exitCode, err := strconv.ParseInt(strings.TrimSpace(output), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("docker wait returned an unreadable exit code: %w", err)
	}

	return int32(exitCode), nil
}

// drainLogs waits for the follower after the command has exited. Slow but
// successful delivery must not hold the container, the concurrency slot, and
// the terminal status report until spec.Timeout, so the drain is bounded and
// output still unread when the bound expires is reported as incomplete.
func (s *Sandbox) drainLogs(
	ctx context.Context,
	executionID string,
	followed <-chan error,
	stopFollowing context.CancelFunc,
	capture *joblogs.Capture,
) {
	timer := time.NewTimer(s.logDrainTimeout)
	defer timer.Stop()
	var err error
	select {
	case err = <-followed:
	case <-timer.C:
		capture.StopDelivery()
		stopFollowing()
		<-followed
		s.logger.WarnContext(ctx, "job log drain exceeded its bound", slog.String("execution_id", executionID),
			slog.Duration("log_drain_timeout", s.logDrainTimeout))
		return
	}
	if err != nil {
		capture.MarkIncomplete()
		s.logger.WarnContext(ctx, "job log capture interrupted", slog.String("execution_id", executionID))
	}
}

// interrupted classifies an execution that stopped before a command exited.
// Cancellation is checked before the deadline because a Runner shutdown must
// be reported as a cancellation even if the time budget also ran out.
func (s *Sandbox) interrupted(ctx context.Context, runCtx context.Context, spec Spec, stage string, err error) Result {
	if ctx.Err() != nil {
		return Result{Outcome: OutcomeCancelled, Message: fmt.Sprintf("execution cancelled during %s", stage)}
	}

	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return Result{
			Outcome: OutcomeTimedOut,
			Message: fmt.Sprintf("execution exceeded its %s time budget during %s", spec.Timeout, stage),
		}
	}

	return Result{Outcome: OutcomeExecutionError, Message: fmt.Sprintf("%s failed: %v", stage, err)}
}

// remove deletes one Docker resource with a context detached from the
// execution, so cleanup still runs after a cancellation or timeout. A failed
// removal is logged with the execution identifier for manual cleanup; it never
// changes the execution result.
func (s *Sandbox) remove(ctx context.Context, executionID string, resource string, args ...string) {
	s.removeResource(ctx, slog.LevelWarn, executionID, resource, args...)
}

// removeAfterFailedCreate removes a resource that a failed docker create may
// have left behind. The named resource often does not exist, because the create
// failed before the daemon acted, so a failed removal here is expected and is
// recorded at debug level rather than as a warning.
func (s *Sandbox) removeAfterFailedCreate(ctx context.Context, executionID string, resource string, args ...string) {
	s.removeResource(ctx, slog.LevelDebug, executionID, resource, args...)
}

func (s *Sandbox) removeResource(
	ctx context.Context,
	level slog.Level,
	executionID string,
	resource string,
	args ...string,
) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cleanupTimeout)
	defer cancel()

	if _, err := s.docker(cleanupCtx, args...); err != nil {
		s.logger.LogAttrs(
			ctx,
			level,
			"docker resource cleanup failed",
			slog.String("execution_id", executionID),
			slog.String("resource", resource),
			slog.String("error", err.Error()),
		)
	}
}

// newExecutionID returns 12 random hexadecimal characters. The identifier is
// generated rather than derived from the Job ID, which is caller-supplied and
// may contain characters Docker does not accept in resource names.
func newExecutionID() (string, error) {
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(value[:]), nil
}

package sandbox

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

const testExecutionID = "0123456789ab"

func TestJobCreateArgsPassTheCommandAndWorkingDirectoryExplicitly(t *testing.T) {
	spec := testSpec()
	spec.Command = []string{"go", "test", "./...", "-run", "Test; rm -rf /"}

	args := jobCreateArgs(spec, newResourceNames(testExecutionID))

	imageIndex := slices.Index(args, spec.Image)
	if imageIndex < 0 {
		t.Fatalf("args %v do not name the job image", args)
	}
	if got := args[imageIndex+1:]; !slices.Equal(got, spec.Command[1:]) {
		t.Errorf("arguments after the image = %v, want %v", got, spec.Command[1:])
	}

	options := args[:imageIndex]
	assertOption(t, options, "--entrypoint", "go")
	assertOption(t, options, "--workdir", "/workspace/runner")
	assertOption(t, options, "--name", "zeroyaml-"+testExecutionID+"-job")
	assertOption(t, options, "--mount", "type=volume,source=zeroyaml-"+testExecutionID+"-workspace,target=/workspace")
	assertOption(t, options, "--security-opt", "no-new-privileges")
	assertOption(t, options, "--memory", "2147483648")
	assertOption(t, options, "--memory-swap", "2147483648")
	assertOption(t, options, "--pids-limit", "512")
	assertOption(t, options, "--pull", "missing")
	assertOption(t, options, "--log-driver", "local")
	assertOption(t, options, "--log-opt", "max-size=10m")
	assertOption(t, options, "--log-opt", "max-file=1")
	assertOption(t, options, "--log-opt", "compress=false")

	if slices.Contains(options, "--privileged") {
		t.Error("job container must never be privileged")
	}
}

func TestCheckoutCreateArgsPassTheSourceAsPositionalParameters(t *testing.T) {
	spec := testSpec()

	args := checkoutCreateArgs(spec, newResourceNames(testExecutionID))

	want := []string{spec.CheckoutImage, "-c", checkoutScript, checkoutScriptName, spec.Source.RemoteURL, spec.Source.Revision}
	if got := args[len(args)-len(want):]; !slices.Equal(got, want) {
		t.Errorf("checkout invocation = %v, want %v", got, want)
	}
	if strings.Contains(checkoutScript, spec.Source.RemoteURL) {
		t.Error("checkout script text must not embed the repository location")
	}
	assertOption(t, args, "--entrypoint", "sh")
	assertOption(t, args, "--workdir", "/workspace")
	for _, option := range args {
		if strings.HasPrefix(option, "type=bind") {
			t.Errorf("remote checkout must not bind-mount a host path, got %q", option)
		}
	}
}

func TestCheckoutCreateArgsMountALocalSourceReadOnly(t *testing.T) {
	spec := testSpec()
	spec.Source = Source{HostPath: "/srv/sources/project", Revision: testRevision}

	args := checkoutCreateArgs(spec, newResourceNames(testExecutionID))

	assertOption(t, args, "--mount", "type=bind,source=/srv/sources/project,target=/source,readonly")
	if args[len(args)-2] != "file:///source" {
		t.Errorf("fetch location = %q, want the read-only mount", args[len(args)-2])
	}
}

func TestRunReportsASuccessfulCommandAndRemovesEveryResource(t *testing.T) {
	docker := newFakeDocker()

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeSucceeded {
		t.Fatalf("Outcome = %s, want %s: %s", result.Outcome, OutcomeSucceeded, result.Message)
	}
	if result.ExitCode == nil || *result.ExitCode != 0 {
		t.Errorf("ExitCode = %v, want 0", result.ExitCode)
	}
	if result.ExecutionID != testExecutionID {
		t.Errorf("ExecutionID = %q, want %q", result.ExecutionID, testExecutionID)
	}

	wantCommands := []string{
		"volume create",
		"create zeroyaml-" + testExecutionID + "-checkout",
		"start checkout-id",
		"wait checkout-id",
		"rm checkout-id",
		"create zeroyaml-" + testExecutionID + "-job",
		"start job-id",
		"wait job-id",
		"rm job-id",
		"volume rm",
	}
	if got := docker.commandSummary(); !slices.Equal(got, wantCommands) {
		t.Errorf("docker commands =\n%v\nwant\n%v", got, wantCommands)
	}
}

func TestRunReportsANonZeroExitCodeDeterministically(t *testing.T) {
	docker := newFakeDocker()
	docker.exitCodes["job-id"] = "42\n"

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeFailed {
		t.Fatalf("Outcome = %s, want %s", result.Outcome, OutcomeFailed)
	}
	if result.ExitCode == nil || *result.ExitCode != 42 {
		t.Errorf("ExitCode = %v, want 42", result.ExitCode)
	}
	if result.Message != "job command exited with code 42" {
		t.Errorf("Message = %q", result.Message)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunReportsAFailedCheckoutAsAnExecutionError(t *testing.T) {
	docker := newFakeDocker()
	docker.exitCodes["checkout-id"] = "128"

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeExecutionError {
		t.Fatalf("Outcome = %s, want %s", result.Outcome, OutcomeExecutionError)
	}
	if result.ExitCode != nil {
		t.Errorf("ExitCode = %d, want none because the job command never ran", *result.ExitCode)
	}
	if !strings.Contains(result.Message, "source checkout exited with code 128") {
		t.Errorf("Message = %q, want the checkout exit code", result.Message)
	}
	if slices.Contains(docker.commandSummary(), "create zeroyaml-"+testExecutionID+"-job") {
		t.Error("job container was created after a failed checkout")
	}
	docker.assertCleanedUp(t, "checkout-id")
}

func TestRunReportsAnUnreachableDockerDaemonAsAnExecutionError(t *testing.T) {
	docker := newFakeDocker()
	docker.failures["volume create"] = errors.New("docker volume: exit status 1: Cannot connect to the Docker daemon")

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeExecutionError {
		t.Fatalf("Outcome = %s, want %s", result.Outcome, OutcomeExecutionError)
	}
	if !strings.Contains(result.Message, "workspace creation failed") || !strings.Contains(result.Message, "Cannot connect") {
		t.Errorf("Message = %q, want the failed stage and the Docker reason", result.Message)
	}
}

// TestRunRemovesTheWorkspaceVolumeWhenItsCreationFails covers the create whose
// response was lost: the daemon may hold the volume even though the CLI
// reported a failure, so removal by name is attempted without waiting for a
// cancellation.
func TestRunRemovesTheWorkspaceVolumeWhenItsCreationFails(t *testing.T) {
	docker := newFakeDocker()
	docker.failures["volume create"] = errors.New("docker volume: exit status 1: connection reset by peer")

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeExecutionError {
		t.Fatalf("Outcome = %s, want %s", result.Outcome, OutcomeExecutionError)
	}

	wantCommands := []string{"volume create", "volume rm"}
	if got := docker.commandSummary(); !slices.Equal(got, wantCommands) {
		t.Errorf("docker commands =\n%v\nwant\n%v", got, wantCommands)
	}
	if !slices.Contains(docker.lastArgs(), "zeroyaml-"+testExecutionID+"-workspace") {
		t.Error("the workspace volume was not removed by its name")
	}
}

// TestRunRemovesAContainerWhoseCreationFails covers the same lost-response case
// for a container: it is removed by name even though create reported an error.
func TestRunRemovesAContainerWhoseCreationFails(t *testing.T) {
	jobContainer := "zeroyaml-" + testExecutionID + "-job"

	docker := newFakeDocker()
	docker.failures["create "+jobContainer] = errors.New("docker create: exit status 1: connection reset by peer")

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeExecutionError {
		t.Fatalf("Outcome = %s, want %s: %s", result.Outcome, OutcomeExecutionError, result.Message)
	}
	docker.assertCleanedUp(t, "checkout-id", jobContainer)
}

func TestRunRemovesAContainerThatFailedToStart(t *testing.T) {
	docker := newFakeDocker()
	docker.failures["start job-id"] = errors.New("docker start: exit status 1: executable file not found")

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeExecutionError {
		t.Fatalf("Outcome = %s, want %s", result.Outcome, OutcomeExecutionError)
	}
	if !strings.Contains(result.Message, "job command failed") {
		t.Errorf("Message = %q, want the failed stage", result.Message)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunStopsAndCleansUpAfterTheTimeout(t *testing.T) {
	docker := newFakeDocker()
	docker.blockOnWait["job-id"] = true

	spec := testSpec()
	spec.Timeout = 50 * time.Millisecond

	result := newTestSandbox(docker).Run(context.Background(), spec)

	if result.Outcome != OutcomeTimedOut {
		t.Fatalf("Outcome = %s, want %s: %s", result.Outcome, OutcomeTimedOut, result.Message)
	}
	if result.ExitCode != nil {
		t.Errorf("ExitCode = %d, want none for a stopped command", *result.ExitCode)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunStopsAndCleansUpAfterCancellation(t *testing.T) {
	docker := newFakeDocker()
	docker.blockOnWait["job-id"] = true

	ctx, cancel := context.WithCancel(context.Background())
	docker.onWait = func(containerID string) {
		if containerID == "job-id" {
			cancel()
		}
	}

	result := newTestSandbox(docker).Run(ctx, testSpec())

	if result.Outcome != OutcomeCancelled {
		t.Fatalf("Outcome = %s, want %s: %s", result.Outcome, OutcomeCancelled, result.Message)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunRemovesAContainerByNameWhenItsCreationIsInterrupted(t *testing.T) {
	docker := newFakeDocker()
	docker.blockOnCreate = "zeroyaml-" + testExecutionID + "-job"

	spec := testSpec()
	spec.Timeout = 50 * time.Millisecond

	result := newTestSandbox(docker).Run(context.Background(), spec)

	if result.Outcome != OutcomeTimedOut {
		t.Fatalf("Outcome = %s, want %s: %s", result.Outcome, OutcomeTimedOut, result.Message)
	}
	docker.assertCleanedUp(t, "checkout-id", "zeroyaml-"+testExecutionID+"-job")
}

func TestRunReportsAnUnreadableExitCodeAsAnExecutionError(t *testing.T) {
	docker := newFakeDocker()
	docker.exitCodes["job-id"] = "not-a-number"

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeExecutionError {
		t.Fatalf("Outcome = %s, want %s", result.Outcome, OutcomeExecutionError)
	}
	docker.assertCleanedUp(t, "checkout-id", "job-id")
}

func TestRunLogsAFailedCleanupWithoutChangingTheResult(t *testing.T) {
	docker := newFakeDocker()
	docker.failures["volume rm"] = errors.New("docker volume: exit status 1: volume is in use")

	result := newTestSandbox(docker).Run(context.Background(), testSpec())

	if result.Outcome != OutcomeSucceeded {
		t.Errorf("Outcome = %s, want %s despite the cleanup failure", result.Outcome, OutcomeSucceeded)
	}
}

func TestBuffersBoundDockerOutput(t *testing.T) {
	head := &headBuffer{limit: 4}
	_, _ = head.Write([]byte("abcdef"))
	if head.String() != "abcd" {
		t.Errorf("headBuffer = %q, want the first bytes", head.String())
	}

	tail := &tailBuffer{limit: 4}
	_, _ = tail.Write([]byte("abc"))
	n, _ := tail.Write([]byte("d€"))
	if n != len("d€") {
		t.Errorf("Write() = %d, want every byte reported as written", n)
	}
	if got := tail.String(); !utf8.ValidString(got) || got != "d€" {
		t.Errorf("tailBuffer = %q, want the last whole characters", got)
	}
}

// fakeDocker scripts Docker CLI answers and records every call with the state
// of its context.
type fakeDocker struct {
	mu          sync.Mutex
	calls       []fakeDockerCall
	exitCodes   map[string]string
	failures    map[string]error
	blockOnWait map[string]bool
	onWait      func(containerID string)

	// blockOnCreate names a container whose creation hangs until the context
	// ends, like a slow image pull.
	blockOnCreate string
}

type fakeDockerCall struct {
	args         []string
	contextAlive bool
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		exitCodes:   map[string]string{"checkout-id": "0\n", "job-id": "0\n"},
		failures:    map[string]error{},
		blockOnWait: map[string]bool{},
	}
}

func (f *fakeDocker) run(ctx context.Context, args ...string) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fakeDockerCall{args: args, contextAlive: ctx.Err() == nil})
	summary := summarize(args)
	failure := f.failures[summary]
	f.mu.Unlock()

	if failure != nil {
		return "", failure
	}

	switch args[0] {
	case "create":
		if args[2] == f.blockOnCreate {
			<-ctx.Done()
			return "", ctx.Err()
		}
		if strings.HasSuffix(args[2], "-checkout") {
			return "checkout-id\n", nil
		}
		return "job-id\n", nil
	case "wait":
		if f.onWait != nil {
			f.onWait(args[1])
		}
		if f.blockOnWait[args[1]] {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return f.exitCodes[args[1]], nil
	default:
		return "", nil
	}
}

// commandSummary lists each call as its command and, for container calls, the
// container it names.
func (f *fakeDocker) commandSummary() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	summary := make([]string, 0, len(f.calls))
	for _, call := range f.calls {
		summary = append(summary, summarize(call.args))
	}

	return summary
}

// lastArgs returns the argument vector of the most recent Docker call.
func (f *fakeDocker) lastArgs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.calls[len(f.calls)-1].args...)
}

// assertCleanedUp checks that every named container and the workspace volume
// were removed, and that removal ran with a live context even when the
// execution itself had been cancelled or had timed out.
func (f *fakeDocker) assertCleanedUp(t *testing.T, containerIDs ...string) {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	removed := map[string]bool{}
	for _, call := range f.calls {
		summary := summarize(call.args)
		if !strings.HasPrefix(summary, "rm ") && summary != "volume rm" {
			continue
		}
		if !call.contextAlive {
			t.Errorf("%q ran with a cancelled context", summary)
		}
		removed[summary] = true
	}

	for _, containerID := range containerIDs {
		if !removed["rm "+containerID] {
			t.Errorf("container %s was not removed", containerID)
		}
	}
	if !removed["volume rm"] {
		t.Error("workspace volume was not removed")
	}
}

func summarize(args []string) string {
	switch args[0] {
	case "volume":
		return "volume " + args[1]
	case "create":
		return "create " + args[2]
	case "rm":
		return "rm " + args[len(args)-1]
	default:
		return strings.Join(args, " ")
	}
}

func newTestSandbox(docker *fakeDocker) *Sandbox {
	return newSandbox(
		docker.run,
		func() (string, error) { return testExecutionID, nil },
		time.Second,
		slog.New(slog.DiscardHandler),
	)
}

func testSpec() Spec {
	return Spec{
		JobID:            "job-1",
		Image:            "alpine:3.22",
		CheckoutImage:    "alpine/git:v2.49.1",
		Command:          []string{"go", "test", "./..."},
		WorkingDirectory: "/workspace/runner",
		Source:           Source{RemoteURL: "https://example.com/repo.git", Revision: testRevision},
		Timeout:          time.Minute,
		Limits:           DefaultLimits(),
	}
}

// assertOption checks that option is immediately followed by value in args.
func assertOption(t *testing.T, args []string, option string, value string) {
	t.Helper()

	for index := 0; index < len(args)-1; index++ {
		if args[index] == option && args[index+1] == value {
			return
		}
	}

	t.Errorf("args %v do not contain %s %s", args, option, value)
}

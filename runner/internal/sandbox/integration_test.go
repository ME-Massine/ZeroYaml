package sandbox

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
)

// dockerTestsVariable opts in to the tests that start real containers. They
// need a reachable Docker daemon, a git binary, and the configured images, so
// they stay out of the default go test run.
const dockerTestsVariable = "ZEROYAML_RUNNER_DOCKER_TESTS"

func TestDockerExecutionOfALocalRepository(t *testing.T) {
	root, revision := newDockerTestRepository(t)
	executor := New("docker", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)

	testCases := []struct {
		name             string
		command          []string
		workingDirectory string
		timeout          time.Duration
		wantOutcome      Outcome
		wantExitCode     int32
	}{
		{
			name:             "command reads the checked-out revision in its working directory",
			command:          []string{"sh", "-c", `test "$(pwd)" = /workspace/service && test "$(cat greeting.txt)" = hello`},
			workingDirectory: "service",
			timeout:          2 * time.Minute,
			wantOutcome:      OutcomeSucceeded,
			wantExitCode:     0,
		},
		{
			name:             "non-zero exit code is reported",
			command:          []string{"sh", "-c", "exit 7"},
			workingDirectory: ".",
			timeout:          2 * time.Minute,
			wantOutcome:      OutcomeFailed,
			wantExitCode:     7,
		},
		{
			name:             "command outliving its time budget is stopped",
			command:          []string{"sleep", "300"},
			workingDirectory: ".",
			timeout:          20 * time.Second,
			wantOutcome:      OutcomeTimedOut,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			settings := Settings{
				Image:           "alpine:3.22",
				CheckoutImage:   "alpine/git:v2.49.1",
				Timeout:         testCase.timeout,
				LocalSourceRoot: root,
				Limits:          DefaultLimits(),
			}
			job := &runnerv1.JobSpecification{
				JobId:      "integration-job",
				Repository: &runnerv1.JobRepository{Location: fileURI(root), Revision: revision},
				Execution: &runnerv1.JobExecution{
					Command:          testCase.command,
					WorkingDirectory: testCase.workingDirectory,
				},
			}

			spec, err := NewSpec(job, settings)
			if err != nil {
				t.Fatalf("NewSpec() returned an error: %v", err)
			}

			result := executor.Run(context.Background(), spec)

			if result.Outcome != testCase.wantOutcome {
				t.Fatalf("Outcome = %s, want %s: %s", result.Outcome, testCase.wantOutcome, result.Message)
			}
			if testCase.wantOutcome == OutcomeTimedOut {
				if result.ExitCode != nil {
					t.Errorf("ExitCode = %d, want none for a stopped command", *result.ExitCode)
				}
			} else if result.ExitCode == nil || *result.ExitCode != testCase.wantExitCode {
				t.Errorf("ExitCode = %v, want %d", result.ExitCode, testCase.wantExitCode)
			}

			assertNoDockerResourcesRemain(t, result.ExecutionID)
		})
	}
}

// newDockerTestRepository creates a git repository with one commit and
// returns its directory and commit SHA. A later commit moves the branch so the
// test proves the dispatched revision, not the branch head, is checked out.
func newDockerTestRepository(t *testing.T) (string, string) {
	t.Helper()

	if os.Getenv(dockerTestsVariable) != "1" {
		t.Skipf("set %s=1 to run tests that start Docker containers", dockerTestsVariable)
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is required to build the test repository")
	}
	if _, err := Probe(context.Background(), "docker", 10*time.Second); err != nil {
		t.Fatalf("%s=1 but Docker is unavailable: %v", dockerTestsVariable, err)
	}

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "service"), 0o755); err != nil {
		t.Fatalf("create service directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "service", "greeting.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write greeting: %v", err)
	}

	runGit(t, root, "init", "--quiet")
	runGit(t, root, "add", ".")
	runGit(t, root, "commit", "--quiet", "-m", "first")
	revision := runGit(t, root, "rev-parse", "HEAD")

	if err := os.WriteFile(filepath.Join(root, "service", "greeting.txt"), []byte("changed"), 0o644); err != nil {
		t.Fatalf("rewrite greeting: %v", err)
	}
	runGit(t, root, "commit", "--quiet", "-am", "second")

	return root, revision
}

func runGit(t *testing.T, directory string, args ...string) string {
	t.Helper()

	command := exec.Command("git", append([]string{"-c", "user.name=ZeroYAML Test", "-c", "user.email=test@zeroyaml.invalid"}, args...)...)
	command.Dir = directory

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v: %s", strings.Join(args, " "), err, output)
	}

	return strings.TrimSpace(string(output))
}

func assertNoDockerResourcesRemain(t *testing.T, executionID string) {
	t.Helper()

	if executionID == "" {
		t.Fatal("result carries no execution ID")
	}

	filter := "label=" + executionLabelName + "=" + executionID
	for _, args := range [][]string{
		{"ps", "--all", "--quiet", "--filter", filter},
		{"volume", "ls", "--quiet", "--filter", filter},
	} {
		output, err := exec.Command("docker", args...).Output()
		if err != nil {
			t.Fatalf("docker %s failed: %v", strings.Join(args, " "), err)
		}
		if remaining := strings.TrimSpace(string(output)); remaining != "" {
			t.Errorf("docker %s found leftover resources: %s", args[0], remaining)
		}
	}
}

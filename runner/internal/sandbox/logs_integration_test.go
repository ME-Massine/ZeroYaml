package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
)

// TestDockerExecutionLogCapture exercises the real Job container, CLI pipes,
// output bounds, and cleanup without needing a source repository or checkout.
func TestDockerExecutionLogCapture(t *testing.T) {
	if os.Getenv(dockerTestsVariable) != "1" {
		t.Skipf("set %s=1 to start real containers", dockerTestsVariable)
	}
	if _, err := Probe(context.Background(), "docker", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name     string
		command  string
		wantCode int32
		large    bool
		cancel   bool
	}{
		{name: "stdout and stderr", command: "printf hello; printf error >&2"},
		{name: "failed command", command: "printf hello; printf error >&2; exit 7", wantCode: 7},
		{name: "short lines", command: "i=0; while [ $i -lt 3000 ]; do printf 'line %04d\\n' $i; i=$((i+1)); done"},
		{name: "large output", command: "head -c 2097152 /dev/zero", large: true},
		{name: "cancel while following", command: "printf 'ready\\n'; sleep 300", cancel: true},
		{name: "cancel after stderr", command: "printf 'failure diagnostics\\n' >&2; sleep 300", cancel: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			sink := &logSink{}
			s := New("docker", slog.New(slog.DiscardHandler), sink)
			if scenario.cancel {
				follow := s.stream
				s.stream = func(ctx context.Context, stdout, stderr io.Writer, containerID string) error {
					return follow(ctx, cancelAfterWrite{Writer: stdout, cancel: cancel}, cancelAfterWrite{Writer: stderr, cancel: cancel}, containerID)
				}
			}
			id, err := newExecutionID()
			if err != nil {
				t.Fatal(err)
			}
			names := newResourceNames(id)
			if _, err := s.docker(ctx, volumeCreateArgs(names)...); err != nil {
				t.Fatal(err)
			}
			// Cleanup remains armed if an assertion or Docker command fails.
			defer s.remove(context.Background(), id, "workspace volume", "volume", "rm", "--force", names.volume)
			spec := testSpec()
			spec.Command = []string{"sh", "-c", scenario.command}
			spec.WorkingDirectory = "/workspace"
			capture := joblogs.NewCapture(ctx, spec.JobID, sink)
			code, err := s.runContainer(ctx, ctx, id, names.job, jobCreateArgs(spec, names, true), capture)
			if closeErr := capture.Close(context.Background(), ctx.Err() != nil); closeErr != nil {
				t.Fatal(closeErr)
			}
			if scenario.cancel {
				if err == nil || ctx.Err() == nil {
					t.Fatalf("cancelled execution returned code %d, error %v", code, err)
				}
			} else if err != nil || code != scenario.wantCode {
				t.Fatalf("code = %d, error = %v", code, err)
			}
			var stdout, stderr bytes.Buffer
			for i, record := range sink.records {
				if record.Sequence != uint64(i+1) || record.JobID != spec.JobID || record.ObservedAt.IsZero() {
					t.Fatal("invalid observation metadata")
				}
				switch record.Stream {
				case joblogs.Stdout:
					stdout.Write(record.Content)
				case joblogs.Stderr:
					stderr.Write(record.Content)
				}
			}
			last := sink.records[len(sink.records)-1]
			if !last.EndOfOutput || last.Incomplete != scenario.cancel || last.Truncated != scenario.large {
				t.Fatalf("final marker: %+v", last)
			}
			switch {
			case scenario.name == "short lines":
				var expected bytes.Buffer
				for i := 0; i < 3000; i++ {
					fmt.Fprintf(&expected, "line %04d\n", i)
				}
				if !bytes.Equal(stdout.Bytes(), expected.Bytes()) || len(sink.records)-1 != (expected.Len()+joblogs.MaxChunkBytes-1)/joblogs.MaxChunkBytes {
					t.Fatal("real Engine short lines truncated or not coalesced")
				}
			case scenario.large:
				if stdout.Len() != joblogs.MaxOutputBytes || len(sink.records) != joblogs.MaxChunks+1 {
					t.Fatalf("large output bytes = %d, chunks = %d", stdout.Len(), len(sink.records))
				}
			case scenario.cancel:
				if scenario.name == "cancel after stderr" {
					if stderr.String() != "failure diagnostics\n" {
						t.Fatalf("stderr = %q", stderr.String())
					}
				} else if stdout.String() != "ready\n" {
					t.Fatalf("stdout = %q", stdout.String())
				}
			default:
				if stdout.String() != "hello" || stderr.String() != "error" {
					t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
				}
			}
			s.remove(context.Background(), id, "workspace volume", "volume", "rm", "--force", names.volume)
			assertNoDockerResourcesRemain(t, id)
		})
	}
}

package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"testing"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
)

// A real child process keeps its stderr pipe open until the follower is killed.
// This catches buffering until successful exit, which scripted streams cannot.
func TestCLIDockerStreamPreservesStderrBeforeCancellation(t *testing.T) {
	t.Setenv("ZEROYAML_TEST_LOG_FOLLOWER", "1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := &logSink{}
	capture := joblogs.NewCapture(ctx, "job", sink)
	stderr := cancelAfterWrite{Writer: capture.Writer(joblogs.Stderr), cancel: cancel}
	err = cliDockerStream(executable)(ctx, io.Discard, stderr, "container")
	if !errors.Is(err, context.Canceled) || ctx.Err() != context.Canceled {
		t.Fatalf("follower error = %v, context = %v", err, ctx.Err())
	}
	if strings.Contains(err.Error(), "failure diagnostics") {
		t.Fatal("command output entered execution diagnostics")
	}
	if err := capture.Close(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	var content strings.Builder
	for _, record := range sink.records {
		if record.Stream == joblogs.Stderr {
			content.Write(record.Content)
		}
	}
	if content.String() != "failure diagnostics\n" {
		t.Fatalf("stderr = %q", content.String())
	}
	last := sink.records[len(sink.records)-1]
	if !last.EndOfOutput || !last.Incomplete {
		t.Fatalf("final marker = %+v", last)
	}
}

func TestCLIDockerStreamStopsFollowingOnceTheCaptureBudgetIsSpent(t *testing.T) {
	t.Setenv("ZEROYAML_TEST_LOG_FOLLOWER", "flood")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := &logSink{}
	capture := joblogs.NewCapture(ctx, "job", sink)
	if err := cliDockerStream(executable)(ctx, capture.Writer(joblogs.Stdout), capture.Writer(joblogs.Stderr), "container"); err != nil || ctx.Err() != nil {
		t.Fatalf("follower error = %v, context = %v", err, ctx.Err())
	}
	if err := capture.Close(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	retained := 0
	for _, record := range sink.records {
		retained += len(record.Content)
	}
	// An early stop is not a stream failure: the log is truncated, not incomplete.
	last := sink.records[len(sink.records)-1]
	if retained != joblogs.MaxOutputBytes || !last.EndOfOutput || !last.Truncated || last.Incomplete {
		t.Fatalf("retained %d bytes, final marker = %+v", retained, last)
	}
}

type cancelAfterWrite struct {
	io.Writer
	cancel context.CancelFunc
}

func (w cancelAfterWrite) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.cancel()
	return n, err
}

func TestMain(m *testing.M) {
	mode := os.Getenv("ZEROYAML_TEST_LOG_FOLLOWER")
	if mode == "" {
		os.Exit(m.Run())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if mode == "failure" {
		fmt.Fprint(os.Stderr, "Docker CLI diagnostic")
		os.Exit(1)
	}
	if mode == "complete" || mode == "frame-then-failure" {
		// Consume the request before replying so an early child exit cannot
		// turn this regression into a request-write failure instead.
		request := bufio.NewReader(os.Stdin)
		for {
			line, err := request.ReadString('\n')
			if err != nil {
				os.Exit(2)
			}
			if line == "\r\n" {
				break
			}
		}
	}
	fmt.Fprint(os.Stdout, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
	if mode == "flood" {
		// Like a running container with unbounded output, frames never end
		// until the follower stops reading.
		var frame [8 + joblogs.MaxChunkBytes]byte
		frame[0] = 1
		binary.BigEndian.PutUint32(frame[4:8], joblogs.MaxChunkBytes)
		for ctx.Err() == nil {
			if _, err := os.Stdout.Write(frame[:]); err != nil {
				os.Exit(0)
			}
		}
		os.Exit(0)
	}
	var header [8]byte
	header[0] = 2
	binary.BigEndian.PutUint32(header[4:], uint32(len("failure diagnostics\n")))
	os.Stdout.Write(header[:])
	os.Stdout.WriteString("failure diagnostics\n")
	if mode == "complete" {
		os.Exit(0)
	}
	if mode == "frame-then-failure" {
		os.Exit(7)
	}
	<-ctx.Done()
}

func TestCLIDockerStreamChecksProcessExitAfterCompleteFrames(t *testing.T) {
	for _, mode := range []string{"complete", "frame-then-failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("ZEROYAML_TEST_LOG_FOLLOWER", mode)
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			sink := &logSink{}
			capture := joblogs.NewCapture(ctx, "job", sink)
			err = cliDockerStream(executable)(ctx, capture.Writer(joblogs.Stdout), capture.Writer(joblogs.Stderr), "container")
			failed := mode == "frame-then-failure"
			if (err != nil) != failed {
				t.Fatalf("follower error = %v, want failure = %t", err, failed)
			}
			if failed && err.Error() != "docker output stream failed" {
				t.Fatalf("unsanitized follower error = %v", err)
			}
			if err != nil {
				capture.MarkIncomplete()
			}
			if err := capture.Close(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			if len(sink.records) != 2 || string(sink.records[0].Content) != "failure diagnostics\n" ||
				sink.records[1].Incomplete != failed || sink.records[1].Truncated {
				t.Fatalf("captured output or completeness lost: %+v", sink.records)
			}
		})
	}
}

func TestCLIDockerStreamRequiresContainerIdentity(t *testing.T) {
	// Validation must fail before attempting to start the Docker CLI.
	if err := cliDockerStream("missing-docker-binary")(context.Background(), io.Discard, io.Discard, ""); err == nil || err.Error() != "Docker container identity is required" {
		t.Fatalf("empty container identity: %v", err)
	}
}

func TestCLIDockerDiagnosticsAreNotJobOutput(t *testing.T) {
	t.Setenv("ZEROYAML_TEST_LOG_FOLLOWER", "failure")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := cliDockerStream(binary)(context.Background(), &stdout, &stderr, "container"); err == nil {
		t.Fatal("expected CLI failure")
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatal("CLI diagnostic was attributed to Job output")
	}
}

func TestDockerFramesPreserveRawBytesAndRejectTruncatedFrames(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		var input, output bytes.Buffer
		payload := []byte{'x', 0, 0xff}
		var header [8]byte
		header[0] = 2
		binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
		input.Write(header[:])
		if truncated {
			payload = payload[:2]
		}
		input.Write(payload)
		err := copyDockerLogFrames(&input, io.Discard, &output)
		if (err != nil) != truncated || !bytes.Equal(output.Bytes(), payload) {
			t.Fatalf("output = %v, error = %v", output.Bytes(), err)
		}
	}
}

func TestDockerFramesRejectInvalidHeaders(t *testing.T) {
	for _, header := range [][]byte{{1}, {3, 0, 0, 0, 0, 0, 0, 0}, {1, 1, 0, 0, 0, 0, 0, 0}} {
		if err := copyDockerLogFrames(bytes.NewReader(header), io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted invalid frame header %v", header)
		}
	}
}

package sandbox

import (
	"bytes"
	"context"
	"encoding/binary"
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
	err = cliDockerStream(executable)(ctx, io.Discard, stderr, "logs", "--follow", "container")
	if err == nil || ctx.Err() != context.Canceled {
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
	fmt.Fprint(os.Stdout, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
	var header [8]byte
	header[0] = 2
	binary.BigEndian.PutUint32(header[4:], uint32(len("failure diagnostics\n")))
	os.Stdout.Write(header[:])
	os.Stdout.WriteString("failure diagnostics\n")
	<-ctx.Done()
}

func TestCLIDockerDiagnosticsAreNotJobOutput(t *testing.T) {
	t.Setenv("ZEROYAML_TEST_LOG_FOLLOWER", "failure")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := cliDockerStream(binary)(context.Background(), &stdout, &stderr, "logs", "container"); err == nil {
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

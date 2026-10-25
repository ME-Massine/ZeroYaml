package joblogs

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type logService struct {
	runnerv1.UnimplementedJobLogServiceServer
	append func(context.Context, *runnerv1.AppendJobLogRequest) (*runnerv1.AppendJobLogResponse, error)
}

func (s logService) AppendJobLog(ctx context.Context, request *runnerv1.AppendJobLogRequest) (*runnerv1.AppendJobLogResponse, error) {
	return s.append(ctx, request)
}

func testReporter(t *testing.T, service logService, output *bytes.Buffer) *Reporter {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	runnerv1.RegisterJobLogServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	connection, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	reporter := &Reporter{connection: connection, client: runnerv1.NewJobLogServiceClient(connection), identity: runneridentity.Identity{RunnerID: "runner", InstanceID: "instance", ProtocolVersion: "runner.v1"}, timeout: time.Second, logger: slog.New(slog.NewTextHandler(output, nil))}
	t.Cleanup(func() { reporter.Close() })
	return reporter
}

func TestReporterSendsChunkIdentityAndLifecycleOverTransport(t *testing.T) {
	requests := make(chan *runnerv1.AppendJobLogRequest, 2)
	var logs bytes.Buffer
	reporter := testReporter(t, logService{append: func(ctx context.Context, request *runnerv1.AppendJobLogRequest) (*runnerv1.AppendJobLogResponse, error) {
		if _, ok := ctx.Deadline(); !ok {
			return nil, status.Error(codes.Internal, "missing deadline")
		}
		requests <- request
		return &runnerv1.AppendJobLogResponse{}, nil
	}}, &logs)
	now := time.Now().UTC()
	for _, record := range []Record{
		{JobID: "job", Sequence: 1, ObservedAt: now, Stream: Stderr, Content: []byte{0xff, '\n'}},
		{JobID: "job", Sequence: 2, ObservedAt: now, EndOfOutput: true, Truncated: true, Incomplete: true},
	} {
		if err := reporter.Append(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	first, last := <-requests, <-requests
	if first.GetJobId() != "job" || first.GetRunnerId() != "runner" || first.GetInstanceId() != "instance" || first.GetProtocolVersion() != "runner.v1" || first.GetStream() != runnerv1.JobLogStream_JOB_LOG_STREAM_STDERR || !bytes.Equal(first.GetContent(), []byte{0xff, '\n'}) || !first.GetObservedAt().AsTime().Equal(now) {
		t.Fatalf("request = %v", first)
	}
	if !last.GetEndOfOutput() || !last.GetTruncated() || !last.GetIncomplete() || last.GetSequence() != 2 {
		t.Fatalf("final request = %v", last)
	}
}

func TestReporterFailureDoesNotLogOutputOrRemoteErrorPayload(t *testing.T) {
	var logs bytes.Buffer
	reporter := testReporter(t, logService{append: func(context.Context, *runnerv1.AppendJobLogRequest) (*runnerv1.AppendJobLogResponse, error) {
		return nil, status.Error(codes.Unavailable, "sensitive-remote-output")
	}}, &logs)
	err := reporter.Append(context.Background(), Record{JobID: "job", Sequence: 1, ObservedAt: time.Now(), Stream: Stdout, Content: []byte("sensitive-job-output")})
	if err == nil || !strings.Contains(logs.String(), "job log delivery failed") || strings.Contains(logs.String(), "sensitive") {
		t.Fatalf("error = %v, logs = %s", err, logs.String())
	}
}

func TestReporterBoundsStalledCallsAndHonorsCancellation(t *testing.T) {
	var logs bytes.Buffer
	reporter := testReporter(t, logService{append: func(ctx context.Context, _ *runnerv1.AppendJobLogRequest) (*runnerv1.AppendJobLogResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}, &logs)
	reporter.timeout = 100 * time.Millisecond
	if err := reporter.Append(context.Background(), Record{ObservedAt: time.Now()}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := reporter.Append(ctx, Record{ObservedAt: time.Now()}); status.Code(err) != codes.Canceled {
		t.Fatalf("cancel error = %v", err)
	}
}

package joblogs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"time"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Endpoint identifies the dedicated mTLS log ingress. Empty disables delivery.
// Certificate, PrivateKey, and CA are file paths supplied outside source control.
type Endpoint struct {
	Address     string
	Certificate string
	PrivateKey  string
	CA          string
}

// Reporter owns one reusable, concurrency-safe client connection for all Jobs.
// Close must run after execution drain, so final markers can still be sent.
type Reporter struct {
	connection *grpc.ClientConn
	client     runnerv1.JobLogServiceClient
	identity   runneridentity.Identity
	timeout    time.Duration
	logger     *slog.Logger
}

// NewReporter configures mTLS without an insecure fallback. A wholly empty
// endpoint disables delivery. The endpoint's completeness and address are
// validated by runnerconfig.Load; unreadable or invalid TLS material fails here,
// at startup.
func NewReporter(endpoint Endpoint, identity runneridentity.Identity, timeout time.Duration, logger *slog.Logger) (*Reporter, error) {
	if endpoint == (Endpoint{}) {
		return nil, nil
	}
	certificate, err := tls.LoadX509KeyPair(endpoint.Certificate, endpoint.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("load job log client certificate and key: %w", err)
	}
	ca, err := os.ReadFile(endpoint.CA)
	if err != nil {
		return nil, fmt.Errorf("read job log server CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("job log CA must contain PEM certificates")
	}
	connection, err := grpc.NewClient(endpoint.Address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{certificate},
	})))
	if err != nil {
		return nil, fmt.Errorf("create job log client: %w", err)
	}
	return &Reporter{connection: connection, client: runnerv1.NewJobLogServiceClient(connection), identity: identity, timeout: timeout, logger: logger}, nil
}

// Close releases the connection after every execution has drained.
func (r *Reporter) Close() error { return r.connection.Close() }

// Append sends a single record and logs only correlation metadata on failure.
// A send stopped because ctx ended was cancelled by the Runner itself, through
// a drain or close bound, the Job deadline, or a cancellation; it says nothing
// about the ingress and is logged at debug level only.
func (r *Reporter) Append(ctx context.Context, record Record) error {
	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	err := r.send(callCtx, record)
	if err != nil {
		// Remote errors may contain payloads; even their messages stay out of logs.
		jobID := record.JobID
		if len(jobID) > 128 {
			jobID = jobID[:128]
		}
		level, message := slog.LevelWarn, "job log delivery failed"
		if stoppedByCaller(ctx, callCtx, err) {
			level, message = slog.LevelDebug, "job log delivery stopped"
		}
		r.logger.Log(ctx, level, message, slog.String("job_id", jobID), slog.Uint64("sequence", record.Sequence), slog.Bool("end_of_output", record.EndOfOutput))
	}
	return err
}

// stoppedByCaller reports whether a failed call ended because of ctx rather
// than the ingress: ctx was cancelled, or its deadline rather than the call
// timeout bounded the call. The server can answer DEADLINE_EXCEEDED slightly
// before ctx reports expiry, because gRPC rounds the transmitted timeout down.
func stoppedByCaller(ctx, callCtx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	deadline, bounded := ctx.Deadline()
	callDeadline, _ := callCtx.Deadline()
	return bounded && deadline.Equal(callDeadline) && status.Code(err) == codes.DeadlineExceeded
}

func (r *Reporter) send(ctx context.Context, record Record) error {
	_, err := r.client.AppendJobLog(ctx, &runnerv1.AppendJobLogRequest{
		ProtocolVersion: r.identity.ProtocolVersion, JobId: record.JobID, RunnerId: r.identity.RunnerID, InstanceId: r.identity.InstanceID,
		Sequence: record.Sequence, ObservedAt: timestamppb.New(record.ObservedAt), Stream: runnerv1.JobLogStream(record.Stream), Content: record.Content,
		EndOfOutput: record.EndOfOutput, Truncated: record.Truncated, Incomplete: record.Incomplete,
	})
	return err
}

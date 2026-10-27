package joblogs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
// endpoint disables delivery; partial or invalid TLS configuration fails startup.
func NewReporter(endpoint Endpoint, identity runneridentity.Identity, timeout time.Duration, logger *slog.Logger) (*Reporter, error) {
	if endpoint == (Endpoint{}) {
		return nil, nil
	}
	if endpoint.Address == "" || endpoint.Certificate == "" || endpoint.PrivateKey == "" || endpoint.CA == "" {
		return nil, fmt.Errorf("job logs require ZEROYAML_JOB_LOGS_ADDRESS, CERTIFICATE, PRIVATE_KEY, and CA")
	}
	host, port, err := net.SplitHostPort(endpoint.Address)
	if err != nil || host == "" || strings.Contains(host, "/") {
		return nil, fmt.Errorf("ZEROYAML_JOB_LOGS_ADDRESS must be a TCP address in host:port form")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, fmt.Errorf("ZEROYAML_JOB_LOGS_ADDRESS must use a numeric port between 1 and 65535")
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
		r.logger.WarnContext(ctx, "job log delivery failed", slog.String("job_id", jobID), slog.Uint64("sequence", record.Sequence), slog.Bool("end_of_output", record.EndOfOutput))
	}
	return err
}

func (r *Reporter) send(ctx context.Context, record Record) error {
	_, err := r.client.AppendJobLog(ctx, &runnerv1.AppendJobLogRequest{
		ProtocolVersion: r.identity.ProtocolVersion, JobId: record.JobID, RunnerId: r.identity.RunnerID, InstanceId: r.identity.InstanceID,
		Sequence: record.Sequence, ObservedAt: timestamppb.New(record.ObservedAt), Stream: runnerv1.JobLogStream(record.Stream), Content: record.Content,
		EndOfOutput: record.EndOfOutput, Truncated: record.Truncated, Incomplete: record.Incomplete,
	})
	return err
}

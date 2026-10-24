package joblogs

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Reporter sends bounded output chunks without copying payloads into
// operational logs. It holds no mutable per-Job state and supports concurrent Jobs.
type Reporter struct {
	address  string
	identity runneridentity.Identity
	timeout  time.Duration
	logger   *slog.Logger
	dial     func(string) (*grpc.ClientConn, error)
}

// NewReporter uses the same Control Plane endpoint and timeout as status reports.
// Each call owns its connection and deadline. No output is retried or persisted
// on the Runner; delivery failures are visible through metadata-only warnings.
func NewReporter(address string, identity runneridentity.Identity, timeout time.Duration, logger *slog.Logger) *Reporter {
	return &Reporter{address: address, identity: identity, timeout: timeout, logger: logger,
		dial: func(address string) (*grpc.ClientConn, error) {
			return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}}
}

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
	connection, err := r.dial(r.address)
	if err != nil {
		return fmt.Errorf("create log connection: %w", err)
	}
	defer connection.Close()
	_, err = runnerv1.NewJobLogServiceClient(connection).AppendJobLog(ctx, &runnerv1.AppendJobLogRequest{
		ProtocolVersion: r.identity.ProtocolVersion, JobId: record.JobID, RunnerId: r.identity.RunnerID, InstanceId: r.identity.InstanceID,
		Sequence: record.Sequence, ObservedAt: timestamppb.New(record.ObservedAt), Stream: runnerv1.JobLogStream(record.Stream), Content: record.Content,
		EndOfOutput: record.EndOfOutput, Truncated: record.Truncated, Incomplete: record.Incomplete,
	})
	return err
}

package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"unicode/utf8"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/jobexecution"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/sandbox"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// acceptedMessage and the rejection messages below are operator-facing
// diagnostics. They describe Runner state and never echo request content, which
// keeps repository details and command arguments out of Control Plane logs.
const acceptedMessage = "job accepted by the runner"

// maxLoggedValueLength bounds caller-supplied identifiers in log records. A
// dispatch arrives from the network, so an oversized job_id or protocol_version
// must not be able to inflate the Runner's logs.
const maxLoggedValueLength = 128

// RunJob validates a dispatched Job and answers whether this process takes
// responsibility for it. Every outcome is recorded as one structured log
// record; see logRunJob for the fields.
//
// A request whose context is already cancelled or past its deadline is answered
// with the matching gRPC status and never acknowledged, because the caller can
// no longer receive the answer and must not be recorded as having dispatched
// the Job, and no execution is started for it.
//
// An accepted Job is handed to the JobSubmitter, which runs it in the
// background under the Runner process lifetime rather than under ctx: the
// acknowledgment ends the RPC, but the Runner stays responsible for the Job
// until it reports a terminal status.
func (s *Server) RunJob(
	ctx context.Context,
	req *runnerv1.RunJobRequest,
) (*runnerv1.RunJobResponse, error) {
	if err := ctx.Err(); err != nil {
		abandoned := status.FromContextError(err).Err()
		s.logRunJob(ctx, req, nil, abandoned)

		return nil, abandoned
	}

	response, err := s.acknowledgeJob(req)
	s.logRunJob(ctx, req, response, err)

	return response, err
}

// acknowledgeJob decides the answer to a dispatch and, only for an accepted
// Job, starts its execution.
//
// Checks run in contract order. The protocol version is examined first, because
// an unsupported version makes the meaning of the remaining fields uncertain.
// Field validation follows, and Runner state is examined last, so that a caller
// defect is reported as such even while the Runner is unavailable.
//
// A Runner without a verified Docker daemon reports accepting_work = false and
// rejects every dispatch with JOB_REJECTION_RUNNER_UNAVAILABLE. So does a
// Runner whose execution slots are all in use or that is shutting down, because
// another Runner may take the Job. A Job this Runner cannot map to a container,
// such as one with an unsupported repository scheme, is answered with
// FAILED_PRECONDITION, since dispatching it again to the same Runner cannot
// succeed.
func (s *Server) acknowledgeJob(req *runnerv1.RunJobRequest) (*runnerv1.RunJobResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "run job request must be present")
	}

	if strings.TrimSpace(req.GetProtocolVersion()) == "" {
		return nil, status.Error(codes.InvalidArgument, "protocol_version must not be empty")
	}

	if req.GetProtocolVersion() != s.identity.ProtocolVersion {
		return s.rejectJob(
			req.GetJob().GetJobId(),
			runnerv1.JobRejectionReason_JOB_REJECTION_UNSUPPORTED_PROTOCOL_VERSION,
			fmt.Sprintf("runner implements protocol version %s", s.identity.ProtocolVersion),
		), nil
	}

	if err := validateJobSpecification(req.GetJob()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !s.identity.AcceptingWork || s.jobs == nil {
		return s.rejectJob(
			req.GetJob().GetJobId(),
			runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE,
			fmt.Sprintf("runner status is %s and it is not accepting work", s.identity.Status),
		), nil
	}

	if err := s.jobs.Submit(req.GetJob()); err != nil {
		return s.answerSubmitFailure(req.GetJob().GetJobId(), err)
	}

	return &runnerv1.RunJobResponse{
		JobId:      req.GetJob().GetJobId(),
		Acceptance: runnerv1.JobAcceptance_JOB_ACCEPTED,
		Message:    acceptedMessage,
		RunnerId:   s.identity.RunnerID,
		InstanceId: s.identity.InstanceID,
	}, nil
}

// answerSubmitFailure maps a Job the JobSubmitter did not start onto the
// dispatch answer. The error messages come from the Runner and never echo the
// repository location or the command.
func (s *Server) answerSubmitFailure(jobID string, err error) (*runnerv1.RunJobResponse, error) {
	switch {
	case errors.Is(err, jobexecution.ErrAtCapacity), errors.Is(err, jobexecution.ErrClosed):
		return s.rejectJob(jobID, runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE, err.Error()), nil
	case errors.Is(err, sandbox.ErrUnsupportedJob):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	default:
		return nil, status.Error(codes.Internal, fmt.Sprintf("start job execution: %v", err))
	}
}

// logRunJob records the outcome of one dispatch. The record names the Job and
// the answering process so that it can be correlated with Control Plane audit
// records. It deliberately omits the repository location, revision, and
// command, which may carry credentials or sensitive arguments.
//
// An acknowledgment is logged at INFO, a contract violation at WARN because it
// indicates a caller defect, an abandoned request at INFO because a caller
// giving up is an ordinary event, and a Job the Runner could not start at WARN.
func (s *Server) logRunJob(
	ctx context.Context,
	req *runnerv1.RunJobRequest,
	response *runnerv1.RunJobResponse,
	err error,
) {
	attributes := []slog.Attr{
		slog.String("job_id", boundLogValue(req.GetJob().GetJobId())),
		slog.String("protocol_version", boundLogValue(req.GetProtocolVersion())),
		slog.String("runner_id", s.identity.RunnerID),
		slog.String("instance_id", s.identity.InstanceID),
	}

	if err != nil {
		rpcStatus := status.Convert(err)
		attributes = append(
			attributes,
			slog.String("grpc_code", rpcStatus.Code().String()),
			slog.String("error", rpcStatus.Message()),
		)

		switch rpcStatus.Code() {
		case codes.InvalidArgument:
			s.logger.LogAttrs(ctx, slog.LevelWarn, "run job request rejected as invalid", attributes...)
		case codes.Canceled, codes.DeadlineExceeded:
			s.logger.LogAttrs(ctx, slog.LevelInfo, "run job request abandoned by the caller", attributes...)
		default:
			s.logger.LogAttrs(ctx, slog.LevelWarn, "run job request could not be started", attributes...)
		}

		return
	}

	attributes = append(attributes, slog.String("acceptance", response.GetAcceptance().String()))
	if response.GetAcceptance() == runnerv1.JobAcceptance_JOB_REJECTED {
		attributes = append(attributes, slog.String("rejection_reason", response.GetRejectionReason().String()))
	}

	s.logger.LogAttrs(ctx, slog.LevelInfo, "run job acknowledged", attributes...)
}

// boundLogValue truncates a caller-supplied value to at most
// maxLoggedValueLength bytes. The cut backs off to a UTF-8 boundary so that a
// multi-byte character is never split into an invalid sequence.
func boundLogValue(value string) string {
	if len(value) <= maxLoggedValueLength {
		return value
	}

	cut := maxLoggedValueLength
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}

	return value[:cut] + "...(truncated)"
}

// rejectJob answers a well-formed dispatch the Runner declines. The response
// carries the answering process so that the Control Plane can record which
// Runner refused the Job.
func (s *Server) rejectJob(
	jobID string,
	reason runnerv1.JobRejectionReason,
	message string,
) *runnerv1.RunJobResponse {
	return &runnerv1.RunJobResponse{
		JobId:           jobID,
		Acceptance:      runnerv1.JobAcceptance_JOB_REJECTED,
		RejectionReason: reason,
		Message:         message,
		RunnerId:        s.identity.RunnerID,
		InstanceId:      s.identity.InstanceID,
	}
}

// validateJobSpecification enforces the executable minimum of the contract at
// the remote boundary. A Runner receives dispatches over the network, so it
// never assumes that a caller validated the request first.
func validateJobSpecification(job *runnerv1.JobSpecification) error {
	if job == nil {
		return fmt.Errorf("job must be present")
	}
	if strings.TrimSpace(job.GetJobId()) == "" {
		return fmt.Errorf("job_id must not be empty")
	}
	if err := validateRepository(job.GetRepository()); err != nil {
		return err
	}

	return validateExecution(job.GetExecution())
}

func validateRepository(repository *runnerv1.JobRepository) error {
	if repository == nil {
		return fmt.Errorf("repository must be present")
	}

	location := strings.TrimSpace(repository.GetLocation())
	if location == "" {
		return fmt.Errorf("repository location must not be empty")
	}

	parsed, err := url.Parse(location)
	if err != nil || !parsed.IsAbs() {
		// The scheme is what tells the Runner how to resolve the source, so a
		// relative or unparsable location cannot be acted on.
		return fmt.Errorf("repository location must be an absolute URI")
	}

	if strings.TrimSpace(repository.GetRevision()) == "" {
		return fmt.Errorf("repository revision must not be empty")
	}

	return nil
}

func validateExecution(execution *runnerv1.JobExecution) error {
	if execution == nil {
		return fmt.Errorf("execution must be present")
	}

	command := execution.GetCommand()
	if len(command) == 0 {
		return fmt.Errorf("execution command must contain at least one argument")
	}
	for _, argument := range command {
		if strings.TrimSpace(argument) == "" {
			return fmt.Errorf("execution command arguments must not be empty")
		}
	}

	return validateWorkingDirectory(execution.GetWorkingDirectory())
}

// validateWorkingDirectory keeps a dispatch inside the workspace the Runner
// controls. An absolute path or a parent traversal would let a caller select a
// directory outside the checked-out repository, which is a privilege the
// dispatch contract does not grant.
func validateWorkingDirectory(workingDirectory string) error {
	if strings.TrimSpace(workingDirectory) == "" {
		return fmt.Errorf("execution working_directory must not be empty")
	}

	// Both separators are normalized because the Control Plane and the Runner
	// may run on different operating systems.
	normalized := strings.ReplaceAll(workingDirectory, `\`, "/")
	if path.IsAbs(normalized) || isWindowsDriveRooted(normalized) {
		return fmt.Errorf("execution working_directory must be relative to the repository")
	}

	for _, element := range strings.Split(normalized, "/") {
		if element == ".." {
			return fmt.Errorf("execution working_directory must not traverse outside the repository")
		}
	}

	return nil
}

// isWindowsDriveRooted reports a path such as "C:/work", which path.IsAbs does
// not treat as absolute because it only understands slash-rooted paths.
func isWindowsDriveRooted(normalizedPath string) bool {
	if len(normalizedPath) < 2 || normalizedPath[1] != ':' {
		return false
	}

	driveLetter := normalizedPath[0]

	return (driveLetter >= 'a' && driveLetter <= 'z') || (driveLetter >= 'A' && driveLetter <= 'Z')
}

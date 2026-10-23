package grpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/jobexecution"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/sandbox"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRunJobAcceptsAValidDispatch covers the acknowledgment a Runner returns
// once it takes responsibility for a Job. The response must name the answering
// process, because the Control Plane records that pair against the Job.
func TestRunJobAcceptsAValidDispatch(t *testing.T) {
	identity := newAcceptingTestIdentity(t)

	response, err := New(identity, &recordingJobSubmitter{}, discardLogger()).RunJob(context.Background(), newValidRunJobRequest())
	if err != nil {
		t.Fatalf("RunJob() returned an error: %v", err)
	}

	if response.GetAcceptance() != runnerv1.JobAcceptance_JOB_ACCEPTED {
		t.Errorf("Acceptance = %s, want %s", response.GetAcceptance(), runnerv1.JobAcceptance_JOB_ACCEPTED)
	}
	if response.GetRejectionReason() != runnerv1.JobRejectionReason_JOB_REJECTION_REASON_UNSPECIFIED {
		t.Errorf("RejectionReason = %s, want unspecified for an accepted job", response.GetRejectionReason())
	}
	if response.GetJobId() != testJobID {
		t.Errorf("JobId = %q, want %q", response.GetJobId(), testJobID)
	}
	if response.GetRunnerId() != identity.RunnerID {
		t.Errorf("RunnerId = %q, want %q", response.GetRunnerId(), identity.RunnerID)
	}
	if response.GetInstanceId() != identity.InstanceID {
		t.Errorf("InstanceId = %q, want %q", response.GetInstanceId(), identity.InstanceID)
	}
}

// TestRunJobRejectsDispatchWhileNotAcceptingWork pins the behavior of a Runner
// that is not ready or has no verified Docker daemon and therefore declines
// work. The refusal is a Runner state decision, so it is an OK response the Control
// Plane can act on rather than a transport error.
func TestRunJobRejectsDispatchWhileNotAcceptingWork(t *testing.T) {
	identity := newTestIdentity(t)

	response, err := New(identity, &recordingJobSubmitter{}, discardLogger()).RunJob(context.Background(), newValidRunJobRequest())
	if err != nil {
		t.Fatalf("RunJob() returned an error: %v", err)
	}

	if response.GetAcceptance() != runnerv1.JobAcceptance_JOB_REJECTED {
		t.Errorf("Acceptance = %s, want %s", response.GetAcceptance(), runnerv1.JobAcceptance_JOB_REJECTED)
	}
	if response.GetRejectionReason() != runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE {
		t.Errorf(
			"RejectionReason = %s, want %s",
			response.GetRejectionReason(),
			runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE,
		)
	}
	if response.GetJobId() != testJobID {
		t.Errorf("JobId = %q, want %q", response.GetJobId(), testJobID)
	}
	if response.GetRunnerId() != identity.RunnerID {
		t.Errorf("RunnerId = %q, want %q", response.GetRunnerId(), identity.RunnerID)
	}
	if response.GetMessage() == "" {
		t.Error("Message is empty, want an operator-facing reason")
	}
}

// TestRunJobRejectsAnUnsupportedProtocolVersion shows that a version the Runner
// does not implement is answered before the request fields are interpreted.
func TestRunJobRejectsAnUnsupportedProtocolVersion(t *testing.T) {
	identity := newAcceptingTestIdentity(t)

	request := newValidRunJobRequest()
	request.ProtocolVersion = "runner.v99"

	response, err := New(identity, &recordingJobSubmitter{}, discardLogger()).RunJob(context.Background(), request)
	if err != nil {
		t.Fatalf("RunJob() returned an error: %v", err)
	}

	if response.GetAcceptance() != runnerv1.JobAcceptance_JOB_REJECTED {
		t.Errorf("Acceptance = %s, want %s", response.GetAcceptance(), runnerv1.JobAcceptance_JOB_REJECTED)
	}
	if response.GetRejectionReason() != runnerv1.JobRejectionReason_JOB_REJECTION_UNSUPPORTED_PROTOCOL_VERSION {
		t.Errorf(
			"RejectionReason = %s, want %s",
			response.GetRejectionReason(),
			runnerv1.JobRejectionReason_JOB_REJECTION_UNSUPPORTED_PROTOCOL_VERSION,
		)
	}
}

// TestRunJobRejectsRequestsThatViolateTheContract covers caller defects. Each
// case is a request no compliant Control Plane sends, so the Runner answers
// INVALID_ARGUMENT instead of producing an acknowledgment that would suggest
// the Job is known to the Runner.
func TestRunJobRejectsRequestsThatViolateTheContract(t *testing.T) {
	testCases := []struct {
		name    string
		request *runnerv1.RunJobRequest
	}{
		{
			name:    "absent request",
			request: nil,
		},
		{
			name: "absent protocol version",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.ProtocolVersion = "  "
			}),
		},
		{
			name: "absent job",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job = nil
			}),
		},
		{
			name: "absent job identity",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.JobId = ""
			}),
		},
		{
			name: "absent repository",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Repository = nil
			}),
		},
		{
			name: "relative repository location",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Repository.Location = "MohamedMBG/ZeroYaml"
			}),
		},
		{
			name: "absent repository revision",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Repository.Revision = ""
			}),
		},
		{
			name: "absent execution",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution = nil
			}),
		},
		{
			name: "empty command",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution.Command = nil
			}),
		},
		{
			name: "blank command argument",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution.Command = []string{"go", "   "}
			}),
		},
		{
			name: "absent working directory",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution.WorkingDirectory = ""
			}),
		},
		{
			name: "absolute working directory",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution.WorkingDirectory = "/etc"
			}),
		},
		{
			name: "drive rooted working directory",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution.WorkingDirectory = `C:\Windows`
			}),
		},
		{
			name: "working directory traversing outside the repository",
			request: mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution.WorkingDirectory = "runner/../../secrets"
			}),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// The identity accepts work so that a rejected request proves the
			// contract check rather than the Runner's availability.
			identity := newAcceptingTestIdentity(t)

			response, err := New(identity, &recordingJobSubmitter{}, discardLogger()).RunJob(context.Background(), testCase.request)
			if err == nil {
				t.Fatalf("RunJob() returned %v, want an error", response)
			}
			if code := status.Code(err); code != codes.InvalidArgument {
				t.Errorf("status code = %s, want %s: %v", code, codes.InvalidArgument, err)
			}
			if response != nil {
				t.Errorf("response = %v, want no response for a rejected request", response)
			}
		})
	}
}

// TestRunJobAcceptsAWorkingDirectoryInsideTheRepository documents the accepted
// shape of a nested working directory, including a Windows-style separator.
func TestRunJobAcceptsAWorkingDirectoryInsideTheRepository(t *testing.T) {
	testCases := []string{".", "services/api", `services\api`, "services/./api"}

	for _, workingDirectory := range testCases {
		t.Run(workingDirectory, func(t *testing.T) {
			request := mutateRunJobRequest(func(request *runnerv1.RunJobRequest) {
				request.Job.Execution.WorkingDirectory = workingDirectory
			})

			response, err := New(newAcceptingTestIdentity(t), &recordingJobSubmitter{}, discardLogger()).RunJob(context.Background(), request)
			if err != nil {
				t.Fatalf("RunJob() returned an error: %v", err)
			}
			if response.GetAcceptance() != runnerv1.JobAcceptance_JOB_ACCEPTED {
				t.Errorf("Acceptance = %s, want %s", response.GetAcceptance(), runnerv1.JobAcceptance_JOB_ACCEPTED)
			}
		})
	}
}

// TestRunJobAnswersACancelledRequestWithoutAcknowledging shows that a caller
// that already gave up receives the matching gRPC status and no
// acknowledgment, so no Job is recorded against a dispatch nobody awaits.
func TestRunJobAnswersACancelledRequestWithoutAcknowledging(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelExpired()

	testCases := []struct {
		name     string
		ctx      context.Context
		wantCode codes.Code
	}{
		{name: "cancelled", ctx: cancelled, wantCode: codes.Canceled},
		{name: "deadline exceeded", ctx: expired, wantCode: codes.DeadlineExceeded},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			logs := &bytes.Buffer{}
			server := New(newAcceptingTestIdentity(t), &recordingJobSubmitter{}, jsonTestLogger(logs))

			response, err := server.RunJob(testCase.ctx, newValidRunJobRequest())
			if err == nil {
				t.Fatalf("RunJob() returned %v, want an error", response)
			}
			if code := status.Code(err); code != testCase.wantCode {
				t.Errorf("status code = %s, want %s: %v", code, testCase.wantCode, err)
			}
			if response != nil {
				t.Errorf("response = %v, want no acknowledgment for an abandoned request", response)
			}

			record := singleLogRecord(t, logs)
			if record["msg"] != "run job request abandoned by the caller" {
				t.Errorf("msg = %v, want the abandoned request record", record["msg"])
			}
			if record["grpc_code"] != testCase.wantCode.String() {
				t.Errorf("grpc_code = %v, want %s", record["grpc_code"], testCase.wantCode)
			}
			if _, found := record["acceptance"]; found {
				t.Errorf("acceptance = %v, want no acceptance for an abandoned request", record["acceptance"])
			}
		})
	}
}

// TestRunJobLogsOneStructuredRecordPerOutcome pins the fields an operator uses
// to correlate a dispatch with the Control Plane: the Job, the answering
// process, and the outcome.
func TestRunJobLogsOneStructuredRecordPerOutcome(t *testing.T) {
	invalidRequest := newValidRunJobRequest()
	invalidRequest.Job.Execution.Command = nil

	testCases := []struct {
		name       string
		identity   func(t *testing.T) runneridentity.Identity
		request    *runnerv1.RunJobRequest
		wantLevel  string
		wantMsg    string
		wantFields map[string]string
	}{
		{
			name:      "accepted",
			identity:  newAcceptingTestIdentity,
			request:   newValidRunJobRequest(),
			wantLevel: "INFO",
			wantMsg:   "run job acknowledged",
			wantFields: map[string]string{
				"acceptance": runnerv1.JobAcceptance_JOB_ACCEPTED.String(),
			},
		},
		{
			name:      "rejected while unavailable",
			identity:  newTestIdentity,
			request:   newValidRunJobRequest(),
			wantLevel: "INFO",
			wantMsg:   "run job acknowledged",
			wantFields: map[string]string{
				"acceptance":       runnerv1.JobAcceptance_JOB_REJECTED.String(),
				"rejection_reason": runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE.String(),
			},
		},
		{
			name:      "invalid",
			identity:  newAcceptingTestIdentity,
			request:   invalidRequest,
			wantLevel: "WARN",
			wantMsg:   "run job request rejected as invalid",
			wantFields: map[string]string{
				"grpc_code": codes.InvalidArgument.String(),
				"error":     "execution command must contain at least one argument",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			identity := testCase.identity(t)
			logs := &bytes.Buffer{}

			// The outcome itself is covered by the other tests; only the log
			// record is examined here.
			_, _ = New(identity, &recordingJobSubmitter{}, jsonTestLogger(logs)).RunJob(context.Background(), testCase.request)

			record := singleLogRecord(t, logs)
			wantFields := map[string]string{
				"level":            testCase.wantLevel,
				"msg":              testCase.wantMsg,
				"job_id":           testJobID,
				"protocol_version": runneridentity.ProtocolVersion,
				"runner_id":        identity.RunnerID,
				"instance_id":      identity.InstanceID,
			}
			for key, value := range testCase.wantFields {
				wantFields[key] = value
			}

			for key, want := range wantFields {
				if got := record[key]; got != want {
					t.Errorf("%s = %v, want %q", key, got, want)
				}
			}
		})
	}
}

// TestRunJobLogsNoRepositoryOrCommandDetails guards against leaking dispatch
// payloads into logs. Repository locations can embed credentials and command
// arguments can carry secrets, so neither may appear in a log record.
func TestRunJobLogsNoRepositoryOrCommandDetails(t *testing.T) {
	const secret = "s3cr3t-token-value"

	request := newValidRunJobRequest()
	request.Job.Repository.Location = "https://user:" + secret + "@example.com/repo.git"
	request.Job.Execution.Command = []string{"deploy", "--token=" + secret}

	logs := &bytes.Buffer{}
	if _, err := New(newAcceptingTestIdentity(t), &recordingJobSubmitter{}, jsonTestLogger(logs)).RunJob(context.Background(), request); err != nil {
		t.Fatalf("RunJob() returned an error: %v", err)
	}

	if strings.Contains(logs.String(), secret) {
		t.Errorf("log output contains dispatch payload details: %s", logs.String())
	}
}

// TestRunJobBoundsCallerSuppliedLogValues shows that an oversized identifier
// from the network cannot inflate a log record without limit.
func TestRunJobBoundsCallerSuppliedLogValues(t *testing.T) {
	request := newValidRunJobRequest()
	request.Job.JobId = strings.Repeat("j", 10*maxLoggedValueLength)

	logs := &bytes.Buffer{}
	if _, err := New(newAcceptingTestIdentity(t), &recordingJobSubmitter{}, jsonTestLogger(logs)).RunJob(context.Background(), request); err != nil {
		t.Fatalf("RunJob() returned an error: %v", err)
	}

	loggedJobID, _ := singleLogRecord(t, logs)["job_id"].(string)
	if len(loggedJobID) > maxLoggedValueLength+len("...(truncated)") {
		t.Errorf("logged job_id has %d bytes, want at most %d", len(loggedJobID), maxLoggedValueLength)
	}
	if !strings.HasSuffix(loggedJobID, "...(truncated)") {
		t.Errorf("logged job_id = %q, want a truncation marker", loggedJobID)
	}
}

// TestBoundLogValueKeepsMultiByteCharactersIntact shows that truncation never
// splits a UTF-8 character, which would otherwise produce an invalid log value.
func TestBoundLogValueKeepsMultiByteCharactersIntact(t *testing.T) {
	// One ASCII byte shifts every three-byte "€" so that the byte limit falls
	// inside a character.
	value := "j" + strings.Repeat("€", maxLoggedValueLength)

	bounded := boundLogValue(value)

	if !utf8.ValidString(bounded) {
		t.Errorf("boundLogValue() = %q, want valid UTF-8", bounded)
	}
	prefix := strings.TrimSuffix(bounded, "...(truncated)")
	if len(prefix) > maxLoggedValueLength {
		t.Errorf("bounded prefix has %d bytes, want at most %d", len(prefix), maxLoggedValueLength)
	}
	if !strings.HasPrefix(value, prefix) {
		t.Errorf("bounded prefix %q is not a prefix of the original value", prefix)
	}
}

const testJobID = "0f4b1a1e-1f2c-4c53-9b3a-3f5b5f4b9a11"

// newValidRunJobRequest builds the dispatch a compliant Control Plane sends.
func newValidRunJobRequest() *runnerv1.RunJobRequest {
	return &runnerv1.RunJobRequest{
		ProtocolVersion: runneridentity.ProtocolVersion,
		Job: &runnerv1.JobSpecification{
			JobId: testJobID,
			Repository: &runnerv1.JobRepository{
				Location: "https://github.com/MohamedMBG/ZeroYaml.git",
				Revision: "3af0394c1d2b4e5f60718293a4b5c6d7e8f90123",
			},
			Execution: &runnerv1.JobExecution{
				Command:          []string{"go", "test", "./..."},
				WorkingDirectory: "runner",
			},
		},
	}
}

// mutateRunJobRequest derives a request from the valid dispatch so that each
// test case states only the field under test.
func mutateRunJobRequest(mutate func(request *runnerv1.RunJobRequest)) *runnerv1.RunJobRequest {
	request := newValidRunJobRequest()
	mutate(request)

	return request
}

// newAcceptingTestIdentity builds the identity of a Runner that reports itself
// ready for work, which the current process does not do on its own because it
// has no execution service yet.
func newAcceptingTestIdentity(t *testing.T) runneridentity.Identity {
	t.Helper()

	identity := newTestIdentity(t)
	identity.Status = runneridentity.StatusReady
	identity.AcceptingWork = true

	return identity
}

// discardLogger is used by tests that do not examine log output.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// jsonTestLogger writes machine-readable records so that a test can assert
// individual fields instead of matching formatted text.
func jsonTestLogger(output *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(output, nil))
}

// singleLogRecord decodes the only record written to output.
func singleLogRecord(t *testing.T, output *bytes.Buffer) map[string]any {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("log output has %d records, want 1: %s", len(lines), output.String())
	}

	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("log record is not valid JSON: %v", err)
	}

	return record
}

// TestRunJobStartsExecutionOnlyForAnAcceptedJob shows that acceptance and
// execution are one decision: the accepted Job is handed to execution exactly
// once, and a declined or invalid dispatch starts nothing.
func TestRunJobStartsExecutionOnlyForAnAcceptedJob(t *testing.T) {
	jobs := &recordingJobSubmitter{}

	if _, err := New(newAcceptingTestIdentity(t), jobs, discardLogger()).RunJob(context.Background(), newValidRunJobRequest()); err != nil {
		t.Fatalf("RunJob() returned an error: %v", err)
	}
	if len(jobs.submitted) != 1 || jobs.submitted[0].GetJobId() != testJobID {
		t.Fatalf("submitted = %v, want exactly the dispatched job", jobs.submitted)
	}

	declined := &recordingJobSubmitter{}
	_, _ = New(newTestIdentity(t), declined, discardLogger()).RunJob(context.Background(), newValidRunJobRequest())

	invalidRequest := newValidRunJobRequest()
	invalidRequest.Job.Execution.Command = nil
	_, _ = New(newAcceptingTestIdentity(t), declined, discardLogger()).RunJob(context.Background(), invalidRequest)

	if len(declined.submitted) != 0 {
		t.Errorf("submitted = %v, want nothing for a declined or invalid dispatch", declined.submitted)
	}
}

// TestRunJobRejectsAsUnavailableWithoutAnExecutionService covers a Runner
// wired without execution support; it must never acknowledge a Job it cannot
// run.
func TestRunJobRejectsAsUnavailableWithoutAnExecutionService(t *testing.T) {
	response, err := New(newAcceptingTestIdentity(t), nil, discardLogger()).RunJob(context.Background(), newValidRunJobRequest())
	if err != nil {
		t.Fatalf("RunJob() returned an error: %v", err)
	}

	if response.GetRejectionReason() != runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE {
		t.Errorf("RejectionReason = %s, want %s", response.GetRejectionReason(), runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE)
	}
}

// TestRunJobMapsSubmitFailures pins how each reason for not starting an
// execution reaches the Control Plane. Runner state is a rejection another
// Runner may resolve; a Job this Runner cannot map is a failed precondition.
func TestRunJobMapsSubmitFailures(t *testing.T) {
	testCases := []struct {
		name       string
		err        error
		wantCode   codes.Code
		wantReason runnerv1.JobRejectionReason
	}{
		{
			name:       "at capacity",
			err:        jobexecution.ErrAtCapacity,
			wantCode:   codes.OK,
			wantReason: runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE,
		},
		{
			name:       "shutting down",
			err:        jobexecution.ErrClosed,
			wantCode:   codes.OK,
			wantReason: runnerv1.JobRejectionReason_JOB_REJECTION_RUNNER_UNAVAILABLE,
		},
		{
			name:     "unsupported job",
			err:      fmt.Errorf("%w: repository scheme %q is not supported", sandbox.ErrUnsupportedJob, "ssh"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "unexpected failure",
			err:      errors.New("boom"),
			wantCode: codes.Internal,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			jobs := &recordingJobSubmitter{err: testCase.err}

			response, err := New(newAcceptingTestIdentity(t), jobs, discardLogger()).RunJob(context.Background(), newValidRunJobRequest())

			if code := status.Code(err); code != testCase.wantCode {
				t.Fatalf("status code = %s, want %s: %v", code, testCase.wantCode, err)
			}
			if testCase.wantCode != codes.OK {
				return
			}
			if response.GetAcceptance() != runnerv1.JobAcceptance_JOB_REJECTED {
				t.Errorf("Acceptance = %s, want %s", response.GetAcceptance(), runnerv1.JobAcceptance_JOB_REJECTED)
			}
			if response.GetRejectionReason() != testCase.wantReason {
				t.Errorf("RejectionReason = %s, want %s", response.GetRejectionReason(), testCase.wantReason)
			}
			if response.GetMessage() != testCase.err.Error() {
				t.Errorf("Message = %q, want %q", response.GetMessage(), testCase.err.Error())
			}
		})
	}
}

// recordingJobSubmitter records every Job handed to execution and answers
// with err.
type recordingJobSubmitter struct {
	err       error
	submitted []*runnerv1.JobSpecification
}

func (r *recordingJobSubmitter) Submit(job *runnerv1.JobSpecification) error {
	if r.err != nil {
		return r.err
	}

	r.submitted = append(r.submitted, job)

	return nil
}

package io.zeroyaml.controlplane.execution;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.springframework.stereotype.Component;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.zeroyaml.contracts.runner.v1.JobExecutionResult;
import io.zeroyaml.contracts.runner.v1.JobExecutionStatusServiceGrpc;
import io.zeroyaml.contracts.runner.v1.JobLifecycleState;
import io.zeroyaml.contracts.runner.v1.JobStatusReportResult;
import io.zeroyaml.contracts.runner.v1.ReportJobStatusRequest;
import io.zeroyaml.contracts.runner.v1.ReportJobStatusResponse;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;

/**
 * gRPC endpoint a Runner calls to report what one execution did. Translation
 * between generated protocol types and {@link JobExecutionStatusRecorder}
 * happens here, so the recorder stays protocol-neutral.
 *
 * <p>Failures are split by who must fix them. A request this Control Plane
 * cannot interpret, such as an unsupported protocol version or a malformed Job
 * identity, fails with {@code INVALID_ARGUMENT} and is a caller defect. A
 * well-formed report that does not change Job state is answered with {@code OK}
 * and a {@link JobStatusReportResult}, because a duplicate, late, or
 * contradicting report is a state decision the Runner must be able to record
 * rather than retry blindly.</p>
 *
 * <p>Terminal reports are deliberately forgiving about detail: an unrecognized
 * failure reason or a missing diagnostic is normalized instead of refused,
 * because refusing a terminal report would leave the Job without an outcome.</p>
 */
@Component
class GrpcJobExecutionStatusService extends JobExecutionStatusServiceGrpc.JobExecutionStatusServiceImplBase {

	/**
	 * Upper bound on the diagnostic a Runner may attach to a failure. Runners
	 * shorten their own messages well below this; the bound exists so that a
	 * misbehaving caller cannot push an unbounded payload into Job records.
	 */
	static final int MAX_FAILURE_MESSAGE_LENGTH = 2048;

	/** Diagnostic recorded when a Runner reports a failure without one. */
	static final String MISSING_FAILURE_MESSAGE = "The runner reported a failure without a diagnostic";

	private final JobExecutionStatusRecorder recorder;

	GrpcJobExecutionStatusService(JobExecutionStatusRecorder recorder) {
		this.recorder = Objects.requireNonNull(recorder, "recorder must not be null");
	}

	@Override
	public void reportJobStatus(
			ReportJobStatusRequest request,
			StreamObserver<ReportJobStatusResponse> responseObserver
	) {
		JobStatusReport report;
		try {
			report = toReport(request);
		} catch (IllegalArgumentException exception) {
			responseObserver.onError(Status.INVALID_ARGUMENT
					.withDescription(exception.getMessage())
					.withCause(exception)
					.asRuntimeException());
			return;
		}

		var outcome = recorder.record(report);
		responseObserver.onNext(toResponse(request.getJobId(), outcome));
		responseObserver.onCompleted();
	}

	private static JobStatusReport toReport(ReportJobStatusRequest request) {
		RunnerRequestValidation.requireSupportedProtocolVersion(request.getProtocolVersion());

		var jobId = toJobId(request.getJobId());
		var reportedBy = toRunnerAssignment(request);
		var startedAt = RunnerRequestValidation.toRequiredInstant(request.hasStartedAt(), request.getStartedAt(), "started_at");

		return switch (request.getState()) {
			case JOB_EXECUTION_RUNNING -> toRunningReport(request, jobId, reportedBy, startedAt);
			case JOB_EXECUTION_SUCCEEDED -> JobStatusReport.succeeded(
					jobId,
					reportedBy,
					startedAt,
					toCompletedAt(request),
					exitCodeOf(request.getResult())
			);
			case JOB_EXECUTION_FAILED -> JobStatusReport.failed(
					jobId,
					reportedBy,
					startedAt,
					toCompletedAt(request),
					exitCodeOf(request.getResult()),
					failureReasonOf(request.getResult()),
					failureMessageOf(request.getResult())
			);
			case JOB_EXECUTION_STATE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException(
					"state must be a known execution state");
		};
	}

	private static JobStatusReport toRunningReport(
			ReportJobStatusRequest request,
			JobId jobId,
			RunnerAssignment reportedBy,
			Instant startedAt
	) {
		// A running report that carries an outcome contradicts itself, so it is a
		// caller defect rather than something to normalize.
		if (request.hasCompletedAt()) {
			throw new IllegalArgumentException("a running report must not carry completed_at");
		}
		if (request.hasResult()) {
			throw new IllegalArgumentException("a running report must not carry a result");
		}

		return JobStatusReport.running(jobId, reportedBy, startedAt);
	}

	private static JobId toJobId(String jobId) {
		if (jobId == null || jobId.isBlank()) {
			throw new IllegalArgumentException("job_id must not be empty");
		}
		try {
			return new JobId(UUID.fromString(jobId));
		} catch (IllegalArgumentException exception) {
			// A Runner echoes the identity it was dispatched, so a malformed value
			// is a caller defect rather than an unknown Job.
			throw new IllegalArgumentException("job_id must be the dispatched job identity");
		}
	}

	private static RunnerAssignment toRunnerAssignment(ReportJobStatusRequest request) {
		try {
			return new RunnerAssignment(request.getRunnerId(), request.getInstanceId());
		} catch (IllegalArgumentException exception) {
			throw new IllegalArgumentException("runner_id and instance_id must identify the reporting process");
		}
	}

	private static Instant toCompletedAt(ReportJobStatusRequest request) {
		var completedAt = RunnerRequestValidation.toRequiredInstant(request.hasCompletedAt(), request.getCompletedAt(), "completed_at");
		var startedAt = RunnerRequestValidation.toRequiredInstant(request.hasStartedAt(), request.getStartedAt(), "started_at");
		if (completedAt.isBefore(startedAt)) {
			throw new IllegalArgumentException("completed_at must not be before started_at");
		}

		return completedAt;
	}

	private static Integer exitCodeOf(JobExecutionResult result) {
		return result.hasExitCode() ? result.getExitCode() : null;
	}

	private static JobExecutionFailureReason failureReasonOf(JobExecutionResult result) {
		return switch (result.getFailureReason()) {
			case JOB_EXECUTION_FAILURE_NON_ZERO_EXIT -> JobExecutionFailureReason.NON_ZERO_EXIT;
			case JOB_EXECUTION_FAILURE_TIMEOUT -> JobExecutionFailureReason.TIMEOUT;
			case JOB_EXECUTION_FAILURE_CANCELLED -> JobExecutionFailureReason.CANCELLED;
			case JOB_EXECUTION_FAILURE_EXECUTION_ERROR -> JobExecutionFailureReason.EXECUTION_ERROR;
			case JOB_EXECUTION_FAILURE_REASON_UNSPECIFIED, UNRECOGNIZED -> JobExecutionFailureReason.UNKNOWN;
		};
	}

	private static String failureMessageOf(JobExecutionResult result) {
		var message = result.getFailureMessage();
		if (message.isBlank()) {
			return MISSING_FAILURE_MESSAGE;
		}
		if (message.length() > MAX_FAILURE_MESSAGE_LENGTH) {
			throw new IllegalArgumentException(
					"failure_message must be at most " + MAX_FAILURE_MESSAGE_LENGTH + " characters");
		}

		return message;
	}

	private static ReportJobStatusResponse toResponse(String jobId, JobStatusReportOutcome outcome) {
		return ReportJobStatusResponse.newBuilder()
				.setJobId(jobId)
				.setResult(toProtoResult(outcome.decision()))
				.setJobState(toProtoState(outcome.jobStatus()))
				.setMessage(outcome.message())
				.build();
	}

	private static JobStatusReportResult toProtoResult(JobStatusReportDecision decision) {
		return switch (decision) {
			case APPLIED -> JobStatusReportResult.JOB_STATUS_REPORT_APPLIED;
			case DUPLICATE -> JobStatusReportResult.JOB_STATUS_REPORT_DUPLICATE;
			case UNKNOWN_JOB -> JobStatusReportResult.JOB_STATUS_REPORT_UNKNOWN_JOB;
			case CONFLICT -> JobStatusReportResult.JOB_STATUS_REPORT_CONFLICT;
		};
	}

	private static JobLifecycleState toProtoState(JobStatus status) {
		if (status == null) {
			return JobLifecycleState.JOB_LIFECYCLE_STATE_UNSPECIFIED;
		}

		return switch (status) {
			case CREATED -> JobLifecycleState.JOB_LIFECYCLE_CREATED;
			case QUEUED -> JobLifecycleState.JOB_LIFECYCLE_QUEUED;
			case RUNNING -> JobLifecycleState.JOB_LIFECYCLE_RUNNING;
			case SUCCEEDED -> JobLifecycleState.JOB_LIFECYCLE_SUCCEEDED;
			case FAILED -> JobLifecycleState.JOB_LIFECYCLE_FAILED;
			case CANCELLED -> JobLifecycleState.JOB_LIFECYCLE_CANCELLED;
		};
	}
}

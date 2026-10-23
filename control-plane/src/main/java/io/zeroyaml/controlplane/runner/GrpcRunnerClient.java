package io.zeroyaml.controlplane.runner;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.zeroyaml.contracts.runner.v1.GetInfoRequest;
import io.zeroyaml.contracts.runner.v1.JobExecution;
import io.zeroyaml.contracts.runner.v1.JobRepository;
import io.zeroyaml.contracts.runner.v1.JobSpecification;
import io.zeroyaml.contracts.runner.v1.PingRequest;
import io.zeroyaml.contracts.runner.v1.RunJobRequest;
import io.zeroyaml.contracts.runner.v1.RunJobResponse;
import io.zeroyaml.contracts.runner.v1.RunnerServiceGrpc;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;

/**
 * gRPC implementation of {@link RunnerClient}. It translates generated protocol
 * types and gRPC transport errors into Control Plane types at this boundary.
 */
public class GrpcRunnerClient implements RunnerClient {

	private final RunnerServiceGrpc.RunnerServiceBlockingStub runnerService;
	private final Duration pingDeadline;
	private final Duration dispatchDeadline;

	public GrpcRunnerClient(ManagedChannel channel, Duration pingDeadline, Duration dispatchDeadline) {
		this(RunnerServiceGrpc.newBlockingStub(channel), pingDeadline, dispatchDeadline);
	}

	GrpcRunnerClient(
			RunnerServiceGrpc.RunnerServiceBlockingStub runnerService,
			Duration pingDeadline,
			Duration dispatchDeadline
	) {
		this.runnerService = Objects.requireNonNull(runnerService, "runnerService must not be null");
		this.pingDeadline = requirePositive(pingDeadline, "pingDeadline");
		this.dispatchDeadline = requirePositive(dispatchDeadline, "dispatchDeadline");
	}

	@Override
	public RunnerPingResult ping(String message) {
		try {
			// Apply a deadline to every call so an unreachable Runner cannot block indefinitely.
			var response = runnerService
					.withDeadlineAfter(pingDeadline.toNanos(), TimeUnit.NANOSECONDS)
					.ping(PingRequest.newBuilder().setMessage(message).build());
			return new RunnerPingResult(response.getMessage(), response.getRunnerVersion());
		} catch (StatusRuntimeException exception) {
			// Preserve the status code for retry and availability decisions outside this adapter.
			throw new RunnerClientException(
					exception.getStatus().getCode(),
					"Runner Ping failed with status " + exception.getStatus().getCode(),
					exception
			);
		}
	}

	@Override
	public RunnerInfo getInfo() {
		try {
			var response = runnerService
					.withDeadlineAfter(pingDeadline.toNanos(), TimeUnit.NANOSECONDS)
					.getInfo(GetInfoRequest.newBuilder().build());
			return RunnerInfoMapper.toDomain(response);
		} catch (StatusRuntimeException exception) {
			throw new RunnerClientException(
					exception.getStatus().getCode(),
					"Runner GetInfo failed with status " + exception.getStatus().getCode(),
					exception
			);
		}
	}

	@Override
	public JobDispatchResult dispatchJob(Job job) {
		Objects.requireNonNull(job, "job must not be null");
		var request = toRunJobRequest(job);
		try {
			var response = runnerService
					.withDeadlineAfter(dispatchDeadline.toNanos(), TimeUnit.NANOSECONDS)
					.runJob(request);
			return toDispatchResult(job.id(), response);
		} catch (StatusRuntimeException exception) {
			// A transport failure, and a request the Runner refuses to interpret at all,
			// are client failures rather than Job-level rejections. A Runner that declines
			// a well-formed dispatch answers with a rejected result instead.
			throw new RunnerClientException(
					exception.getStatus().getCode(),
					"Runner RunJob failed with status " + exception.getStatus().getCode(),
					exception
			);
		}
	}

	private static RunJobRequest toRunJobRequest(Job job) {
		var repository = job.repository();
		var execution = job.execution();
		return RunJobRequest.newBuilder()
				.setProtocolVersion(RunnerProtocolVersion.CURRENT)
				.setJob(JobSpecification.newBuilder()
						.setJobId(job.id().value().toString())
						.setRepository(JobRepository.newBuilder()
								.setLocation(repository.location().toString())
								.setRevision(repository.revision())
								.build())
						.setExecution(JobExecution.newBuilder()
								.addAllCommand(execution.command())
								.setWorkingDirectory(execution.workingDirectory())
								.build())
						.build())
				.build();
	}

	private static JobDispatchResult toDispatchResult(JobId jobId, RunJobResponse response) {
		// A mismatched echo means the acknowledgment cannot be attributed to this Job,
		// so it must not be recorded against it.
		if (!jobId.value().toString().equals(response.getJobId())) {
			throw new IllegalStateException("Runner acknowledged a different job than the one dispatched");
		}

		var assignment = toRunnerAssignment(response);
		return switch (response.getAcceptance()) {
			case JOB_ACCEPTED -> JobDispatchResult.accepted(
					jobId,
					assignment.orElseThrow(() -> new IllegalStateException(
							"Runner accepted a job without identifying the executing process")),
					response.getMessage()
			);
			case JOB_REJECTED -> JobDispatchResult.rejected(
					jobId,
					toRejectionReason(response.getRejectionReason()),
					assignment.orElse(null),
					response.getMessage()
			);
			case JOB_ACCEPTANCE_UNSPECIFIED, UNRECOGNIZED ->
					throw new IllegalStateException("Runner returned an unspecified job acceptance");
		};
	}

	private static Optional<RunnerAssignment> toRunnerAssignment(RunJobResponse response) {
		if (response.getRunnerId().isBlank() || response.getInstanceId().isBlank()) {
			return Optional.empty();
		}
		return Optional.of(new RunnerAssignment(response.getRunnerId(), response.getInstanceId()));
	}

	private static JobRejectionReason toRejectionReason(
			io.zeroyaml.contracts.runner.v1.JobRejectionReason reason) {
		// Rejection reasons evolve additively, so an unrecognized value stays a rejection
		// this Control Plane version cannot interpret.
		return switch (reason) {
			case JOB_REJECTION_UNSUPPORTED_PROTOCOL_VERSION -> JobRejectionReason.UNSUPPORTED_PROTOCOL_VERSION;
			case JOB_REJECTION_RUNNER_UNAVAILABLE -> JobRejectionReason.RUNNER_UNAVAILABLE;
			case JOB_REJECTION_REASON_UNSPECIFIED, UNRECOGNIZED -> JobRejectionReason.UNKNOWN;
		};
	}

	private static Duration requirePositive(Duration value, String fieldName) {
		Objects.requireNonNull(value, fieldName + " must not be null");
		// Reject invalid configuration before it can create a gRPC call without a usable deadline.
		if (value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(fieldName + " must be positive");
		}
		return value;
	}
}

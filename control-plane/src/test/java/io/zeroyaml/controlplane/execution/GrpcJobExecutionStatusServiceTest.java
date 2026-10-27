package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;

import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.zeroyaml.contracts.runner.v1.JobExecutionFailureReason;
import io.zeroyaml.contracts.runner.v1.JobExecutionResult;
import io.zeroyaml.contracts.runner.v1.JobExecutionState;
import io.zeroyaml.contracts.runner.v1.JobExecutionStatusServiceGrpc;
import io.zeroyaml.contracts.runner.v1.JobLifecycleState;
import io.zeroyaml.contracts.runner.v1.JobStatusReportResult;
import io.zeroyaml.contracts.runner.v1.ReportJobStatusRequest;
import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.runner.RunnerProtocolVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the status-reporting endpoint through the generated client over an
 * in-process channel, backed by the real recorder and Job store, so the
 * transport, validation, and lifecycle boundaries are tested together.
 */
class GrpcJobExecutionStatusServiceTest {

	private static final Instant CREATED_AT = Instant.parse("2026-01-01T10:00:00Z");
	private static final Instant STARTED_AT = CREATED_AT.plusSeconds(5);
	private static final Instant COMPLETED_AT = STARTED_AT.plusSeconds(30);
	private static final RunnerAssignment RUNNER = new RunnerAssignment("runner-1", "instance-1");

	private final JobStore jobs = new InMemoryJobStore();

	private ManagedChannel channel;
	private Server server;

	@AfterEach
	void tearDown() {
		if (channel != null) {
			channel.shutdownNow();
		}
		if (server != null) {
			server.shutdownNow();
		}
	}

	@Test
	void recordsThatAnAcceptedJobIsRunning() throws IOException {
		var client = startService();
		var job = queuedJob();

		var response = client.reportJobStatus(runningRequest(job.id()).build());

		assertEquals(JobStatusReportResult.JOB_STATUS_REPORT_APPLIED, response.getResult());
		assertEquals(JobLifecycleState.JOB_LIFECYCLE_RUNNING, response.getJobState());
		assertEquals(job.id().value().toString(), response.getJobId());
		assertEquals(JobStatus.RUNNING, job.status());
	}

	@Test
	void recordsASuccessfulExecutionAsSucceeded() throws IOException {
		var client = startService();
		var job = queuedJob();
		client.reportJobStatus(runningRequest(job.id()).build());

		var response = client.reportJobStatus(terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_SUCCEEDED)
				.setResult(JobExecutionResult.newBuilder().setExitCode(0).build())
				.build());

		assertEquals(JobStatusReportResult.JOB_STATUS_REPORT_APPLIED, response.getResult());
		assertEquals(JobLifecycleState.JOB_LIFECYCLE_SUCCEEDED, response.getJobState());
		assertEquals(JobStatus.SUCCEEDED, job.status());
		assertEquals(COMPLETED_AT, job.completedAt().orElseThrow());
	}

	@Test
	void recordsANonZeroExitAsFailedWithTheReasonAndTheExitCode() throws IOException {
		var client = startService();
		var job = queuedJob();
		client.reportJobStatus(runningRequest(job.id()).build());

		var response = client.reportJobStatus(terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_FAILED)
				.setResult(JobExecutionResult.newBuilder()
						.setExitCode(2)
						.setFailureReason(JobExecutionFailureReason.JOB_EXECUTION_FAILURE_NON_ZERO_EXIT)
						.setFailureMessage("The command exited with code 2")
						.build())
				.build());

		assertEquals(JobStatusReportResult.JOB_STATUS_REPORT_APPLIED, response.getResult());
		assertEquals(JobLifecycleState.JOB_LIFECYCLE_FAILED, response.getJobState());

		var failure = job.failure().orElseThrow();
		assertEquals("NON_ZERO_EXIT", failure.code());
		assertEquals(OptionalInt.of(2), failure.exit());
	}

	@Test
	void recordsATimeoutAsFailedWithoutAnExitCode() throws IOException {
		var client = startService();
		var job = queuedJob();
		client.reportJobStatus(runningRequest(job.id()).build());

		client.reportJobStatus(terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_FAILED)
				.setResult(JobExecutionResult.newBuilder()
						.setFailureReason(JobExecutionFailureReason.JOB_EXECUTION_FAILURE_TIMEOUT)
						.setFailureMessage("The job exceeded its time budget")
						.build())
				.build());

		var failure = job.failure().orElseThrow();
		assertEquals("TIMEOUT", failure.code());
		assertEquals(OptionalInt.empty(), failure.exit());
	}

	@Test
	void answersARepeatedTerminalReportWithoutChangingTheJob() throws IOException {
		var client = startService();
		var job = queuedJob();
		var request = terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_SUCCEEDED)
				.setResult(JobExecutionResult.newBuilder().setExitCode(0).build())
				.build();
		client.reportJobStatus(request);

		var response = client.reportJobStatus(request);

		assertEquals(JobStatusReportResult.JOB_STATUS_REPORT_DUPLICATE, response.getResult());
		assertEquals(JobLifecycleState.JOB_LIFECYCLE_SUCCEEDED, response.getJobState());
		assertEquals(COMPLETED_AT, job.completedAt().orElseThrow());
	}

	@Test
	void answersAReportForAnUnknownJobWithoutAJobState() throws IOException {
		var client = startService();

		var response = client.reportJobStatus(runningRequest(JobId.newId()).build());

		assertEquals(JobStatusReportResult.JOB_STATUS_REPORT_UNKNOWN_JOB, response.getResult());
		assertEquals(JobLifecycleState.JOB_LIFECYCLE_STATE_UNSPECIFIED, response.getJobState());
	}

	@Test
	void answersAReportFromAnotherRunnerInstanceAsAConflict() throws IOException {
		var client = startService();
		var job = queuedJob();
		client.reportJobStatus(runningRequest(job.id()).build());

		var response = client.reportJobStatus(terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_SUCCEEDED)
				.setInstanceId("instance-2")
				.setResult(JobExecutionResult.newBuilder().setExitCode(0).build())
				.build());

		assertEquals(JobStatusReportResult.JOB_STATUS_REPORT_CONFLICT, response.getResult());
		assertEquals(JobLifecycleState.JOB_LIFECYCLE_RUNNING, response.getJobState());
		assertEquals(JobStatus.RUNNING, job.status());
	}

	@Test
	void recordsAFailureWhoseReasonThisVersionDoesNotKnow() throws IOException {
		var client = startService();
		var job = queuedJob();

		// Refusing the report would leave the Job without an outcome, so an
		// unrecognized reason is still recorded as a failure.
		var response = client.reportJobStatus(terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_FAILED)
				.setResult(JobExecutionResult.newBuilder()
						.setFailureReasonValue(4096)
						.setFailureMessage("A newer runner reported an unfamiliar failure")
						.build())
				.build());

		assertEquals(JobStatusReportResult.JOB_STATUS_REPORT_APPLIED, response.getResult());
		assertEquals("UNKNOWN", job.failure().orElseThrow().code());
	}

	@Test
	void recordsAFailureReportedWithoutADiagnostic() throws IOException {
		var client = startService();
		var job = queuedJob();

		client.reportJobStatus(terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_FAILED)
				.setResult(JobExecutionResult.newBuilder()
						.setFailureReason(JobExecutionFailureReason.JOB_EXECUTION_FAILURE_EXECUTION_ERROR)
						.build())
				.build());

		assertEquals(JobStatus.FAILED, job.status());
		assertEquals(
				GrpcJobExecutionStatusService.MISSING_FAILURE_MESSAGE,
				job.failure().orElseThrow().message()
		);
	}

	@Test
	void refusesAReportSentUnderAnotherProtocolVersion() throws IOException {
		var client = startService();
		var job = queuedJob();

		var exception = assertThrows(StatusRuntimeException.class, () -> client.reportJobStatus(
				runningRequest(job.id()).setProtocolVersion("runner.v2").build()));

		assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
		assertEquals(JobStatus.QUEUED, job.status());
	}

	@Test
	void refusesReportsThatCannotBeInterpreted() throws IOException {
		var client = startService();
		var job = queuedJob();

		record Malformed(String name, ReportJobStatusRequest request) {
		}

		var malformedReports = List.of(
				new Malformed("blank protocol version",
						runningRequest(job.id()).setProtocolVersion(" ").build()),
				new Malformed("malformed job identity",
						runningRequest(job.id()).setJobId("not-a-job-identity").build()),
				new Malformed("missing job identity",
						runningRequest(job.id()).setJobId("").build()),
				new Malformed("missing reporting process",
						runningRequest(job.id()).setInstanceId("").build()),
				new Malformed("unspecified state",
						runningRequest(job.id()).setState(JobExecutionState.JOB_EXECUTION_STATE_UNSPECIFIED).build()),
				new Malformed("missing start time",
						runningRequest(job.id()).clearStartedAt().build()),
				new Malformed("running report with a completion time",
						runningRequest(job.id()).setCompletedAt(timestampOf(COMPLETED_AT)).build()),
				new Malformed("running report with a result",
						runningRequest(job.id())
								.setResult(JobExecutionResult.newBuilder().setExitCode(0).build())
								.build()),
				new Malformed("terminal report without a completion time",
						runningRequest(job.id()).setState(JobExecutionState.JOB_EXECUTION_SUCCEEDED).build()),
				new Malformed("completion before the start",
						terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_SUCCEEDED)
								.setCompletedAt(timestampOf(STARTED_AT.minusSeconds(1)))
								.build()),
				new Malformed("oversized failure diagnostic",
						terminalRequest(job.id(), JobExecutionState.JOB_EXECUTION_FAILED)
								.setResult(JobExecutionResult.newBuilder()
										.setFailureReason(
												JobExecutionFailureReason.JOB_EXECUTION_FAILURE_EXECUTION_ERROR)
										.setFailureMessage("e".repeat(
												GrpcJobExecutionStatusService.MAX_FAILURE_MESSAGE_LENGTH + 1))
										.build())
								.build())
		);

		for (var malformed : malformedReports) {
			var exception = assertThrows(
					StatusRuntimeException.class,
					() -> client.reportJobStatus(malformed.request()),
					malformed.name()
			);
			assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode(), malformed.name());
		}

		assertTrue(job.status() == JobStatus.QUEUED, "a refused report must not change the job");
	}

	private JobExecutionStatusServiceGrpc.JobExecutionStatusServiceBlockingStub startService() throws IOException {
		var serverName = InProcessServerBuilder.generateName();
		server = InProcessServerBuilder.forName(serverName)
				.directExecutor()
				.addService(new GrpcJobExecutionStatusService(new JobExecutionStatusRecorder(jobs, event -> { })))
				.build()
				.start();
		channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

		return JobExecutionStatusServiceGrpc.newBlockingStub(channel);
	}

	private ReportJobStatusRequest.Builder runningRequest(JobId jobId) {
		return ReportJobStatusRequest.newBuilder()
				.setProtocolVersion(RunnerProtocolVersion.CURRENT)
				.setJobId(jobId.value().toString())
				.setRunnerId(RUNNER.runnerId())
				.setInstanceId(RUNNER.instanceId())
				.setState(JobExecutionState.JOB_EXECUTION_RUNNING)
				.setStartedAt(timestampOf(STARTED_AT));
	}

	private ReportJobStatusRequest.Builder terminalRequest(JobId jobId, JobExecutionState state) {
		return runningRequest(jobId)
				.setState(state)
				.setCompletedAt(timestampOf(COMPLETED_AT));
	}

	private Job queuedJob() {
		var job = Job.create(JobId.newId(), repository(), execution(), CREATED_AT);
		job.queue(CREATED_AT.plusSeconds(1));
		jobs.add(job);

		return job;
	}

	private static Timestamp timestampOf(Instant instant) {
		return Timestamp.newBuilder()
				.setSeconds(instant.getEpochSecond())
				.setNanos(instant.getNano())
				.build();
	}

	private static RepositoryReference repository() {
		return new RepositoryReference(URI.create("https://github.com/example/repository.git"), "a1b2c3d4");
	}

	private static ExecutionDefinition execution() {
		return new ExecutionDefinition(List.of("bash", "-lc", "make build"), ".");
	}
}

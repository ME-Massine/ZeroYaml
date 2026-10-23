package io.zeroyaml.controlplane.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.zeroyaml.contracts.runner.v1.JobAcceptance;
import io.zeroyaml.contracts.runner.v1.PingRequest;
import io.zeroyaml.contracts.runner.v1.PingResponse;
import io.zeroyaml.contracts.runner.v1.RunJobRequest;
import io.zeroyaml.contracts.runner.v1.RunJobResponse;
import io.zeroyaml.contracts.runner.v1.RunnerCapabilities;
import io.zeroyaml.contracts.runner.v1.RunnerInfo;
import io.zeroyaml.contracts.runner.v1.RunnerStatus;
import io.zeroyaml.contracts.runner.v1.RunnerServiceGrpc;
import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the generated gRPC client against an in-process Runner, keeping the
 * protocol integration test fast and independent of an external Runner process.
 */
class GrpcRunnerClientTest {

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
	void dispatchesTheJobAndReturnsTheRunnerAcknowledgment() throws IOException {
		var runnerService = new DispatchService(response -> response
				.setAcceptance(JobAcceptance.JOB_ACCEPTED)
				.setMessage("job accepted by the runner")
				.setRunnerId("runner-dev-01")
				.setInstanceId("instance-1"));
		var client = startRunner(runnerService);
		var job = newJob();

		var result = client.dispatchJob(job);

		assertEquals(JobDispatchOutcome.ACCEPTED, result.outcome());
		assertEquals(job.id(), result.jobId());
		assertEquals(
				new RunnerAssignment("runner-dev-01", "instance-1"),
				result.runnerAssignment().orElseThrow()
		);
		assertEquals(Optional.empty(), result.rejection());

		// The dispatched request must carry the executable job data unchanged.
		var request = runnerService.lastRequest;
		assertEquals("runner.v1", request.getProtocolVersion());
		assertEquals(job.id().value().toString(), request.getJob().getJobId());
		assertEquals(
				"https://github.com/MohamedMBG/ZeroYaml.git",
				request.getJob().getRepository().getLocation()
		);
		assertEquals("3af0394", request.getJob().getRepository().getRevision());
		assertEquals(List.of("go", "test", "./..."), request.getJob().getExecution().getCommandList());
		assertEquals("runner", request.getJob().getExecution().getWorkingDirectory());
	}

	@Test
	void mapsARejectedDispatchToItsReason() throws IOException {
		var client = startRunner(new DispatchService(response -> response
				.setAcceptance(JobAcceptance.JOB_REJECTED)
				.setRejectionReason(io.zeroyaml.contracts.runner.v1.JobRejectionReason.JOB_REJECTION_RUNNER_UNAVAILABLE)
				.setMessage("runner status is unavailable and it is not accepting work")
				.setRunnerId("runner-dev-01")
				.setInstanceId("instance-1")));

		var result = client.dispatchJob(newJob());

		assertEquals(JobDispatchOutcome.REJECTED, result.outcome());
		assertEquals(JobRejectionReason.RUNNER_UNAVAILABLE, result.rejection().orElseThrow());
		assertEquals(
				new RunnerAssignment("runner-dev-01", "instance-1"),
				result.runnerAssignment().orElseThrow()
		);
	}

	@Test
	void mapsARejectionReasonThisVersionDoesNotKnowToUnknown() throws IOException {
		// A newer Runner may report a reason added after this Control Plane version.
		var client = startRunner(new DispatchService(response -> response
				.setAcceptance(JobAcceptance.JOB_REJECTED)
				.setMessage("declined")));

		var result = client.dispatchJob(newJob());

		assertEquals(JobDispatchOutcome.REJECTED, result.outcome());
		assertEquals(JobRejectionReason.UNKNOWN, result.rejection().orElseThrow());
		assertEquals(Optional.empty(), result.runnerAssignment());
	}

	@Test
	void mapsAnInvalidDispatchToAnExplicitClientException() throws IOException {
		var client = startRunner(new DispatchService(Status.INVALID_ARGUMENT
				.withDescription("job_id must not be empty")
				.asRuntimeException()));

		var job = newJob();
		var exception = assertThrows(RunnerClientException.class, () -> client.dispatchJob(job));

		assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatusCode());
	}

	@Test
	void mapsAnUnavailableRunnerDispatchToAnExplicitClientException() {
		channel = InProcessChannelBuilder
				.forName("missing-runner-dispatch-" + UUID.randomUUID())
				.directExecutor()
				.build();
		var client = new GrpcRunnerClient(channel, Duration.ofSeconds(1), Duration.ofSeconds(1));
		var job = newJob();

		var exception = assertThrows(RunnerClientException.class, () -> client.dispatchJob(job));

		assertEquals(Status.Code.UNAVAILABLE, exception.getStatusCode());
	}

	@Test
	void rejectsAnAcknowledgmentForADifferentJob() throws IOException {
		var client = startRunner(new DispatchService(response -> response
				.setJobId(UUID.randomUUID().toString())
				.setAcceptance(JobAcceptance.JOB_ACCEPTED)
				.setRunnerId("runner-dev-01")
				.setInstanceId("instance-1")));

		var job = newJob();

		assertThrows(IllegalStateException.class, () -> client.dispatchJob(job));
	}

	@Test
	void rejectsAnAcknowledgmentWithoutAnAcceptance() throws IOException {
		var client = startRunner(new DispatchService(response -> response.setMessage("no decision")));

		var job = newJob();

		assertThrows(IllegalStateException.class, () -> client.dispatchJob(job));
	}

	@Test
	void returnsTheRunnerPingResponse() throws IOException {
		var serverName = InProcessServerBuilder.generateName();
		// Direct executors make this focused protocol test deterministic and synchronous.
		server = InProcessServerBuilder
				.forName(serverName)
				.directExecutor()
				.addService(new PingService())
				.build()
				.start();
		channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

		var client = new GrpcRunnerClient(channel, Duration.ofSeconds(1), Duration.ofSeconds(1));

		var response = client.ping("control-plane");

		assertEquals("pong: control-plane", response.message());
		assertEquals("test-runner", response.runnerVersion());
	}

	@Test
	void returnsTheRunnerIdentityAndCapabilities() throws IOException {
		var serverName = InProcessServerBuilder.generateName();
		server = InProcessServerBuilder
				.forName(serverName)
				.directExecutor()
				.addService(new PingService())
				.build()
				.start();
		channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

		var client = new GrpcRunnerClient(channel, Duration.ofSeconds(1), Duration.ofSeconds(1));

		var response = client.getInfo();

		assertEquals("runner-dev-01", response.runnerId());
		assertEquals("instance-1", response.instanceId());
		assertEquals("0.1.0", response.runnerVersion());
		assertEquals("runner.v1", response.protocolVersion());
		assertEquals(RunnerState.UNAVAILABLE, response.state());
		assertEquals(false, response.acceptingWork());
		assertEquals("windows", response.capabilities().operatingSystem());
		assertEquals("amd64", response.capabilities().architecture());
		assertEquals(true, response.capabilities().dockerAvailable());
		assertEquals(java.util.List.of("docker"), response.capabilities().supportedExecutors());
		assertEquals("local", response.capabilities().labels().get("region"));
	}

	@Test
	void mapsAnUnavailableRunnerToAnExplicitClientException() {
		// No server is registered under this name, which produces gRPC UNAVAILABLE.
		channel = InProcessChannelBuilder
				.forName("missing-runner-" + UUID.randomUUID())
				.directExecutor()
				.build();
		var client = new GrpcRunnerClient(channel, Duration.ofSeconds(1), Duration.ofSeconds(1));

		var exception = assertThrows(RunnerClientException.class, () -> client.ping("control-plane"));

		assertEquals(Status.Code.UNAVAILABLE, exception.getStatusCode());
	}

	@Test
	void mapsAnUnavailableRunnerGetInfoToAnExplicitClientException() {
		channel = InProcessChannelBuilder
				.forName("missing-runner-info-" + UUID.randomUUID())
				.directExecutor()
				.build();
		var client = new GrpcRunnerClient(channel, Duration.ofSeconds(1), Duration.ofSeconds(1));

		var exception = assertThrows(RunnerClientException.class, client::getInfo);

		assertEquals(Status.Code.UNAVAILABLE, exception.getStatusCode());
	}

	/**
	 * Starts the given Runner service in process and returns a client bound to it.
	 * Direct executors keep the protocol test deterministic and synchronous.
	 */
	private GrpcRunnerClient startRunner(RunnerServiceGrpc.RunnerServiceImplBase runnerService) throws IOException {
		var serverName = InProcessServerBuilder.generateName();
		server = InProcessServerBuilder
				.forName(serverName)
				.directExecutor()
				.addService(runnerService)
				.build()
				.start();
		channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

		return new GrpcRunnerClient(channel, Duration.ofSeconds(1), Duration.ofSeconds(1));
	}

	/**
	 * Builds the Job a Control Plane dispatches after it has decided what to run.
	 */
	private static Job newJob() {
		return Job.create(
				new RepositoryReference(URI.create("https://github.com/MohamedMBG/ZeroYaml.git"), "3af0394"),
				new ExecutionDefinition(List.of("go", "test", "./..."), "runner"),
				Instant.parse("2026-09-20T10:15:30Z")
		);
	}

	/**
	 * Answers RunJob with a caller-supplied acknowledgment or gRPC status, which
	 * lets one test state exactly the Runner answer it covers. The response job
	 * identity defaults to the dispatched one, so only a test about correlation
	 * has to set it.
	 */
	private static final class DispatchService extends RunnerServiceGrpc.RunnerServiceImplBase {

		private final UnaryOperator<RunJobResponse.Builder> acknowledgment;
		private final StatusRuntimeException failure;

		private RunJobRequest lastRequest;

		private DispatchService(UnaryOperator<RunJobResponse.Builder> acknowledgment) {
			this.acknowledgment = acknowledgment;
			this.failure = null;
		}

		private DispatchService(StatusRuntimeException failure) {
			this.acknowledgment = null;
			this.failure = failure;
		}

		@Override
		public void runJob(RunJobRequest request, StreamObserver<RunJobResponse> responseObserver) {
			lastRequest = request;
			if (failure != null) {
				responseObserver.onError(failure);
				return;
			}

			responseObserver.onNext(
					acknowledgment.apply(RunJobResponse.newBuilder().setJobId(request.getJob().getJobId())).build()
			);
			responseObserver.onCompleted();
		}
	}

	private static final class PingService extends RunnerServiceGrpc.RunnerServiceImplBase {

		@Override
		public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
			responseObserver.onNext(PingResponse.newBuilder()
					.setMessage("pong: " + request.getMessage())
					.setRunnerVersion("test-runner")
					.build());
			responseObserver.onCompleted();
		}

		@Override
		public void getInfo(io.zeroyaml.contracts.runner.v1.GetInfoRequest request,
				StreamObserver<RunnerInfo> responseObserver) {
			responseObserver.onNext(RunnerInfo.newBuilder()
					.setRunnerId("runner-dev-01")
					.setInstanceId("instance-1")
					.setRunnerVersion("0.1.0")
					.setProtocolVersion("runner.v1")
					.setStatus(RunnerStatus.RUNNER_STATUS_UNAVAILABLE)
					.setAcceptingWork(false)
					.setCapabilities(RunnerCapabilities.newBuilder()
							.setOperatingSystem("windows")
							.setArchitecture("amd64")
							.setDockerAvailable(true)
							.addSupportedExecutors("docker")
							.putLabels("region", "local")
							.build())
					.build());
			responseObserver.onCompleted();
		}
	}
}

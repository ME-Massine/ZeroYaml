package io.zeroyaml.controlplane.registration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.zeroyaml.contracts.runner.v1.HeartbeatRequest;
import io.zeroyaml.contracts.runner.v1.HeartbeatResult;
import io.zeroyaml.contracts.runner.v1.RegisterRunnerRequest;
import io.zeroyaml.contracts.runner.v1.RegistrationResult;
import io.zeroyaml.contracts.runner.v1.RunnerCapabilities;
import io.zeroyaml.contracts.runner.v1.RunnerInfo;
import io.zeroyaml.contracts.runner.v1.RunnerRegistrationServiceGrpc;
import io.zeroyaml.contracts.runner.v1.RunnerStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the registration gRPC endpoint through the generated client over
 * an in-process channel, backed by the real {@link InMemoryRunnerRegistry} so
 * the transport and registry boundaries are tested together.
 */
class GrpcRunnerRegistrationServiceTest {

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
	void acceptsTheFirstRegistrationForARunnerId() throws IOException {
		var client = startService(newRegistry());

		var response = client.register(registerRequest("runner-1", "instance-1"));

		assertEquals(RegistrationResult.REGISTRATION_ACCEPTED, response.getResult());
		assertFalse(response.getRegistrationId().isBlank());
	}

	@Test
	void returnsTheSameRegistrationIdForARepeatedRequestFromTheSameInstance() throws IOException {
		var client = startService(newRegistry());

		var first = client.register(registerRequest("runner-1", "instance-1"));
		var second = client.register(registerRequest("runner-1", "instance-1"));

		assertEquals(RegistrationResult.REGISTRATION_ALREADY_REGISTERED, second.getResult());
		assertEquals(first.getRegistrationId(), second.getRegistrationId());
	}

	@Test
	void rejectsADifferentInstanceForAnAlreadyRegisteredRunnerId() throws IOException {
		var client = startService(newRegistry());

		client.register(registerRequest("runner-1", "instance-1"));
		var conflict = client.register(registerRequest("runner-1", "instance-2"));

		assertEquals(RegistrationResult.REGISTRATION_IDENTITY_CONFLICT, conflict.getResult());
		assertTrue(conflict.getRegistrationId().isEmpty());
	}

	@Test
	void rejectsAnUnspecifiedRunnerStatusAsAnInvalidArgument() throws IOException {
		var client = startService(newRegistry());

		var request = RegisterRunnerRequest.newBuilder()
				.setRunner(RunnerInfo.newBuilder()
						.setRunnerId("runner-1")
						.setInstanceId("instance-1")
						.setRunnerVersion("0.1.0")
						.setProtocolVersion("runner.v1")
						.setStatus(RunnerStatus.RUNNER_STATUS_UNSPECIFIED)
						.setCapabilities(RunnerCapabilities.newBuilder().build())
						.build())
				.build();

		assertThrows(StatusRuntimeException.class, () -> client.register(request));
	}

	@Test
	void acknowledgesAHeartbeatForARegisteredInstance() throws IOException {
		var client = startService(newRegistry());
		client.register(registerRequest("runner-1", "instance-1"));

		var response = client.heartbeat(HeartbeatRequest.newBuilder()
				.setRunnerId("runner-1")
				.setInstanceId("instance-1")
				.build());

		assertEquals(HeartbeatResult.HEARTBEAT_ACKNOWLEDGED, response.getResult());
	}

	@Test
	void reportsUnknownRunnerForAHeartbeatBeforeRegistration() throws IOException {
		var client = startService(newRegistry());

		var response = client.heartbeat(HeartbeatRequest.newBuilder()
				.setRunnerId("runner-1")
				.setInstanceId("instance-1")
				.build());

		assertEquals(HeartbeatResult.HEARTBEAT_UNKNOWN_RUNNER, response.getResult());
	}

	@Test
	void reportsUnknownRunnerForAHeartbeatFromADifferentInstance() throws IOException {
		var client = startService(newRegistry());
		client.register(registerRequest("runner-1", "instance-1"));

		var response = client.heartbeat(HeartbeatRequest.newBuilder()
				.setRunnerId("runner-1")
				.setInstanceId("instance-2")
				.build());

		assertEquals(HeartbeatResult.HEARTBEAT_UNKNOWN_RUNNER, response.getResult());
	}

	private static InMemoryRunnerRegistry newRegistry() {
		return new InMemoryRunnerRegistry(
				Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
				new RunnerRegistrationServerProperties()
		);
	}

	private RunnerRegistrationServiceGrpc.RunnerRegistrationServiceBlockingStub startService(
			InMemoryRunnerRegistry registry) throws IOException {
		var serverName = InProcessServerBuilder.generateName();
		server = InProcessServerBuilder
				.forName(serverName)
				.directExecutor()
				.addService(new GrpcRunnerRegistrationService(registry))
				.build()
				.start();
		channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

		return RunnerRegistrationServiceGrpc.newBlockingStub(channel);
	}

	private static RegisterRunnerRequest registerRequest(String runnerId, String instanceId) {
		return RegisterRunnerRequest.newBuilder()
				.setRunner(RunnerInfo.newBuilder()
						.setRunnerId(runnerId)
						.setInstanceId(instanceId)
						.setRunnerVersion("0.1.0")
						.setProtocolVersion("runner.v1")
						.setStatus(RunnerStatus.RUNNER_STATUS_STARTING)
						.setAcceptingWork(false)
						.setCapabilities(RunnerCapabilities.newBuilder()
								.setOperatingSystem("windows")
								.setArchitecture("amd64")
								.build())
						.build())
				.build();
	}
}

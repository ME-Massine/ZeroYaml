package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import io.zeroyaml.contracts.runner.v1.AppendJobLogRequest;
import io.zeroyaml.contracts.runner.v1.JobLogServiceGrpc;
import io.zeroyaml.contracts.runner.v1.JobLogStream;
import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real TLS transport regressions; certificates and keys exist only in temporary test storage. */
class JobLogTlsTest {
	@TempDir
	static Path directory;
	private static Identity serverIdentity;
	private static Identity runnerIdentity;
	private static Identity otherIdentity;
	private static Identity anonymousIdentity;
	private static Path clientTrust;

	@BeforeAll
	static void certificates() throws Exception {
		serverIdentity = identity("server", "dns:localhost", "serverAuth");
		runnerIdentity = identity("runner", "uri:urn:zeroyaml:runner:runner-1", "clientAuth");
		otherIdentity = identity("other", "uri:urn:zeroyaml:runner:other-runner", "clientAuth");
		anonymousIdentity = identity("anonymous", "dns:anonymous", "clientAuth");
		clientTrust = directory.resolve("clients.pem");
		Files.writeString(clientTrust, Files.readString(runnerIdentity.certificate())
				+ Files.readString(otherIdentity.certificate()) + Files.readString(anonymousIdentity.certificate()));
	}

	@Test
	void authenticatesAssignedRunnerAndRejectsForgedIdentityAndUnauthenticatedPeers() throws Exception {
		var jobs = new InMemoryJobStore();
		var logs = new InMemoryJobLogStore();
		var created = Instant.now();
		var job = Job.create(new RepositoryReference(URI.create("https://example.com/repo.git"), "abc123"),
				new ExecutionDefinition(List.of("test"), "."), created);
		job.queue(created);
		job.start(new RunnerAssignment("runner-1", "instance-1"), created);
		jobs.add(job);
		// Port zero is used only by this directly constructed test instance.
		var server = new JobLogGrpcServer(new JobLogServerProperties(0, serverIdentity.certificate(), serverIdentity.key(), clientTrust),
				new JobLogRecorder(jobs, logs));
		server.start();
		try {
			var request = AppendJobLogRequest.newBuilder().setProtocolVersion("runner.v1")
					.setJobId(job.id().value().toString()).setRunnerId("runner-1").setInstanceId("instance-1")
					.setSequence(1).setObservedAt(Timestamp.newBuilder().setSeconds(created.getEpochSecond() - 60))
					.setStream(JobLogStream.JOB_LOG_STREAM_STDOUT).setContent(ByteString.copyFromUtf8("output")).build();
			assertRpc(Status.Code.UNAVAILABLE, channel(server.port(), null, false), request);
			assertRpc(Status.Code.UNAVAILABLE, channel(server.port(), null, true), request);
			// The server certificate is not trusted as a Runner certificate.
			assertRpc(Status.Code.UNAVAILABLE, channel(server.port(), serverIdentity, true), request);
			assertRpc(Status.Code.UNAUTHENTICATED, channel(server.port(), anonymousIdentity, true), request);
			assertRpc(Status.Code.PERMISSION_DENIED, channel(server.port(), otherIdentity, true), request);
			assertTrue(logs.snapshot(job.id()).isEmpty());
			var channel = channel(server.port(), runnerIdentity, true);
			try {
				var client = JobLogServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
				client.appendJobLog(request);
				var wrongInstance = assertThrows(StatusRuntimeException.class,
						() -> client.appendJobLog(request.toBuilder().setSequence(2).setInstanceId("forged").build()));
				assertEquals(Status.Code.FAILED_PRECONDITION, wrongInstance.getStatus().getCode());
				client.appendJobLog(request.toBuilder().setSequence(2).clearContent().clearStream().setEndOfOutput(true).build());
				assertEquals(2, logs.snapshot(job.id()).size());
			} finally {
				channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
			}
		} finally {
			server.stop();
		}
		assertFalse(server.isRunning());
	}

	@Test
	void disabledIngressDoesNotOpenAListenerAndPartialConfigurationFails() {
		var disabled = new JobLogServerProperties(null, null, null, null);
		disabled.validate();
		var server = new JobLogGrpcServer(disabled, new JobLogRecorder(new InMemoryJobStore(), new InMemoryJobLogStore()));
		server.start();
		assertFalse(server.isRunning());
		assertThrows(IllegalStateException.class, () -> new JobLogServerProperties(50053, null, null, null).validate());
		assertThrows(IllegalStateException.class, () -> new JobLogServerProperties(null, serverIdentity.certificate(), null, null).validate());
		assertThrows(IllegalStateException.class, () -> new JobLogServerProperties(0, serverIdentity.certificate(), serverIdentity.key(), clientTrust).validate());
		assertThrows(IllegalStateException.class, () -> new JobLogServerProperties(50053, directory.resolve("missing"), serverIdentity.key(), clientTrust).validate());
	}

	private static ManagedChannel channel(int port, Identity identity, boolean tls) throws Exception {
		if (!tls) { return ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build(); }
		var credentials = TlsChannelCredentials.newBuilder().trustManager(serverIdentity.certificate().toFile());
		if (identity != null) { credentials.keyManager(identity.certificate().toFile(), identity.key().toFile()); }
		return Grpc.newChannelBuilderForAddress("localhost", port, credentials.build()).build();
	}

	private static void assertRpc(Status.Code code, ManagedChannel channel, AppendJobLogRequest request) throws Exception {
		try {
			var error = assertThrows(StatusRuntimeException.class,
					() -> JobLogServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS).appendJobLog(request));
			assertEquals(code, error.getStatus().getCode());
		} finally {
			channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	private static Identity identity(String name, String san, String usage) throws Exception {
		var store = directory.resolve(name + ".p12");
		// The JDK tool avoids test-only crypto dependencies and checked-in keys.
		var executable = Path.of(System.getProperty("java.home"), "bin", System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool");
		var output = directory.resolve(name + ".txt");
		var process = new ProcessBuilder(executable.toString(), "-genkeypair", "-alias", "test", "-keyalg", "EC",
				"-groupname", "secp256r1", "-dname", "CN=" + name, "-validity", "2", "-ext", "SAN=" + san,
				"-ext", "EKU=" + usage, "-storetype", "PKCS12", "-keystore", store.toString(), "-storepass", "test-only-password")
				.redirectErrorStream(true).redirectOutput(output.toFile()).start();
		if (!process.waitFor(30, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new IllegalStateException("test certificate generation timed out");
		}
		assertEquals(0, process.exitValue(), "test certificate generation failed");
		var keyStore = KeyStore.getInstance("PKCS12");
		try (var input = Files.newInputStream(store)) { keyStore.load(input, "test-only-password".toCharArray()); }
		var certificate = directory.resolve(name + ".pem");
		var key = directory.resolve(name + "-key.pem");
		Files.writeString(certificate, pem("CERTIFICATE", keyStore.getCertificate("test").getEncoded()), StandardCharsets.US_ASCII);
		Files.writeString(key, pem("PRIVATE KEY", keyStore.getKey("test", "test-only-password".toCharArray()).getEncoded()), StandardCharsets.US_ASCII);
		return new Identity(certificate, key);
	}

	private static String pem(String type, byte[] bytes) {
		return "-----BEGIN " + type + "-----\n" + Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(bytes)
				+ "\n-----END " + type + "-----\n";
	}

	private record Identity(Path certificate, Path key) { }
}

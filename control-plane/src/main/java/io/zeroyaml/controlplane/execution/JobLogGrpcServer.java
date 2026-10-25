package io.zeroyaml.controlplane.execution;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.TlsServerCredentials;

/** Owns the optional mTLS log listener, isolated from the legacy Runner transport. */
@Component
class JobLogGrpcServer implements SmartLifecycle {
	private final JobLogServerProperties properties;
	private final JobLogRecorder recorder;
	private Server server;

	JobLogGrpcServer(JobLogServerProperties properties, JobLogRecorder recorder) {
		this.properties = properties;
		this.recorder = recorder;
	}

	@Override
	public synchronized void start() {
		if (server != null || !properties.enabled()) {
			return;
		}
		try {
			var credentials = TlsServerCredentials.newBuilder()
					.keyManager(properties.certificate().toFile(), properties.privateKey().toFile())
					.trustManager(properties.clientCa().toFile())
					.clientAuth(TlsServerCredentials.ClientAuth.REQUIRE).build();
			server = Grpc.newServerBuilderForPort(properties.port(), credentials)
					.maxInboundMessageSize(8192)
					.handshakeTimeout(5, TimeUnit.SECONDS)
					.addService(ServerInterceptors.intercept(new GrpcJobLogService(recorder), new JobLogAuthentication()))
					.build().start();
		} catch (IOException | RuntimeException exception) {
			throw new IllegalStateException("Failed to start mTLS Job log ingress; check zeroyaml.job-logs TLS configuration and port", exception);
		}
	}

	@Override
	public synchronized void stop() {
		if (server == null) {
			return;
		}
		server.shutdown();
		try {
			if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
				server.shutdownNow();
			}
		} catch (InterruptedException exception) {
			server.shutdownNow();
			Thread.currentThread().interrupt();
		} finally {
			server = null;
		}
	}

	/** Bound port, including an OS-assigned port for isolated transport tests. */
	synchronized int port() { return server.getPort(); }

	@Override
	public synchronized boolean isRunning() { return server != null; }
}

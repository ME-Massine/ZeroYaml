package io.zeroyaml.controlplane.execution;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.TlsServerCredentials;

/**
 * Owns the optional mTLS log listener, isolated from the legacy Runner transport.
 *
 * <p>Calls run on a fixed pool instead of grpc-java's unbounded cached
 * executor. The retention lock and the Job lookup serialize most work, so more
 * threads would only wait. Each Runner sends one call at a time per Job and
 * retention holds at most 32 Jobs, so {@value #MAX_CONCURRENT_CALLS} concurrent
 * calls leave headroom; {@link JobLogCallLimit} refuses the rest with
 * {@code RESOURCE_EXHAUSTED}. The work queue is bounded well above the tasks
 * those calls can schedule; a task beyond it is rejected rather than queued.</p>
 */
@Component
class JobLogGrpcServer implements SmartLifecycle {
	static final int WORKER_THREADS = 8;
	static final int MAX_CONCURRENT_CALLS = 64;
	private static final int MAX_QUEUED_TASKS = MAX_CONCURRENT_CALLS * 8;
	private final JobLogServerProperties properties;
	private final JobLogRecorder recorder;
	private Server server;
	private ExecutorService executor;

	JobLogGrpcServer(JobLogServerProperties properties, JobLogRecorder recorder) {
		this.properties = properties;
		this.recorder = recorder;
	}

	@Override
	public synchronized void start() {
		if (server != null || !properties.enabled()) {
			return;
		}
		executor = newExecutor();
		try {
			var credentials = TlsServerCredentials.newBuilder()
					.keyManager(properties.certificate().toFile(), properties.privateKey().toFile())
					.trustManager(properties.clientCa().toFile())
					.clientAuth(TlsServerCredentials.ClientAuth.REQUIRE).build();
			// The last interceptor runs first: the call limit applies before authentication work.
			server = Grpc.newServerBuilderForPort(properties.port(), credentials)
					.executor(executor)
					.maxInboundMessageSize(8192)
					.handshakeTimeout(5, TimeUnit.SECONDS)
					.addService(ServerInterceptors.intercept(new GrpcJobLogService(recorder),
							new JobLogAuthentication(), new JobLogCallLimit(MAX_CONCURRENT_CALLS)))
					.build().start();
		} catch (IOException | RuntimeException exception) {
			executor.shutdownNow();
			executor = null;
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
			// The server no longer schedules work once it has terminated or been forced down.
			executor.shutdownNow();
			server = null;
			executor = null;
		}
	}

	/** Bound port, including an OS-assigned port for isolated transport tests. */
	synchronized int port() { return server.getPort(); }

	@Override
	public synchronized boolean isRunning() { return server != null; }

	private static ExecutorService newExecutor() {
		var count = new AtomicInteger();
		ThreadFactory threads = task -> {
			var thread = new Thread(task, "job-log-ingress-" + count.incrementAndGet());
			thread.setDaemon(true);
			return thread;
		};
		return new ThreadPoolExecutor(WORKER_THREADS, WORKER_THREADS, 0, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(MAX_QUEUED_TASKS), threads, new ThreadPoolExecutor.AbortPolicy());
	}
}

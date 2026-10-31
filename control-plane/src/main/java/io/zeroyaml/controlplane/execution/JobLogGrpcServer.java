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

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.TlsServerCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

/**
 * Owns the optional mTLS log listener, isolated from the legacy Runner transport.
 *
 * <p>Calls run on a fixed pool instead of grpc-java's unbounded cached
 * executor. The retention lock and the Job lookup serialize most work, so more
 * threads would only wait. Each Runner sends one call at a time per Job and
 * retention holds at most 32 Jobs, so {@value #MAX_CONCURRENT_CALLS} concurrent
 * calls leave headroom; {@link JobLogCallLimit} refuses the rest with
 * {@code RESOURCE_EXHAUSTED}.</p>
 *
 * <p>The call limit runs on the executor, so a refused call still takes a queue
 * slot for its start task. grpc-java serializes each call's events into at most
 * one queued task, plus one context-cancellation task when the call fails, so an
 * admitted call holds at most two slots. The queue holds those of every admitted
 * call, 2 &times; {@value #MAX_CONCURRENT_CALLS}, plus the start tasks of 48
 * connections at their {@value #MAX_CALLS_PER_CONNECTION}-stream cap; every
 * connection needs a valid Runner certificate. A task beyond the queue is
 * rejected, which resets that stream instead of queueing it. The rejected
 * events never reach the call, so {@link JobLogCallLimit#streamTracerFactory()}
 * releases the permit when the transport closes the stream. Do not replace the
 * rejection with a caller-runs policy: the caller is the Netty event loop, and
 * a Job lookup there would stall every connection on that loop.</p>
 */
@Component
class JobLogGrpcServer implements SmartLifecycle {
	static final int WORKER_THREADS = 8;
	static final int MAX_CONCURRENT_CALLS = 64;
	/**
	 * Concurrent streams one connection may open. A Runner sends one call at a
	 * time per Job and runs at most 16 Jobs, but its default is one; 8 covers a
	 * typical Runner and stops a single connection from filling the queue.
	 * Streams beyond it wait on the client side under HTTP/2 flow control.
	 */
	static final int MAX_CALLS_PER_CONNECTION = 8;
	private static final int MAX_QUEUED_TASKS = 2 * MAX_CONCURRENT_CALLS + 48 * MAX_CALLS_PER_CONNECTION;
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
			var callLimit = new JobLogCallLimit(MAX_CONCURRENT_CALLS);
			// The last interceptor runs first: the call limit applies before authentication work.
			server = NettyServerBuilder.forPort(properties.port(), credentials)
					.executor(executor)
					.maxConcurrentCallsPerConnection(MAX_CALLS_PER_CONNECTION)
					.maxInboundMessageSize(8192)
					.handshakeTimeout(5, TimeUnit.SECONDS)
					.addStreamTracerFactory(callLimit.streamTracerFactory())
					.addService(ServerInterceptors.intercept(new GrpcJobLogService(recorder),
							new JobLogAuthentication(), callLimit))
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

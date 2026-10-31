package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.zeroyaml.contracts.runner.v1.AppendJobLogRequest;
import io.zeroyaml.contracts.runner.v1.AppendJobLogResponse;
import io.zeroyaml.contracts.runner.v1.JobLogServiceGrpc;
import org.junit.jupiter.api.Test;

/** Permit accounting is checked directly; refusal and executor rejection are also checked over transports. */
class JobLogCallLimitTest {
	private final AtomicInteger started = new AtomicInteger();
	private final ServerCallHandler<String, String> handler = (call, headers) -> {
		started.incrementAndGet();
		return new ServerCall.Listener<>() { };
	};

	@Test
	void refusesCallsOverTheLimitWithoutStartingThem() {
		var limit = new JobLogCallLimit(1);
		limit.interceptCall(call(), new Metadata(), handler);
		var refused = call();
		limit.interceptCall(refused, new Metadata(), handler);
		assertEquals(1, started.get());
		verify(refused).close(argThat(status -> status.getCode() == Status.Code.RESOURCE_EXHAUSTED), any());
	}

	@Test
	void releasesEachPermitExactlyOnceAcrossCloseCompleteAndCancel() {
		var limit = new JobLogCallLimit(1);
		var captured = new ServerCall<?, ?>[1];
		ServerCallHandler<String, String> capturing = (call, headers) -> {
			captured[0] = call;
			return handler.startCall(call, headers);
		};
		var listener = limit.interceptCall(call(), new Metadata(), capturing);
		captured[0].close(Status.OK, new Metadata());
		listener.onComplete();
		listener.onCancel();
		limit.interceptCall(call(), new Metadata(), handler);
		var refused = call();
		limit.interceptCall(refused, new Metadata(), handler);
		verify(refused).close(argThat(status -> status.getCode() == Status.Code.RESOURCE_EXHAUSTED), any());
	}

	@Test
	void cancelledCallsReleaseTheirPermit() {
		var limit = new JobLogCallLimit(1);
		limit.interceptCall(call(), new Metadata(), handler).onCancel();
		var next = call();
		limit.interceptCall(next, new Metadata(), handler);
		verify(next, never()).close(any(), any());
		assertEquals(2, started.get());
	}

	@Test
	void handlerFailuresReleaseTheirPermit() {
		var limit = new JobLogCallLimit(1);
		ServerCallHandler<String, String> failing = (call, headers) -> { throw new IllegalStateException("start failed"); };
		assertThrows(IllegalStateException.class, () -> limit.interceptCall(call(), new Metadata(), failing));
		var next = call();
		limit.interceptCall(next, new Metadata(), handler);
		verify(next, never()).close(any(), any());
	}

	@Test
	void rejectsNonPositiveLimits() {
		assertThrows(IllegalArgumentException.class, () -> new JobLogCallLimit(0));
	}

	@Test
	void concurrentCallOverTheLimitFailsFastOverTheTransport() throws IOException, InterruptedException {
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var service = new JobLogServiceGrpc.JobLogServiceImplBase() {
			@Override
			public void appendJobLog(AppendJobLogRequest request, StreamObserver<AppendJobLogResponse> observer) {
				entered.countDown();
				try {
					release.await(5, TimeUnit.SECONDS);
				} catch (InterruptedException exception) {
					Thread.currentThread().interrupt();
				}
				observer.onNext(AppendJobLogResponse.getDefaultInstance());
				observer.onCompleted();
			}
		};
		ExecutorService workers = Executors.newFixedThreadPool(2);
		var name = InProcessServerBuilder.generateName();
		var server = InProcessServerBuilder.forName(name).executor(workers)
				.addService(ServerInterceptors.intercept(service, new JobLogCallLimit(1))).build().start();
		var channel = InProcessChannelBuilder.forName(name).build();
		try {
			var client = JobLogServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
			var first = JobLogServiceGrpc.newFutureStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
					.appendJobLog(AppendJobLogRequest.getDefaultInstance());
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			var refused = assertThrows(StatusRuntimeException.class,
					() -> client.appendJobLog(AppendJobLogRequest.getDefaultInstance()));
			assertEquals(Status.Code.RESOURCE_EXHAUSTED, refused.getStatus().getCode());
			release.countDown();
			first.get(5, TimeUnit.SECONDS);
			client.appendJobLog(AppendJobLogRequest.getDefaultInstance());
		} catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException exception) {
			throw new AssertionError("first call did not complete", exception);
		} finally {
			release.countDown();
			channel.shutdownNow();
			server.shutdownNow();
			workers.shutdownNow();
		}
	}

	@Test
	void rejectedExecutorTasksDoNotLeakPermits() throws Exception {
		var limit = new JobLogCallLimit(2);
		// One worker and one queue slot, so the call's next event is rejected once both are taken.
		var executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
		var ready = new CountDownLatch(1);
		ServerCallHandler<String, String> streaming = (call, headers) -> {
			call.request(1);
			return new ServerCall.Listener<>() {
				@Override
				public void onReady() { ready.countDown(); }

				@Override
				public void onHalfClose() { call.close(Status.OK, new Metadata()); }
			};
		};
		var service = ServerServiceDefinition.builder(STREAMING.getServiceName()).addMethod(STREAMING, streaming).build();
		var server = NettyServerBuilder.forPort(0, InsecureServerCredentials.create()).executor(executor)
				.addStreamTracerFactory(limit.streamTracerFactory())
				.addService(ServerInterceptors.intercept(service, limit)).build().start();
		var channel = Grpc.newChannelBuilderForAddress("localhost", server.getPort(), InsecureChannelCredentials.create()).build();
		var blocker = new CountDownLatch(1);
		try {
			var call = channel.newCall(STREAMING, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS));
			var closed = new CompletableFuture<Status>();
			call.start(new ClientCall.Listener<>() {
				@Override
				public void onClose(Status status, Metadata trailers) { closed.complete(status); }
			}, new Metadata());
			assertTrue(ready.await(5, TimeUnit.SECONDS));
			assertEquals(1, limit.availablePermits());

			var blocking = new CountDownLatch(1);
			executor.execute(() -> {
				blocking.countDown();
				awaitQuietly(blocker);
			});
			assertTrue(blocking.await(5, TimeUnit.SECONDS));
			executor.execute(() -> { });
			call.sendMessage("chunk");
			call.halfClose();

			// The rejected events never reach the call, so only the stream close can free its permit.
			assertNotEquals(Status.Code.OK, closed.get(5, TimeUnit.SECONDS).getCode());
			blocker.countDown();
			awaitAvailablePermits(limit, 2);
		} finally {
			blocker.countDown();
			channel.shutdownNow();
			server.shutdownNow();
			executor.shutdownNow();
		}
	}

	private static final MethodDescriptor<String, String> STREAMING = MethodDescriptor.<String, String>newBuilder()
			.setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
			.setFullMethodName(MethodDescriptor.generateFullMethodName("test.CallLimit", "Stream"))
			.setRequestMarshaller(new Utf8Marshaller()).setResponseMarshaller(new Utf8Marshaller()).build();

	private static final class Utf8Marshaller implements MethodDescriptor.Marshaller<String> {
		@Override
		public InputStream stream(String value) { return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)); }

		@Override
		public String parse(InputStream stream) {
			try {
				return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
			} catch (IOException exception) {
				throw new UncheckedIOException(exception);
			}
		}
	}

	private static void awaitQuietly(CountDownLatch latch) {
		try {
			latch.await(5, TimeUnit.SECONDS);
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
		}
	}

	/** The server transport reports the stream close on its own thread, possibly after the client has seen it. */
	private static void awaitAvailablePermits(JobLogCallLimit limit, int expected) {
		var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (limit.availablePermits() != expected && System.nanoTime() < deadline) {
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
		}
		assertEquals(expected, limit.availablePermits(), "a call-limit permit leaked");
	}

	@SuppressWarnings("unchecked")
	private static ServerCall<String, String> call() {
		return mock(ServerCall.class);
	}
}

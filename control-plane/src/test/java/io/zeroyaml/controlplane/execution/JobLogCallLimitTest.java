package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.zeroyaml.contracts.runner.v1.AppendJobLogRequest;
import io.zeroyaml.contracts.runner.v1.AppendJobLogResponse;
import io.zeroyaml.contracts.runner.v1.JobLogServiceGrpc;
import org.junit.jupiter.api.Test;

/** Permit accounting is checked directly; refusal is also checked over a transport. */
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

	@SuppressWarnings("unchecked")
	private static ServerCall<String, String> call() {
		return mock(ServerCall.class);
	}
}

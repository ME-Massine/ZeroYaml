package io.zeroyaml.controlplane.execution;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Caps concurrent log ingress calls. A call over the limit is answered with
 * {@code RESOURCE_EXHAUSTED} at once instead of waiting for a worker, so a burst
 * never accumulates threads or queued work behind the retention lock and the
 * Job lookup. The Runner does not retry log chunks, so a refused chunk is
 * reported as lost in its final marker, like any other delivery failure.
 *
 * <p>A permit is released exactly once, when the call closes, completes, or is
 * cancelled, including when the next handler fails to start the call.</p>
 */
final class JobLogCallLimit implements ServerInterceptor {
	private final Semaphore permits;

	JobLogCallLimit(int maxConcurrentCalls) {
		if (maxConcurrentCalls < 1) {
			throw new IllegalArgumentException("maxConcurrentCalls must be positive");
		}
		this.permits = new Semaphore(maxConcurrentCalls);
	}

	@Override
	public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
			Metadata headers, ServerCallHandler<ReqT, RespT> next) {
		if (!permits.tryAcquire()) {
			call.close(Status.RESOURCE_EXHAUSTED.withDescription("Job log ingress is at its concurrent call limit"),
					new Metadata());
			return new ServerCall.Listener<>() { };
		}
		var released = new AtomicBoolean();
		Runnable release = () -> {
			if (released.compareAndSet(false, true)) {
				permits.release();
			}
		};
		var tracked = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
			@Override
			public void close(Status status, Metadata trailers) {
				try {
					super.close(status, trailers);
				} finally {
					release.run();
				}
			}
		};
		ServerCall.Listener<ReqT> listener;
		try {
			listener = next.startCall(tracked, headers);
		} catch (RuntimeException exception) {
			release.run();
			throw exception;
		}
		return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
			@Override
			public void onCancel() {
				try {
					super.onCancel();
				} finally {
					release.run();
				}
			}

			@Override
			public void onComplete() {
				try {
					super.onComplete();
				} finally {
					release.run();
				}
			}
		};
	}
}

package io.zeroyaml.controlplane.execution;

import java.time.Instant;
import java.util.UUID;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.zeroyaml.contracts.runner.v1.AppendJobLogRequest;
import io.zeroyaml.contracts.runner.v1.AppendJobLogResponse;
import io.zeroyaml.contracts.runner.v1.JobLogServiceGrpc;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.runner.RunnerProtocolVersion;

/** Bounded log ingress registered only on the authenticated TLS listener. */
class GrpcJobLogService extends JobLogServiceGrpc.JobLogServiceImplBase {
	private final JobLogRecorder recorder;

	GrpcJobLogService(JobLogRecorder recorder) { this.recorder = recorder; }

	@Override
	public void appendJobLog(AppendJobLogRequest request, StreamObserver<AppendJobLogResponse> observer) {
		try {
			var principal = JobLogAuthentication.RUNNER_ID.get();
			if (principal == null) {
				observer.onError(Status.UNAUTHENTICATED.withDescription("Runner certificate required").asRuntimeException());
				return;
			}
			var record = toRecord(request);
			if (!principal.equals(record.reportedBy().runnerId())) {
				observer.onError(Status.PERMISSION_DENIED.withDescription("Runner certificate identity mismatch").asRuntimeException());
				return;
			}
			if (!recorder.append(record)) {
				observer.onError(Status.NOT_FOUND.withDescription("Job is unknown").asRuntimeException());
				return;
			}
			observer.onNext(AppendJobLogResponse.getDefaultInstance());
			observer.onCompleted();
		} catch (JobLogCapacityException exception) {
			observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("Active Job log retention is full").asRuntimeException());
		} catch (IllegalArgumentException exception) {
			observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid job log record").asRuntimeException());
		} catch (JobLogAttributionException | JobLogSequenceException exception) {
			observer.onError(Status.FAILED_PRECONDITION.withDescription("Job log attribution or sequence conflict").asRuntimeException());
		} catch (RuntimeException exception) {
			observer.onError(Status.INTERNAL.withDescription("Job log storage failed").withCause(exception).asRuntimeException());
		}
	}

	private static JobLogRecord toRecord(AppendJobLogRequest request) {
		if (!RunnerProtocolVersion.CURRENT.equals(request.getProtocolVersion())) {
			throw new IllegalArgumentException("unsupported protocol version");
		}
		var timestamp = request.getObservedAt();
		if (!request.hasObservedAt() || timestamp.getSeconds() < -62135596800L
				|| timestamp.getSeconds() > 253402300799L || timestamp.getNanos() < 0 || timestamp.getNanos() > 999999999) {
			throw new IllegalArgumentException("observed_at must be a valid protobuf timestamp");
		}
		if (request.getContent().size() > JobLogRecord.MAX_CHUNK_BYTES) {
			throw new IllegalArgumentException("chunk is too large");
		}
		var stream = switch (request.getStream()) {
			case JOB_LOG_STREAM_UNSPECIFIED -> JobLogRecord.Stream.NONE;
			case JOB_LOG_STREAM_STDOUT -> JobLogRecord.Stream.STDOUT;
			case JOB_LOG_STREAM_STDERR -> JobLogRecord.Stream.STDERR;
			case UNRECOGNIZED -> throw new IllegalArgumentException("unknown output stream");
		};
		return new JobLogRecord(new JobId(UUID.fromString(request.getJobId())),
				new RunnerAssignment(request.getRunnerId(), request.getInstanceId()), request.getSequence(),
				Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()), stream,
				request.getContent().toByteArray(), request.getEndOfOutput(), request.getTruncated(), request.getIncomplete());
	}
}

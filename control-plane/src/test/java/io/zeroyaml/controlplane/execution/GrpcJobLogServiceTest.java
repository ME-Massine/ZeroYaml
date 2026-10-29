package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ServerInterceptors;
import io.grpc.ServerInterceptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.zeroyaml.contracts.runner.v1.AppendJobLogRequest;
import io.zeroyaml.contracts.runner.v1.JobLogServiceGrpc;
import io.zeroyaml.contracts.runner.v1.JobLogStream;
import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Exercises validation, attribution, bounded retention, and byte fidelity over gRPC. */
class GrpcJobLogServiceTest {
	private static final Instant CREATED = Instant.parse("2026-01-01T00:00:00Z");
	private static final RunnerAssignment RUNNER = new RunnerAssignment("runner-1", "instance-1");
	private final JobStore jobs = spy(new InMemoryJobStore());
	private final JobLogStore logs = spy(new InMemoryJobLogStore());
	private ManagedChannel channel;
	private Server server;
	private JobLogServiceGrpc.JobLogServiceBlockingStub client;
	private Job job;

	@BeforeEach
	void setUp() throws IOException {
		job = runningJob();
		var name = InProcessServerBuilder.generateName();
		server = InProcessServerBuilder.forName(name).directExecutor()
				.addService(ServerInterceptors.intercept(new GrpcJobLogService(new JobLogRecorder(jobs, logs)), new ServerInterceptor() {
					@Override
					public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
						// Transport authentication is exercised separately over real mTLS.
						return Contexts.interceptCall(Context.current().withValue(JobLogAuthentication.RUNNER_ID, RUNNER.runnerId()), call, headers, next);
					}
				})).build().start();
		channel = InProcessChannelBuilder.forName(name).directExecutor().build();
		client = JobLogServiceGrpc.newBlockingStub(channel);
	}

	@AfterEach
	void tearDown() {
		if (channel != null) { channel.shutdownNow(); }
		if (server != null) { server.shutdownNow(); }
	}

	@Test
	void refusesCallsWithoutAuthenticatedTransportContext() throws IOException {
		var name = InProcessServerBuilder.generateName();
		var unprotected = InProcessServerBuilder.forName(name).directExecutor()
				.addService(new GrpcJobLogService(new JobLogRecorder(jobs, logs))).build().start();
		var connection = InProcessChannelBuilder.forName(name).directExecutor().build();
		try {
			var error = assertThrows(StatusRuntimeException.class,
					() -> JobLogServiceGrpc.newBlockingStub(connection).appendJobLog(request(job, 1).build()));
			assertEquals(Status.Code.UNAUTHENTICATED, error.getStatus().getCode());
			assertTrue(logs.snapshot(job.id()).isEmpty());
		} finally {
			connection.shutdownNow();
			unprotected.shutdownNow();
		}
	}

	@Test
	void attributionUsesReadOnlyLookupWithoutUpdatingTheJob() {
		client.appendJobLog(request(job, 1).build());
		verify(jobs).find(job.id());
		verify(jobs, never()).update(any(), any());
	}

	@Test
	void retainedChunksAndFinalMarkerNeedOnlyOneJobLookup() {
		for (int sequence = 1; sequence <= JobLogRecord.MAX_CHUNKS; sequence++) {
			client.appendJobLog(request(job, sequence).build());
		}
		client.appendJobLog(end(job, JobLogRecord.MAX_CHUNKS + 1).build());
		verify(jobs).find(job.id());
		assertEquals(JobLogRecord.MAX_CHUNKS + 1, logs.snapshot(job.id()).size());
	}

	@Test
	void retainedStreamStillRejectsAnotherRunnerInstanceWithoutLookup() {
		client.appendJobLog(request(job, 1).build());
		assertStatus(Status.Code.FAILED_PRECONDITION,
				request(job, 2).setInstanceId("another-instance").build());
		verify(jobs).find(job.id());
		assertEquals(1, logs.snapshot(job.id()).size());
	}

	@Test
	void evictedStreamMustPassAuthoritativeAttributionAgain() {
		client.appendJobLog(end(job, 1).build());
		for (int index = 0; index < 32; index++) {
			client.appendJobLog(request(runningJob(), 1).build());
		}
		assertTrue(logs.snapshot(job.id()).isEmpty());
		assertStatus(Status.Code.FAILED_PRECONDITION,
				request(job, 2).setInstanceId("another-instance").build());
		verify(jobs, org.mockito.Mockito.times(2)).find(job.id());
		assertTrue(logs.snapshot(job.id()).isEmpty());
	}

	@Test
	void storageFailuresAreInternalErrorsWithoutStorageDetails() {
		for (var failure : List.of(new IllegalStateException("private storage detail"),
				new org.springframework.dao.DataAccessResourceFailureException("private database detail"))) {
			doThrow(failure).when(logs).append(any());
			var error = assertThrows(StatusRuntimeException.class, () -> client.appendJobLog(request(job, 1).build()));
			assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
			assertEquals("Job log storage failed", error.getStatus().getDescription());
		}
		assertTrue(logs.snapshot(job.id()).isEmpty());
	}

	@Test
	void jobLookupStateFailuresAreInternalErrors() {
		doThrow(new IllegalStateException("private lookup detail")).when(jobs).find(job.id());
		assertStatus(Status.Code.INTERNAL, request(job, 1).build());
		assertTrue(logs.snapshot(job.id()).isEmpty());
	}

	@Test
	void recordsBothStreamsAndRawBytesWithoutChangingJobState() {
		byte[] raw = { 'a', (byte) 0xff, '\n' };
		client.appendJobLog(request(job, 1).setContent(ByteString.copyFrom(raw)).build());
		client.appendJobLog(request(job, 2).setStream(JobLogStream.JOB_LOG_STREAM_STDERR).build());
		client.appendJobLog(end(job, 3).build());
		var records = logs.snapshot(job.id());
		assertEquals(3, records.size());
		assertArrayEquals(raw, records.getFirst().content());
		assertEquals(JobLogRecord.Stream.STDOUT, records.getFirst().stream());
		assertEquals(JobLogRecord.Stream.STDERR, records.get(1).stream());
		assertEquals(CREATED.plusSeconds(2), records.getFirst().observedAt());
		assertEquals(RUNNER, records.getFirst().reportedBy());
		assertTrue(records.getLast().endOfOutput());
		assertFalse(records.getLast().incomplete());
		assertEquals(JobStatus.RUNNING, job.status());
	}

	@Test
	void permitsLogsForFailedJobsAndExplicitlyIncompleteFinalGaps() {
		job.fail(new io.zeroyaml.controlplane.domain.job.JobFailure("NON_ZERO_EXIT", "command failed"), CREATED.plusSeconds(3));
		client.appendJobLog(request(job, 1).setStream(JobLogStream.JOB_LOG_STREAM_STDERR).build());
		client.appendJobLog(end(job, 3).setIncomplete(true).setTruncated(true).build());
		assertEquals(JobStatus.FAILED, job.status());
		assertTrue(logs.snapshot(job.id()).getLast().incomplete());
		assertTrue(logs.snapshot(job.id()).getLast().truncated());
	}

	@Test
	void refusesUnknownUnassignedAndWrongRunnerLogs() {
		assertStatus(Status.Code.NOT_FOUND, request(job, 1).setJobId(JobId.newId().value().toString()).build());
		assertStatus(Status.Code.PERMISSION_DENIED, request(job, 1).setRunnerId("other").build());
		assertStatus(Status.Code.FAILED_PRECONDITION, request(job, 1).setInstanceId("other").build());
		var queued = newJob();
		assertStatus(Status.Code.FAILED_PRECONDITION, request(queued, 1).build());
		assertTrue(logs.snapshot(job.id()).isEmpty());
	}

	@Test
	void refusesOutOfOrderDuplicateAndPostFinalChunks() {
		client.appendJobLog(request(job, 1).build());
		assertStatus(Status.Code.FAILED_PRECONDITION, request(job, 1).build());
		client.appendJobLog(end(job, 2).build());
		assertStatus(Status.Code.FAILED_PRECONDITION, request(job, 3).build());
		assertEquals(2, logs.snapshot(job.id()).size());
	}

	@Test
	void finalMarkerWithMissingRetainedChunksIsAcknowledgedAndNormalized() {
		client.appendJobLog(request(job, 2).build());
		client.appendJobLog(end(job, 3).build());
		var marker = logs.snapshot(job.id()).getLast();
		assertTrue(marker.endOfOutput());
		assertTrue(marker.incomplete());
		assertFalse(marker.truncated());
		assertEquals(3, marker.sequence());
		assertStatus(Status.Code.FAILED_PRECONDITION, end(job, 4).build());
	}

	@Test
	void validatesAllBoundaryFieldsWithoutRetainingPayloads() {
		var invalid = List.of(
				request(job, 1).setProtocolVersion("runner.v2").build(),
				request(job, 1).setJobId("bad-id").build(),
				request(job, 1).clearRunnerId().build(),
				request(job, 1).clearInstanceId().build(),
				request(job, 1).setRunnerId("r".repeat(257)).build(),
				request(job, 1).setInstanceId("i".repeat(257)).build(),
				request(job, 0).build(),
				request(job, -1).build(),
				request(job, 257).build(),
				end(job, 258).build(),
				request(job, 1).clearObservedAt().build(),
				request(job, 1).setObservedAt(Timestamp.newBuilder().setSeconds(253402300800L)).build(),
				request(job, 1).setObservedAt(Timestamp.newBuilder().setNanos(-1)).build(),
				request(job, 1).setObservedAt(Timestamp.newBuilder().setNanos(1000000000)).build(),
				request(job, 1).clearStream().build(),
				request(job, 1).setStreamValue(99).build(),
				request(job, 1).clearContent().build(),
				request(job, 1).setContent(ByteString.copyFrom(new byte[4097])).build(),
				request(job, 1).setTruncated(true).build(),
				request(job, 1).setIncomplete(true).build(),
				request(job, 1).setEndOfOutput(true).build(),
				end(job, 1).setStream(JobLogStream.JOB_LOG_STREAM_STDERR).build()
		);
		for (var value : invalid) { assertStatus(Status.Code.INVALID_ARGUMENT, value); }
		assertTrue(logs.snapshot(job.id()).isEmpty());
	}

	@Test
	void retainsAtMostOneMiBAnd256DataChunksWithAFinalMarker() {
		var chunk = ByteString.copyFrom(new byte[4096]);
		for (int sequence = 1; sequence <= 256; sequence++) {
			client.appendJobLog(request(job, sequence).setContent(chunk).build());
		}
		assertStatus(Status.Code.INVALID_ARGUMENT, request(job, 257).build());
		client.appendJobLog(end(job, 257).setTruncated(true).build());
		var records = logs.snapshot(job.id());
		assertEquals(257, records.size());
		assertEquals(1 << 20, records.stream().mapToInt(JobLogRecord::contentSize).sum());
	}

	@Test
	void preservesActiveJobsAndRejectsNewLogsAtCapacity() {
		client.appendJobLog(request(job, 1).build());
		for (int count = 1; count < 32; count++) {
			client.appendJobLog(request(runningJob(), 1).build());
		}
		var overflow = runningJob();
		assertStatus(Status.Code.RESOURCE_EXHAUSTED, request(overflow, 1).build());
		assertStatus(Status.Code.RESOURCE_EXHAUSTED, end(overflow, 2).setIncomplete(true).build());
		assertEquals(1, logs.snapshot(job.id()).size());
		client.appendJobLog(request(job, 2).build());
		client.appendJobLog(end(job, 3).build());
		client.appendJobLog(request(overflow, 2).build());
		assertTrue(logs.snapshot(job.id()).isEmpty());
		assertEquals(2, logs.snapshot(overflow.id()).getFirst().sequence());
	}

	@Test
	void evictsOldestFinishedOutputWhilePreservingOlderActiveOutput() {
		client.appendJobLog(request(job, 1).build());
		var completed = runningJob();
		client.appendJobLog(end(completed, 1).build());
		for (int count = 2; count < 33; count++) {
			client.appendJobLog(request(runningJob(), 1).build());
		}
		assertFalse(logs.snapshot(job.id()).isEmpty());
		assertTrue(logs.snapshot(completed.id()).isEmpty());
		client.appendJobLog(end(job, 2).build());
	}

	@Test
	void acceptsClockSkewAndClockChangesWithoutLosingSequenceOrder() {
		client.appendJobLog(request(job, 1).setObservedAt(Timestamp.newBuilder().setSeconds(CREATED.minusSeconds(3600).getEpochSecond())).build());
		client.appendJobLog(request(job, 2).setObservedAt(Timestamp.newBuilder().setSeconds(CREATED.minusSeconds(7200).getEpochSecond())).build());
		client.appendJobLog(end(job, 3).setObservedAt(Timestamp.newBuilder().setSeconds(CREATED.minusSeconds(7200).getEpochSecond())).build());
		assertEquals(List.of(1L, 2L, 3L), logs.snapshot(job.id()).stream().map(JobLogRecord::sequence).toList());
	}

	@Test
	void acceptsLaterChunksAfterDeliveryLossAndPreservesExplicitFinalFlags() {
		client.appendJobLog(request(job, 2).build());
		client.appendJobLog(request(job, 4).build());
		assertStatus(Status.Code.FAILED_PRECONDITION, request(job, 3).build());
		client.appendJobLog(end(job, 5).setIncomplete(true).setTruncated(true).build());
		assertEquals(List.of(2L, 4L, 5L), logs.snapshot(job.id()).stream().map(JobLogRecord::sequence).toList());
		assertTrue(logs.snapshot(job.id()).getLast().incomplete());
		assertTrue(logs.snapshot(job.id()).getLast().truncated());
	}

	@Test
	void snapshotsAndRecordsDoNotExposeMutableStorageOrPayloadInStringForm() {
		byte[] bytes = "sensitive-output".getBytes(java.nio.charset.StandardCharsets.UTF_8);
		var record = new JobLogRecord(job.id(), RUNNER, 1, CREATED.plusSeconds(2), JobLogRecord.Stream.STDOUT, bytes, false, false, false);
		logs.append(record);
		bytes[0] = 'X';
		var snapshot = logs.snapshot(job.id());
		var copy = snapshot.getFirst().content();
		copy[0] = 'Y';
		assertEquals('s', logs.snapshot(job.id()).getFirst().content()[0]);
		assertThrows(UnsupportedOperationException.class, () -> snapshot.clear());
		assertFalse(record.toString().contains("sensitive-output"));
	}

	@Test
	void unexpectedLookupFailureIsLoggedWithJobIdentityAndWithoutChunkContent() {
		var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(GrpcJobLogService.class);
		var appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
		appender.start();
		logger.addAppender(appender);
		try {
			doThrow(new IllegalStateException("database unavailable")).when(jobs).find(job.id());
			assertStatus(Status.Code.INTERNAL, request(job, 1).setContent(ByteString.copyFromUtf8("sensitive-chunk")).build());
			assertEquals(1, appender.list.size());
			var event = appender.list.getFirst();
			assertEquals(ch.qos.logback.classic.Level.ERROR, event.getLevel());
			assertTrue(event.getFormattedMessage().contains(job.id().value().toString()));
			assertFalse(event.getFormattedMessage().contains("sensitive-chunk"));
			assertEquals(IllegalStateException.class.getName(), event.getThrowableProxy().getClassName());
		} finally {
			logger.detachAppender(appender);
			appender.stop();
		}
	}

	private void assertStatus(Status.Code code, AppendJobLogRequest request) {
		var error = assertThrows(StatusRuntimeException.class, () -> client.appendJobLog(request));
		assertEquals(code, error.getStatus().getCode());
	}

	private Job newJob() {
		var created = Job.create(new RepositoryReference(URI.create("https://example.com/repo.git"), "abc123"),
				new ExecutionDefinition(List.of("test"), "."), CREATED);
		created.queue(CREATED.plusSeconds(1));
		jobs.add(created);
		return created;
	}

	private Job runningJob() {
		var running = newJob();
		running.start(RUNNER, CREATED.plusSeconds(2));
		return running;
	}

	private AppendJobLogRequest.Builder request(Job target, long sequence) {
		return AppendJobLogRequest.newBuilder().setProtocolVersion("runner.v1")
				.setJobId(target.id().value().toString()).setRunnerId(RUNNER.runnerId()).setInstanceId(RUNNER.instanceId())
				.setSequence(sequence).setObservedAt(Timestamp.newBuilder().setSeconds(CREATED.plusSeconds(2).getEpochSecond()))
				.setStream(JobLogStream.JOB_LOG_STREAM_STDOUT).setContent(ByteString.copyFromUtf8("output"));
	}

	private AppendJobLogRequest.Builder end(Job target, long sequence) {
		return request(target, sequence).clearContent().clearStream().setEndOfOutput(true);
	}
}

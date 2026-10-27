package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import io.zeroyaml.controlplane.PostgresTestContainer;
import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.InvalidJobTransitionException;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.domain.pipeline.Pipeline;
import io.zeroyaml.controlplane.domain.pipeline.PipelineId;
import io.zeroyaml.controlplane.domain.pipeline.PipelineStep;
import io.zeroyaml.controlplane.events.JobLifecycleEventPublisher;
import tools.jackson.databind.json.JsonMapper;

/** Exercises committed records and row locking against the real migration. */
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportTestcontainers(PostgresTestContainer.class)
@Import({JdbcJobStore.class, JdbcJobStoreTest.JsonConfiguration.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class JdbcJobStoreTest {

	private static final Instant NOW = Instant.parse("2026-10-26T10:00:00.123456Z");
	private static final RepositoryReference SOURCE = new RepositoryReference(
			URI.create("https://example.test/acme/project.git"), "abc123");
	private static final PipelineId PIPELINE_ID = new PipelineId(UUID.fromString("d40205cb-fef4-4e35-b012-a785d0f3536f"));
	private static final RunnerAssignment RUNNER = new RunnerAssignment("runner-1", "instance-1");

	@Autowired private JdbcJobStore store;
	@Autowired private JdbcClient jdbc;
	@Autowired private TransactionTemplate transactions;
	@Autowired private JsonMapper json;

	@BeforeEach
	void clearRecords() {
		jdbc.sql("DELETE FROM job").update();
		jdbc.sql("DELETE FROM pipeline_run").update();
	}

	@Test
	void recordsPipelineJobsAndFindsThemByPipelineRepositoryRevisionAndIdentity() {
		var pipeline = pipeline(SOURCE);
		var creation = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW, ZoneOffset.UTC)).createJobs(pipeline);
		var build = creation.jobs().get(0);
		var test = creation.jobs().get(1);

		assertEquals(2, creation.createdCount());
		assertEquals(List.of(build, test), store.findByPipeline(PIPELINE_ID, SOURCE));
		assertEquals(List.of(build, test), store.findByRepository(SOURCE.location()));
		assertEquals(List.of(build, test), store.findBySource(SOURCE));
		assertEquals(SOURCE, store.find(build.jobId()).orElseThrow().repository());
		assertEquals(List.of("mvn", "package"), store.find(build.jobId()).orElseThrow().execution().command());
		assertEquals(1, jdbc.sql("SELECT count(*) FROM pipeline_run").query(Integer.class).single());

		var retried = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC))
				.createJobs(pipeline);
		assertEquals(0, retried.createdCount());
		assertEquals(2, jdbc.sql("SELECT count(*) FROM job").query(Integer.class).single());
		assertEquals(NOW, store.find(test.jobId()).orElseThrow().createdAt());
	}

	@Test
	void runningSuccessAndFailureSurviveANewStoreInstance() {
		var creation = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW, ZoneOffset.UTC))
				.createJobs(pipeline(SOURCE));
		var success = creation.jobs().get(0).jobId();
		var failure = creation.jobs().get(1).jobId();
		for (var id : List.of(success, failure)) {
			store.update(id, job -> { job.queue(NOW.plusSeconds(1)); return job.status(); });
		}
		var recorder = new JobExecutionStatusRecorder(store, event -> { });
		for (var id : List.of(success, failure)) {
			assertEquals(JobStatusReportDecision.APPLIED,
					recorder.record(JobStatusReport.running(id, RUNNER, NOW.plusSeconds(2))).decision());
		}
		assertEquals(JobStatus.RUNNING, freshStore().find(success).orElseThrow().status());

		assertEquals(JobStatusReportDecision.APPLIED, recorder.record(JobStatusReport.succeeded(
				success, RUNNER, NOW.plusSeconds(2), NOW.plusSeconds(3), 0)).decision());
		assertEquals(JobStatusReportDecision.APPLIED, recorder.record(JobStatusReport.failed(
				failure, RUNNER, NOW.plusSeconds(2), NOW.plusSeconds(4), 7,
				JobExecutionFailureReason.NON_ZERO_EXIT, "Command exited with status 7")).decision());
		var restarted = freshStore();
		var succeeded = restarted.find(success).orElseThrow();
		var failed = restarted.find(failure).orElseThrow();
		assertEquals(JobStatus.SUCCEEDED, succeeded.status());
		assertEquals(0, succeeded.exit().orElseThrow());
		assertEquals(JobStatus.FAILED, failed.status());
		assertEquals(NOW.plusSeconds(1), failed.queuedAt().orElseThrow());
		assertEquals(NOW.plusSeconds(2), failed.startedAt().orElseThrow());
		assertEquals(NOW.plusSeconds(4), failed.completedAt().orElseThrow());
		assertEquals(RUNNER, failed.runnerAssignment().orElseThrow());
		assertEquals("NON_ZERO_EXIT", failed.failure().orElseThrow().code());
		assertEquals("Command exited with status 7", failed.failure().orElseThrow().message());
		assertEquals(7, failed.failure().orElseThrow().exitCode());
	}

	@Test
	void rejectsInvalidTransitionWithoutChangingCommittedState() {
		var id = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW, ZoneOffset.UTC))
				.createJobs(pipeline(SOURCE)).jobs().get(0).jobId();
		assertThrows(InvalidJobTransitionException.class,
				() -> store.update(id, job -> { job.succeed(NOW.plusSeconds(1)); return job; }));
		assertEquals(JobStatus.CREATED, freshStore().find(id).orElseThrow().status());
		assertThrows(IllegalStateException.class, () -> store.update(id, job -> {
			job.queue(NOW.plusSeconds(1));
			throw new IllegalStateException("change failed after mutation");
		}));
		assertEquals(JobStatus.CREATED, freshStore().find(id).orElseThrow().status());

		store.update(id, job -> { job.queue(NOW.plusSeconds(1)); return job.status(); });
		assertThrows(IllegalArgumentException.class, () -> store.update(id, job -> {
			job.start(RUNNER, NOW.minusSeconds(1));
			return job;
		}));
		assertEquals(JobStatus.QUEUED, freshStore().find(id).orElseThrow().status());
	}

	@Test
	void unknownJobAndUnsafeRepositoryLocationAreRefused() {
		assertTrue(store.find(JobId.newId()).isEmpty());
		assertTrue(store.update(JobId.newId(), job -> { throw new AssertionError("callback ran"); }).isEmpty());
		var unsafe = new RepositoryReference(URI.create("https://token@example.test/project.git"), "abc123");
		assertThrows(IllegalArgumentException.class, () -> store.recordIfAbsent(Job.create(
				unsafe, new ExecutionDefinition(List.of("mvn", "test"), "."), NOW)));
		assertEquals(0, jdbc.sql("SELECT count(*) FROM job").query(Integer.class).single());
	}

	@Test
	void directJobsAlsoPersistAndCanBeLinkedAfterPartialCreation() {
		var pipeline = pipeline(SOURCE);
		var translated = new io.zeroyaml.controlplane.domain.pipeline.PipelineJobTranslator().translate(pipeline, NOW);
		var direct = translated.get(0).job();
		assertSame(direct, store.recordIfAbsent(direct));
		var creation = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW, ZoneOffset.UTC)).createJobs(pipeline);
		assertEquals(1, creation.createdCount());
		assertEquals(creation.jobs(), freshStore().findByPipeline(PIPELINE_ID, SOURCE));
	}

	@Test
	void reorderedPlanKeepsJobsAndUpdatesTheirStepOrder() {
		var pipeline = pipeline(SOURCE);
		var service = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW, ZoneOffset.UTC));
		var first = service.createJobs(pipeline);
		var buildId = first.jobs().get(0).jobId();
		store.update(buildId, job -> { job.queue(NOW.plusSeconds(1)); return job.status(); });

		var reordered = new Pipeline(PIPELINE_ID, SOURCE, List.of(
				pipeline.steps().get(1), pipeline.steps().get(0)));
		var second = service.createJobs(reordered);

		assertEquals(0, second.createdCount());
		assertEquals(List.of("test", "build"), second.jobs().stream().map(PipelineJobReference::stepName).toList());
		assertEquals(second.jobs(), freshStore().findByPipeline(PIPELINE_ID, SOURCE));
		assertEquals(JobStatus.QUEUED, freshStore().find(buildId).orElseThrow().status());
		assertEquals(2, jdbc.sql("SELECT count(*) FROM job").query(Integer.class).single());
	}

	@Test
	void pipelineLookupSeparatesSourcesSharingAnIdentity() {
		var service = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW, ZoneOffset.UTC));
		var original = service.createJobs(pipeline(SOURCE));
		var otherRepository = new RepositoryReference(URI.create("https://example.test/acme/other.git"), "abc123");
		var otherRevision = new RepositoryReference(SOURCE.location(), "def456");
		var second = service.createJobs(pipeline(otherRepository));
		var third = service.createJobs(pipeline(otherRevision));

		assertEquals(original.jobs(), store.findByPipeline(PIPELINE_ID, SOURCE));
		assertEquals(second.jobs(), store.findByPipeline(PIPELINE_ID, otherRepository));
		assertEquals(third.jobs(), store.findByPipeline(PIPELINE_ID, otherRevision));
		assertEquals(3, jdbc.sql("SELECT count(*) FROM pipeline_run").query(Integer.class).single());
	}

	@Test
	void concurrentTerminalReportsCommitOneOutcome() throws Exception {
		var id = new PipelineJobCreationService(store, event -> { }, Clock.fixed(NOW, ZoneOffset.UTC))
				.createJobs(pipeline(SOURCE)).jobs().get(0).jobId();
		store.update(id, job -> { job.queue(NOW.plusSeconds(1)); return job.status(); });
		var report = JobStatusReport.succeeded(id, RUNNER, NOW.plusSeconds(2), NOW.plusSeconds(3), 0);
		var start = new CyclicBarrier(2);
		var executor = Executors.newFixedThreadPool(2);
		try {
			var first = executor.submit(() -> {
				start.await(10, TimeUnit.SECONDS);
				return new JobExecutionStatusRecorder(store, event -> { }).record(report).decision();
			});
			var second = executor.submit(() -> {
				start.await(10, TimeUnit.SECONDS);
				return new JobExecutionStatusRecorder(freshStore(), event -> { }).record(report).decision();
			});
			assertEquals(List.of(JobStatusReportDecision.APPLIED, JobStatusReportDecision.DUPLICATE),
					List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS)).stream().sorted().toList());
			assertEquals(JobStatus.SUCCEEDED, freshStore().find(id).orElseThrow().status());
		} finally {
			executor.shutdownNow();
		}
	}

	@Test
	void lifecycleEventsAreHandedOverOnlyAfterTheirTransitionIsCommitted() {
		// Each event reads its Job on another thread, so through another connection
		// that sees committed rows only. An event handed over inside the transaction
		// would find no Job, or the state before the transition.
		var observed = new ArrayList<String>();
		JobLifecycleEventPublisher events = event -> observed.add(event.type() + ":" + CompletableFuture
				.supplyAsync(() -> freshStore().find(event.jobId()).map(job -> job.status().name()).orElse("MISSING"))
				.join());
		var id = new PipelineJobCreationService(store, events, Clock.fixed(NOW, ZoneOffset.UTC))
				.createJobs(pipeline(SOURCE)).jobs().get(0).jobId();
		store.update(id, job -> { job.queue(NOW.plusSeconds(1)); return job.status(); });

		var recorder = new JobExecutionStatusRecorder(store, events);
		recorder.record(JobStatusReport.running(id, RUNNER, NOW.plusSeconds(2)));
		recorder.record(JobStatusReport.succeeded(id, RUNNER, NOW.plusSeconds(2), NOW.plusSeconds(3), 0));

		assertEquals(List.of("ACCEPTED:CREATED", "ACCEPTED:CREATED", "STARTED:RUNNING", "SUCCEEDED:SUCCEEDED"), observed);
	}

	private JdbcJobStore freshStore() {
		return new JdbcJobStore(jdbc, transactions, json);
	}

	private static Pipeline pipeline(RepositoryReference source) {
		return new Pipeline(PIPELINE_ID, source, List.of(
				new PipelineStep("build", new ExecutionDefinition(List.of("mvn", "package"), ".")),
				new PipelineStep("test", new ExecutionDefinition(List.of("mvn", "test"), "."))));
	}

	@TestConfiguration
	static class JsonConfiguration {
		@Bean JsonMapper jsonMapper() { return JsonMapper.builder().build(); }
	}
}

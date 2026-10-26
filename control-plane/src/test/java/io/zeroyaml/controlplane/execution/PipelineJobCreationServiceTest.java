package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.pipeline.Pipeline;
import io.zeroyaml.controlplane.domain.pipeline.PipelineId;
import io.zeroyaml.controlplane.domain.pipeline.PipelineJobTranslator;
import io.zeroyaml.controlplane.domain.pipeline.PipelineStep;

/**
 * Covers recording the Jobs of a pipeline, including repeated input that must
 * not create duplicate Jobs and changed plans that must not reuse recorded Jobs.
 */
class PipelineJobCreationServiceTest {

	private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");
	private static final RepositoryReference SOURCE =
			new RepositoryReference(URI.create("https://example.test/acme/service.git"), "0123456789abcdef");
	private static final PipelineId PIPELINE_ID = new PipelineId(UUID.fromString("7d3c5a1e-0000-4000-8000-000000000001"));

	private final JobStore jobs = new InMemoryJobStore();
	private final PipelineJobCreationService service =
			new PipelineJobCreationService(jobs, Clock.fixed(NOW, ZoneOffset.UTC));

	@Test
	void recordsOneJobPerStepOfAMultiStepPipeline() {
		var pipeline = pipeline(step("build", "./mvnw", "package"), step("test", "./mvnw", "verify"));

		var creation = service.createJobs(pipeline);

		assertEquals(PIPELINE_ID, creation.pipelineId());
		assertEquals(2, creation.createdCount());
		assertFalse(creation.alreadyRecorded());
		assertEquals(List.of("build", "test"), creation.jobs().stream().map(PipelineJobReference::stepName).toList());
		assertEquals(List.of(1, 2), creation.jobs().stream().map(PipelineJobReference::position).toList());
		for (var reference : creation.jobs()) {
			assertEquals(PIPELINE_ID, reference.pipelineId());
			assertEquals(jobIdOf(pipeline, reference.stepName()), reference.jobId());
			var recorded = jobs.find(reference.jobId()).orElseThrow();
			assertEquals(SOURCE, recorded.repository());
			assertEquals(JobStatus.CREATED, recorded.status());
			assertEquals(NOW, recorded.createdAt());
		}
	}

	@Test
	void repeatedDeliveryReturnsRecordedJobsWithoutDuplicates() {
		var pipeline = pipeline(step("build", "make"), step("test", "make", "test"));
		var first = service.createJobs(pipeline);
		var buildId = first.jobs().get(0).jobId();
		var buildJob = jobs.find(buildId).orElseThrow();
		jobs.update(buildId, job -> {
			job.queue(NOW.plusSeconds(1));
			return job;
		});

		var repeated = service.createJobs(pipeline);

		assertTrue(repeated.alreadyRecorded());
		assertEquals(0, repeated.createdCount());
		assertEquals(first.jobs(), repeated.jobs());
		// The repeat neither resets progress nor replaces the recorded Job.
		assertSame(buildJob, jobs.find(buildId).orElseThrow());
		assertEquals(JobStatus.QUEUED, jobs.find(buildId).orElseThrow().status());
	}

	@Test
	void equalPipelineRebuiltForTheSameRevisionMapsToTheSameJobs() {
		var first = service.createJobs(pipeline(step("build", "make")));

		var retried = service.createJobs(pipeline(step("build", "make")));

		assertTrue(retried.alreadyRecorded());
		assertEquals(first.jobs(), retried.jobs());
	}

	@Test
	void recordsOnlyTheMissingJobsAfterAPartialEarlierAttempt() {
		var pipeline = pipeline(step("build", "make"), step("test", "make", "test"));
		// An earlier attempt recorded the first Job before failing.
		var earlierBuild = new PipelineJobTranslator().translate(pipeline, NOW).get(0).job();
		jobs.add(earlierBuild);

		var creation = service.createJobs(pipeline);

		assertEquals(1, creation.createdCount());
		assertEquals(earlierBuild.id(), creation.jobs().get(0).jobId());
		assertSame(earlierBuild, jobs.find(earlierBuild.id()).orElseThrow());
		assertEquals(jobIdOf(pipeline, "test"), creation.jobs().get(1).jobId());
		assertTrue(jobs.find(creation.jobs().get(1).jobId()).isPresent());
	}

	@Test
	void changedPlanRecordsItsOwnJobsAndLeavesTheEarlierOnesUntouched() {
		var original = service.createJobs(pipeline(step("build", "make"), step("test", "make", "test")));
		var originalBuildId = original.jobs().get(0).jobId();
		var originalTestId = original.jobs().get(1).jobId();
		var changed = pipeline(step("build", "make", "all"));

		var creation = service.createJobs(changed);

		assertEquals(1, creation.createdCount());
		var changedBuildId = creation.jobs().get(0).jobId();
		assertNotEquals(originalBuildId, changedBuildId);
		assertEquals(List.of("make", "all"), jobs.find(changedBuildId).orElseThrow().execution().command());
		// The earlier plan's Jobs, including the dropped step, keep their definition and state.
		assertEquals(List.of("make"), jobs.find(originalBuildId).orElseThrow().execution().command());
		assertEquals(List.of("make", "test"), jobs.find(originalTestId).orElseThrow().execution().command());
	}

	@Test
	void samePipelineIdentityForADifferentRepositoryRecordsSeparateJobs() {
		var recorded = service.createJobs(pipeline(step("build", "make")));
		var otherSource = new RepositoryReference(URI.create("https://example.test/acme/other.git"), "0123456789abcdef");

		var other = service.createJobs(new Pipeline(PIPELINE_ID, otherSource, List.of(step("build", "make"))));

		assertEquals(1, other.createdCount());
		assertNotEquals(recorded.jobs().get(0).jobId(), other.jobs().get(0).jobId());
		assertEquals(SOURCE, jobs.find(recorded.jobs().get(0).jobId()).orElseThrow().repository());
		assertEquals(otherSource, jobs.find(other.jobs().get(0).jobId()).orElseThrow().repository());
	}

	@Test
	void concurrentDeliveriesRecordEachJobExactlyOnce() throws Exception {
		var pipeline = pipeline(step("build", "make"), step("test", "make", "test"));
		var callers = 8;
		var start = new CountDownLatch(1);
		var executor = Executors.newFixedThreadPool(callers);
		try {
			List<Future<PipelineJobCreation>> futures = IntStream.range(0, callers)
					.mapToObj(i -> executor.submit((Callable<PipelineJobCreation>) () -> {
						start.await();
						return service.createJobs(pipeline);
					}))
					.toList();
			start.countDown();
			executor.shutdown();
			assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

			var totalCreated = 0;
			for (var future : futures) {
				totalCreated += future.get(10, TimeUnit.SECONDS).createdCount();
			}
			assertEquals(2, totalCreated);
		} finally {
			executor.shutdownNow();
		}
	}

	@Test
	void rejectsMissingPipeline() {
		assertThrows(NullPointerException.class, () -> service.createJobs(null));
	}

	private static JobId jobIdOf(Pipeline pipeline, String stepName) {
		return new PipelineJobTranslator().translate(pipeline, NOW).stream()
				.filter(pipelineJob -> pipelineJob.stepName().equals(stepName))
				.findFirst().orElseThrow().job().id();
	}

	private static Pipeline pipeline(PipelineStep... steps) {
		return new Pipeline(PIPELINE_ID, SOURCE, List.of(steps));
	}

	private static PipelineStep step(String name, String... command) {
		return new PipelineStep(name, new ExecutionDefinition(List.of(command), "."));
	}
}

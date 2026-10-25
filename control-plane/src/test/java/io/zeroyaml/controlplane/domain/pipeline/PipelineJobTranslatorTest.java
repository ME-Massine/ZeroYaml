package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.pipeline.inference.PipelineInference.Inferred;
import io.zeroyaml.controlplane.domain.pipeline.inference.PipelineInferrer;
import io.zeroyaml.controlplane.domain.pipeline.inference.RepositorySnapshot;

class PipelineJobTranslatorTest {

	private static final RepositoryReference SOURCE =
			new RepositoryReference(URI.create("https://example.test/acme/service.git"), "0123456789abcdef");
	private static final Instant CREATED_AT = Instant.parse("2026-01-01T10:00:00Z");
	private static final PipelineId PIPELINE_ID = new PipelineId(UUID.fromString("7d3c5a1e-0000-4000-8000-000000000001"));

	private final PipelineJobTranslator translator = new PipelineJobTranslator();

	@Test
	void createsOneCreatedJobPerStepInPipelineOrder() {
		var lint = step("lint", "./mvnw", "checkstyle:check");
		var build = step("build", "./mvnw", "package");
		var test = step("test", "./mvnw", "verify");
		var pipeline = new Pipeline(PIPELINE_ID, SOURCE, List.of(lint, build, test));

		var jobs = translator.translate(pipeline, CREATED_AT);

		assertEquals(List.of("lint", "build", "test"), jobs.stream().map(PipelineJob::stepName).toList());
		assertEquals(List.of(1, 2, 3), jobs.stream().map(PipelineJob::position).toList());
		for (var i = 0; i < jobs.size(); i++) {
			var pipelineJob = jobs.get(i);
			assertEquals(PIPELINE_ID, pipelineJob.pipelineId());
			assertEquals(pipeline.steps().get(i).execution(), pipelineJob.job().execution());
			assertEquals(JobStatus.CREATED, pipelineJob.job().status());
			assertEquals(CREATED_AT, pipelineJob.job().createdAt());
		}
	}

	@Test
	void preservesRepositoryAndCommitContextOnEveryJob() {
		var pipeline = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make"), step("test", "make", "test")));

		var jobs = translator.translate(pipeline, CREATED_AT);

		for (var pipelineJob : jobs) {
			assertEquals(SOURCE, pipelineJob.job().repository());
			assertEquals("0123456789abcdef", pipelineJob.job().repository().revision());
		}
	}

	@Test
	void assignsDistinctIdentitiesWithinAPipeline() {
		var pipeline = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make"), step("test", "make", "test")));

		var ids = translator.translate(pipeline, CREATED_AT).stream().map(pipelineJob -> pipelineJob.job().id()).toList();

		assertEquals(2, Set.copyOf(ids).size());
	}

	@Test
	void sameInputYieldsSameJobIdentitiesAndOrder() {
		var pipeline = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make"), step("test", "make", "test")));

		var first = translator.translate(pipeline, CREATED_AT);
		var retried = translator.translate(pipeline, CREATED_AT.plusSeconds(60));

		assertEquals(jobIds(first), jobIds(retried));
		assertEquals(
				first.stream().map(PipelineJob::stepName).toList(),
				retried.stream().map(PipelineJob::stepName).toList());
	}

	@Test
	void reinferringTheSameRevisionYieldsSameJobIdentities() {
		var inferrer = new PipelineInferrer();
		var snapshot = new RepositorySnapshot(Set.of("pom.xml", "mvnw"));
		var first = (Inferred) inferrer.infer(SOURCE, snapshot);
		var retry = (Inferred) inferrer.infer(SOURCE, snapshot);

		assertEquals(
				jobIds(translator.translate(first.pipeline(), CREATED_AT)),
				jobIds(translator.translate(retry.pipeline(), CREATED_AT)));
	}

	@Test
	void sameStepNameInAnotherPipelineYieldsAnotherJobIdentity() {
		var steps = List.of(step("build", "make"));
		var other = new PipelineId(UUID.fromString("7d3c5a1e-0000-4000-8000-000000000002"));

		var first = translator.translate(new Pipeline(PIPELINE_ID, SOURCE, steps), CREATED_AT);
		var second = translator.translate(new Pipeline(other, SOURCE, steps), CREATED_AT);

		assertNotEquals(jobIds(first), jobIds(second));
	}

	@Test
	void changedExecutionOrSourceYieldsAnotherJobIdentity() {
		var original = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make")));
		var changedCommand = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make", "all")));
		var otherSource = new RepositoryReference(URI.create("https://example.test/acme/other.git"), SOURCE.revision());
		var changedSource = new Pipeline(PIPELINE_ID, otherSource, List.of(step("build", "make")));

		var originalIds = jobIds(translator.translate(original, CREATED_AT));
		assertNotEquals(originalIds, jobIds(translator.translate(changedCommand, CREATED_AT)));
		assertNotEquals(originalIds, jobIds(translator.translate(changedSource, CREATED_AT)));
	}

	@Test
	void argumentBoundariesDoNotShareAJobIdentity() {
		var joined = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make test")));
		var split = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make", "test")));

		assertNotEquals(jobIds(translator.translate(joined, CREATED_AT)), jobIds(translator.translate(split, CREATED_AT)));
	}

	@Test
	void rejectsMissingPipelineOrCreationTime() {
		var pipeline = new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make")));

		assertThrows(NullPointerException.class, () -> translator.translate(null, CREATED_AT));
		assertThrows(NullPointerException.class, () -> translator.translate(pipeline, null));
	}

	@ParameterizedTest
	@ValueSource(strings = {"", "Build", "-build", "build step"})
	void invalidStepNameNeverReachesTranslation(String name) {
		var execution = new ExecutionDefinition(List.of("make"), ".");

		assertThrows(IllegalArgumentException.class, () -> new PipelineStep(name, execution));
	}

	@Test
	void invalidStepCommandNeverReachesTranslation() {
		assertThrows(IllegalArgumentException.class, () -> new ExecutionDefinition(List.of(), "."));
		assertThrows(IllegalArgumentException.class, () -> new ExecutionDefinition(List.of(" "), "."));
	}

	@Test
	void pipelineJobRejectsPositionBelowOne() {
		var job = translator.translate(new Pipeline(PIPELINE_ID, SOURCE, List.of(step("build", "make"))), CREATED_AT)
				.get(0).job();

		assertThrows(IllegalArgumentException.class, () -> new PipelineJob(PIPELINE_ID, 0, "build", job));
	}

	private static List<JobId> jobIds(List<PipelineJob> jobs) {
		return jobs.stream().map(pipelineJob -> pipelineJob.job().id()).toList();
	}

	private static PipelineStep step(String name, String... command) {
		return new PipelineStep(name, new ExecutionDefinition(List.of(command), "."));
	}
}

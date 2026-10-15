package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;

class PipelineTest {

	private static final RepositoryReference SOURCE =
			new RepositoryReference(URI.create("https://example.test/acme/service.git"), "0123456789abcdef");

	@Test
	void representsValidBuildAndTestPipelineInDeclaredOrder() {
		var id = new PipelineId(UUID.randomUUID());
		var build = step("build", "./mvnw", "package");
		var test = step("test", "./mvnw", "verify");

		var pipeline = new Pipeline(id, SOURCE, List.of(build, test));

		assertEquals(id, pipeline.id());
		assertEquals(SOURCE, pipeline.source());
		assertEquals(List.of(build, test), pipeline.steps());
	}

	@Test
	void keepsStepOrderIndependentOfNames() {
		var steps = List.of(step("zeta", "echo", "1"), step("alpha", "echo", "2"), step("mid", "echo", "3"));

		var pipeline = new Pipeline(PipelineId.newId(), SOURCE, steps);

		assertEquals(List.of("zeta", "alpha", "mid"), pipeline.steps().stream().map(PipelineStep::name).toList());
	}

	@Test
	void rejectsEmptyStepList() {
		var failure = assertThrows(IllegalArgumentException.class,
				() -> new Pipeline(PipelineId.newId(), SOURCE, List.of()));

		assertEquals("steps must contain at least one step", failure.getMessage());
	}

	@Test
	void rejectsMissingIdentitySourceOrSteps() {
		var steps = List.of(step("build", "make"));

		assertThrows(NullPointerException.class, () -> new Pipeline(null, SOURCE, steps));
		assertThrows(NullPointerException.class, () -> new Pipeline(PipelineId.newId(), null, steps));
		assertThrows(NullPointerException.class, () -> new Pipeline(PipelineId.newId(), SOURCE, null));
	}

	@Test
	void rejectsNullStep() {
		var steps = Arrays.asList(step("build", "make"), null);

		assertThrows(IllegalArgumentException.class, () -> new Pipeline(PipelineId.newId(), SOURCE, steps));
	}

	@Test
	void rejectsDuplicateStepNamesAndNamesTheDuplicate() {
		var steps = List.of(step("build", "make"), step("test", "make", "test"), step("build", "make", "again"));

		var failure = assertThrows(IllegalArgumentException.class,
				() -> new Pipeline(PipelineId.newId(), SOURCE, steps));

		assertEquals("step names must be unique: build", failure.getMessage());
	}

	@Test
	void treatsDifferentSpellingsAsDifferentSteps() {
		var steps = List.of(step("unit-test", "make"), step("unit_test", "make"));

		assertEquals(2, new Pipeline(PipelineId.newId(), SOURCE, steps).steps().size());
	}

	@Test
	void acceptsMaximumStepCountAndRejectsOneMore() {
		var atLimit = steps(Pipeline.MAX_STEPS);

		assertEquals(Pipeline.MAX_STEPS, new Pipeline(PipelineId.newId(), SOURCE, atLimit).steps().size());
		assertThrows(IllegalArgumentException.class,
				() -> new Pipeline(PipelineId.newId(), SOURCE, steps(Pipeline.MAX_STEPS + 1)));
	}

	@Test
	void isUnaffectedByLaterChangesToTheSuppliedList() {
		var supplied = new ArrayList<>(List.of(step("build", "make")));
		var pipeline = new Pipeline(PipelineId.newId(), SOURCE, supplied);

		supplied.add(step("test", "make", "test"));

		assertEquals(1, pipeline.steps().size());
		assertThrows(UnsupportedOperationException.class, () -> pipeline.steps().add(step("extra", "make")));
	}

	@Test
	void mapsEachStepOntoAJobWithoutTranslation() {
		var pipeline = new Pipeline(PipelineId.newId(), SOURCE,
				List.of(step("build", "./mvnw", "package"), step("test", "./mvnw", "verify")));
		var createdAt = Instant.parse("2026-09-24T10:00:00Z");

		var jobs = pipeline.steps().stream()
				.map(step -> Job.create(pipeline.source(), step.execution(), createdAt))
				.toList();

		assertEquals(2, jobs.size());
		assertSame(pipeline.source(), jobs.get(0).repository());
		assertEquals(pipeline.steps().get(1).execution(), jobs.get(1).execution());
		assertTrue(jobs.stream().allMatch(job -> job.status() == JobStatus.CREATED));
	}

	private static List<PipelineStep> steps(int count) {
		return IntStream.range(0, count).mapToObj(index -> step("step-" + index, "make")).toList();
	}

	private static PipelineStep step(String name, String... command) {
		return new PipelineStep(name, new ExecutionDefinition(List.of(command), "."));
	}
}

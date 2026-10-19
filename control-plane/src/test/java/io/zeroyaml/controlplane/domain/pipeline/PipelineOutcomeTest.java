package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.JobFailure;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;

class PipelineOutcomeTest {

	private static final Pipeline PIPELINE = new Pipeline(PipelineId.newId(),
			new RepositoryReference(URI.create("https://example.test/acme/service.git"), "0123456789abcdef"),
			List.of(new PipelineStep("build", new ExecutionDefinition(List.of("make"), ".")),
					new PipelineStep("test", new ExecutionDefinition(List.of("make", "test"), "."))));

	private static final PipelineFailure TEST_FAILURE =
			new PipelineFailure("test", new JobFailure("STEP_FAILED", "The command exited with status 1", 1));

	@Test
	void retainsFailureOfAStepInThePipeline() {
		var outcome = new PipelineOutcome(PIPELINE, PipelineStatus.FAILED, TEST_FAILURE);

		assertEquals(TEST_FAILURE, outcome.failure());
	}

	@ParameterizedTest
	@EnumSource(value = PipelineStatus.class, names = "FAILED", mode = EnumSource.Mode.EXCLUDE)
	void acceptsEveryOtherStatusWithoutFailure(PipelineStatus status) {
		assertNull(new PipelineOutcome(PIPELINE, status, null).failure());
	}

	@ParameterizedTest
	@EnumSource(value = PipelineStatus.class, names = "FAILED", mode = EnumSource.Mode.EXCLUDE)
	void rejectsFailureForAnyOtherStatus(PipelineStatus status) {
		assertThrows(IllegalArgumentException.class, () -> new PipelineOutcome(PIPELINE, status, TEST_FAILURE));
	}

	@Test
	void rejectsFailedStatusWithoutFailure() {
		assertThrows(IllegalArgumentException.class, () -> new PipelineOutcome(PIPELINE, PipelineStatus.FAILED, null));
	}

	@Test
	void rejectsFailureOfAStepOutsideThePipeline() {
		var deploy = new PipelineFailure("deploy", new JobFailure("STEP_FAILED", "failed"));

		var failure = assertThrows(IllegalArgumentException.class,
				() -> new PipelineOutcome(PIPELINE, PipelineStatus.FAILED, deploy));

		assertEquals("failure names a step outside the pipeline: deploy", failure.getMessage());
	}

	@Test
	void rejectsMissingPipelineOrStatus() {
		assertThrows(NullPointerException.class, () -> new PipelineOutcome(null, PipelineStatus.PENDING, null));
		assertThrows(NullPointerException.class, () -> new PipelineOutcome(PIPELINE, null, null));
	}
}

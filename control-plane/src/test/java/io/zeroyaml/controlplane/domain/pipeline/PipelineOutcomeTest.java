package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Optional;

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

	@ParameterizedTest
	@EnumSource(value = PipelineStatus.class, names = { "PENDING", "RUNNING", "SUCCEEDED" })
	void recordsStatusesNoStepEnded(PipelineStatus status) {
		var outcome = PipelineOutcome.of(PIPELINE, status);

		assertEquals(PIPELINE.id(), outcome.pipelineId());
		assertEquals(status, outcome.status());
		assertTrue(outcome.failure().isEmpty());
		assertTrue(outcome.cancelledStep().isEmpty());
	}

	@ParameterizedTest
	@EnumSource(value = PipelineStatus.class, names = { "FAILED", "CANCELLED" })
	void rejectsStatusesThatNeedTheEndingStep(PipelineStatus status) {
		assertThrows(IllegalArgumentException.class, () -> PipelineOutcome.of(PIPELINE, status));
	}

	@Test
	void retainsFailureOfAStepInThePipeline() {
		var outcome = PipelineOutcome.failed(PIPELINE, TEST_FAILURE);

		assertEquals(PipelineStatus.FAILED, outcome.status());
		assertEquals(Optional.of(TEST_FAILURE), outcome.failure());
		assertTrue(outcome.cancelledStep().isEmpty());
	}

	@Test
	void retainsTheCancelledStep() {
		var outcome = PipelineOutcome.cancelled(PIPELINE, "test");

		assertEquals(PipelineStatus.CANCELLED, outcome.status());
		assertEquals(Optional.of("test"), outcome.cancelledStep());
		assertTrue(outcome.failure().isEmpty());
	}

	@Test
	void rejectsAStepOutsideThePipeline() {
		var deploy = new PipelineFailure("deploy", new JobFailure("STEP_FAILED", "failed"));

		var failure = assertThrows(IllegalArgumentException.class, () -> PipelineOutcome.failed(PIPELINE, deploy));

		assertEquals("step is not part of the pipeline: deploy", failure.getMessage());
		assertThrows(IllegalArgumentException.class, () -> PipelineOutcome.cancelled(PIPELINE, "deploy"));
		assertThrows(IllegalArgumentException.class, () -> PipelineOutcome.cancelled(PIPELINE, null));
	}

	@Test
	void rejectsMissingValues() {
		assertThrows(NullPointerException.class, () -> PipelineOutcome.of(null, PipelineStatus.PENDING));
		assertThrows(NullPointerException.class, () -> PipelineOutcome.of(PIPELINE, null));
		assertThrows(NullPointerException.class, () -> PipelineOutcome.failed(PIPELINE, null));
		assertThrows(NullPointerException.class, () -> PipelineOutcome.failed(null, TEST_FAILURE));
		assertThrows(NullPointerException.class, () -> PipelineOutcome.cancelled(null, "test"));
	}

	@Test
	void comparesByValue() {
		assertEquals(PipelineOutcome.failed(PIPELINE, TEST_FAILURE), PipelineOutcome.failed(PIPELINE, TEST_FAILURE));
		assertEquals(PipelineOutcome.cancelled(PIPELINE, "build").hashCode(),
				PipelineOutcome.cancelled(PIPELINE, "build").hashCode());
	}
}

package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PipelineFailureTest {

	@Test
	void retainsFailedStepCodeAndMessage() {
		var failure = new PipelineFailure("test", "STEP_FAILED", "The command exited with status 1");

		assertEquals("test", failure.stepName());
		assertEquals("STEP_FAILED", failure.code());
		assertEquals("The command exited with status 1", failure.message());
	}

	@Test
	void rejectsBlankFields() {
		assertThrows(IllegalArgumentException.class, () -> new PipelineFailure(" ", "STEP_FAILED", "failed"));
		assertThrows(IllegalArgumentException.class, () -> new PipelineFailure("test", "", "failed"));
		assertThrows(IllegalArgumentException.class, () -> new PipelineFailure("test", "STEP_FAILED", null));
	}
}

package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.zeroyaml.controlplane.domain.job.JobFailure;

class PipelineFailureTest {

	@Test
	void retainsFailedStepAndJobFailureIncludingExitCode() {
		var jobFailure = new JobFailure("STEP_FAILED", "The command exited with status 1", 1);
		var failure = new PipelineFailure("test", jobFailure);

		assertEquals("test", failure.stepName());
		assertSame(jobFailure, failure.failure());
		assertEquals("STEP_FAILED", failure.failure().code());
		assertEquals("The command exited with status 1", failure.failure().message());
		assertEquals(1, failure.failure().exit().orElseThrow());
	}

	@Test
	void retainsAbsenceOfExitCodeWhenACommandNeverStarted() {
		var failure = new PipelineFailure("build", new JobFailure("START_FAILED", "Command could not start"));

		assertTrue(failure.failure().exit().isEmpty());
	}

	@Test
	void rejectsMissingStepName() {
		assertThrows(NullPointerException.class,
				() -> new PipelineFailure(null, new JobFailure("STEP_FAILED", "failed")));
	}

	@Test
	void rejectsMissingJobFailure() {
		assertThrows(NullPointerException.class, () -> new PipelineFailure("test", null));
	}
}

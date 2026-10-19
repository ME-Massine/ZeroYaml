package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

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

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", "Build", "-build", "build-", "build step", "build\n", "a/b" })
	void rejectsStepNamesOutsideTheStepFormat(String name) {
		assertThrows(IllegalArgumentException.class,
				() -> new PipelineFailure(name, new JobFailure("STEP_FAILED", "failed")));
	}

	@Test
	void acceptsMaximumStepNameLengthAndRejectsOneMore() {
		var jobFailure = new JobFailure("STEP_FAILED", "failed");

		assertEquals(64, new PipelineFailure("a".repeat(64), jobFailure).stepName().length());
		assertThrows(IllegalArgumentException.class,
				() -> new PipelineFailure("a".repeat(65), jobFailure));
	}

	@Test
	void rejectsMissingJobFailure() {
		assertThrows(NullPointerException.class, () -> new PipelineFailure("test", null));
	}
}

package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;

class PipelineStepTest {

	private static final ExecutionDefinition EXECUTION = new ExecutionDefinition(List.of("./mvnw", "verify"), "service");

	@Test
	void carriesNameAndRunnerExecutionContext() {
		var step = new PipelineStep("build", EXECUTION);

		assertEquals("build", step.name());
		assertEquals(List.of("./mvnw", "verify"), step.execution().command());
		assertEquals("service", step.execution().workingDirectory());
	}

	@ParameterizedTest
	@ValueSource(strings = { "a", "build", "unit-test", "unit_test", "go1.22", "0build", "build-2" })
	void acceptsDocumentedNameFormats(String name) {
		assertEquals(name, new PipelineStep(name, EXECUTION).name());
	}

	@ParameterizedTest
	@ValueSource(strings = { "", " ", "Build", "-build", "build-", ".build", "build.", "build step", "build\n", "a/b", "café" })
	void rejectsNamesOutsideTheDocumentedFormat(String name) {
		assertThrows(IllegalArgumentException.class, () -> new PipelineStep(name, EXECUTION));
	}

	@Test
	void rejectsNullName() {
		assertThrows(IllegalArgumentException.class, () -> new PipelineStep(null, EXECUTION));
	}

	@Test
	void acceptsNameAtMaximumLengthAndRejectsOneMore() {
		assertEquals(64, new PipelineStep("a".repeat(64), EXECUTION).name().length());
		assertThrows(IllegalArgumentException.class, () -> new PipelineStep("a".repeat(65), EXECUTION));
	}

	@Test
	void rejectsMissingExecution() {
		assertThrows(NullPointerException.class, () -> new PipelineStep("build", null));
	}
}

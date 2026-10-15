package io.zeroyaml.controlplane.domain.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PipelineStatusTest {

	@Test
	void marksOnlySucceededAndFailedAsTerminal() {
		assertFalse(PipelineStatus.PENDING.isTerminal());
		assertFalse(PipelineStatus.RUNNING.isTerminal());
		assertTrue(PipelineStatus.SUCCEEDED.isTerminal());
		assertTrue(PipelineStatus.FAILED.isTerminal());
	}
}

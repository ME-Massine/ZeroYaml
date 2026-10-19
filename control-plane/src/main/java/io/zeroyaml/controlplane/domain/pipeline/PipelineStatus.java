package io.zeroyaml.controlplane.domain.pipeline;

/**
 * Minimal outcome states of a pipeline for the first end-to-end flow. Richer
 * lifecycle states, such as cancellation, belong to later phases.
 */
public enum PipelineStatus {
	PENDING,
	RUNNING,
	SUCCEEDED,
	FAILED;

	/**
	 * Indicates whether this state ends the pipeline.
	 *
	 * @return {@code true} for terminal states
	 */
	public boolean isTerminal() {
		return this == SUCCEEDED || this == FAILED;
	}
}

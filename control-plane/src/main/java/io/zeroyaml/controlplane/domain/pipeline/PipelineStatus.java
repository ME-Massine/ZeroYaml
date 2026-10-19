package io.zeroyaml.controlplane.domain.pipeline;

/**
 * Minimal outcome states of a pipeline for the first end-to-end flow.
 *
 * <p>{@code CANCELLED} mirrors the terminal Job state of the same name, so a step
 * whose Job was cancelled has a pipeline state without a fabricated failure. The
 * rules that move a pipeline between states belong to later lifecycle work.</p>
 */
public enum PipelineStatus {
	PENDING,
	RUNNING,
	SUCCEEDED,
	FAILED,
	CANCELLED
}

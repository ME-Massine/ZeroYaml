package io.zeroyaml.controlplane.domain.pipeline;

import java.util.Objects;

/**
 * Status of one pipeline together with the failure that ended it, if any.
 *
 * <p>This is the owner of status and failure consistency: a failure is present
 * exactly when the status is {@link PipelineStatus#FAILED}, and it must name a step
 * of {@link #pipeline()}. Inconsistent combinations are rejected with an
 * {@link IllegalArgumentException}.</p>
 *
 * @param pipeline pipeline the outcome belongs to
 * @param status current status
 * @param failure failure that ended the pipeline, or {@code null} unless the status is
 *        {@link PipelineStatus#FAILED}
 */
public record PipelineOutcome(Pipeline pipeline, PipelineStatus status, PipelineFailure failure) {

	public PipelineOutcome {
		Objects.requireNonNull(pipeline, "pipeline must not be null");
		Objects.requireNonNull(status, "status must not be null");
		if ((status == PipelineStatus.FAILED) != (failure != null)) {
			throw new IllegalArgumentException("failure must be present exactly when status is FAILED");
		}
		if (failure != null
				&& pipeline.steps().stream().noneMatch(step -> step.name().equals(failure.stepName()))) {
			throw new IllegalArgumentException("failure names a step outside the pipeline: " + failure.stepName());
		}
	}
}

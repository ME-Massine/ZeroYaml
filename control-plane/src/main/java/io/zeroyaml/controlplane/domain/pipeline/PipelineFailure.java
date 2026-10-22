package io.zeroyaml.controlplane.domain.pipeline;

import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.JobFailure;

/**
 * Safe, structured reason a pipeline ended as {@link PipelineStatus#FAILED}.
 *
 * <p>{@link PipelineOutcome#failed} checks that the step name belongs to the
 * pipeline, which also guarantees it follows {@link PipelineStep} naming rules.</p>
 *
 * @param stepName name of the step that failed
 * @param failure Job failure details, including the exit code when reported
 */
public record PipelineFailure(String stepName, JobFailure failure) {

	public PipelineFailure {
		Objects.requireNonNull(stepName, "stepName must not be null");
		Objects.requireNonNull(failure, "failure must not be null");
	}
}

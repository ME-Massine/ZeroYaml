package io.zeroyaml.controlplane.domain.pipeline;

import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.JobFailure;

/**
 * Safe, structured reason a pipeline ended as {@link PipelineStatus#FAILED}.
 *
 * <p>The step name follows {@link PipelineStep} naming rules.
 * {@link PipelineOutcome} checks that it names a step of its pipeline.</p>
 *
 * @param stepName name of the step that failed
 * @param failure Job failure details, including the exit code when reported
 */
public record PipelineFailure(String stepName, JobFailure failure) {

	public PipelineFailure {
		stepName = PipelineStep.requireValidName(stepName);
		Objects.requireNonNull(failure, "failure must not be null");
	}
}

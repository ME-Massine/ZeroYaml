package io.zeroyaml.controlplane.domain.pipeline;

import java.util.Objects;
import java.util.Optional;

/**
 * Status of one pipeline and, when a step ended it, which step and why.
 *
 * <p>This type owns status and failure consistency, and its factories are the only
 * way to create one: a failure is present exactly when the status is
 * {@link PipelineStatus#FAILED}, a cancelled step exactly when it is
 * {@link PipelineStatus#CANCELLED}, and either must name a step of the pipeline
 * the outcome is created from. Inconsistent combinations are rejected with an
 * {@link IllegalArgumentException}.</p>
 *
 * <p>Only the pipeline's {@link PipelineId} is retained, so comparing, hashing, or
 * storing an outcome never walks the plan.</p>
 */
public final class PipelineOutcome {

	private final PipelineId pipelineId;
	private final PipelineStatus status;
	private final PipelineFailure failure;
	private final String cancelledStep;

	private PipelineOutcome(PipelineId pipelineId, PipelineStatus status, PipelineFailure failure,
			String cancelledStep) {
		this.pipelineId = pipelineId;
		this.status = status;
		this.failure = failure;
		this.cancelledStep = cancelledStep;
	}

	/**
	 * Creates an outcome that no step has ended.
	 *
	 * @param pipeline pipeline the outcome belongs to
	 * @param status {@code PENDING}, {@code RUNNING}, or {@code SUCCEEDED}
	 * @return the outcome
	 */
	public static PipelineOutcome of(Pipeline pipeline, PipelineStatus status) {
		Objects.requireNonNull(pipeline, "pipeline must not be null");
		Objects.requireNonNull(status, "status must not be null");
		if (status == PipelineStatus.FAILED || status == PipelineStatus.CANCELLED) {
			throw new IllegalArgumentException(status + " must name the step that ended the pipeline");
		}
		return new PipelineOutcome(pipeline.id(), status, null, null);
	}

	/**
	 * Creates the outcome of a pipeline ended by a failed step.
	 *
	 * @param pipeline pipeline the outcome belongs to
	 * @param failure failure of one of the pipeline's steps
	 * @return a {@code FAILED} outcome
	 */
	public static PipelineOutcome failed(Pipeline pipeline, PipelineFailure failure) {
		Objects.requireNonNull(failure, "failure must not be null");
		requireStep(pipeline, failure.stepName());
		return new PipelineOutcome(pipeline.id(), PipelineStatus.FAILED, failure, null);
	}

	/**
	 * Creates the outcome of a pipeline ended by a step whose Job was cancelled.
	 *
	 * @param pipeline pipeline the outcome belongs to
	 * @param stepName name of the cancelled step
	 * @return a {@code CANCELLED} outcome
	 */
	public static PipelineOutcome cancelled(Pipeline pipeline, String stepName) {
		requireStep(pipeline, stepName);
		return new PipelineOutcome(pipeline.id(), PipelineStatus.CANCELLED, null, stepName);
	}

	public PipelineId pipelineId() {
		return pipelineId;
	}

	public PipelineStatus status() {
		return status;
	}

	public Optional<PipelineFailure> failure() {
		return Optional.ofNullable(failure);
	}

	public Optional<String> cancelledStep() {
		return Optional.ofNullable(cancelledStep);
	}

	@Override
	public boolean equals(Object other) {
		return this == other || other instanceof PipelineOutcome outcome
				&& pipelineId.equals(outcome.pipelineId)
				&& status == outcome.status
				&& Objects.equals(failure, outcome.failure)
				&& Objects.equals(cancelledStep, outcome.cancelledStep);
	}

	@Override
	public int hashCode() {
		return Objects.hash(pipelineId, status, failure, cancelledStep);
	}

	private static void requireStep(Pipeline pipeline, String stepName) {
		Objects.requireNonNull(pipeline, "pipeline must not be null");
		if (pipeline.steps().stream().noneMatch(step -> step.name().equals(stepName))) {
			throw new IllegalArgumentException("step is not part of the pipeline: " + stepName);
		}
	}
}

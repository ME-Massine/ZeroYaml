package io.zeroyaml.controlplane.domain.pipeline;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.RepositoryReference;

/**
 * Immutable plan describing what ZeroYAML runs for one repository revision.
 *
 * <p>A pipeline sits between repository inference and executable Jobs: it is the
 * ordered list of steps the Control Plane decided to run, and each step later
 * becomes one Job built from {@link #source()} and the step's execution. It is not
 * a workflow language: there are no triggers, matrices, reusable workflows, secret
 * definitions, or deployment graphs.</p>
 *
 * <p>Ordering rules: the plan orders steps one after another exactly as supplied;
 * executing them in that order belongs to the application boundary. Identity
 * rules: a pipeline is referenced by its {@link PipelineId}, and a step by its
 * name, which is unique within the pipeline. As an immutable value, two instances
 * are equal only when identity, source, and steps all match.</p>
 *
 * <p>Construction is the validation point. A pipeline with no steps, more than
 * {@value #MAX_STEPS} steps, or two steps with the same name is rejected with an
 * {@link IllegalArgumentException}, and a missing step with a
 * {@link NullPointerException}, so an existing instance is always executable.</p>
 *
 * @param id pipeline identity
 * @param source repository and immutable revision every step runs against
 * @param steps ordered steps; copied defensively
 */
public record Pipeline(PipelineId id, RepositoryReference source, List<PipelineStep> steps) {

	/** Upper bound that keeps a pipeline, and the Jobs created from it, finite. */
	static final int MAX_STEPS = 50;

	public Pipeline {
		Objects.requireNonNull(id, "id must not be null");
		Objects.requireNonNull(source, "source must not be null");
		Objects.requireNonNull(steps, "steps must not be null");
		if (steps.isEmpty()) {
			throw new IllegalArgumentException("steps must contain at least one step");
		}
		if (steps.size() > MAX_STEPS) {
			throw new IllegalArgumentException("steps must not contain more than " + MAX_STEPS + " steps");
		}
		var names = new HashSet<String>();
		for (var step : steps) {
			Objects.requireNonNull(step, "steps must not contain null");
			if (!names.add(step.name())) {
				throw new IllegalArgumentException("step names must be unique: " + step.name());
			}
		}
		steps = List.copyOf(steps);
	}
}

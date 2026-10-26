package io.zeroyaml.controlplane.domain.pipeline;

import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.Job;

/**
 * One executable Job created from one {@link PipelineStep}, together with the
 * link back to the step it came from.
 *
 * <p>The Job model stays unaware of pipelines, so this value is what makes a Job
 * traceable: it names the pipeline, the step, and the step's position in the
 * pipeline order.</p>
 *
 * @param pipelineId pipeline the step belongs to
 * @param position 1-based position of the step in {@link Pipeline#steps()}
 * @param stepName name of the step the Job executes
 * @param job Job executing the step
 */
public record PipelineJob(PipelineId pipelineId, int position, String stepName, Job job) {

	public PipelineJob {
		Objects.requireNonNull(pipelineId, "pipelineId must not be null");
		if (position < 1) {
			throw new IllegalArgumentException("position must be at least 1");
		}
		Objects.requireNonNull(stepName, "stepName must not be null");
		Objects.requireNonNull(job, "job must not be null");
	}
}

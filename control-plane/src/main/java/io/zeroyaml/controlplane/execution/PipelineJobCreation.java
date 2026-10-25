package io.zeroyaml.controlplane.execution;

import java.util.List;
import java.util.Objects;

import io.zeroyaml.controlplane.domain.pipeline.PipelineId;

/**
 * Result of recording the Jobs of one pipeline.
 *
 * @param pipelineId pipeline the Jobs belong to
 * @param jobs a reference to the recorded Job of every step, in pipeline order; a Job recorded
 *        by an earlier call keeps its identity and state
 * @param createdCount how many of {@code jobs} this call recorded; the rest already existed
 */
public record PipelineJobCreation(PipelineId pipelineId, List<PipelineJobReference> jobs, int createdCount) {

	public PipelineJobCreation {
		Objects.requireNonNull(pipelineId, "pipelineId must not be null");
		jobs = List.copyOf(jobs);
		if (createdCount < 0 || createdCount > jobs.size()) {
			throw new IllegalArgumentException("createdCount must be between 0 and the number of jobs");
		}
	}

	/**
	 * @return {@code true} when every Job already existed, as for a repeated delivery
	 */
	public boolean alreadyRecorded() {
		return createdCount == 0;
	}
}

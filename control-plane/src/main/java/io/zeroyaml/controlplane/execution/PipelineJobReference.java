package io.zeroyaml.controlplane.execution;

import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.pipeline.PipelineId;

/**
 * Identity of one recorded pipeline Job, linked to the step it executes.
 *
 * <p>It carries the Job identity rather than the Job, because the recorded Job
 * is a mutable aggregate owned by the {@link JobStore}. Its state is read
 * through {@link JobStore#find} and changed only through {@link JobStore#update},
 * which serializes changes per Job.</p>
 *
 * @param pipelineId pipeline the step belongs to
 * @param position 1-based position of the step in the pipeline order
 * @param stepName name of the step the Job executes
 * @param jobId identity of the recorded Job
 */
public record PipelineJobReference(PipelineId pipelineId, int position, String stepName, JobId jobId) {

	public PipelineJobReference {
		Objects.requireNonNull(pipelineId, "pipelineId must not be null");
		if (position < 1) {
			throw new IllegalArgumentException("position must be at least 1");
		}
		Objects.requireNonNull(stepName, "stepName must not be null");
		Objects.requireNonNull(jobId, "jobId must not be null");
	}
}

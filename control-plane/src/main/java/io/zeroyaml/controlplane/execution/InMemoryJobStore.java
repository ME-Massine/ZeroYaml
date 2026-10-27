package io.zeroyaml.controlplane.execution;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.pipeline.PipelineId;
import io.zeroyaml.controlplane.domain.pipeline.PipelineJob;

/**
 * Process-local Job record retained for focused application tests.
 *
 * <p>{@link #update(JobId, Function)} runs inside
 * {@link ConcurrentHashMap#computeIfPresent}, which gives exclusive access per
 * Job identity. That is what makes concurrent status reports for one Job
 * serialize, while reports for different Jobs stay independent.</p>
 */
class InMemoryJobStore implements JobStore {

	private final ConcurrentHashMap<JobId, Job> jobsById = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<JobId, PipelineJobReference> pipelineJobs = new ConcurrentHashMap<>();

	@Override
	public Job recordIfAbsent(Job job) {
		Objects.requireNonNull(job, "job must not be null");

		var existing = jobsById.putIfAbsent(job.id(), job);
		return existing == null ? job : existing;
	}

	@Override
	public Job recordIfAbsent(PipelineJob pipelineJob) {
		Objects.requireNonNull(pipelineJob, "pipelineJob must not be null");
		var recorded = recordIfAbsent(pipelineJob.job());
		pipelineJobs.put(recorded.id(), new PipelineJobReference(
				pipelineJob.pipelineId(), pipelineJob.position(), pipelineJob.stepName(), recorded.id()));
		return recorded;
	}

	@Override
	public List<PipelineJobReference> findByPipeline(PipelineId pipelineId, RepositoryReference source) {
		Objects.requireNonNull(pipelineId, "pipelineId must not be null");
		Objects.requireNonNull(source, "source must not be null");
		return references().stream().filter(reference -> reference.pipelineId().equals(pipelineId)
				&& jobsById.get(reference.jobId()).repository().equals(source)).toList();
	}

	@Override
	public List<PipelineJobReference> findByRepository(URI location) {
		Objects.requireNonNull(location, "location must not be null");
		return references().stream().filter(reference -> jobsById.get(reference.jobId()).repository().location().equals(location)).toList();
	}

	@Override
	public List<PipelineJobReference> findBySource(RepositoryReference source) {
		Objects.requireNonNull(source, "source must not be null");
		return references().stream().filter(reference -> jobsById.get(reference.jobId()).repository().equals(source)).toList();
	}

	private List<PipelineJobReference> references() {
		return pipelineJobs.values().stream()
				.sorted(Comparator.comparing((PipelineJobReference reference) -> jobsById.get(reference.jobId()).createdAt())
						.thenComparing(PipelineJobReference::position).thenComparing(reference -> reference.jobId().value()))
				.toList();
	}

	@Override
	public Optional<Job> find(JobId jobId) {
		Objects.requireNonNull(jobId, "jobId must not be null");

		return Optional.ofNullable(jobsById.get(jobId));
	}

	@Override
	public <R> Optional<R> update(JobId jobId, Function<Job, R> change) {
		Objects.requireNonNull(jobId, "jobId must not be null");
		Objects.requireNonNull(change, "change must not be null");

		var result = new AtomicReference<R>();
		jobsById.computeIfPresent(jobId, (id, job) -> {
			result.set(change.apply(job));
			// The Job aggregate is mutated in place, so the same instance stays mapped.
			return job;
		});

		return Optional.ofNullable(result.get());
	}
}

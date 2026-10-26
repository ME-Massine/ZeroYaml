package io.zeroyaml.controlplane.execution;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.springframework.stereotype.Component;

import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;

/**
 * Process-local Job record used while durable persistence is still out of
 * scope. Jobs are lost on restart, so this is a development and test vehicle
 * rather than a production store.
 *
 * <p>{@link #update(JobId, Function)} runs inside
 * {@link ConcurrentHashMap#computeIfPresent}, which gives exclusive access per
 * Job identity. That is what makes concurrent status reports for one Job
 * serialize, while reports for different Jobs stay independent.</p>
 */
@Component
class InMemoryJobStore implements JobStore {

	private final ConcurrentHashMap<JobId, Job> jobsById = new ConcurrentHashMap<>();

	@Override
	public Job recordIfAbsent(Job job) {
		Objects.requireNonNull(job, "job must not be null");

		var existing = jobsById.putIfAbsent(job.id(), job);
		return existing == null ? job : existing;
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

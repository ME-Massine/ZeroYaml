package io.zeroyaml.controlplane.execution;

import java.util.Optional;
import java.util.function.Function;

import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;

/**
 * The Control Plane's record of the Jobs it owns.
 *
 * <p>The interface exists so that Job state stays reachable through one
 * boundary while the storage technology is chosen separately. Durable
 * persistence is tracked as its own work; the current implementation keeps
 * Jobs in memory.</p>
 */
public interface JobStore {

	/**
	 * Records a Job the Control Plane created.
	 *
	 * @param job Job to record
	 * @throws IllegalStateException when a Job with the same identity is already recorded
	 */
	default void add(Job job) {
		if (recordIfAbsent(job) != job) {
			throw new IllegalStateException("Job " + job.id().value() + " is already recorded");
		}
	}

	/**
	 * Records a Job unless a Job with the same identity is already recorded. The
	 * check and the insert are one atomic step, so concurrent callers recording
	 * the same identity record it exactly once.
	 *
	 * @param job Job to record
	 * @return the recorded Job: {@code job} itself when it was recorded by this call, otherwise
	 *         the Job already recorded under its identity, which is left unchanged
	 */
	Job recordIfAbsent(Job job);

	/**
	 * Looks up a Job for reading.
	 *
	 * @param jobId identity to look up
	 * @return the Job, or empty when this Control Plane does not know it
	 */
	Optional<Job> find(JobId jobId);

	/**
	 * Applies {@code change} to the stored Job with exclusive access to it, so
	 * that two status reports for the same Job cannot interleave and leave the
	 * lifecycle half-applied. Changes to different Jobs stay independent.
	 *
	 * @param jobId Job to change
	 * @param change work to perform on the Job; it must not block, call back into the store,
	 *        or return {@code null}, which is reserved for an unknown Job
	 * @param <R> result produced by {@code change}
	 * @return the result of {@code change}, or empty when the Job is unknown
	 */
	<R> Optional<R> update(JobId jobId, Function<Job, R> change);
}

package io.zeroyaml.controlplane.scheduling;

import io.zeroyaml.controlplane.domain.job.Job;

/**
 * Control Plane policy for choosing the Runner that executes a queued Job. The
 * Control Plane owns this decision; a Runner never selects or reprioritizes its
 * own work. Callers depend on this interface so a richer scheduling strategy
 * can replace the first one without touching dispatch code.
 */
public interface RunnerSelector {

	/**
	 * Chooses a Runner for {@code job} from the Runners that are currently
	 * eligible. The Job is not modified and nothing is dispatched.
	 *
	 * @param job Job waiting in {@link io.zeroyaml.controlplane.domain.job.JobStatus#QUEUED}
	 * @return {@link RunnerSelectionOutcome.Selected} naming the Runner, or
	 *         {@link RunnerSelectionOutcome.NoAvailableRunner} when none is eligible
	 * @throws IllegalStateException if {@code job} is not queued
	 */
	RunnerSelectionOutcome select(Job job);
}

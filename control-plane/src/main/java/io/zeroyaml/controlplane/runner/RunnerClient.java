package io.zeroyaml.controlplane.runner;

import io.zeroyaml.controlplane.domain.job.Job;

/**
 * Control Plane boundary for the small set of Runner calls needed by this service.
 * Keeping callers behind this interface prevents application code from depending on
 * generated gRPC classes directly.
 */
public interface RunnerClient {

	RunnerPingResult ping(String message);

	RunnerInfo getInfo();

	/**
	 * Dispatches one executable Job to the Runner and returns its acknowledgment.
	 *
	 * <p>The call completes as soon as the Runner answers; it does not wait for
	 * execution. A Runner that declines a well-formed dispatch returns a rejected
	 * {@link JobDispatchResult}, while a transport failure or a request the
	 * Runner rejects as invalid raises {@link RunnerClientException}.</p>
	 *
	 * @param job Job to execute, already selected by the Control Plane
	 * @return the Runner acknowledgment for that Job
	 */
	JobDispatchResult dispatchJob(Job job);
}

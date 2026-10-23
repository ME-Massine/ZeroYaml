package io.zeroyaml.controlplane.scheduling;

import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;

/**
 * The Control Plane's decision of where one Job runs: the Job paired with the
 * Runner process selected by policy. Future dispatch coordination can read the
 * decision from here instead of choosing a Runner itself.
 *
 * <p>This value is not wired to {@code RunnerClient}: the existing client calls
 * one configured endpoint and cannot route to a selected process. Routing and
 * enforcing the assignment across dispatch and status reports require separate
 * coordination work before this context can drive production dispatch.</p>
 *
 * <p>The context records a decision only. The Job stays queued until the
 * executing Runner reports that it started, which is when the Job aggregate
 * records the assignment.</p>
 *
 * @param jobId Job the decision applies to
 * @param runner Runner process selected to execute the Job
 */
public record JobDispatchContext(JobId jobId, RunnerAssignment runner) {

	public JobDispatchContext {
		Objects.requireNonNull(jobId, "jobId must not be null");
		Objects.requireNonNull(runner, "runner must not be null");
	}
}

package io.zeroyaml.controlplane.scheduling;

import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.JobId;

/**
 * Result of one attempt to choose a Runner for a Job. It is either
 * {@link Selected}, which carries the {@link JobDispatchContext}, or
 * {@link NoAvailableRunner}, which leaves the Job queued, so "no Runner
 * available" is an explicit result rather than an exception or a silent drop.
 *
 * <p>The two cases are separate types so that a dispatch context exists only
 * where a Runner was selected; a caller handles both with a {@code switch}
 * and cannot read a target from a blocked outcome.</p>
 */
public sealed interface RunnerSelectionOutcome {

	/** Job the selection was attempted for. */
	JobId jobId();

	/** Operator-facing explanation of the result. */
	String message();

	/**
	 * A Runner was chosen for the Job.
	 *
	 * @param dispatch Job paired with the selected Runner process
	 * @param message operator-facing explanation of the result
	 */
	record Selected(JobDispatchContext dispatch, String message) implements RunnerSelectionOutcome {

		public Selected {
			Objects.requireNonNull(dispatch, "dispatch must not be null");
			Objects.requireNonNull(message, "message must not be null");
		}

		@Override
		public JobId jobId() {
			return dispatch.jobId();
		}
	}

	/**
	 * No registered Runner can take work right now. The Job stays queued and
	 * nothing is dispatched; the caller decides when to ask again.
	 *
	 * @param jobId Job that stays queued
	 * @param message operator-facing explanation of the result
	 */
	record NoAvailableRunner(JobId jobId, String message) implements RunnerSelectionOutcome {

		public NoAvailableRunner {
			Objects.requireNonNull(jobId, "jobId must not be null");
			Objects.requireNonNull(message, "message must not be null");
		}
	}
}

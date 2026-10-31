package io.zeroyaml.controlplane.execution;

import java.util.List;

import io.zeroyaml.controlplane.domain.job.JobId;

/** Application boundary for bounded command output retention, separate from Job state. */
interface JobLogStore {
	/** Appends one increasing observation; a final marker after any sequence gap is retained as incomplete. */
	void append(JobLogRecord record);

	/**
	 * Atomically appends only to an already retained stream, enforcing its Runner
	 * identity and sequence. Returns false without mutation if absent or evicted,
	 * so the caller must verify authoritative attribution before admitting it.
	 */
	boolean appendIfRetained(JobLogRecord record);

	/**
	 * Reports whether a new stream could currently be admitted, so callers can
	 * refuse it before an authoritative lookup. The answer is advisory: a
	 * concurrent admission can still make {@link #append} reject the stream.
	 */
	boolean hasAdmissionCapacity();

	/** Returns an immutable ordered snapshot; empty also covers unknown or evicted logs. */
	List<JobLogRecord> snapshot(JobId jobId);
}

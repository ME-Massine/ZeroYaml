package io.zeroyaml.controlplane.execution;

import java.util.List;

import io.zeroyaml.controlplane.domain.job.JobId;

/** Application boundary for bounded command output retention, separate from Job state. */
interface JobLogStore {
	/** Appends one increasing observation; sequence gaps require an incomplete final marker. */
	void append(JobLogRecord record);

	/** Returns an immutable ordered snapshot; empty also covers unknown or evicted logs. */
	List<JobLogRecord> snapshot(JobId jobId);
}

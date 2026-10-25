package io.zeroyaml.controlplane.execution;

import java.util.Objects;

import org.springframework.stereotype.Component;

/**
 * Attributes output to an existing assigned Job under the same per-Job lock as
 * status reports. Recording output never mutates the authoritative Job state.
 */
@Component
class JobLogRecorder {
	private final JobStore jobs;
	private final JobLogStore logs;

	JobLogRecorder(JobStore jobs, JobLogStore logs) {
		this.jobs = Objects.requireNonNull(jobs);
		this.logs = Objects.requireNonNull(logs);
	}

	boolean append(JobLogRecord record) {
		return jobs.update(record.jobId(), job -> {
			if (job.runnerAssignment().isEmpty() || !job.runnerAssignment().get().equals(record.reportedBy())) {
				throw new IllegalStateException("log must come from the assigned Runner process");
			}
			// Runner and Control Plane clocks are independent. Sequence orders
			// observations without comparing timestamps from different hosts.
			logs.append(record);
			return true;
		}).orElse(false);
	}
}

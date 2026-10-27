package io.zeroyaml.controlplane.execution;

import java.util.Objects;

import org.springframework.stereotype.Component;

/**
 * Attributes output through a read-only Job lookup. Assignment is immutable
 * once execution starts, so output never needs the status-report write lock.
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
		return jobs.find(record.jobId()).map(job -> {
			if (job.runnerAssignment().isEmpty() || !job.runnerAssignment().get().equals(record.reportedBy())) {
				throw new JobLogAttributionException();
			}
			// Runner and Control Plane clocks are independent. Sequence orders
			// observations without comparing timestamps from different hosts.
			logs.append(record);
			return true;
		}).orElse(false);
	}
}

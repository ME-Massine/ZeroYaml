package io.zeroyaml.controlplane.execution;

import java.util.Objects;

import org.springframework.stereotype.Component;

/**
 * Attributes new retained streams through a read-only Job lookup. Assignment
 * is immutable once execution starts; later chunks use the retained identity.
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
		// The retained check and append are atomic with eviction. No database
		// lookup or separate attribution cache is needed for a retained stream.
		if (logs.appendIfRetained(record)) {
			return true;
		}
		return jobs.find(record.jobId()).map(job -> {
			if (job.runnerAssignment().isEmpty() || !job.runnerAssignment().get().equals(record.reportedBy())) {
				throw new JobLogAttributionException();
			}
			// The Runner sends its final marker before the terminal status report,
			// so a legitimate record never reaches a terminal Job. Refusing it here
			// keeps a closed log closed after its retained stream was evicted.
			if (job.status().isTerminal()) {
				throw new JobLogSequenceException("log is closed because the Job has finished");
			}
			// Runner and Control Plane clocks are independent. Sequence orders
			// observations without comparing timestamps from different hosts.
			logs.append(record);
			return true;
		}).orElse(false);
	}
}

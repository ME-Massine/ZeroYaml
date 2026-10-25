package io.zeroyaml.controlplane.execution;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import io.zeroyaml.controlplane.domain.job.JobId;

/**
 * Process-local retention of at most 32 Jobs and 1 MiB / 256 data chunks each.
 * Only finished output is evicted; full active retention rejects new Jobs.
 * Snapshots and appends
 * are serialized; no caller can mutate stored byte arrays. Restart loses logs.
 */
@Component
class InMemoryJobLogStore implements JobLogStore {
	private static final int MAX_JOBS = 32;
	private final Map<JobId, List<JobLogRecord>> logs = new LinkedHashMap<>();

	@Override
	public synchronized void append(JobLogRecord record) {
		var existing = logs.get(record.jobId());
		var previous = existing == null || existing.isEmpty() ? null : existing.getLast();
		long expected = previous == null ? 1 : previous.sequence() + 1;
		if (previous != null && (previous.endOfOutput() || !previous.reportedBy().equals(record.reportedBy()))) {
			throw new IllegalStateException("log is closed or belongs to another Runner process");
		}
		if (record.sequence() < expected) {
			throw new IllegalStateException("log sequence must increase");
		}
		// Failed chunks must not poison subsequent delivery. Sequence gaps stay
		// visible and require an incomplete final marker.
		boolean missingChunks = record.sequence() > expected
				|| (previous != null && previous.sequence() > existing.size());
		if (record.endOfOutput() && missingChunks && !record.incomplete()) {
			throw new IllegalStateException("sequence gaps require an incomplete final marker");
		}
		int bytes = existing == null ? 0 : existing.stream().mapToInt(JobLogRecord::contentSize).sum();
		if (bytes + record.contentSize() > JobLogRecord.MAX_OUTPUT_BYTES) {
			throw new IllegalArgumentException("log exceeds the per-Job output budget");
		}
		if (existing == null) {
			if (logs.size() == MAX_JOBS) {
				var completed = logs.entrySet().stream()
						.filter(entry -> entry.getValue().getLast().endOfOutput())
						.map(Map.Entry::getKey).findFirst();
				if (completed.isEmpty()) {
					throw new JobLogCapacityException();
				}
				logs.remove(completed.get());
			}
			existing = new ArrayList<>();
			logs.put(record.jobId(), existing);
		}
		existing.add(record);
	}

	@Override
	public synchronized List<JobLogRecord> snapshot(JobId jobId) {
		return List.copyOf(logs.getOrDefault(jobId, List.of()));
	}
}

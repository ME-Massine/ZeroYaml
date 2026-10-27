package io.zeroyaml.controlplane.execution;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.zeroyaml.controlplane.domain.job.JobId;

/**
 * Process-local retention of at most 32 Jobs and 1 MiB / 256 data chunks each.
 * At capacity, evicts the oldest admitted stream that has finished or has been
 * idle for five minutes. Recent active output is preserved. Snapshots and
 * appends are serialized; no caller can mutate stored bytes. Restart loses logs.
 */
@Component
class InMemoryJobLogStore implements JobLogStore {
	private static final int MAX_JOBS = 32;
	private static final Duration MAX_IDLE = Duration.ofMinutes(5);
	private final Clock clock;
	private final Map<JobId, RetainedOutput> logs = new LinkedHashMap<>();

	@Autowired
	InMemoryJobLogStore() {
		this(Clock.systemUTC());
	}

	InMemoryJobLogStore(Clock clock) {
		this.clock = Objects.requireNonNull(clock);
	}

	@Override
	public synchronized void append(JobLogRecord record) {
		var output = logs.get(record.jobId());
		var existing = output == null ? null : output.records;
		var previous = existing == null || existing.isEmpty() ? null : existing.getLast();
		long expected = previous == null ? 1 : previous.sequence() + 1;
		if (previous != null && (previous.endOfOutput() || !previous.reportedBy().equals(record.reportedBy()))) {
			throw new JobLogSequenceException("log is closed or belongs to another Runner process");
		}
		if (record.sequence() < expected) {
			throw new JobLogSequenceException("log sequence must increase");
		}
		// Failed chunks must not poison subsequent delivery. Sequence gaps stay
		// visible and require an incomplete final marker.
		boolean missingChunks = record.sequence() > expected
				|| (previous != null && previous.sequence() > existing.size());
		if (record.endOfOutput() && missingChunks && !record.incomplete()) {
			throw new JobLogSequenceException("sequence gaps require an incomplete final marker");
		}
		int bytes = existing == null ? 0 : existing.stream().mapToInt(JobLogRecord::contentSize).sum();
		if (bytes + record.contentSize() > JobLogRecord.MAX_OUTPUT_BYTES) {
			throw new IllegalArgumentException("log exceeds the per-Job output budget");
		}
		// Use local receipt time: Runner timestamps may be skewed or move backwards.
		var receivedAt = clock.instant();
		if (existing == null) {
			if (logs.size() == MAX_JOBS) {
				var evictable = logs.entrySet().stream()
						.filter(entry -> entry.getValue().records.getLast().endOfOutput()
								|| !entry.getValue().lastReceivedAt.isAfter(receivedAt.minus(MAX_IDLE)))
						.map(Map.Entry::getKey).findFirst();
				if (evictable.isEmpty()) {
					throw new JobLogCapacityException();
				}
				logs.remove(evictable.get());
			}
			existing = new ArrayList<>();
			output = new RetainedOutput(existing);
			logs.put(record.jobId(), output);
		}
		existing.add(record);
		output.lastReceivedAt = receivedAt;
	}

	@Override
	public synchronized List<JobLogRecord> snapshot(JobId jobId) {
		var output = logs.get(jobId);
		return output == null ? List.of() : List.copyOf(output.records);
	}

	private static final class RetainedOutput {
		private final List<JobLogRecord> records;
		private Instant lastReceivedAt;

		private RetainedOutput(List<JobLogRecord> records) {
			this.records = records;
		}
	}
}

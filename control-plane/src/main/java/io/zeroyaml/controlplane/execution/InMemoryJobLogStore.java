package io.zeroyaml.controlplane.execution;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
	public synchronized boolean appendIfRetained(JobLogRecord record) {
		if (!logs.containsKey(record.jobId())) {
			return false;
		}
		append(record);
		return true;
	}

	@Override
	public synchronized void append(JobLogRecord record) {
		var output = logs.get(record.jobId());
		var previous = output == null ? null : output.records.getLast();
		long expected = previous == null ? 1 : previous.sequence() + 1;
		if (previous != null && (previous.endOfOutput() || !previous.reportedBy().equals(record.reportedBy()))) {
			throw new JobLogSequenceException("log is closed or belongs to another Runner process");
		}
		if (record.sequence() < expected) {
			throw new JobLogSequenceException("log sequence must increase");
		}
		// Missing retained chunks can also result from idle eviction after delivery
		// was acknowledged. The sender cannot detect that loss, so normalize the
		// retained final marker instead of rejecting its completeness claim.
		boolean missingChunks = record.sequence() > expected
				|| (previous != null && previous.sequence() > output.records.size());
		if (record.endOfOutput() && missingChunks && !record.incomplete()) {
			record = new JobLogRecord(record.jobId(), record.reportedBy(), record.sequence(),
					record.observedAt(), record.stream(), record.content(), true, record.truncated(), true);
		}
		int bytes = output == null ? 0 : output.bytes;
		if (bytes + record.contentSize() > JobLogRecord.MAX_OUTPUT_BYTES) {
			throw new IllegalArgumentException("log exceeds the per-Job output budget");
		}
		// Use local receipt time: Runner timestamps may be skewed or move backwards.
		var receivedAt = clock.instant();
		if (output == null) {
			if (logs.size() == MAX_JOBS) {
				var evictable = evictable(receivedAt);
				if (evictable.isEmpty()) {
					throw new JobLogCapacityException();
				}
				logs.remove(evictable.get());
			}
			output = new RetainedOutput();
			logs.put(record.jobId(), output);
		}
		output.records.add(record);
		output.bytes += record.contentSize();
		output.lastReceivedAt = receivedAt;
	}

	@Override
	public synchronized boolean hasAdmissionCapacity() {
		return logs.size() < MAX_JOBS || evictable(clock.instant()).isPresent();
	}

	/** Oldest admitted stream that has finished or has been idle for MAX_IDLE. */
	private Optional<JobId> evictable(Instant now) {
		return logs.entrySet().stream()
				.filter(entry -> entry.getValue().records.getLast().endOfOutput()
						|| !entry.getValue().lastReceivedAt.isAfter(now.minus(MAX_IDLE)))
				.map(Map.Entry::getKey).findFirst();
	}

	@Override
	public synchronized List<JobLogRecord> snapshot(JobId jobId) {
		var output = logs.get(jobId);
		return output == null ? List.of() : List.copyOf(output.records);
	}

	private static final class RetainedOutput {
		private final List<JobLogRecord> records = new ArrayList<>();
		private int bytes;
		private Instant lastReceivedAt;
	}
}

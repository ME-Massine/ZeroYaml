package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import org.junit.jupiter.api.Test;

/** Retention uses local receipt activity and bounded, oldest-first eviction. */
class InMemoryJobLogStoreTest {
	private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");
	private static final RunnerAssignment RUNNER = new RunnerAssignment("runner-1", "instance-1");
	private final Clock clock = mock(Clock.class);
	private final InMemoryJobLogStore logs = new InMemoryJobLogStore(clock);

	@Test
	void recoversAllAbandonedSlotsAtTheIdleBoundaryWithoutFinalMarkers() {
		var abandoned = fillRetention();
		var incoming = JobId.newId();
		when(clock.instant()).thenReturn(NOW.plusSeconds(299));
		assertThrows(JobLogCapacityException.class, () -> logs.append(record(incoming, 1)));
		assertFalse(logs.snapshot(abandoned.getFirst()).isEmpty());

		when(clock.instant()).thenReturn(NOW.plusSeconds(300));
		logs.append(record(incoming, 1));
		assertTrue(logs.snapshot(abandoned.getFirst()).isEmpty());
		assertFalse(logs.snapshot(abandoned.get(1)).isEmpty());
		for (int index = 1; index < 32; index++) {
			logs.append(record(JobId.newId(), 1));
		}
		assertTrue(abandoned.stream().allMatch(id -> logs.snapshot(id).isEmpty()));
		assertThrows(JobLogCapacityException.class, () -> logs.append(record(JobId.newId(), 1)));
		assertEquals(1, logs.snapshot(incoming).size());
	}

	@Test
	void refreshesActiveOutputAndEvictsTheOldestEligibleStream() {
		var admitted = fillRetention();
		when(clock.instant()).thenReturn(NOW.plusSeconds(299));
		logs.append(record(admitted.getFirst(), 2));
		when(clock.instant()).thenReturn(NOW.plusSeconds(300));
		logs.append(record(JobId.newId(), 1));
		assertEquals(2, logs.snapshot(admitted.getFirst()).size());
		assertTrue(logs.snapshot(admitted.get(1)).isEmpty());
		assertFalse(logs.snapshot(admitted.get(2)).isEmpty());
	}

	@Test
	void rejectedChunksAndSnapshotsDoNotRefreshTheIdleInterval() {
		var admitted = fillRetention();
		when(clock.instant()).thenReturn(NOW.plusSeconds(299));
		assertThrows(JobLogSequenceException.class, () -> logs.append(record(admitted.getFirst(), 1)));
		assertEquals(1, logs.snapshot(admitted.getFirst()).size());
		when(clock.instant()).thenReturn(NOW.plusSeconds(300));
		logs.append(record(JobId.newId(), 1));
		assertTrue(logs.snapshot(admitted.getFirst()).isEmpty());
	}

	@Test
	void lateOutputAfterEvictionRetainsItsOriginalSequenceAndRequiresIncompleteEnd() {
		var admitted = fillRetention();
		when(clock.instant()).thenReturn(NOW.plusSeconds(300));
		logs.append(record(JobId.newId(), 1));
		var resumed = admitted.getFirst();
		logs.append(record(resumed, 2));
		assertEquals(2, logs.snapshot(resumed).getFirst().sequence());
		assertThrows(JobLogSequenceException.class, () -> logs.append(
				new JobLogRecord(resumed, RUNNER, 3, NOW, JobLogRecord.Stream.NONE, new byte[0], true, false, false)));
		logs.append(new JobLogRecord(resumed, RUNNER, 3, NOW, JobLogRecord.Stream.NONE, new byte[0], true, true, true));
		assertTrue(logs.snapshot(resumed).getLast().incomplete());
	}

	private List<JobId> fillRetention() {
		when(clock.instant()).thenReturn(NOW);
		var admitted = new ArrayList<JobId>();
		for (int index = 0; index < 32; index++) {
			var id = JobId.newId();
			admitted.add(id);
			logs.append(record(id, 1));
		}
		return List.copyOf(admitted);
	}

	private JobLogRecord record(JobId id, long sequence) {
		// Sender clocks cannot expire a newly received stream.
		return new JobLogRecord(id, RUNNER, sequence, NOW.minusSeconds(3600),
				JobLogRecord.Stream.STDOUT, new byte[] { 'x' }, false, false, false);
	}
}

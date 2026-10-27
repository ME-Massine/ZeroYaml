package io.zeroyaml.controlplane.events;

import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.COMPLETED_AT;
import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.CREATED_AT;
import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.JOB_ID;
import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.PIPELINE_ID;
import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.REVISION;
import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.RUNNER;
import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.STARTED_AT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.junit.jupiter.api.Test;

/** Covers which facts each lifecycle event carries and which it refuses. */
class JobLifecycleEventTest {

	@Test
	void acceptedCarriesThePipelineStepAndTheCreationTime() {
		var event = JobLifecycleEvent.accepted(JobLifecycleEventFixtures.acceptedJob());

		assertEquals(JobLifecycleEventType.ACCEPTED, event.type());
		assertEquals(CREATED_AT, event.occurredAt());
		assertEquals(JOB_ID, event.jobId());
		assertEquals(REVISION, event.revision());
		assertEquals(PIPELINE_ID.value(), event.pipelineId());
		assertEquals("test", event.stepName());
		assertEquals(2, event.stepPosition());
		assertNull(event.runner());
	}

	@Test
	void startedCarriesTheRunnerAndTheStartTime() {
		var event = JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob());

		assertEquals(JobLifecycleEventType.STARTED, event.type());
		assertEquals(STARTED_AT, event.occurredAt());
		assertEquals(RUNNER, event.runner());
		assertNull(event.pipelineId());
		assertNull(event.exitCode());
	}

	@Test
	void startedDescribesTheStartOfAJobThatAlreadyCompleted() {
		var event = JobLifecycleEvent.started(JobLifecycleEventFixtures.succeededJob(0));

		assertEquals(JobLifecycleEventType.STARTED, event.type());
		assertEquals(STARTED_AT, event.occurredAt());
	}

	@Test
	void completedDescribesASuccessWithItsExitCode() {
		var event = JobLifecycleEvent.completed(JobLifecycleEventFixtures.succeededJob(0));

		assertEquals(JobLifecycleEventType.SUCCEEDED, event.type());
		assertEquals(COMPLETED_AT, event.occurredAt());
		assertEquals(RUNNER, event.runner());
		assertEquals(0, event.exitCode());
		assertNull(event.failureCode());
	}

	@Test
	void completedDescribesASuccessWithoutAReportedExitCode() {
		var event = JobLifecycleEvent.completed(JobLifecycleEventFixtures.succeededJob(null));

		assertNull(event.exitCode());
	}

	@Test
	void completedDescribesAFailureByItsCodeAndNotItsMessage() {
		var event = JobLifecycleEvent.completed(JobLifecycleEventFixtures.failedJob());

		assertEquals(JobLifecycleEventType.FAILED, event.type());
		assertEquals("NON_ZERO_EXIT", event.failureCode());
		assertEquals(2, event.exitCode());
	}

	@Test
	void everyEventHasItsOwnIdentity() {
		var job = JobLifecycleEventFixtures.runningJob();

		assertNotEquals(JobLifecycleEvent.started(job).eventId(), JobLifecycleEvent.started(job).eventId());
	}

	@Test
	void refusesToDescribeAStartOrOutcomeThatWasNotRecorded() {
		var created = JobLifecycleEventFixtures.createdJob();
		var running = JobLifecycleEventFixtures.runningJob();
		var cancelled = JobLifecycleEventFixtures.runningJob();
		cancelled.cancel(COMPLETED_AT);

		assertThrows(IllegalArgumentException.class, () -> JobLifecycleEvent.started(created));
		assertThrows(IllegalArgumentException.class, () -> JobLifecycleEvent.completed(running));
		assertThrows(IllegalArgumentException.class, () -> JobLifecycleEvent.completed(cancelled));
	}

	@Test
	void refusesFieldsThatDoNotBelongToTheEventType() {
		var id = UUID.randomUUID();

		assertThrows(IllegalArgumentException.class, () -> new JobLifecycleEvent(
				id, JobLifecycleEventType.STARTED, STARTED_AT, JOB_ID, REVISION,
				PIPELINE_ID.value(), "test", 2, RUNNER, null, null));
		assertThrows(IllegalArgumentException.class, () -> new JobLifecycleEvent(
				id, JobLifecycleEventType.ACCEPTED, CREATED_AT, JOB_ID, REVISION,
				PIPELINE_ID.value(), "test", 2, RUNNER, null, null));
		assertThrows(IllegalArgumentException.class, () -> new JobLifecycleEvent(
				id, JobLifecycleEventType.FAILED, COMPLETED_AT, JOB_ID, REVISION,
				null, null, null, RUNNER, null, 1));
		assertThrows(IllegalArgumentException.class, () -> new JobLifecycleEvent(
				id, JobLifecycleEventType.SUCCEEDED, COMPLETED_AT, JOB_ID, REVISION,
				null, null, null, RUNNER, "NON_ZERO_EXIT", 0));
		assertThrows(IllegalArgumentException.class, () -> new JobLifecycleEvent(
				id, JobLifecycleEventType.STARTED, STARTED_AT, JOB_ID, REVISION,
				null, null, null, RUNNER, null, 0));
		assertThrows(NullPointerException.class, () -> new JobLifecycleEvent(
				id, JobLifecycleEventType.STARTED, STARTED_AT, null, REVISION,
				null, null, null, RUNNER, null, null));
	}
}

package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.events.JobLifecycleEvent;
import io.zeroyaml.controlplane.events.JobLifecycleEventType;
import org.junit.jupiter.api.Test;

/**
 * Covers how Runner execution reports are reconciled with the authoritative
 * Job, including the reports that must never change it.
 */
class JobExecutionStatusRecorderTest {

	private static final Instant CREATED_AT = Instant.parse("2026-01-01T10:00:00Z");
	private static final Instant STARTED_AT = CREATED_AT.plusSeconds(5);
	private static final Instant COMPLETED_AT = STARTED_AT.plusSeconds(30);
	private static final RunnerAssignment RUNNER = new RunnerAssignment("runner-1", "instance-1");

	private final JobStore jobs = new InMemoryJobStore();
	private final List<JobLifecycleEvent> published = new CopyOnWriteArrayList<>();
	private final JobExecutionStatusRecorder recorder = new JobExecutionStatusRecorder(jobs, published::add);

	@Test
	void recordsThatAQueuedJobStartedRunningOnTheReportingRunner() {
		var job = queuedJob();

		var outcome = recorder.record(JobStatusReport.running(job.id(), RUNNER, STARTED_AT));

		assertEquals(JobStatusReportDecision.APPLIED, outcome.decision());
		assertEquals(JobStatus.RUNNING, outcome.jobStatus());
		assertEquals(JobStatus.RUNNING, job.status());
		assertEquals(STARTED_AT, job.startedAt().orElseThrow());
		assertEquals(RUNNER, job.runnerAssignment().orElseThrow());
	}

	@Test
	void recordsASuccessfulExecutionAsATerminalSuccess() {
		var job = runningJob();

		var outcome = recorder.record(
				JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));

		assertEquals(JobStatusReportDecision.APPLIED, outcome.decision());
		assertEquals(JobStatus.SUCCEEDED, job.status());
		assertEquals(COMPLETED_AT, job.completedAt().orElseThrow());
		assertTrue(job.failure().isEmpty());
	}

	@Test
	void recordsANonZeroExitAsAFailureWithTheReasonAndTheExitCode() {
		var job = runningJob();

		var outcome = recorder.record(JobStatusReport.failed(
				job.id(),
				RUNNER,
				STARTED_AT,
				COMPLETED_AT,
				2,
				JobExecutionFailureReason.NON_ZERO_EXIT,
				"The command exited with code 2"
		));

		assertEquals(JobStatusReportDecision.APPLIED, outcome.decision());
		assertEquals(JobStatus.FAILED, job.status());

		var failure = job.failure().orElseThrow();
		assertEquals(JobExecutionFailureReason.NON_ZERO_EXIT.name(), failure.code());
		assertEquals(OptionalInt.of(2), failure.exit());
	}

	@Test
	void recordsATimeoutAsAFailureWithoutAnExitCode() {
		var job = runningJob();

		recorder.record(JobStatusReport.failed(
				job.id(),
				RUNNER,
				STARTED_AT,
				COMPLETED_AT,
				null,
				JobExecutionFailureReason.TIMEOUT,
				"The job exceeded its time budget"
		));

		assertEquals(JobStatus.FAILED, job.status());

		var failure = job.failure().orElseThrow();
		assertEquals(JobExecutionFailureReason.TIMEOUT.name(), failure.code());
		// A stopped command produced no code, and an absent code must never be
		// recorded as a successful 0.
		assertEquals(OptionalInt.empty(), failure.exit());
	}

	@Test
	void recordsACancelledExecutionAsAFailureWithTheCancelledReason() {
		var job = runningJob();

		recorder.record(JobStatusReport.failed(
				job.id(),
				RUNNER,
				STARTED_AT,
				COMPLETED_AT,
				null,
				JobExecutionFailureReason.CANCELLED,
				"Execution stopped during runner shutdown"
		));

		assertEquals(JobStatus.FAILED, job.status());
		assertEquals(JobExecutionFailureReason.CANCELLED.name(), job.failure().orElseThrow().code());
	}

	@Test
	void completesAQueuedJobWhenTheRunningReportWasLost() {
		var job = queuedJob();

		var outcome = recorder.record(
				JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));

		// A lost running report must not strand a finished execution, and the
		// repeated start time keeps the recorded timeline truthful.
		assertEquals(JobStatusReportDecision.APPLIED, outcome.decision());
		assertEquals(JobStatus.SUCCEEDED, job.status());
		assertEquals(STARTED_AT, job.startedAt().orElseThrow());
		assertEquals(RUNNER, job.runnerAssignment().orElseThrow());
	}

	@Test
	void treatsARepeatedRunningReportAsADuplicate() {
		var job = queuedJob();
		var report = JobStatusReport.running(job.id(), RUNNER, STARTED_AT);
		recorder.record(report);

		var outcome = recorder.record(report);

		assertEquals(JobStatusReportDecision.DUPLICATE, outcome.decision());
		assertEquals(JobStatus.RUNNING, outcome.jobStatus());
		assertEquals(STARTED_AT, job.startedAt().orElseThrow());
	}

	@Test
	void treatsARepeatedTerminalReportAsADuplicate() {
		var job = runningJob();
		var report = JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0);
		recorder.record(report);

		var outcome = recorder.record(report);

		assertEquals(JobStatusReportDecision.DUPLICATE, outcome.decision());
		assertEquals(JobStatus.SUCCEEDED, job.status());
		assertEquals(COMPLETED_AT, job.completedAt().orElseThrow());
	}

	@Test
	void treatsARunningReportThatArrivesAfterTheOutcomeAsADuplicate() {
		var job = runningJob();
		recorder.record(JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));

		var outcome = recorder.record(JobStatusReport.running(job.id(), RUNNER, STARTED_AT));

		assertEquals(JobStatusReportDecision.DUPLICATE, outcome.decision());
		assertEquals(JobStatus.SUCCEEDED, job.status());
	}

	@Test
	void refusesASecondAndDifferentOutcomeForTheSameJob() {
		var job = runningJob();
		recorder.record(JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));

		var outcome = recorder.record(JobStatusReport.failed(
				job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 1, JobExecutionFailureReason.NON_ZERO_EXIT, "late failure"));

		assertEquals(JobStatusReportDecision.CONFLICT, outcome.decision());
		assertEquals(JobStatus.SUCCEEDED, job.status());
		assertTrue(job.failure().isEmpty());
	}

	@Test
	void keepsACancellationWhenTheOutcomeArrivesAfterIt() {
		var job = runningJob();
		job.cancel(COMPLETED_AT.minusSeconds(1));

		var outcome = recorder.record(
				JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));

		// Cancellation is the Control Plane's own decision, so a late outcome from
		// the runner does not overturn it.
		assertEquals(JobStatusReportDecision.DUPLICATE, outcome.decision());
		assertEquals(JobStatus.CANCELLED, job.status());
	}

	@Test
	void refusesAReportFromAProcessTheJobIsNotAssignedTo() {
		var job = runningJob();

		var outcome = recorder.record(JobStatusReport.succeeded(
				job.id(), new RunnerAssignment("runner-1", "instance-2"), STARTED_AT, COMPLETED_AT, 0));

		assertEquals(JobStatusReportDecision.CONFLICT, outcome.decision());
		assertEquals(JobStatus.RUNNING, job.status());
	}

	@Test
	void refusesAReportForAJobThatWasNeverQueued() {
		var job = Job.create(JobId.newId(), repository(), execution(), CREATED_AT);
		jobs.add(job);

		var outcome = recorder.record(JobStatusReport.running(job.id(), RUNNER, STARTED_AT));

		assertEquals(JobStatusReportDecision.CONFLICT, outcome.decision());
		assertEquals(JobStatus.CREATED, job.status());
	}

	@Test
	void refusesAReportThatStartedBeforeTheJobExisted() {
		var job = queuedJob();

		var outcome = recorder.record(
				JobStatusReport.running(job.id(), RUNNER, CREATED_AT.minusSeconds(1)));

		assertEquals(JobStatusReportDecision.CONFLICT, outcome.decision());
		assertEquals(JobStatus.QUEUED, job.status());
	}

	@Test
	void refusesAnOutcomeThatEndedBeforeTheRecordedStart() {
		var job = runningJob();
		var earlierStart = CREATED_AT.plusSeconds(1);

		var outcome = recorder.record(JobStatusReport.succeeded(
				job.id(), RUNNER, earlierStart, earlierStart.plusSeconds(1), 0));

		assertEquals(JobStatusReportDecision.CONFLICT, outcome.decision());
		assertEquals(JobStatus.RUNNING, job.status());
		assertTrue(job.completedAt().isEmpty());
	}

	@Test
	void reportsAnUnknownJobWithoutAState() {
		var outcome = recorder.record(JobStatusReport.running(JobId.newId(), RUNNER, STARTED_AT));

		assertEquals(JobStatusReportDecision.UNKNOWN_JOB, outcome.decision());
		assertTrue(outcome.status().isEmpty());
	}

	@Test
	void appliesOnlyOneOfManyConcurrentReportsOfTheSameOutcome() throws Exception {
		var job = runningJob();
		var report = JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0);
		var callers = 16;

		List<JobStatusReportOutcome> outcomes;
		try (var executor = Executors.newFixedThreadPool(callers)) {
			List<Callable<JobStatusReportOutcome>> reports = Collections.nCopies(
					callers, () -> recorder.record(report));
			var futures = executor.invokeAll(reports, 10, TimeUnit.SECONDS);
			outcomes = futures.stream().map(future -> {
				try {
					return future.get();
				} catch (Exception exception) {
					throw new IllegalStateException(exception);
				}
			}).toList();
		}

		var applied = outcomes.stream().filter(JobStatusReportOutcome::isApplied).count();
		assertEquals(1, applied, "exactly one concurrent report may change the job");
		assertEquals(JobStatus.SUCCEEDED, job.status());
		assertEquals(List.of(JobLifecycleEventType.SUCCEEDED), publishedTypes());
	}

	@Test
	void publishesAStartedEventWithTheRunnerOnceTheRunningReportIsApplied() {
		var job = queuedJob();

		recorder.record(JobStatusReport.running(job.id(), RUNNER, STARTED_AT));

		assertEquals(1, published.size());
		var event = published.get(0);
		assertEquals(JobLifecycleEventType.STARTED, event.type());
		assertEquals(job.id(), event.jobId());
		assertEquals(STARTED_AT, event.occurredAt());
		assertEquals(RUNNER, event.runner());
		assertEquals("a1b2c3d4", event.revision());
	}

	@Test
	void publishesASucceededEventWithTheExitCode() {
		var job = runningJob();

		recorder.record(JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));

		assertEquals(List.of(JobLifecycleEventType.SUCCEEDED), publishedTypes());
		var event = published.get(0);
		assertEquals(COMPLETED_AT, event.occurredAt());
		assertEquals(0, event.exitCode());
		assertNull(event.failureCode());
	}

	@Test
	void publishesAFailedEventWithTheFailureCodeAndTheExitCode() {
		var job = runningJob();

		recorder.record(JobStatusReport.failed(
				job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 2, JobExecutionFailureReason.NON_ZERO_EXIT, "exit 2"));

		assertEquals(List.of(JobLifecycleEventType.FAILED), publishedTypes());
		var event = published.get(0);
		assertEquals(JobExecutionFailureReason.NON_ZERO_EXIT.name(), event.failureCode());
		assertEquals(2, event.exitCode());
	}

	@Test
	void publishesTheStartAndTheOutcomeWhenTheRunningReportWasLost() {
		var job = queuedJob();

		recorder.record(JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));

		assertEquals(List.of(JobLifecycleEventType.STARTED, JobLifecycleEventType.SUCCEEDED), publishedTypes());
		assertEquals(STARTED_AT, published.get(0).occurredAt());
		assertEquals(COMPLETED_AT, published.get(1).occurredAt());
	}

	@Test
	void publishesNothingForReportsThatDoNotChangeTheJob() {
		var job = runningJob();
		recorder.record(JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));
		published.clear();

		recorder.record(JobStatusReport.succeeded(job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 0));
		recorder.record(JobStatusReport.failed(
				job.id(), RUNNER, STARTED_AT, COMPLETED_AT, 1, JobExecutionFailureReason.NON_ZERO_EXIT, "late"));
		recorder.record(JobStatusReport.running(JobId.newId(), RUNNER, STARTED_AT));

		assertTrue(published.isEmpty());
	}

	@Test
	void publishesNothingWhenTheTransitionIsNotCommitted() {
		var job = queuedJob();
		var failingStore = new InMemoryJobStore() {
			@Override
			public <R> Optional<R> update(JobId jobId, Function<Job, R> change) {
				throw new IllegalStateException("database unavailable");
			}
		};
		failingStore.add(job);
		var failingRecorder = new JobExecutionStatusRecorder(failingStore, published::add);

		assertThrows(IllegalStateException.class,
				() -> failingRecorder.record(JobStatusReport.running(job.id(), RUNNER, STARTED_AT)));

		assertTrue(published.isEmpty());
	}

	@Test
	void leavesAStoredJobInPlaceWhenAReportIsRefused() {
		var job = runningJob();

		recorder.record(JobStatusReport.succeeded(
				job.id(), new RunnerAssignment("runner-9", "instance-9"), STARTED_AT, COMPLETED_AT, 0));

		assertSame(job, jobs.find(job.id()).orElseThrow());
		assertFalse(job.status().isTerminal());
	}

	private List<JobLifecycleEventType> publishedTypes() {
		return published.stream().map(JobLifecycleEvent::type).toList();
	}

	private Job queuedJob() {
		var job = Job.create(JobId.newId(), repository(), execution(), CREATED_AT);
		job.queue(CREATED_AT.plusSeconds(1));
		jobs.add(job);

		return job;
	}

	private Job runningJob() {
		var job = queuedJob();
		job.start(RUNNER, STARTED_AT);

		return job;
	}

	private static RepositoryReference repository() {
		return new RepositoryReference(URI.create("https://github.com/example/repository.git"), "a1b2c3d4");
	}

	private static ExecutionDefinition execution() {
		return new ExecutionDefinition(List.of("bash", "-lc", "make build"), ".");
	}
}

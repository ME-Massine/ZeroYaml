package io.zeroyaml.controlplane.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobFailure;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.events.JobLifecycleEvent;
import io.zeroyaml.controlplane.events.JobLifecycleEventPublisher;

/**
 * Applies Runner execution reports to the Jobs the Control Plane owns.
 *
 * <p>The Runner reports facts; this component decides what each fact means for
 * the Job lifecycle. Every report is reconciled against the recorded state, so
 * the same report may be delivered any number of times and in any order without
 * corrupting the lifecycle:</p>
 *
 * <ul>
 *   <li>a report the Job already records, or has moved past, is a
 *       {@link JobStatusReportDecision#DUPLICATE} and changes nothing;</li>
 *   <li>a terminal report for a Job that is still queued starts the Job from
 *       the reported start time first, so a <em>lost</em> running report cannot
 *       leave a finished execution without a Job outcome;</li>
 *   <li>a report that contradicts the recorded state, or that comes from a
 *       process the Job is not assigned to, is a
 *       {@link JobStatusReportDecision#CONFLICT} and leaves the Job untouched.</li>
 * </ul>
 *
 * <p>Reconciliation runs with exclusive access to the Job through
 * {@link JobStore#update}, so concurrent reports for one Job are serialized.</p>
 *
 * <p>An applied report publishes one lifecycle event per transition, and only
 * after {@link JobStore#update} has returned, when the transition is
 * committed. A report that is not applied publishes nothing, so a repeated
 * report never repeats an event.</p>
 */
@Component
public class JobExecutionStatusRecorder {

	private static final Logger log = LoggerFactory.getLogger(JobExecutionStatusRecorder.class);

	private final JobStore jobs;
	private final JobLifecycleEventPublisher events;

	JobExecutionStatusRecorder(JobStore jobs, JobLifecycleEventPublisher events) {
		this.jobs = Objects.requireNonNull(jobs, "jobs must not be null");
		this.events = Objects.requireNonNull(events, "events must not be null");
	}

	/**
	 * Reconciles one execution report with the authoritative Job.
	 *
	 * @param report execution fact observed by a Runner
	 * @return how the report was reconciled and the resulting Job state
	 */
	public JobStatusReportOutcome record(JobStatusReport report) {
		Objects.requireNonNull(report, "report must not be null");

		var reconciliation = jobs.update(report.jobId(), job -> {
			var before = job.status();
			var outcome = reconcile(job, report);
			// The events are built while the Job is still exclusively held, so they
			// describe exactly this transition and not one applied concurrently.
			return new Reconciliation(outcome, outcome.isApplied() ? transitionEvents(before, job) : List.of());
		}).orElseGet(() -> new Reconciliation(
				JobStatusReportOutcome.unknownJob("Job " + report.jobId().value() + " is not known to this control plane"),
				List.of()));

		logOutcome(report, reconciliation.outcome());
		reconciliation.events().forEach(events::publish);

		return reconciliation.outcome();
	}

	/**
	 * Lists the events for an applied report. A terminal report for a queued
	 * Job applies two transitions, so it yields a start and an outcome event.
	 */
	private static List<JobLifecycleEvent> transitionEvents(JobStatus before, Job job) {
		var transitions = new ArrayList<JobLifecycleEvent>(2);
		if (before == JobStatus.QUEUED) {
			transitions.add(JobLifecycleEvent.started(job));
		}
		if (job.status().isTerminal()) {
			transitions.add(JobLifecycleEvent.completed(job));
		}
		return transitions;
	}

	private JobStatusReportOutcome reconcile(Job job, JobStatusReport report) {
		var assignment = job.runnerAssignment();
		if (assignment.isPresent() && !assignment.get().equals(report.reportedBy())) {
			// Only the process that runs the Job observes these facts, so a report
			// from another process cannot be attributed to this execution.
			return JobStatusReportOutcome.conflict(
					job.status(),
					"Job " + job.id().value() + " is assigned to runner instance "
							+ assignment.get().instanceId() + " and not to " + report.reportedBy().instanceId()
			);
		}

		if (report.startedAt().isBefore(job.createdAt())) {
			// Accepting this would record a Job that ran before it existed.
			return JobStatusReportOutcome.conflict(
					job.status(),
					"Reported start time precedes the creation of job " + job.id().value()
			);
		}

		return switch (report.state()) {
			case RUNNING -> applyRunning(job, report);
			case SUCCEEDED, FAILED -> applyTerminal(job, report);
		};
	}

	private JobStatusReportOutcome applyRunning(Job job, JobStatusReport report) {
		return switch (job.status()) {
			case RUNNING -> JobStatusReportOutcome.duplicate(
					job.status(), "Job " + job.id().value() + " is already recorded as running");
			case SUCCEEDED, FAILED, CANCELLED -> JobStatusReportOutcome.duplicate(
					job.status(),
					"Running report for job " + job.id().value() + " arrived after it reached " + job.status()
			);
			case CREATED -> JobStatusReportOutcome.conflict(
					job.status(),
					"Job " + job.id().value() + " was never queued for execution"
			);
			case QUEUED -> {
				job.start(report.reportedBy(), report.startedAt());
				yield JobStatusReportOutcome.applied(job.status(), "Job " + job.id().value() + " is running");
			}
		};
	}

	private JobStatusReportOutcome applyTerminal(Job job, JobStatusReport report) {
		var reportedStatus = terminalStatusOf(report.state());

		if (job.status() == reportedStatus) {
			return JobStatusReportOutcome.duplicate(
					job.status(), "Job " + job.id().value() + " already reached " + reportedStatus);
		}
		if (job.status() == JobStatus.CANCELLED) {
			// The Control Plane cancelled the Job while it was still running. The
			// cancellation is the decision that stands; the late outcome is ignored.
			return JobStatusReportOutcome.duplicate(
					job.status(),
					"Job " + job.id().value() + " was cancelled before the runner reported " + reportedStatus
			);
		}
		if (job.status().isTerminal()) {
			return JobStatusReportOutcome.conflict(
					job.status(),
					"Job " + job.id().value() + " already reached " + job.status()
							+ " and cannot also be " + reportedStatus
			);
		}
		if (job.status() == JobStatus.CREATED) {
			return JobStatusReportOutcome.conflict(
					job.status(),
					"Job " + job.id().value() + " was never queued for execution"
			);
		}

		// The timeline is checked before anything is changed, so a rejected report
		// never leaves the Job half-transitioned.
		var startedAt = job.startedAt().orElse(report.startedAt());
		if (report.completedAt().isBefore(startedAt)) {
			return JobStatusReportOutcome.conflict(
					job.status(),
					"Reported completion time precedes the recorded start of job " + job.id().value()
			);
		}

		if (job.status() == JobStatus.QUEUED) {
			// The running report was lost. The terminal report repeats the start
			// time, so the Job can still be completed with a truthful timeline.
			job.start(report.reportedBy(), report.startedAt());
		}

		if (report.state() == JobExecutionState.SUCCEEDED) {
			job.succeed(report.completedAt(), report.exitCode());
		} else {
			job.fail(toJobFailure(report), report.completedAt());
		}

		return JobStatusReportOutcome.applied(
				job.status(), "Job " + job.id().value() + " reached " + job.status());
	}

	private static JobFailure toJobFailure(JobStatusReport report) {
		return new JobFailure(report.failureReason().name(), report.failureMessage(), report.exitCode());
	}

	private static JobStatus terminalStatusOf(JobExecutionState state) {
		return state == JobExecutionState.SUCCEEDED ? JobStatus.SUCCEEDED : JobStatus.FAILED;
	}

	/**
	 * Records one audit line per report. The Job, the reporting process, and the
	 * decision are kept; the failure diagnostic is not, because it is derived
	 * from execution output.
	 */
	private static void logOutcome(JobStatusReport report, JobStatusReportOutcome outcome) {
		RunnerAssignment reportedBy = report.reportedBy();
		switch (outcome.decision()) {
			case APPLIED -> log.info(
					"Job {} reported {} by runner {} instance {}: {}",
					report.jobId().value(), report.state(), reportedBy.runnerId(), reportedBy.instanceId(),
					outcome.message());
			case DUPLICATE -> log.debug(
					"Job {} reported {} by runner {} instance {} was not applied: {}",
					report.jobId().value(), report.state(), reportedBy.runnerId(), reportedBy.instanceId(),
					outcome.message());
			case UNKNOWN_JOB, CONFLICT -> log.warn(
					"Job {} reported {} by runner {} instance {} was refused: {}",
					report.jobId().value(), report.state(), reportedBy.runnerId(), reportedBy.instanceId(),
					outcome.message());
		}
	}

	private record Reconciliation(JobStatusReportOutcome outcome, List<JobLifecycleEvent> events) { }
}

package io.zeroyaml.controlplane.events;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobFailure;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.domain.pipeline.PipelineJob;

/**
 * One recorded Job lifecycle transition, as published to operational consumers.
 *
 * <p>The event describes a transition that PostgreSQL already holds; it is a
 * notification, never the source of truth. It deliberately leaves out the
 * repository location, which can carry credentials, the command, and the
 * failure message, which is derived from execution output. A consumer that
 * needs more reads it from the Control Plane.</p>
 *
 * <p>Which optional fields are present depends on {@link #type()}:</p>
 * <ul>
 *   <li>{@link JobLifecycleEventType#ACCEPTED}: the pipeline step
 *       ({@code pipelineId}, {@code stepName}, {@code stepPosition});</li>
 *   <li>{@link JobLifecycleEventType#STARTED}: the {@code runner};</li>
 *   <li>{@link JobLifecycleEventType#SUCCEEDED}: the {@code runner} and, when
 *       reported, the {@code exitCode};</li>
 *   <li>{@link JobLifecycleEventType#FAILED}: the {@code runner}, the
 *       {@code failureCode}, and, when reported, the {@code exitCode}.</li>
 * </ul>
 *
 * @param eventId unique identity of this event, for consumer-side de-duplication
 * @param type transition that happened
 * @param occurredAt when the transition happened, taken from the recorded Job
 * @param jobId Job that changed; also the delivery key
 * @param revision immutable source revision the Job executes
 * @param pipelineId pipeline the Job belongs to; {@code null} unless {@code ACCEPTED}
 * @param stepName pipeline step the Job executes; {@code null} unless {@code ACCEPTED}
 * @param stepPosition 1-based step position; {@code null} unless {@code ACCEPTED}
 * @param runner Runner process the Job is assigned to; {@code null} for {@code ACCEPTED}
 * @param failureCode structured failure code; present only for {@code FAILED}
 * @param exitCode command exit code; only for a terminal event, and {@code null} when none was reported
 */
public record JobLifecycleEvent(
		UUID eventId,
		JobLifecycleEventType type,
		Instant occurredAt,
		JobId jobId,
		String revision,
		UUID pipelineId,
		String stepName,
		Integer stepPosition,
		RunnerAssignment runner,
		String failureCode,
		Integer exitCode
) {

	public JobLifecycleEvent {
		Objects.requireNonNull(eventId, "eventId must not be null");
		Objects.requireNonNull(type, "type must not be null");
		Objects.requireNonNull(occurredAt, "occurredAt must not be null");
		Objects.requireNonNull(jobId, "jobId must not be null");
		Objects.requireNonNull(revision, "revision must not be null");

		var accepted = type == JobLifecycleEventType.ACCEPTED;
		if (accepted != (pipelineId != null) || accepted != (stepName != null) || accepted != (stepPosition != null)) {
			throw new IllegalArgumentException("pipeline step fields are required for, and only for, " + type.wireName());
		}
		if (accepted == (runner != null)) {
			throw new IllegalArgumentException("runner is required for every event except job.accepted");
		}
		if ((type == JobLifecycleEventType.FAILED) != (failureCode != null)) {
			throw new IllegalArgumentException("failureCode is required for, and only for, job.failed");
		}
		if (exitCode != null && type != JobLifecycleEventType.SUCCEEDED && type != JobLifecycleEventType.FAILED) {
			throw new IllegalArgumentException("exitCode is allowed only for a terminal event");
		}
	}

	/**
	 * Describes a Job that was just recorded for a pipeline step.
	 *
	 * @param pipelineJob newly recorded Job and its pipeline step
	 * @return a {@code job.accepted} event dated at the Job's creation
	 */
	public static JobLifecycleEvent accepted(PipelineJob pipelineJob) {
		Objects.requireNonNull(pipelineJob, "pipelineJob must not be null");
		var job = pipelineJob.job();
		return new JobLifecycleEvent(
				UUID.randomUUID(), JobLifecycleEventType.ACCEPTED, job.createdAt(), job.id(),
				job.repository().revision(), pipelineJob.pipelineId().value(), pipelineJob.stepName(),
				pipelineJob.position(), null, null, null);
	}

	/**
	 * Describes the start of a Job. The Job may already have moved past
	 * {@link JobStatus#RUNNING}, as when one report both starts and completes it.
	 *
	 * @param job Job that has a recorded start
	 * @return a {@code job.started} event dated at the recorded start
	 * @throws IllegalArgumentException when the Job has not started
	 */
	public static JobLifecycleEvent started(Job job) {
		Objects.requireNonNull(job, "job must not be null");
		var startedAt = job.startedAt()
				.orElseThrow(() -> new IllegalArgumentException("Job " + job.id().value() + " has not started"));
		var runner = job.runnerAssignment()
				.orElseThrow(() -> new IllegalArgumentException("Job " + job.id().value() + " has no runner"));
		return new JobLifecycleEvent(
				UUID.randomUUID(), JobLifecycleEventType.STARTED, startedAt, job.id(),
				job.repository().revision(), null, null, null, runner, null, null);
	}

	/**
	 * Describes the outcome of a Job that a Runner completed.
	 *
	 * @param job Job in {@link JobStatus#SUCCEEDED} or {@link JobStatus#FAILED}
	 * @return a {@code job.succeeded} or {@code job.failed} event dated at completion
	 * @throws IllegalArgumentException when the Job has no Runner outcome
	 */
	public static JobLifecycleEvent completed(Job job) {
		Objects.requireNonNull(job, "job must not be null");
		if (job.status() != JobStatus.SUCCEEDED && job.status() != JobStatus.FAILED) {
			throw new IllegalArgumentException(
					"Job " + job.id().value() + " has no runner outcome; it is " + job.status());
		}
		var type = job.status() == JobStatus.SUCCEEDED ? JobLifecycleEventType.SUCCEEDED : JobLifecycleEventType.FAILED;
		var runner = job.runnerAssignment()
				.orElseThrow(() -> new IllegalArgumentException("Job " + job.id().value() + " has no runner"));
		var exit = job.exit();
		return new JobLifecycleEvent(
				UUID.randomUUID(), type, job.completedAt().orElseThrow(), job.id(), job.repository().revision(),
				null, null, null, runner, job.failure().map(JobFailure::code).orElse(null),
				exit.isPresent() ? exit.getAsInt() : null);
	}
}

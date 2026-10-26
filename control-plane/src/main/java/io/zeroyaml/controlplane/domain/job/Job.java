package io.zeroyaml.controlplane.domain.job;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Control Plane aggregate representing one executable unit of work.
 *
 * <p>The aggregate owns lifecycle transitions but does not select a Runner,
 * execute commands, persist itself, or interpret a workflow definition.</p>
 */
public final class Job {

	private static final Map<JobStatus, Set<JobStatus>> LEGAL_TRANSITIONS = Map.of(
			JobStatus.CREATED, Set.of(JobStatus.QUEUED, JobStatus.CANCELLED),
			JobStatus.QUEUED, Set.of(JobStatus.RUNNING, JobStatus.CANCELLED),
			JobStatus.RUNNING, Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED),
			JobStatus.SUCCEEDED, Set.of(),
			JobStatus.FAILED, Set.of(),
			JobStatus.CANCELLED, Set.of()
	);

	private final JobId id;
	private final RepositoryReference repository;
	private final ExecutionDefinition execution;
	private final Instant createdAt;

	private JobStatus status;
	private Instant queuedAt;
	private Instant startedAt;
	private Instant completedAt;
	private RunnerAssignment runnerAssignment;
	private JobFailure failure;
	private Integer exitCode;

	private Job(JobId id, RepositoryReference repository, ExecutionDefinition execution, Instant createdAt) {
		this.id = Objects.requireNonNull(id, "id must not be null");
		this.repository = Objects.requireNonNull(repository, "repository must not be null");
		this.execution = Objects.requireNonNull(execution, "execution must not be null");
		this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
		this.status = JobStatus.CREATED;
	}

	/**
	 * Creates a new Job with a generated identity.
	 *
	 * @param repository source repository and revision
	 * @param execution concrete command definition
	 * @param createdAt creation timestamp
	 * @return a newly created Job in {@link JobStatus#CREATED}
	 */
	public static Job create(RepositoryReference repository, ExecutionDefinition execution, Instant createdAt) {
		return new Job(JobId.newId(), repository, execution, createdAt);
	}

	/**
	 * Creates a Job with a caller-provided identity, useful for rehydration and
	 * deterministic application tests without coupling the model to persistence.
	 */
	public static Job create(JobId id, RepositoryReference repository, ExecutionDefinition execution, Instant createdAt) {
		return new Job(id, repository, execution, createdAt);
	}

	public JobId id() {
		return id;
	}

	public RepositoryReference repository() {
		return repository;
	}

	public ExecutionDefinition execution() {
		return execution;
	}

	public JobStatus status() {
		return status;
	}

	public Instant createdAt() {
		return createdAt;
	}

	public Optional<Instant> startedAt() {
		return Optional.ofNullable(startedAt);
	}

	/** Returns when the Job entered the queue, if it has. */
	public Optional<Instant> queuedAt() {
		return Optional.ofNullable(queuedAt);
	}

	public Optional<Instant> completedAt() {
		return Optional.ofNullable(completedAt);
	}

	public Optional<RunnerAssignment> runnerAssignment() {
		return Optional.ofNullable(runnerAssignment);
	}

	public Optional<JobFailure> failure() {
		return Optional.ofNullable(failure);
	}

	/** Returns the command exit code when execution produced one. */
	public OptionalInt exit() {
		return exitCode == null ? OptionalInt.empty() : OptionalInt.of(exitCode);
	}

	/**
	 * Moves a newly created Job into the scheduler queue.
	 */
	public void queue(Instant queuedAt) {
		transitionTo(JobStatus.QUEUED, queuedAt);
		this.queuedAt = queuedAt;
	}

	/**
	 * Starts a queued Job on the Runner process selected by the application layer.
	 */
	public void start(RunnerAssignment assignment, Instant startedAt) {
		Objects.requireNonNull(assignment, "assignment must not be null");
		transitionTo(JobStatus.RUNNING, startedAt);
		this.runnerAssignment = assignment;
		this.startedAt = startedAt;
	}

	/**
	 * Completes a running Job successfully.
	 */
	public void succeed(Instant completedAt) {
		succeed(completedAt, null);
	}

	/** Completes a running Job and retains the command exit code, when reported. */
	public void succeed(Instant completedAt, Integer exitCode) {
		transitionTo(JobStatus.SUCCEEDED, completedAt);
		this.exitCode = exitCode;
		this.completedAt = completedAt;
	}

	/**
	 * Completes a running Job with structured failure information.
	 */
	public void fail(JobFailure failure, Instant completedAt) {
		Objects.requireNonNull(failure, "failure must not be null");
		transitionTo(JobStatus.FAILED, completedAt);
		this.failure = failure;
		this.exitCode = failure.exitCode();
		this.completedAt = completedAt;
	}

	/**
	 * Cancels a non-terminal Job. A running Job retains its Runner linkage so
	 * application code can issue a later cancellation request to that process.
	 */
	public void cancel(Instant completedAt) {
		transitionTo(JobStatus.CANCELLED, completedAt);
		this.completedAt = completedAt;
	}

	private void transitionTo(JobStatus target, Instant transitionAt) {
		Objects.requireNonNull(target, "target must not be null");
		Objects.requireNonNull(transitionAt, "transitionAt must not be null");
		if (!LEGAL_TRANSITIONS.get(status).contains(target)) {
			throw new InvalidJobTransitionException(status, target);
		}
		if (transitionAt.isBefore(createdAt)) {
			throw new IllegalArgumentException("transition timestamp must not be before createdAt");
		}
		if (target == JobStatus.SUCCEEDED || target == JobStatus.FAILED || target == JobStatus.CANCELLED) {
			if (startedAt != null && transitionAt.isBefore(startedAt)) {
				throw new IllegalArgumentException("completion timestamp must not be before startedAt");
			}
		}
		status = target;
	}
}

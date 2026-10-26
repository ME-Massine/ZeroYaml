package io.zeroyaml.controlplane.execution;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobFailure;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.domain.pipeline.PipelineId;
import io.zeroyaml.controlplane.domain.pipeline.PipelineJob;
import tools.jackson.databind.json.JsonMapper;

/**
 * PostgreSQL source of truth for Job state and pipeline links. A row lock holds
 * each update through its commit, including domain validation and the callback.
 * Separate Control Plane processes therefore reconcile reports for one Job in
 * the same order without blocking updates to unrelated Jobs.
 */
@Repository
class JdbcJobStore implements JobStore {

	private static final String JOB_COLUMNS = """
			SELECT id, repository_location, revision, command_json, working_directory,
			       status, created_at, queued_at, started_at, completed_at,
			       runner_id, runner_instance_id, failure_code, failure_message, exit_code
			FROM job WHERE id = :id
			""";
	private static final String REFERENCE_COLUMNS = """
			SELECT r.pipeline_id, j.step_position, j.step_name, j.id
			FROM job j JOIN pipeline_run r ON r.id = j.pipeline_run_id
			""";

	private final JdbcClient jdbc;
	private final TransactionTemplate transactions;
	private final JsonMapper json;

	JdbcJobStore(JdbcClient jdbc, TransactionTemplate transactions, JsonMapper json) {
		this.jdbc = Objects.requireNonNull(jdbc, "jdbc must not be null");
		this.transactions = Objects.requireNonNull(transactions, "transactions must not be null");
		this.json = Objects.requireNonNull(json, "json must not be null");
	}

	@Override
	public Job recordIfAbsent(Job job) {
		Objects.requireNonNull(job, "job must not be null");
		return record(job, null, null, null);
	}

	@Override
	public Job recordIfAbsent(PipelineJob pipelineJob) {
		Objects.requireNonNull(pipelineJob, "pipelineJob must not be null");
		var job = pipelineJob.job();
		return Objects.requireNonNull(transactions.execute(transaction -> {
			var source = job.repository();
			validateLocation(source.location());
			jdbc.sql("""
						INSERT INTO pipeline_run (pipeline_id, repository_location, revision, created_at)
						VALUES (:pipelineId, :location, :revision, :createdAt)
						ON CONFLICT ON CONSTRAINT pipeline_run_source_key DO NOTHING
						""")
					.param("pipelineId", pipelineJob.pipelineId().value())
					.param("location", source.location().toString())
					.param("revision", source.revision())
					.param("createdAt", toColumn(job.createdAt()))
					.update();
			long runId = jdbc.sql("""
						SELECT id FROM pipeline_run
						WHERE pipeline_id = :pipelineId AND repository_location = :location AND revision = :revision
						""")
					.param("pipelineId", pipelineJob.pipelineId().value())
					.param("location", source.location().toString())
					.param("revision", source.revision())
					.query(Long.class).single();
			return record(job, runId, pipelineJob.stepName(), pipelineJob.position());
		}), "transaction produced no result");
	}

	private Job record(Job job, Long runId, String stepName, Integer position) {
		if (job.status() != JobStatus.CREATED) {
			throw new IllegalArgumentException("Only a newly created Job can be recorded");
		}
		validateLocation(job.repository().location());
		int inserted = jdbc.sql("""
						INSERT INTO job (id, pipeline_run_id, step_name, step_position,
						                 repository_location, revision, command_json, working_directory,
						                 status, created_at)
						VALUES (:id, :runId, :stepName, :position, :location, :revision,
						        CAST(:command AS jsonb), :workingDirectory, 'CREATED', :createdAt)
						ON CONFLICT (id) DO NOTHING
						""")
				.param("id", job.id().value())
				.param("runId", runId, Types.BIGINT)
				.param("stepName", stepName, Types.VARCHAR)
				.param("position", position, Types.INTEGER)
				.param("location", job.repository().location().toString())
				.param("revision", job.repository().revision())
				.param("command", json.writeValueAsString(job.execution().command()))
				.param("workingDirectory", job.execution().workingDirectory())
				.param("createdAt", toColumn(job.createdAt()))
				.update();
		if (inserted == 1) {
			return job;
		}

		var existing = find(job.id()).orElseThrow();
		if (!existing.repository().equals(job.repository()) || !existing.execution().equals(job.execution())) {
			throw new IllegalStateException("Job " + job.id().value() + " already has another definition");
		}
		if (runId != null) {
			// shortcut: positions follow the last request per step; version plans when concurrent re-planning is supported.
			jdbc.sql("""
						UPDATE job SET pipeline_run_id = :runId, step_name = :stepName, step_position = :position
						WHERE id = :id AND (pipeline_run_id IS NULL
						    OR (pipeline_run_id = :runId AND step_name = :stepName))
						""")
					.param("runId", runId).param("stepName", stepName).param("position", position)
					.param("id", job.id().value()).update();
			var link = jdbc.sql("SELECT pipeline_run_id, step_name, step_position FROM job WHERE id = :id")
					.param("id", job.id().value())
					.query((row, number) -> new JobLink(row.getObject("pipeline_run_id", Long.class),
							row.getString("step_name"), row.getObject("step_position", Integer.class)))
					.single();
			if (!new JobLink(runId, stepName, position).equals(link)) {
				throw new IllegalStateException("Job " + job.id().value() + " already belongs to another pipeline step");
			}
		}
		return existing;
	}

	@Override
	public Optional<Job> find(JobId jobId) {
		Objects.requireNonNull(jobId, "jobId must not be null");
		return jdbc.sql(JOB_COLUMNS).param("id", jobId.value()).query(this::toJob).optional();
	}

	@Override
	public <R> Optional<R> update(JobId jobId, Function<Job, R> change) {
		Objects.requireNonNull(jobId, "jobId must not be null");
		Objects.requireNonNull(change, "change must not be null");
		return Objects.requireNonNull(transactions.execute(transaction -> {
			var locked = jdbc.sql(JOB_COLUMNS + "FOR UPDATE")
					.param("id", jobId.value()).query(this::toJob).optional();
			if (locked.isEmpty()) {
				return Optional.<R>empty();
			}
			var job = locked.get();
			R result = Objects.requireNonNull(change.apply(job), "change must not return null");
			var assignment = job.runnerAssignment().orElse(null);
			var failure = job.failure().orElse(null);
			jdbc.sql("""
						UPDATE job SET status = :status, queued_at = :queuedAt, started_at = :startedAt,
						               completed_at = :completedAt, runner_id = :runnerId,
						               runner_instance_id = :instanceId, failure_code = :failureCode,
						               failure_message = :failureMessage, exit_code = :exitCode
						WHERE id = :id
						""")
					.param("status", job.status().name())
					.param("queuedAt", toColumn(job.queuedAt().orElse(null)), Types.TIMESTAMP_WITH_TIMEZONE)
					.param("startedAt", toColumn(job.startedAt().orElse(null)), Types.TIMESTAMP_WITH_TIMEZONE)
					.param("completedAt", toColumn(job.completedAt().orElse(null)), Types.TIMESTAMP_WITH_TIMEZONE)
					.param("runnerId", assignment == null ? null : assignment.runnerId(), Types.VARCHAR)
					.param("instanceId", assignment == null ? null : assignment.instanceId(), Types.VARCHAR)
					.param("failureCode", failure == null ? null : failure.code(), Types.VARCHAR)
					.param("failureMessage", failure == null ? null : failure.message(), Types.VARCHAR)
					.param("exitCode", job.exit().isPresent() ? job.exit().getAsInt() : null, Types.INTEGER)
					.param("id", jobId.value()).update();
			return Optional.of(result);
		}), "transaction produced no result");
	}

	@Override
	public List<PipelineJobReference> findByPipeline(PipelineId pipelineId, RepositoryReference source) {
		Objects.requireNonNull(pipelineId, "pipelineId must not be null");
		Objects.requireNonNull(source, "source must not be null");
		return jdbc.sql(REFERENCE_COLUMNS + """
				WHERE r.pipeline_id = :pipelineId AND r.repository_location = :location AND r.revision = :revision
				ORDER BY j.step_position, j.id
				""")
				.param("pipelineId", pipelineId.value())
				.param("location", source.location().toString()).param("revision", source.revision())
				.query(JdbcJobStore::toReference).list();
	}

	@Override
	public List<PipelineJobReference> findByRepository(URI location) {
		Objects.requireNonNull(location, "location must not be null");
		return jdbc.sql(REFERENCE_COLUMNS + "WHERE r.repository_location = :value ORDER BY r.created_at, j.step_position, j.id")
				.param("value", location.toString()).query(JdbcJobStore::toReference).list();
	}

	@Override
	public List<PipelineJobReference> findBySource(RepositoryReference source) {
		Objects.requireNonNull(source, "source must not be null");
		return jdbc.sql(REFERENCE_COLUMNS + """
				WHERE r.repository_location = :location AND r.revision = :revision
				ORDER BY r.created_at, j.step_position, j.id
				""")
				.param("location", source.location().toString()).param("revision", source.revision())
				.query(JdbcJobStore::toReference).list();
	}

	private Job toJob(ResultSet row, int rowNumber) throws SQLException {
		var source = new RepositoryReference(URI.create(row.getString("repository_location")), row.getString("revision"));
		var execution = new ExecutionDefinition(
				Arrays.asList(json.readValue(row.getString("command_json"), String[].class)),
				row.getString("working_directory"));
		var job = Job.create(new JobId(row.getObject("id", UUID.class)), source, execution,
				fromColumn(row, "created_at"));
		var status = JobStatus.valueOf(row.getString("status"));
		if (status != JobStatus.CREATED && fromColumn(row, "queued_at") != null) {
			job.queue(fromColumn(row, "queued_at"));
		}
		if (fromColumn(row, "started_at") != null) {
			job.start(new RunnerAssignment(row.getString("runner_id"), row.getString("runner_instance_id")),
					fromColumn(row, "started_at"));
		}
		if (status == JobStatus.SUCCEEDED) {
			job.succeed(fromColumn(row, "completed_at"), row.getObject("exit_code", Integer.class));
		} else if (status == JobStatus.FAILED) {
			job.fail(new JobFailure(row.getString("failure_code"), row.getString("failure_message"),
					row.getObject("exit_code", Integer.class)), fromColumn(row, "completed_at"));
		} else if (status == JobStatus.CANCELLED) {
			job.cancel(fromColumn(row, "completed_at"));
		}
		return job;
	}

	private static PipelineJobReference toReference(ResultSet row, int rowNumber) throws SQLException {
		return new PipelineJobReference(new PipelineId(row.getObject("pipeline_id", UUID.class)),
				row.getInt("step_position"), row.getString("step_name"), new JobId(row.getObject("id", UUID.class)));
	}

	private static OffsetDateTime toColumn(Instant instant) {
		return instant == null ? null : OffsetDateTime.ofInstant(instant.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC);
	}

	private static Instant fromColumn(ResultSet row, String column) throws SQLException {
		var value = row.getObject(column, OffsetDateTime.class);
		return value == null ? null : value.toInstant();
	}

	private static void validateLocation(URI location) {
		if (location.getRawUserInfo() != null || location.getRawQuery() != null || location.getRawFragment() != null) {
			throw new IllegalArgumentException("Repository location must not contain credentials, query, or fragment");
		}
	}

	private record JobLink(Long runId, String stepName, Integer position) { }
}

package io.zeroyaml.controlplane.events;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobFailure;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.domain.pipeline.PipelineId;
import io.zeroyaml.controlplane.domain.pipeline.PipelineJob;

/** Jobs at each recorded lifecycle stage, shared by the event tests. */
final class JobLifecycleEventFixtures {

	static final Instant CREATED_AT = Instant.parse("2026-10-26T10:00:00Z");
	static final Instant STARTED_AT = CREATED_AT.plusSeconds(5);
	static final Instant COMPLETED_AT = STARTED_AT.plusSeconds(30);
	static final JobId JOB_ID = new JobId(UUID.fromString("0f6a3c2e-5b1d-4e8a-9c7f-2d4b6e8a1c3e"));
	static final PipelineId PIPELINE_ID = new PipelineId(UUID.fromString("7d3c5a1e-0000-4000-8000-000000000001"));
	static final RunnerAssignment RUNNER = new RunnerAssignment("runner-1", "instance-1");
	static final String REVISION = "0123456789abcdef0123456789abcdef01234567";

	private JobLifecycleEventFixtures() {
	}

	static Job createdJob() {
		// The location carries user info on purpose: no event may ever expose it.
		var source = new RepositoryReference(URI.create("https://token@example.test/acme/service.git"), REVISION);
		return Job.create(JOB_ID, source, new ExecutionDefinition(List.of("./mvnw", "verify"), "."), CREATED_AT);
	}

	static PipelineJob acceptedJob() {
		return new PipelineJob(PIPELINE_ID, 2, "test", createdJob());
	}

	static Job runningJob() {
		var job = createdJob();
		job.queue(CREATED_AT.plusSeconds(1));
		job.start(RUNNER, STARTED_AT);
		return job;
	}

	static Job succeededJob(Integer exitCode) {
		var job = runningJob();
		job.succeed(COMPLETED_AT, exitCode);
		return job;
	}

	static Job failedJob() {
		var job = runningJob();
		job.fail(new JobFailure("NON_ZERO_EXIT", "secret-looking output from the build", 2), COMPLETED_AT);
		return job;
	}
}

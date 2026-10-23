package io.zeroyaml.controlplane.scheduling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.registration.HeartbeatOutcome;
import io.zeroyaml.controlplane.registration.RegisteredRunner;
import io.zeroyaml.controlplane.registration.RegistrationOutcome;
import io.zeroyaml.controlplane.registration.RunnerLiveness;
import io.zeroyaml.controlplane.registration.RunnerRegistry;
import io.zeroyaml.controlplane.runner.RunnerCapabilities;
import io.zeroyaml.controlplane.runner.RunnerInfo;
import io.zeroyaml.controlplane.runner.RunnerState;

class HealthyRunnerSelectorTest {

	private static final Instant CREATED_AT = Instant.parse("2026-10-04T10:00:00Z");
	private static final Instant QUEUED_AT = Instant.parse("2026-10-04T10:01:00Z");

	@Test
	void selectsTheOnlyAvailableRunnerAndAttachesItToTheDispatchContext() {
		var job = queuedJob();
		var selector = new HealthyRunnerSelector(registryOf(registered("runner-1", "instance-1")));

		var outcome = selector.select(job);

		var selected = assertInstanceOf(RunnerSelectionOutcome.Selected.class, outcome);
		assertEquals(job.id(), selected.jobId());
		assertEquals(job.id(), selected.dispatch().jobId());
		assertEquals(new RunnerAssignment("runner-1", "instance-1"), selected.dispatch().runner());
	}

	@Test
	void selectsTheSmallestRunnerIdAmongSeveralAvailableRunners() {
		var selector = new HealthyRunnerSelector(registryOf(
				registered("runner-c", "instance-c"),
				registered("runner-a", "instance-a"),
				registered("runner-b", "instance-b")));

		assertEquals(new RunnerAssignment("runner-a", "instance-a"), selectedRunner(selector, queuedJob()));
	}

	@Test
	void selectsTheSameRunnerRegardlessOfTheOrderTheRegistryListsThem() {
		var runners = new ArrayList<>(List.of(
				registered("runner-a", "instance-a"),
				registered("runner-b", "instance-b"),
				registered("runner-c", "instance-c")));
		var job = queuedJob();

		var expected = new RunnerAssignment("runner-a", "instance-a");
		for (var attempt = 0; attempt < runners.size(); attempt++) {
			Collections.rotate(runners, 1);
			var selector = new HealthyRunnerSelector(registryOf(runners.toArray(RegisteredRunner[]::new)));

			assertEquals(expected, selectedRunner(selector, job));
		}
		Collections.reverse(runners);
		var reversed = new HealthyRunnerSelector(registryOf(runners.toArray(RegisteredRunner[]::new)));
		assertEquals(expected, selectedRunner(reversed, job));
	}

	@Test
	void selectsTheSameRunnerForRepeatedCallsAgainstUnchangedRegistryState() {
		var selector = new HealthyRunnerSelector(registryOf(
				registered("runner-b", "instance-b"),
				registered("runner-a", "instance-a")));
		var job = queuedJob();

		var first = selector.select(job);
		var second = selector.select(job);

		assertEquals(first, second);
	}

	@Test
	void blocksTheJobWhenNoRunnerIsAvailable() {
		var job = queuedJob();
		var selector = new HealthyRunnerSelector(registryOf());

		var outcome = selector.select(job);

		var blocked = assertInstanceOf(RunnerSelectionOutcome.NoAvailableRunner.class, outcome);
		assertEquals(job.id(), blocked.jobId());
		assertTrue(blocked.message().contains(job.id().toString()));
	}

	@Test
	void leavesTheJobQueuedAndUnassignedWhetherOrNotARunnerIsSelected() {
		var withRunner = queuedJob();
		var withoutRunner = queuedJob();

		new HealthyRunnerSelector(registryOf(registered("runner-1", "instance-1"))).select(withRunner);
		new HealthyRunnerSelector(registryOf()).select(withoutRunner);

		assertEquals(JobStatus.QUEUED, withRunner.status());
		assertTrue(withRunner.runnerAssignment().isEmpty());
		assertEquals(JobStatus.QUEUED, withoutRunner.status());
		assertTrue(withoutRunner.runnerAssignment().isEmpty());
	}

	@Test
	void rejectsAJobThatIsNotQueued() {
		var created = Job.create(repository(), execution(), CREATED_AT);
		var selector = new HealthyRunnerSelector(registryOf(registered("runner-1", "instance-1")));

		var failure = assertThrows(IllegalStateException.class, () -> selector.select(created));

		assertTrue(failure.getMessage().contains("QUEUED"));
		assertTrue(failure.getMessage().contains("CREATED"));
	}

	@Test
	void rejectsANullJob() {
		var selector = new HealthyRunnerSelector(registryOf());

		assertThrows(NullPointerException.class, () -> selector.select(null));
	}

	@Test
	void rejectsASelectedOutcomeWithoutADispatchContext() {
		assertThrows(NullPointerException.class, () -> new RunnerSelectionOutcome.Selected(null, "selected"));
	}

	@Test
	void rejectsABlockedOutcomeWithoutAJob() {
		assertThrows(NullPointerException.class, () -> new RunnerSelectionOutcome.NoAvailableRunner(null, "blocked"));
	}

	private static RunnerAssignment selectedRunner(RunnerSelector selector, Job job) {
		return assertInstanceOf(RunnerSelectionOutcome.Selected.class, selector.select(job)).dispatch().runner();
	}

	private static Job queuedJob() {
		var job = Job.create(repository(), execution(), CREATED_AT);
		job.queue(QUEUED_AT);
		return job;
	}

	private static RepositoryReference repository() {
		return new RepositoryReference(URI.create("https://git.example/acme/service"), "abc123");
	}

	private static ExecutionDefinition execution() {
		return new ExecutionDefinition(List.of("go", "test", "./..."), "/workspace/service");
	}

	private static RegisteredRunner registered(String runnerId, String instanceId) {
		var info = new RunnerInfo(
				runnerId,
				instanceId,
				"0.1.0",
				"runner.v1",
				RunnerState.READY,
				true,
				new RunnerCapabilities("linux", "amd64", true, List.of(), Map.of()));
		return new RegisteredRunner("registration-" + runnerId, info, CREATED_AT, CREATED_AT);
	}

	private static RunnerRegistry registryOf(RegisteredRunner... available) {
		return new FixedAvailableRegistry(List.of(available));
	}

	/**
	 * Registry double that reports a fixed available set, so the ordering rule is verified against
	 * exact registry state and listing order. Which Runners count as available is the registry's
	 * policy; it is covered with the real registry in {@code RunnerSelectionIntegrationTest}.
	 */
	private record FixedAvailableRegistry(List<RegisteredRunner> available) implements RunnerRegistry {

		@Override
		public RegistrationOutcome register(RunnerInfo runner) {
			throw new UnsupportedOperationException("selection must not register Runners");
		}

		@Override
		public HeartbeatOutcome heartbeat(String runnerId, String instanceId) {
			throw new UnsupportedOperationException("selection must not record heartbeats");
		}

		@Override
		public RunnerLiveness livenessOf(String runnerId) {
			throw new UnsupportedOperationException("selection must use availableRunners()");
		}

		@Override
		public List<RegisteredRunner> availableRunners() {
			return available;
		}
	}
}

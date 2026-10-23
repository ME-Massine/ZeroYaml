package io.zeroyaml.controlplane.registration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.runner.RunnerCapabilities;
import io.zeroyaml.controlplane.runner.RunnerInfo;
import io.zeroyaml.controlplane.runner.RunnerState;
import io.zeroyaml.controlplane.scheduling.RunnerSelectionOutcome;
import io.zeroyaml.controlplane.scheduling.RunnerSelector;

/**
 * Exercises the Runner selector Spring wires from the scheduling package against
 * the real {@link InMemoryRunnerRegistry}, driven only through the calls a Runner
 * makes: registration and heartbeats. It lives in the registration package because
 * the registry and its clock double are package-private; the selector is reached
 * through its public {@link RunnerSelector} interface.
 */
class RunnerSelectionIntegrationTest {

	private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);
	private static final Instant STARTED_AT = Instant.parse("2026-10-04T10:00:00Z");

	private MutableClock clock;
	private RunnerRegistry registry;
	private AnnotationConfigApplicationContext context;
	private RunnerSelector selector;

	@BeforeEach
	void wireTheSelectorOntoTheRealRegistry() {
		clock = new MutableClock(STARTED_AT);
		var properties = new RunnerRegistrationServerProperties();
		properties.setHeartbeatTimeout(HEARTBEAT_TIMEOUT);
		registry = new InMemoryRunnerRegistry(clock, properties);

		context = new AnnotationConfigApplicationContext();
		context.registerBean(RunnerRegistry.class, () -> registry);
		context.scan(RunnerSelector.class.getPackageName());
		context.refresh();
		selector = context.getBean(RunnerSelector.class);
	}

	@AfterEach
	void closeTheContext() {
		context.close();
	}

	@Test
	void selectsAHealthyDockerCapableRunnerUsingTheExistingRegistrationSnapshot() {
		registry.register(runner("runner-1", "instance-1", true));

		assertEquals(new RunnerAssignment("runner-1", "instance-1"), selectedRunner(selector.select(queuedJob())));
	}

	@Test
	void blocksWhenNoRunnerIsRegistered() {
		assertBlocked(selector.select(queuedJob()));
	}

	@ParameterizedTest
	@MethodSource("malformedIdentities")
	void skipsMalformedIdentitiesBeforeRankingOnEverySelection(String runnerId, String instanceId) {
		assertEquals(RegistrationDecision.ACCEPTED, registry.register(runner(runnerId, instanceId, true)).decision());
		assertEquals(RunnerLiveness.HEALTHY, registry.livenessOf(runnerId));
		registerAvailable("runner-z", "instance-z");
		var job = queuedJob();

		assertEquals(new RunnerAssignment("runner-z", "instance-z"), selectedRunner(selector.select(job)));
		assertEquals(new RunnerAssignment("runner-z", "instance-z"), selectedRunner(selector.select(job)));
		assertEquals(JobStatus.QUEUED, job.status());
		assertTrue(job.runnerAssignment().isEmpty());
	}

	@ParameterizedTest
	@MethodSource("malformedIdentities")
	void blocksWhenOnlyAMalformedIdentityIsRegistered(String runnerId, String instanceId) {
		assertEquals(RegistrationDecision.ACCEPTED, registry.register(runner(runnerId, instanceId, true)).decision());
		var job = queuedJob();

		assertBlocked(selector.select(job));
		assertEquals(JobStatus.QUEUED, job.status());
		assertTrue(job.runnerAssignment().isEmpty());
	}

	private static Stream<Arguments> malformedIdentities() {
		return Stream.of(
				Arguments.of("", "instance-a"),
				Arguments.of(" \t ", "instance-a"),
				Arguments.of("runner-a", ""),
				Arguments.of("runner-a", " \t "));
	}

	@Test
	void skipsAHealthyDockerLessRunnerEvenWhenItSortsFirst() {
		// A Runner without a Docker daemon keeps sending heartbeats but rejects every dispatch.
		registry.register(runner("runner-a", "instance-a", false));
		registry.heartbeat("runner-a", "instance-a");
		registerAvailable("runner-b", "instance-b");

		assertEquals(new RunnerAssignment("runner-b", "instance-b"), selectedRunner(selector.select(queuedJob())));
	}

	@Test
	void blocksWhenOnlyDockerLessRunnersAreHealthy() {
		registry.register(runner("runner-a", "instance-a", false));
		registry.register(runner("runner-b", "instance-b", false));

		assertBlocked(selector.select(queuedJob()));
	}

	@Test
	void selectsTheSmallestIdentityIndependentlyOfRegistrationOrder() {
		registerAvailable("runner-c", "instance-c");
		registerAvailable("runner-a", "instance-a");
		registerAvailable("runner-b", "instance-b");

		assertEquals(new RunnerAssignment("runner-a", "instance-a"), selectedRunner(selector.select(queuedJob())));
	}

	@Test
	void selectsARunnerLastSeenExactlyTheHeartbeatTimeoutAgo() {
		registerAvailable("runner-1", "instance-1");

		clock.advance(HEARTBEAT_TIMEOUT);

		assertEquals(new RunnerAssignment("runner-1", "instance-1"), selectedRunner(selector.select(queuedJob())));
	}

	@Test
	void doesNotSelectARunnerLastSeenLongerAgoThanTheHeartbeatTimeout() {
		registerAvailable("runner-1", "instance-1");

		clock.advance(HEARTBEAT_TIMEOUT.plusNanos(1));

		assertBlocked(selector.select(queuedJob()));
	}

	@Test
	void selectsTheFreshRunnerWhenAStaleOneSortsFirst() {
		registerAvailable("runner-a", "instance-a");
		registerAvailable("runner-b", "instance-b");

		clock.advance(HEARTBEAT_TIMEOUT);
		registry.heartbeat("runner-b", "instance-b");
		clock.advance(Duration.ofSeconds(1));

		assertEquals(new RunnerAssignment("runner-b", "instance-b"), selectedRunner(selector.select(queuedJob())));
	}

	@Test
	void selectsARunnerAgainOnceItsHeartbeatResumes() {
		registerAvailable("runner-1", "instance-1");
		clock.advance(HEARTBEAT_TIMEOUT.plusSeconds(1));
		assertBlocked(selector.select(queuedJob()));

		registry.heartbeat("runner-1", "instance-1");

		assertEquals(new RunnerAssignment("runner-1", "instance-1"), selectedRunner(selector.select(queuedJob())));
	}

	@Test
	void recoversAfterALongPartitionEvenIfAnotherRunnerRegistersMeanwhile() {
		registerAvailable("runner-a", "instance-a");
		clock.advance(Duration.ofMinutes(11));
		registerAvailable("runner-b", "instance-b");
		assertEquals(new RunnerAssignment("runner-b", "instance-b"), selectedRunner(selector.select(queuedJob())));

		assertEquals(HeartbeatDecision.ACKNOWLEDGED, registry.heartbeat("runner-a", "instance-a").decision());

		assertEquals(new RunnerAssignment("runner-a", "instance-a"), selectedRunner(selector.select(queuedJob())));
	}

	@Test
	void rejectsAConflictingInstanceAndDoesNotLetItsHeartbeatRefreshTheSelectedRunner() {
		registerAvailable("runner-a", "instance-a");
		assertEquals(RegistrationDecision.IDENTITY_CONFLICT,
				registry.register(runner("runner-a", "other-instance", true)).decision());
		clock.advance(HEARTBEAT_TIMEOUT.plusNanos(1));

		assertEquals(HeartbeatDecision.UNKNOWN_RUNNER, registry.heartbeat("runner-a", "other-instance").decision());

		assertBlocked(selector.select(queuedJob()));
	}

	private void registerAvailable(String runnerId, String instanceId) {
		registry.register(runner(runnerId, instanceId, true));
	}

	private static RunnerAssignment selectedRunner(RunnerSelectionOutcome outcome) {
		return assertInstanceOf(RunnerSelectionOutcome.Selected.class, outcome).dispatch().runner();
	}

	private static void assertBlocked(RunnerSelectionOutcome outcome) {
		assertInstanceOf(RunnerSelectionOutcome.NoAvailableRunner.class, outcome);
	}

	private static Job queuedJob() {
		var job = Job.create(
				new RepositoryReference(URI.create("https://git.example/acme/service"), "abc123"),
				new ExecutionDefinition(List.of("go", "test", "./..."), "/workspace/service"),
				STARTED_AT);
		job.queue(STARTED_AT);
		return job;
	}

	/** Registration carries the Docker probe result before readiness is known. */
	private static RunnerInfo runner(String runnerId, String instanceId, boolean dockerAvailable) {
		return new RunnerInfo(
				runnerId,
				instanceId,
				"0.1.0",
				"runner.v1",
				RunnerState.UNAVAILABLE,
				false,
				new RunnerCapabilities("linux", "amd64", dockerAvailable, List.of(), Map.of()));
	}
}

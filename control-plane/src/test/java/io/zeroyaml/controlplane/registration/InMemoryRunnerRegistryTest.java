package io.zeroyaml.controlplane.registration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.zeroyaml.controlplane.runner.RunnerCapabilities;
import io.zeroyaml.controlplane.runner.RunnerInfo;
import io.zeroyaml.controlplane.runner.RunnerState;

class InMemoryRunnerRegistryTest {

	private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);

	@Test
	void acceptsTheFirstRegistrationForARunnerId() {
		var registry = newRegistry(Instant.EPOCH);

		var outcome = registry.register(runner("runner-1", "instance-1"));

		assertEquals(RegistrationDecision.ACCEPTED, outcome.decision());
		assertFalse(outcome.registrationId().isBlank());
	}

	@Test
	void treatsTheSameInstanceRegisteringAgainAsAlreadyRegistered() {
		var registry = newRegistry(Instant.EPOCH);

		var first = registry.register(runner("runner-1", "instance-1"));
		var second = registry.register(runner("runner-1", "instance-1"));

		assertEquals(RegistrationDecision.ALREADY_REGISTERED, second.decision());
		assertEquals(first.registrationId(), second.registrationId());
	}

	@Test
	void treatsADifferentInstanceForTheSameRunnerIdAsAnIdentityConflict() {
		var registry = newRegistry(Instant.EPOCH);

		registry.register(runner("runner-1", "instance-1"));
		var conflict = registry.register(runner("runner-1", "instance-2"));

		assertEquals(RegistrationDecision.IDENTITY_CONFLICT, conflict.decision());
		assertTrue(conflict.registrationId().isEmpty());
	}

	@Test
	void registersDifferentRunnerIdsIndependently() {
		var registry = newRegistry(Instant.EPOCH);

		var first = registry.register(runner("runner-1", "instance-1"));
		var second = registry.register(runner("runner-2", "instance-1"));

		assertEquals(RegistrationDecision.ACCEPTED, first.decision());
		assertEquals(RegistrationDecision.ACCEPTED, second.decision());
		assertNotEquals(first.registrationId(), second.registrationId());
	}

	@Test
	void registersConcurrentAttemptsForTheSameRunnerIdExactlyOnce() throws InterruptedException {
		var registry = newRegistry(Instant.EPOCH);
		var attempts = 16;
		var readyLatch = new CountDownLatch(attempts);
		var startLatch = new CountDownLatch(1);
		var accepted = new AtomicInteger();
		ExecutorService pool = Executors.newFixedThreadPool(attempts);

		try {
			for (int i = 0; i < attempts; i++) {
				pool.submit(() -> {
					readyLatch.countDown();
					try {
						startLatch.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					}

					if (registry.register(runner("runner-1", "instance-1")).decision() == RegistrationDecision.ACCEPTED) {
						accepted.incrementAndGet();
					}
				});
			}

			readyLatch.await();
			startLatch.countDown();
			pool.shutdown();
			assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
		} finally {
			pool.shutdownNow();
		}

		assertEquals(1, accepted.get());
	}

	@Test
	void reportsUnknownLivenessForARunnerIdThatNeverRegistered() {
		var registry = newRegistry(Instant.EPOCH);

		assertEquals(RunnerLiveness.UNKNOWN, registry.livenessOf("runner-1"));
	}

	@Test
	void reportsHealthyLivenessRightAfterRegistration() {
		var registry = newRegistry(Instant.EPOCH);

		registry.register(runner("runner-1", "instance-1"));

		assertEquals(RunnerLiveness.HEALTHY, registry.livenessOf("runner-1"));
	}

	@Test
	void acknowledgesAHeartbeatFromTheRegisteredInstance() {
		var registry = newRegistry(Instant.EPOCH);
		registry.register(runner("runner-1", "instance-1"));

		var outcome = registry.heartbeat("runner-1", "instance-1");

		assertEquals(HeartbeatDecision.ACKNOWLEDGED, outcome.decision());
	}

	@Test
	void rejectsAHeartbeatForARunnerIdThatNeverRegistered() {
		var registry = newRegistry(Instant.EPOCH);

		var outcome = registry.heartbeat("runner-1", "instance-1");

		assertEquals(HeartbeatDecision.UNKNOWN_RUNNER, outcome.decision());
	}

	@Test
	void rejectsAHeartbeatFromADifferentInstanceThanTheRegisteredOne() {
		var registry = newRegistry(Instant.EPOCH);
		registry.register(runner("runner-1", "instance-1"));

		var outcome = registry.heartbeat("runner-1", "instance-2");

		assertEquals(HeartbeatDecision.UNKNOWN_RUNNER, outcome.decision());
		assertEquals(RunnerLiveness.HEALTHY, registry.livenessOf("runner-1"));
	}

	@Test
	void becomesUnavailableAfterTheHeartbeatTimeoutElapsesWithNoHeartbeat() {
		var clock = new MutableClock(Instant.EPOCH);
		var registry = newRegistry(clock);
		registry.register(runner("runner-1", "instance-1"));

		clock.advance(HEARTBEAT_TIMEOUT.plusSeconds(1));

		assertEquals(RunnerLiveness.UNAVAILABLE, registry.livenessOf("runner-1"));
	}

	@Test
	void staysHealthyWhileAHeartbeatArrivesBeforeEachTimeoutWindowElapses() {
		var clock = new MutableClock(Instant.EPOCH);
		var registry = newRegistry(clock);
		registry.register(runner("runner-1", "instance-1"));

		clock.advance(HEARTBEAT_TIMEOUT.minusSeconds(1));
		assertEquals(HeartbeatDecision.ACKNOWLEDGED, registry.heartbeat("runner-1", "instance-1").decision());
		assertEquals(RunnerLiveness.HEALTHY, registry.livenessOf("runner-1"));

		clock.advance(HEARTBEAT_TIMEOUT.minusSeconds(1));
		assertEquals(RunnerLiveness.HEALTHY, registry.livenessOf("runner-1"));
	}

	@Test
	void recoversToHealthyAfterAMissedWindowWithoutADuplicateRegistryEntry() {
		var clock = new MutableClock(Instant.EPOCH);
		var registry = newRegistry(clock);
		var registered = registry.register(runner("runner-1", "instance-1"));

		clock.advance(HEARTBEAT_TIMEOUT.plusSeconds(1));
		assertEquals(RunnerLiveness.UNAVAILABLE, registry.livenessOf("runner-1"));

		var heartbeat = registry.heartbeat("runner-1", "instance-1");
		assertEquals(HeartbeatDecision.ACKNOWLEDGED, heartbeat.decision());
		assertEquals(RunnerLiveness.HEALTHY, registry.livenessOf("runner-1"));

		// Recovery must not create a second registration for the same runnerId.
		var reRegistered = registry.register(runner("runner-1", "instance-1"));
		assertEquals(RegistrationDecision.ALREADY_REGISTERED, reRegistered.decision());
		assertEquals(registered.registrationId(), reRegistered.registrationId());
	}

	@Test
	void listsNoAvailableRunnersWhenNothingIsRegistered() {
		assertTrue(newRegistry(Instant.EPOCH).availableRunners().isEmpty());
	}

	@Test
	void listsOnlyHealthyDockerCapableRunnersDespitePreRegistrationReadinessFlags() {
		var clock = new MutableClock(Instant.EPOCH);
		var registry = newRegistry(clock);
		registry.register(dockerRunner("stale"));
		clock.advance(HEARTBEAT_TIMEOUT.plusNanos(1));
		registry.register(runner("docker-less", "instance-1"));
		registry.register(dockerRunner("available"));

		var available = registry.availableRunners();

		assertEquals(List.of("available"), available.stream().map(entry -> entry.runner().runnerId()).toList());
		assertEquals(RunnerState.UNAVAILABLE, available.getFirst().runner().state());
		assertFalse(available.getFirst().runner().acceptingWork());
	}

	@Test
	void includesTheTimeoutBoundaryAndExcludesTheNextInstant() {
		var clock = new MutableClock(Instant.EPOCH);
		var registry = newRegistry(clock);
		registry.register(dockerRunner("runner-1"));
		clock.advance(HEARTBEAT_TIMEOUT);

		assertEquals(1, registry.availableRunners().size());

		clock.advance(Duration.ofNanos(1));
		assertTrue(registry.availableRunners().isEmpty());
	}

	@Test
	void returnsAnImmutablePointInTimeSnapshot() {
		var clock = new MutableClock(Instant.EPOCH);
		var registry = newRegistry(clock);
		registry.register(dockerRunner("runner-1"));
		var snapshot = registry.availableRunners();

		assertThrows(UnsupportedOperationException.class, snapshot::clear);
		clock.advance(HEARTBEAT_TIMEOUT.plusNanos(1));

		assertEquals(1, snapshot.size());
		assertTrue(registry.availableRunners().isEmpty());
	}

	private static RunnerInfo dockerRunner(String runnerId) {
		return new RunnerInfo(runnerId, "instance-1", "0.1.0", "runner.v1", RunnerState.UNAVAILABLE,
				false, new RunnerCapabilities("linux", "amd64", true, List.of(), Map.of()));
	}

	private static InMemoryRunnerRegistry newRegistry(Instant instant) {
		return newRegistry(Clock.fixed(instant, ZoneOffset.UTC));
	}

	private static InMemoryRunnerRegistry newRegistry(Clock clock) {
		var properties = new RunnerRegistrationServerProperties();
		properties.setHeartbeatTimeout(HEARTBEAT_TIMEOUT);
		return new InMemoryRunnerRegistry(clock, properties);
	}

	private static RunnerInfo runner(String runnerId, String instanceId) {
		return new RunnerInfo(
				runnerId,
				instanceId,
				"0.1.0",
				"runner.v1",
				RunnerState.STARTING,
				false,
				new RunnerCapabilities("windows", "amd64", false, List.of(), Map.of())
		);
	}
}

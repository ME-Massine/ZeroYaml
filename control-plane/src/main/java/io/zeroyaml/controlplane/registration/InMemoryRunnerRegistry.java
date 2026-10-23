package io.zeroyaml.controlplane.registration;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

import io.zeroyaml.controlplane.runner.RunnerInfo;

/**
 * Process-local Runner inventory for the Phase 1 proof. Entries are keyed by
 * {@code runnerId}, the logical identity a Runner reports across restarts.
 *
 * <p>A registration request for a {@code runnerId} that already holds a
 * registered Runner is reconciled deterministically: the same
 * {@code instanceId} re-registering (for example, after a retried request) is
 * idempotent, and a different {@code instanceId} is an identity conflict.
 * Replacing a registered instance requires a lease or heartbeat policy, which
 * is out of scope for this first contract.
 *
 * <p>Liveness is a simple last-seen policy derived from {@code lastSeenAt}:
 * registration and every acknowledged heartbeat advance it, and
 * {@link #livenessOf(String)} compares the elapsed time against the
 * configured heartbeat timeout. There is no background sweep; a Runner that
 * stops sending heartbeats is only reported {@link RunnerLiveness#UNAVAILABLE}
 * when something asks, which keeps this component free of scheduling policy.
 */
@Component
class InMemoryRunnerRegistry implements RunnerRegistry {

	private final ConcurrentHashMap<String, RegisteredRunner> runnersByRunnerId = new ConcurrentHashMap<>();
	private final Clock clock;
	private final Duration heartbeatTimeout;

	InMemoryRunnerRegistry(Clock clock, RunnerRegistrationServerProperties properties) {
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
		this.heartbeatTimeout = Objects.requireNonNull(properties, "properties must not be null").getHeartbeatTimeout();
	}

	@Override
	public RegistrationOutcome register(RunnerInfo runner) {
		Objects.requireNonNull(runner, "runner must not be null");

		var outcome = new AtomicReference<RegistrationOutcome>();
		var now = clock.instant();
		runnersByRunnerId.compute(runner.runnerId(), (runnerId, existing) -> {
			if (existing == null) {
				var registrationId = UUID.randomUUID().toString();
				outcome.set(new RegistrationOutcome(
						RegistrationDecision.ACCEPTED,
						registrationId,
						"Runner " + runnerId + " registered"
				));
				return new RegisteredRunner(registrationId, runner, now, now);
			}

			if (existing.runner().instanceId().equals(runner.instanceId())) {
				outcome.set(new RegistrationOutcome(
						RegistrationDecision.ALREADY_REGISTERED,
						existing.registrationId(),
						"Runner " + runnerId + " instance " + runner.instanceId() + " is already registered"
				));
				// A repeated registration is itself a liveness signal, so it counts as a heartbeat too.
				return existing.seenAt(now);
			}

			outcome.set(new RegistrationOutcome(
					RegistrationDecision.IDENTITY_CONFLICT,
					"",
					"Runner " + runnerId + " is already registered under instance " + existing.runner().instanceId()
			));
			return existing;
		});

		return outcome.get();
	}

	@Override
	public HeartbeatOutcome heartbeat(String runnerId, String instanceId) {
		Objects.requireNonNull(runnerId, "runnerId must not be null");
		Objects.requireNonNull(instanceId, "instanceId must not be null");

		var outcome = new AtomicReference<HeartbeatOutcome>();
		var now = clock.instant();
		runnersByRunnerId.computeIfPresent(runnerId, (id, existing) -> {
			if (!existing.runner().instanceId().equals(instanceId)) {
				outcome.set(new HeartbeatOutcome(
						HeartbeatDecision.UNKNOWN_RUNNER,
						"Runner " + id + " is registered under a different instance"
				));
				return existing;
			}

			outcome.set(new HeartbeatOutcome(HeartbeatDecision.ACKNOWLEDGED, "Runner " + id + " heartbeat acknowledged"));
			return existing.seenAt(now);
		});

		if (outcome.get() == null) {
			outcome.set(new HeartbeatOutcome(
					HeartbeatDecision.UNKNOWN_RUNNER,
					"Runner " + runnerId + " is not registered"
			));
		}

		return outcome.get();
	}

	@Override
	public RunnerLiveness livenessOf(String runnerId) {
		Objects.requireNonNull(runnerId, "runnerId must not be null");

		var registered = runnersByRunnerId.get(runnerId);
		if (registered == null) {
			return RunnerLiveness.UNKNOWN;
		}

		return livenessAt(registered, clock.instant());
	}

	@Override
	public List<RegisteredRunner> availableRunners() {
		// Judge the whole snapshot at one instant, including entries near the timeout.
		var now = clock.instant();
		return runnersByRunnerId.values().stream()
				.filter(registered -> livenessAt(registered, now) == RunnerLiveness.HEALTHY)
				.filter(registered -> registered.runner().capabilities().dockerAvailable())
				.toList();
	}

	private RunnerLiveness livenessAt(RegisteredRunner registered, Instant now) {
		var elapsed = Duration.between(registered.lastSeenAt(), now);
		return elapsed.compareTo(heartbeatTimeout) > 0 ? RunnerLiveness.UNAVAILABLE : RunnerLiveness.HEALTHY;
	}
}

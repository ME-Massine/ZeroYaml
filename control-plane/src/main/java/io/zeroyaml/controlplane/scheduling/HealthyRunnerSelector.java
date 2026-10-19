package io.zeroyaml.controlplane.scheduling;

import java.util.Comparator;
import java.util.Objects;

import org.springframework.stereotype.Component;

import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobStatus;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.registration.RegisteredRunner;
import io.zeroyaml.controlplane.registration.RunnerRegistry;

/**
 * First scheduling strategy: pick the available registered Runner with the
 * lexicographically smallest {@code runnerId}.
 *
 * <p>Eligibility is the registry's decision. A Runner is eligible exactly when
 * {@link RunnerRegistry#availableRunners()} lists it: its heartbeat is recent,
 * and that heartbeat reported the Runner ready and accepting work. A Runner
 * that is alive but cannot execute, for example one without a Docker daemon
 * or one that reports it is draining, is therefore never selected and cannot
 * keep Jobs away from a Runner that can run them.
 *
 * <p>{@code runnerId} is the registry key and therefore unique, so the
 * ordering is total and the same registry state always yields the same
 * Runner, regardless of registration order or map iteration order. The
 * strategy is intentionally stateless: it does not rotate, track load, or
 * reserve the Runner it returns. Concurrent callers can be handed the same
 * Runner; capacity and leasing belong to later scheduling work.
 */
@Component
class HealthyRunnerSelector implements RunnerSelector {

	private static final Comparator<RegisteredRunner> BY_RUNNER_ID =
			Comparator.comparing(registered -> registered.runner().runnerId());

	private final RunnerRegistry registry;

	HealthyRunnerSelector(RunnerRegistry registry) {
		this.registry = Objects.requireNonNull(registry, "registry must not be null");
	}

	@Override
	public RunnerSelectionOutcome select(Job job) {
		Objects.requireNonNull(job, "job must not be null");
		if (job.status() != JobStatus.QUEUED) {
			throw new IllegalStateException(
					"Job " + job.id() + " must be QUEUED to select a Runner but is " + job.status());
		}

		return registry.availableRunners().stream()
				.min(BY_RUNNER_ID)
				.map(registered -> toSelectedOutcome(job, registered))
				.orElseGet(() -> new RunnerSelectionOutcome.NoAvailableRunner(
						job.id(), "No Runner is healthy and accepting work; Job " + job.id() + " stays queued"));
	}

	private static RunnerSelectionOutcome toSelectedOutcome(Job job, RegisteredRunner registered) {
		var runner = registered.runner();
		var context = new JobDispatchContext(job.id(), new RunnerAssignment(runner.runnerId(), runner.instanceId()));
		return new RunnerSelectionOutcome.Selected(
				context, "Runner " + runner.runnerId() + " selected for Job " + job.id());
	}
}

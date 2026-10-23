package io.zeroyaml.controlplane.runner;

import java.util.Objects;

/**
 * Protocol-neutral identity and current lifecycle facts reported by a Runner.
 */
public record RunnerInfo(
		String runnerId,
		String instanceId,
		String runnerVersion,
		String protocolVersion,
		RunnerState state,
		boolean acceptingWork,
		RunnerCapabilities capabilities
) {

	public RunnerInfo {
		Objects.requireNonNull(runnerId, "runnerId must not be null");
		Objects.requireNonNull(instanceId, "instanceId must not be null");
		Objects.requireNonNull(runnerVersion, "runnerVersion must not be null");
		Objects.requireNonNull(protocolVersion, "protocolVersion must not be null");
		Objects.requireNonNull(state, "state must not be null");
		Objects.requireNonNull(capabilities, "capabilities must not be null");
	}
}

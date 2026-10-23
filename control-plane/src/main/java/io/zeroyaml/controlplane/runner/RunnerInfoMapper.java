package io.zeroyaml.controlplane.runner;

/**
 * Maps the generated {@code RunnerInfo} protocol message to the protocol-neutral
 * {@link RunnerInfo} domain type. Both the outbound Runner {@code GetInfo} client
 * and the inbound Runner registration service report this same identity shape.
 */
public final class RunnerInfoMapper {

	private RunnerInfoMapper() {
	}

	public static RunnerInfo toDomain(io.zeroyaml.contracts.runner.v1.RunnerInfo proto) {
		var capabilities = proto.getCapabilities();
		return new RunnerInfo(
				proto.getRunnerId(),
				proto.getInstanceId(),
				proto.getRunnerVersion(),
				proto.getProtocolVersion(),
				toRunnerState(proto.getStatus()),
				proto.getAcceptingWork(),
				new RunnerCapabilities(
						capabilities.getOperatingSystem(),
						capabilities.getArchitecture(),
						capabilities.getDockerAvailable(),
						capabilities.getSupportedExecutorsList(),
						capabilities.getLabelsMap()
				)
		);
	}

	private static RunnerState toRunnerState(io.zeroyaml.contracts.runner.v1.RunnerStatus status) {
		return switch (status) {
			case RUNNER_STATUS_STARTING -> RunnerState.STARTING;
			case RUNNER_STATUS_READY -> RunnerState.READY;
			case RUNNER_STATUS_DRAINING -> RunnerState.DRAINING;
			case RUNNER_STATUS_UNAVAILABLE -> RunnerState.UNAVAILABLE;
			case RUNNER_STATUS_UNSPECIFIED, UNRECOGNIZED ->
					throw new IllegalArgumentException("Runner reported an unspecified status");
		};
	}
}

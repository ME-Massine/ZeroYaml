package io.zeroyaml.controlplane.registration;

import java.util.Objects;

import org.springframework.stereotype.Component;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.zeroyaml.contracts.runner.v1.HeartbeatRequest;
import io.zeroyaml.contracts.runner.v1.HeartbeatResponse;
import io.zeroyaml.contracts.runner.v1.HeartbeatResult;
import io.zeroyaml.contracts.runner.v1.RegisterRunnerRequest;
import io.zeroyaml.contracts.runner.v1.RegisterRunnerResponse;
import io.zeroyaml.contracts.runner.v1.RegistrationResult;
import io.zeroyaml.contracts.runner.v1.RunnerRegistrationServiceGrpc;
import io.zeroyaml.controlplane.runner.RunnerInfoMapper;

/**
 * gRPC endpoint the Runner calls to register with the Control Plane and to
 * report liveness afterward. Translation between generated protocol types and
 * the {@link RunnerRegistry} boundary happens here so the registry stays
 * protocol-neutral.
 */
@Component
class GrpcRunnerRegistrationService extends RunnerRegistrationServiceGrpc.RunnerRegistrationServiceImplBase {

	private final RunnerRegistry registry;

	GrpcRunnerRegistrationService(RunnerRegistry registry) {
		this.registry = Objects.requireNonNull(registry, "registry must not be null");
	}

	@Override
	public void register(RegisterRunnerRequest request, StreamObserver<RegisterRunnerResponse> responseObserver) {
		try {
			var runner = RunnerInfoMapper.toDomain(request.getRunner());
			var outcome = registry.register(runner);
			responseObserver.onNext(toResponse(outcome));
			responseObserver.onCompleted();
		} catch (IllegalArgumentException exception) {
			// A malformed identity, such as an unspecified status, is a client error;
			// surface it explicitly instead of registering a Runner with unusable state.
			responseObserver.onError(Status.INVALID_ARGUMENT
					.withDescription(exception.getMessage())
					.withCause(exception)
					.asRuntimeException());
		}
	}

	@Override
	public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
		var outcome = registry.heartbeat(request.getRunnerId(), request.getInstanceId());
		responseObserver.onNext(toResponse(outcome));
		responseObserver.onCompleted();
	}

	private static RegisterRunnerResponse toResponse(RegistrationOutcome outcome) {
		return RegisterRunnerResponse.newBuilder()
				.setResult(toProtoResult(outcome.decision()))
				.setRegistrationId(outcome.registrationId())
				.setMessage(outcome.message())
				.build();
	}

	private static HeartbeatResponse toResponse(HeartbeatOutcome outcome) {
		return HeartbeatResponse.newBuilder()
				.setResult(toProtoResult(outcome.decision()))
				.setMessage(outcome.message())
				.build();
	}

	private static RegistrationResult toProtoResult(RegistrationDecision decision) {
		return switch (decision) {
			case ACCEPTED -> RegistrationResult.REGISTRATION_ACCEPTED;
			case ALREADY_REGISTERED -> RegistrationResult.REGISTRATION_ALREADY_REGISTERED;
			case IDENTITY_CONFLICT -> RegistrationResult.REGISTRATION_IDENTITY_CONFLICT;
		};
	}

	private static HeartbeatResult toProtoResult(HeartbeatDecision decision) {
		return switch (decision) {
			case ACKNOWLEDGED -> HeartbeatResult.HEARTBEAT_ACKNOWLEDGED;
			case UNKNOWN_RUNNER -> HeartbeatResult.HEARTBEAT_UNKNOWN_RUNNER;
		};
	}
}

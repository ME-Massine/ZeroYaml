package io.zeroyaml.controlplane.execution;

import java.time.Instant;
import java.util.UUID;

import com.google.protobuf.Timestamp;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;
import io.zeroyaml.controlplane.runner.RunnerProtocolVersion;

/**
 * Boundary checks shared by every Runner-facing execution RPC, so a fix to
 * protocol, identity, or timestamp validation cannot land in only one service. Failures
 * are {@link IllegalArgumentException}s that callers map to
 * {@code INVALID_ARGUMENT}.
 */
final class RunnerRequestValidation {
	/** Protobuf Timestamp range: 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z. */
	private static final long MIN_TIMESTAMP_SECONDS = -62135596800L;
	private static final long MAX_TIMESTAMP_SECONDS = 253402300799L;
	private static final int MAX_TIMESTAMP_NANOS = 999_999_999;

	private RunnerRequestValidation() {
	}

	static void requireSupportedProtocolVersion(String protocolVersion) {
		if (protocolVersion == null || protocolVersion.isBlank()) {
			throw new IllegalArgumentException("protocol_version must not be empty");
		}
		if (!RunnerProtocolVersion.CURRENT.equals(protocolVersion)) {
			// The remaining fields cannot be interpreted safely under a contract
			// this Control Plane does not implement.
			throw new IllegalArgumentException(
					"this control plane implements protocol version " + RunnerProtocolVersion.CURRENT);
		}
	}

	static JobId toJobId(String jobId) {
		if (jobId == null || jobId.isBlank()) {
			throw new IllegalArgumentException("job_id must not be empty");
		}
		try {
			return new JobId(UUID.fromString(jobId));
		} catch (IllegalArgumentException exception) {
			// A Runner echoes the identity it was dispatched, so a malformed value
			// is a caller defect rather than an unknown Job.
			throw new IllegalArgumentException("job_id must be the dispatched job identity");
		}
	}

	static RunnerAssignment toRunnerAssignment(String runnerId, String instanceId) {
		try {
			return new RunnerAssignment(runnerId, instanceId);
		} catch (IllegalArgumentException exception) {
			throw new IllegalArgumentException("runner_id and instance_id must identify the reporting process");
		}
	}

	/**
	 * Converts a required protobuf timestamp. Values outside the protobuf range
	 * are refused instead of reaching {@link Instant}, whose wider range would
	 * otherwise accept them or fail with an unmapped exception.
	 */
	static Instant toRequiredInstant(boolean present, Timestamp timestamp, String fieldName) {
		if (!present) {
			throw new IllegalArgumentException(fieldName + " must be set");
		}
		if (timestamp.getSeconds() < MIN_TIMESTAMP_SECONDS || timestamp.getSeconds() > MAX_TIMESTAMP_SECONDS
				|| timestamp.getNanos() < 0 || timestamp.getNanos() > MAX_TIMESTAMP_NANOS) {
			throw new IllegalArgumentException(fieldName + " must be a valid protobuf timestamp");
		}

		return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
	}
}

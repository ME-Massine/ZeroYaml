package io.zeroyaml.controlplane.execution;

import java.time.Instant;

import com.google.protobuf.Timestamp;
import io.zeroyaml.controlplane.runner.RunnerProtocolVersion;

/**
 * Boundary checks shared by every Runner-facing execution RPC, so a fix to
 * protocol or timestamp validation cannot land in only one service. Failures
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

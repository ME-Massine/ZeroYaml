package io.zeroyaml.controlplane.execution;

import java.time.Instant;
import java.util.Objects;

import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;

/**
 * Immutable observation of command bytes or a final output marker. Bytes may
 * contain sensitive user output and must never enter operational logging.
 * Sequence orders observations across both pipes; timestamps are Runner time.
 * Records are not value-comparable: generated equality compares byte array identities.
 */
record JobLogRecord(JobId jobId, RunnerAssignment reportedBy, long sequence,
		Instant observedAt, Stream stream, byte[] content, boolean endOfOutput,
		boolean truncated, boolean incomplete) {

	public static final int MAX_CHUNK_BYTES = 4096;
	public static final int MAX_OUTPUT_BYTES = 1 << 20;
	public static final int MAX_CHUNKS = 256;

	/** Stream.NONE is reserved for the empty final marker. */
	public enum Stream { NONE, STDOUT, STDERR }

	public JobLogRecord {
		Objects.requireNonNull(jobId, "jobId must be set");
		Objects.requireNonNull(reportedBy, "reportedBy must be set");
		if (reportedBy.runnerId().length() > 256 || reportedBy.instanceId().length() > 256) {
			throw new IllegalArgumentException("Runner identity fields must be at most 256 characters");
		}
		Objects.requireNonNull(observedAt, "observedAt must be set");
		Objects.requireNonNull(stream, "stream must be set");
		Objects.requireNonNull(content, "content must be set");
		if (sequence < 1 || sequence > MAX_CHUNKS + 1) {
			throw new IllegalArgumentException("sequence must be between 1 and 257");
		}
		if (endOfOutput) {
			if (stream != Stream.NONE || content.length != 0) {
				throw new IllegalArgumentException("final marker must be empty and have no stream");
			}
		} else if (stream == Stream.NONE || content.length == 0 || content.length > MAX_CHUNK_BYTES
				|| sequence > MAX_CHUNKS || truncated || incomplete) {
			throw new IllegalArgumentException("data chunk must name a stream, contain 1-4096 bytes, and have no final flags");
		}
		content = content.clone();
	}

	@Override
	public byte[] content() { return content.clone(); }

	int contentSize() { return content.length; }

	@Override
	public String toString() {
		return "JobLogRecord[jobId=" + jobId + ", sequence=" + sequence + ", endOfOutput=" + endOfOutput + "]";
	}
}

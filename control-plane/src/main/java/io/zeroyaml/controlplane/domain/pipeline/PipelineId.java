package io.zeroyaml.controlplane.domain.pipeline;

import java.util.Objects;
import java.util.UUID;

/**
 * Stable identifier for one pipeline.
 */
public record PipelineId(UUID value) {

	public PipelineId {
		Objects.requireNonNull(value, "value must not be null");
	}

	/**
	 * Creates a new identifier for a pipeline.
	 *
	 * @return a randomly generated pipeline identifier
	 */
	public static PipelineId newId() {
		return new PipelineId(UUID.randomUUID());
	}
}

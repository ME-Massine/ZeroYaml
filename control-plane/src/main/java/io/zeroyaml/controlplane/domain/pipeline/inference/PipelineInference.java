package io.zeroyaml.controlplane.domain.pipeline.inference;

import java.util.List;
import java.util.Objects;

import io.zeroyaml.controlplane.domain.pipeline.Pipeline;

/**
 * Result of inferring a pipeline. Inference never guesses: a repository either
 * yields a pipeline or an explicit reason why it did not.
 */
public sealed interface PipelineInference {

	/**
	 * A pipeline was inferred.
	 *
	 * @param pipeline the inferred plan
	 */
	record Inferred(Pipeline pipeline) implements PipelineInference {
		public Inferred {
			Objects.requireNonNull(pipeline, "pipeline must not be null");
		}
	}

	/**
	 * No supported project shape was recognized.
	 *
	 * @param reason operator-facing explanation; contains no repository content
	 */
	record Unsupported(String reason) implements PipelineInference {
		public Unsupported {
			if (reason == null || reason.isBlank()) {
				throw new IllegalArgumentException("reason must not be blank");
			}
		}
	}

	/**
	 * More than one project shape matched, so choosing one would be a guess.
	 *
	 * @param candidates names of the matching build files, sorted; at least two
	 */
	record Ambiguous(List<String> candidates) implements PipelineInference {
		public Ambiguous {
			candidates = List.copyOf(candidates);
			if (candidates.size() < 2) {
				throw new IllegalArgumentException("candidates must contain at least two entries");
			}
		}
	}
}

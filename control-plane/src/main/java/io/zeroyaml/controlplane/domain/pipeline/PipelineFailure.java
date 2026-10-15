package io.zeroyaml.controlplane.domain.pipeline;

/**
 * Safe, structured reason a pipeline ended as {@link PipelineStatus#FAILED}.
 *
 * @param stepName name of the step that failed
 * @param code stable failure category
 * @param message human-readable diagnostic without credentials or secrets
 */
public record PipelineFailure(String stepName, String code, String message) {

	public PipelineFailure {
		stepName = requireNonBlank(stepName, "stepName");
		code = requireNonBlank(code, "code");
		message = requireNonBlank(message, "message");
	}

	private static String requireNonBlank(String value, String fieldName) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(fieldName + " must not be blank");
		}
		return value;
	}
}

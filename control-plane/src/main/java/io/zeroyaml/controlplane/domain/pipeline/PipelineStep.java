package io.zeroyaml.controlplane.domain.pipeline;

import java.util.Objects;
import java.util.regex.Pattern;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;

/**
 * One executable step of a {@link Pipeline}.
 *
 * <p>A step carries exactly what one Runner execution needs: the command and the
 * directory it runs in. It is intentionally not a general workflow step, so it
 * has no conditions, matrices, secrets, or dependencies on other steps. Because
 * the execution part is the Job model's {@link ExecutionDefinition}, a step maps
 * onto one Job without translation.</p>
 *
 * @param name identity of the step within its pipeline: 1 to 64 characters, lowercase letters,
 *        digits, {@code '.'}, {@code '_'} and {@code '-'}, starting and ending with a letter or digit;
 *        the value is never normalized, so two spellings are two different steps
 * @param execution concrete command and working directory
 */
public record PipelineStep(String name, ExecutionDefinition execution) {

	private static final int MAX_NAME_LENGTH = 64;
	private static final Pattern NAME_FORMAT = Pattern.compile("[a-z0-9]([a-z0-9._-]*[a-z0-9])?");

	public PipelineStep {
		if (name == null || name.length() > MAX_NAME_LENGTH || !NAME_FORMAT.matcher(name).matches()) {
			throw new IllegalArgumentException(
					"name must be 1 to " + MAX_NAME_LENGTH + " lowercase letters, digits, '.', '_' or '-', "
							+ "starting and ending with a letter or digit");
		}
		Objects.requireNonNull(execution, "execution must not be null");
	}
}

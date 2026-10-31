package io.zeroyaml.controlplane.execution;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Bearer token that authorizes reading retained Job logs. Reading is disabled
 * while the token is unset, because logs can contain secrets printed by user
 * commands. Supply it through {@code ZEROYAML_JOB_LOGS_READ_TOKEN} or an
 * approved secret store; it has no default and is never logged.
 */
@ConfigurationProperties(prefix = "zeroyaml.job-logs.read")
record JobLogReadProperties(String token) {

	/** Shortest accepted token; rules out short, guessable values. */
	static final int MINIMUM_TOKEN_LENGTH = 32;

	boolean enabled() { return token != null && !token.isBlank(); }

	@PostConstruct
	void validate() {
		if (enabled() && (token.length() < MINIMUM_TOKEN_LENGTH || !token.equals(token.strip()))) {
			// The configured value is never included, because startup failures are logged.
			throw new IllegalStateException("zeroyaml.job-logs.read.token must be at least " + MINIMUM_TOKEN_LENGTH
					+ " characters without surrounding whitespace; supply it through ZEROYAML_JOB_LOGS_READ_TOKEN");
		}
	}
}

package io.zeroyaml.controlplane.github.webhook;

/**
 * A verified push delivery whose fields cannot form a supported repository
 * event. The message identifies the invalid field without exposing payload data.
 */
public final class GitHubPushEventNormalizationException extends RuntimeException {

	GitHubPushEventNormalizationException(String reason) {
		super(reason);
	}

	GitHubPushEventNormalizationException(String reason, Throwable cause) {
		super(reason, cause);
	}
}

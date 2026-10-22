package io.zeroyaml.controlplane.github.webhook;

import java.util.Objects;

import org.springframework.http.HttpStatus;

/**
 * A delivery whose envelope or supported payload failed validation. It carries
 * the HTTP status that tells GitHub, and the operator reading the delivery log,
 * what was wrong.
 */
class GitHubWebhookRejectedException extends RuntimeException {

	private final HttpStatus status;
	private final String deliveryId;
	private final String event;

	/**
	 * @param status client error returned to GitHub
	 * @param deliveryId validated delivery identifier, or {@code null} when it was not yet validated
	 * @param event validated event name, or {@code null} when it was not yet validated
	 * @param reason operator-facing reason that never echoes request content
	 */
	GitHubWebhookRejectedException(HttpStatus status, String deliveryId, String event, String reason) {
		super(reason);
		this.status = Objects.requireNonNull(status, "status must not be null");
		this.deliveryId = deliveryId;
		this.event = event;
	}

	HttpStatus status() {
		return status;
	}

	String deliveryId() {
		return deliveryId;
	}

	String event() {
		return event;
	}
}

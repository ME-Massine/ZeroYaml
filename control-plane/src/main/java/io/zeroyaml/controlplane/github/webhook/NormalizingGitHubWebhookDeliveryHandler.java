package io.zeroyaml.controlplane.github.webhook;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import io.zeroyaml.controlplane.domain.event.RepositoryPushEvent;

/**
 * Normalizes accepted push deliveries and records the resulting event metadata.
 *
 * <p>It does not start pipeline work. A later application consumer can use the
 * provider-neutral event after inference and job creation are implemented.</p>
 */
@Component
class NormalizingGitHubWebhookDeliveryHandler implements GitHubWebhookDeliveryHandler {

	private static final Logger log = LoggerFactory.getLogger(NormalizingGitHubWebhookDeliveryHandler.class);

	private final GitHubPushEventNormalizer normalizer;

	NormalizingGitHubWebhookDeliveryHandler(GitHubPushEventNormalizer normalizer) {
		this.normalizer = Objects.requireNonNull(normalizer, "normalizer must not be null");
	}

	@Override
	public void handle(GitHubWebhookDelivery delivery) {
		final java.util.Optional<RepositoryPushEvent> event;
		try {
			event = normalizer.normalize(delivery);
		}
		catch (GitHubPushEventNormalizationException ex) {
			throw new GitHubWebhookRejectedException(
					HttpStatus.BAD_REQUEST, delivery.deliveryId(), delivery.event(), ex.getMessage());
		}

		if (event.isEmpty()) {
			log.info("GitHub push delivery {} has no executable branch commit; no repository event was emitted",
					delivery.deliveryId());
			return;
		}

		var normalized = event.orElseThrow();
		log.info(
				"Normalized GitHub push delivery {} for repository {} on branch {} at commit {}",
				normalized.deliveryId(), normalized.repository().fullName(), normalized.branch().value(),
				normalized.commitSha().value()
		);
	}
}

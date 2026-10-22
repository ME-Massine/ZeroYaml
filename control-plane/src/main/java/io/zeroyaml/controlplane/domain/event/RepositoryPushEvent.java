package io.zeroyaml.controlplane.domain.event;

import java.time.Instant;
import java.util.Objects;

import io.zeroyaml.controlplane.domain.repository.BranchName;
import io.zeroyaml.controlplane.domain.repository.CommitSha;
import io.zeroyaml.controlplane.domain.repository.RepositoryIdentity;

/**
 * A normalized push of a repository branch to a concrete commit.
 *
 * <p>The event time is the tip commit's timestamp from the source delivery.
 * This keeps a redelivery of the same GitHub delivery deterministic; it is not
 * the time at which the Control Plane received the HTTP request.</p>
 *
 * @param repository repository that received the push
 * @param branch branch name without the {@code refs/heads/} prefix
 * @param commitSha SHA of the tip commit after the push
 * @param deliveryId provider delivery identifier used for traceability and deduplication
 * @param eventTime timestamp of the tip commit in the source event
 */
public record RepositoryPushEvent(
		RepositoryIdentity repository,
		BranchName branch,
		CommitSha commitSha,
		String deliveryId,
		Instant eventTime
) {
	public RepositoryPushEvent {
		Objects.requireNonNull(repository, "repository must not be null");
		Objects.requireNonNull(branch, "branch must not be null");
		Objects.requireNonNull(commitSha, "commitSha must not be null");
		Objects.requireNonNull(eventTime, "eventTime must not be null");
		if (deliveryId == null || deliveryId.isBlank()) {
			throw new IllegalArgumentException("deliveryId must not be blank");
		}
	}
}

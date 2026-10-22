package io.zeroyaml.controlplane.github.webhook;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.zeroyaml.controlplane.domain.event.RepositoryPushEvent;
import io.zeroyaml.controlplane.domain.repository.BranchName;
import io.zeroyaml.controlplane.domain.repository.CommitSha;
import io.zeroyaml.controlplane.domain.repository.RepositoryIdentity;
import io.zeroyaml.controlplane.domain.repository.RepositoryProvider;

/**
 * Converts a verified GitHub push delivery into the Control Plane's repository
 * event model.
 *
 * <p>The conversion is pure: identical deliveries with the same delivery ID
 * produce equal events. Tag pushes and branch deletions have no executable
 * branch commit and are ignored. Durable deduplication by delivery ID belongs
 * to the event store tracked separately in issue #203.</p>
 */
@Component
public class GitHubPushEventNormalizer {

	private static final String PUSH_EVENT = "push";
	private static final String HEADS_REF_PREFIX = "refs/heads/";
	private static final String TAGS_REF_PREFIX = "refs/tags/";
	private final JsonMapper jsonMapper;

	public GitHubPushEventNormalizer(JsonMapper jsonMapper) {
		this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper must not be null");
	}

	/**
	 * Normalizes a verified push delivery. A valid tag push or branch deletion
	 * returns empty because it has no buildable branch commit.
	 *
	 * @param delivery verified webhook delivery
	 * @return a branch push event, or empty for a valid non-buildable push
	 * @throws GitHubPushEventNormalizationException when required push data is missing or invalid
	 */
	public Optional<RepositoryPushEvent> normalize(GitHubWebhookDelivery delivery) {
		Objects.requireNonNull(delivery, "delivery must not be null");
		if (!PUSH_EVENT.equals(delivery.event())) {
			throw new GitHubPushEventNormalizationException("only push deliveries can be normalized");
		}

		var payload = readPayload(delivery.rawBody());
		var ref = requiredText(payload, "ref", "ref");
		var deleted = requiredBoolean(payload, "deleted", "deleted");
		if (deleted || ref.startsWith(TAGS_REF_PREFIX)) {
			return Optional.empty();
		}
		if (!ref.startsWith(HEADS_REF_PREFIX)) {
			throw invalid("ref must identify a branch under refs/heads/");
		}

		var branchValue = ref.substring(HEADS_REF_PREFIX.length());
		if (branchValue.isBlank()) {
			throw invalid("payload field `ref` must include a branch name");
		}
		var branch = createBranchName(branchValue);

		var repositoryNode = requiredObject(payload, "repository", "repository");
		var ownerNode = requiredObject(repositoryNode, "owner", "repository.owner");
		var owner = requiredText(ownerNode, "login", "repository.owner.login");
		var name = requiredText(repositoryNode, "name", "repository.name");
		var repository = createRepositoryIdentity(owner, name);

		var commitSha = createCommitSha(requiredText(payload, "after", "after"));

		var headCommit = requiredObject(payload, "head_commit", "head_commit");
		var timestamp = requiredText(headCommit, "timestamp", "head_commit.timestamp");
		var eventTime = parseTimestamp(timestamp);

		try {
			return Optional.of(new RepositoryPushEvent(repository, branch, commitSha, delivery.deliveryId(), eventTime));
		}
		catch (IllegalArgumentException ex) {
			throw new GitHubPushEventNormalizationException("push event contains invalid repository data", ex);
		}
	}

	private JsonNode readPayload(byte[] rawBody) {
		try {
			var payload = jsonMapper.readTree(rawBody);
			if (payload == null || !payload.isObject()) {
				throw invalid("push payload must be a JSON object");
			}
			return payload;
		}
		catch (JacksonException ex) {
			throw new GitHubPushEventNormalizationException("push payload must contain valid JSON", ex);
		}
	}

	private static JsonNode requiredObject(JsonNode parent, String field, String path) {
		var value = parent.get(field);
		if (value == null || !value.isObject()) {
			throw invalid("payload field `" + path + "` must be an object");
		}
		return value;
	}

	private static String requiredText(JsonNode parent, String field, String path) {
		var value = parent.get(field);
		if (value == null || !value.isString() || value.stringValue().isBlank()) {
			throw invalid("payload field `" + path + "` must be a non-empty string");
		}
		return value.stringValue();
	}

	private static boolean requiredBoolean(JsonNode parent, String field, String path) {
		var value = parent.get(field);
		if (value == null || !value.isBoolean()) {
			throw invalid("payload field `" + path + "` must be a boolean");
		}
		return value.booleanValue();
	}

	private static BranchName createBranchName(String value) {
		try {
			return new BranchName(value);
		}
		catch (IllegalArgumentException ex) {
			throw new GitHubPushEventNormalizationException("payload field `ref` does not contain a valid branch name", ex);
		}
	}

	private static RepositoryIdentity createRepositoryIdentity(String owner, String name) {
		try {
			return new RepositoryIdentity(RepositoryProvider.GITHUB, owner, name);
		}
		catch (IllegalArgumentException ex) {
			throw new GitHubPushEventNormalizationException("payload repository identity is invalid", ex);
		}
	}

	private static CommitSha createCommitSha(String value) {
		try {
			return new CommitSha(value);
		}
		catch (IllegalArgumentException ex) {
			throw new GitHubPushEventNormalizationException(
					"payload field `after` must be a 40- or 64-character hexadecimal commit SHA", ex);
		}
	}

	private static Instant parseTimestamp(String value) {
		try {
			return OffsetDateTime.parse(value).toInstant();
		}
		catch (DateTimeException ex) {
			throw new GitHubPushEventNormalizationException(
					"payload field `head_commit.timestamp` must be an ISO-8601 timestamp with an offset", ex);
		}
	}

	private static GitHubPushEventNormalizationException invalid(String reason) {
		return new GitHubPushEventNormalizationException(reason);
	}
}

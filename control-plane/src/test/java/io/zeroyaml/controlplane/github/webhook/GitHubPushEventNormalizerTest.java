package io.zeroyaml.controlplane.github.webhook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

import io.zeroyaml.controlplane.domain.event.RepositoryPushEvent;
import io.zeroyaml.controlplane.domain.repository.BranchName;
import io.zeroyaml.controlplane.domain.repository.CommitSha;
import io.zeroyaml.controlplane.domain.repository.RepositoryIdentity;

class GitHubPushEventNormalizerTest {

	private static final String DELIVERY_ID = "72d3162e-cc78-11e3-81ab-4c9367dc0958";
	private static final String COMMIT_SHA = "0123456789abcdef0123456789abcdef01234567";

	private final JsonMapper jsonMapper = JsonMapper.builder().build();
	private final GitHubPushEventNormalizer normalizer = new GitHubPushEventNormalizer(jsonMapper);

	@Test
	void normalizesTheSupportedPushFixtureIntoARepositoryEvent() throws IOException {
		var event = normalize(fixture("push-main.json"));

		assertEquals(RepositoryIdentity.github("MohamedMBG", "ZeroYaml"), event.repository());
		assertEquals(new BranchName("main"), event.branch());
		assertEquals(new CommitSha(COMMIT_SHA), event.commitSha());
		assertEquals(DELIVERY_ID, event.deliveryId());
		assertEquals(Instant.parse("2026-09-30T12:15:30Z"), event.eventTime());
	}

	@Test
	void identicalRedeliveriesProduceEqualEvents() throws IOException {
		var first = normalize(fixture("push-main.json"));
		var redelivery = normalize(fixture("push-main.json"));

		assertEquals(first, redelivery);
	}

	@Test
	void tagPushesAndBranchDeletionsDoNotProduceBuildEvents() throws IOException {
		var branchPush = (ObjectNode) jsonMapper.readTree(fixture("push-main.json"));
		var tagPush = branchPush.deepCopy();
		tagPush.put("ref", "refs/tags/v1.0.0");
		var branchDeletion = branchPush.deepCopy();
		branchDeletion.put("deleted", true);

		assertFalse(normalizeOptional(jsonMapper.writeValueAsBytes(tagPush)).isPresent());
		assertFalse(normalizeOptional(jsonMapper.writeValueAsBytes(branchDeletion)).isPresent());
	}

	@Test
	void rejectsAValidJsonFixtureThatOmitsTheTipCommitSha() throws IOException {
		var exception = assertThrows(
				GitHubPushEventNormalizationException.class,
				() -> normalizeOptional(fixture("push-missing-after.json"))
		);

		assertEquals("payload field `after` must be a non-empty string", exception.getMessage());
	}

	@Test
	void rejectsInvalidBranchShaAndTimestampWithFieldSpecificReasons() throws IOException {
		var invalidBranchPayload = (ObjectNode) jsonMapper.readTree(fixture("push-main.json"));
		invalidBranchPayload.put("ref", "refs/heads/../invalid");
		var invalidBranch = assertThrows(
				GitHubPushEventNormalizationException.class,
				() -> normalizeOptional(jsonMapper.writeValueAsBytes(invalidBranchPayload))
		);
		assertEquals("payload field `ref` does not contain a valid branch name", invalidBranch.getMessage());

		var invalidShaPayload = (ObjectNode) jsonMapper.readTree(fixture("push-main.json"));
		invalidShaPayload.put("after", "not-a-commit-sha");
		var invalidSha = assertThrows(
				GitHubPushEventNormalizationException.class,
				() -> normalizeOptional(jsonMapper.writeValueAsBytes(invalidShaPayload))
		);
		assertEquals("payload field `after` must be a 40- or 64-character hexadecimal commit SHA",
				invalidSha.getMessage());

		var invalidTimestampPayload = (ObjectNode) jsonMapper.readTree(fixture("push-main.json"));
		((ObjectNode) invalidTimestampPayload.path("head_commit")).put("timestamp", "not-a-timestamp");
		var invalidTimestamp = assertThrows(
				GitHubPushEventNormalizationException.class,
				() -> normalizeOptional(jsonMapper.writeValueAsBytes(invalidTimestampPayload))
		);
		assertEquals("payload field `head_commit.timestamp` must be an ISO-8601 timestamp with an offset",
				invalidTimestamp.getMessage());
	}

	@Test
	void rejectsMalformedJsonWithoutEchoingPayloadContent() {
		var exception = assertThrows(
				GitHubPushEventNormalizationException.class,
				() -> normalizeOptional("not-json".getBytes(StandardCharsets.UTF_8))
		);

		assertEquals("push payload must contain valid JSON", exception.getMessage());
	}

	private RepositoryPushEvent normalize(byte[] payload) {
		return normalizeOptional(payload).orElseThrow();
	}

	private java.util.Optional<RepositoryPushEvent> normalizeOptional(byte[] payload) {
		return normalizer.normalize(new GitHubWebhookDelivery(
				DELIVERY_ID, "push", null, null, null, null, payload));
	}

	private byte[] fixture(String name) throws IOException {
		try (var input = getClass().getResourceAsStream("/fixtures/github/" + name)) {
			if (input == null) {
				throw new IOException("missing GitHub push fixture: " + name);
			}
			return input.readAllBytes();
		}
	}
}

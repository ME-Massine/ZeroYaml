package io.zeroyaml.controlplane.github.webhook;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

/** Request-level check of signature verification followed by push normalization. */
@WebMvcTest(GitHubWebhookController.class)
@Import(GitHubPushWebhookFlowTest.WebhookTestConfiguration.class)
@TestPropertySource(properties = {
		"zeroyaml.github.webhook.max-payload-size=1MB",
		"zeroyaml.github.webhook.secret=zeroyaml-test-webhook-secret"
})
class GitHubPushWebhookFlowTest {

	private static final String SECRET = "zeroyaml-test-webhook-secret";
	private static final String DELIVERY_ID = "72d3162e-cc78-11e3-81ab-4c9367dc0958";

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private JsonMapper jsonMapper;

	@Test
	void acceptsAndNormalizesASignedBranchPushFixture() throws Exception {
		var payload = fixture("push-main.json");

		mockMvc.perform(signedPush(payload))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.deliveryId").value(DELIVERY_ID))
				.andExpect(jsonPath("$.outcome").value("ACCEPTED"));
	}

	@Test
	void reportsAnActionableBadRequestForASignedMalformedPushFixture() throws Exception {
		var payload = fixture("push-missing-after.json");

		mockMvc.perform(signedPush(payload))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.outcome").value("REJECTED"))
				.andExpect(jsonPath("$.message").value("payload field `after` must be a non-empty string"));
	}

	@Test
	void acceptsValidTagPushWithoutCreatingABranchEvent() throws Exception {
		var payload = (ObjectNode) jsonMapper.readTree(fixture("push-main.json"));
		payload.put("ref", "refs/tags/v1.0.0");

		mockMvc.perform(signedPush(jsonMapper.writeValueAsBytes(payload)))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.outcome").value("ACCEPTED"));
	}

	private org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder signedPush(byte[] payload) {
		return post(GitHubWebhookController.PATH)
				.header(GitHubWebhookEnvelopeReader.EVENT_HEADER, "push")
				.header(GitHubWebhookEnvelopeReader.DELIVERY_HEADER, DELIVERY_ID)
				.header(GitHubWebhookEnvelopeReader.SIGNATURE_256_HEADER,
						"sha256=" + HexFormat.of().formatHex(hmac(payload)))
				.contentType(MediaType.APPLICATION_JSON)
				.content(payload);
	}

	private byte[] fixture(String name) throws IOException {
		try (var input = getClass().getResourceAsStream("/fixtures/github/" + name)) {
			if (input == null) {
				throw new IOException("missing GitHub push fixture: " + name);
			}
			return input.readAllBytes();
		}
	}

	private static byte[] hmac(byte[] payload) {
		try {
			var mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
			return mac.doFinal(payload);
		}
		catch (GeneralSecurityException ex) {
			throw new IllegalStateException("the test fixture cannot be signed", ex);
		}
	}

	@TestConfiguration
	@EnableConfigurationProperties(GitHubWebhookProperties.class)
	@Import({
			GitHubWebhookEnvelopeReader.class,
			GitHubWebhookSignatureVerifier.class,
			GitHubPushEventNormalizer.class,
			NormalizingGitHubWebhookDeliveryHandler.class
	})
	static class WebhookTestConfiguration {
	}
}

package io.zeroyaml.controlplane.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/**
 * Pins the version 1 envelope. A failure here means the published contract
 * changed; see {@code docs/architecture/job-lifecycle-events.md} before
 * updating an expectation.
 */
class JobLifecycleEventSerializerTest {

	private final JsonMapper json = JsonMapper.builder().build();
	private final JobLifecycleEventSerializer serializer = new JobLifecycleEventSerializer(json);

	@Test
	void writesAnAcceptedEventWithItsPipelineStep() {
		var event = JobLifecycleEvent.accepted(JobLifecycleEventFixtures.acceptedJob());

		assertJson("""
				{
				  "schemaVersion": 1,
				  "eventId": "%s",
				  "eventType": "job.accepted",
				  "occurredAt": "2026-10-26T10:00:00Z",
				  "job": {"id": "0f6a3c2e-5b1d-4e8a-9c7f-2d4b6e8a1c3e", "revision": "0123456789abcdef0123456789abcdef01234567"},
				  "pipeline": {"id": "7d3c5a1e-0000-4000-8000-000000000001", "stepName": "test", "stepPosition": 2}
				}
				""".formatted(event.eventId()), serializer.serialize(event));
	}

	@Test
	void writesAStartedEventWithItsRunner() {
		var event = JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob());

		assertJson("""
				{
				  "schemaVersion": 1,
				  "eventId": "%s",
				  "eventType": "job.started",
				  "occurredAt": "2026-10-26T10:00:05Z",
				  "job": {"id": "0f6a3c2e-5b1d-4e8a-9c7f-2d4b6e8a1c3e", "revision": "0123456789abcdef0123456789abcdef01234567"},
				  "runner": {"id": "runner-1", "instanceId": "instance-1"}
				}
				""".formatted(event.eventId()), serializer.serialize(event));
	}

	@Test
	void writesASucceededEventWithItsExitCode() {
		var event = JobLifecycleEvent.completed(JobLifecycleEventFixtures.succeededJob(0));

		assertJson("""
				{
				  "schemaVersion": 1,
				  "eventId": "%s",
				  "eventType": "job.succeeded",
				  "occurredAt": "2026-10-26T10:00:35Z",
				  "job": {"id": "0f6a3c2e-5b1d-4e8a-9c7f-2d4b6e8a1c3e", "revision": "0123456789abcdef0123456789abcdef01234567"},
				  "runner": {"id": "runner-1", "instanceId": "instance-1"},
				  "outcome": {"exitCode": 0}
				}
				""".formatted(event.eventId()), serializer.serialize(event));
	}

	@Test
	void writesAnEmptyOutcomeWhenNoExitCodeWasReported() {
		var event = JobLifecycleEvent.completed(JobLifecycleEventFixtures.succeededJob(null));

		assertEquals(json.readTree("{}"), json.readTree(serializer.serialize(event)).get("outcome"));
	}

	@Test
	void writesAFailedEventWithItsCodeAndExitCode() {
		var event = JobLifecycleEvent.completed(JobLifecycleEventFixtures.failedJob());

		assertJson("""
				{
				  "schemaVersion": 1,
				  "eventId": "%s",
				  "eventType": "job.failed",
				  "occurredAt": "2026-10-26T10:00:35Z",
				  "job": {"id": "0f6a3c2e-5b1d-4e8a-9c7f-2d4b6e8a1c3e", "revision": "0123456789abcdef0123456789abcdef01234567"},
				  "runner": {"id": "runner-1", "instanceId": "instance-1"},
				  "outcome": {"failureCode": "NON_ZERO_EXIT", "exitCode": 2}
				}
				""".formatted(event.eventId()), serializer.serialize(event));
	}

	@Test
	void neverWritesTheRepositoryLocationTheCommandOrTheFailureMessage() {
		var payload = serializer.serialize(JobLifecycleEvent.completed(JobLifecycleEventFixtures.failedJob()));

		assertFalse(payload.contains("token"), payload);
		assertFalse(payload.contains("example.test"), payload);
		assertFalse(payload.contains("mvnw"), payload);
		assertFalse(payload.contains("secret-looking output"), payload);
	}

	private void assertJson(String expected, String actual) {
		assertEquals(json.readTree(expected), json.readTree(actual));
	}
}

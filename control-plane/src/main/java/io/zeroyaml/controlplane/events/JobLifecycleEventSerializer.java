package io.zeroyaml.controlplane.events;

import java.util.Objects;

import tools.jackson.databind.json.JsonMapper;

/**
 * Writes the version 1 JSON envelope of a {@link JobLifecycleEvent}.
 *
 * <p>The envelope is built field by field instead of serializing the record, so
 * renaming a Java field can never change the published contract. Any change to
 * the envelope other than adding an optional field needs a new
 * {@link #SCHEMA_VERSION} and a new topic. The format is documented in
 * {@code docs/architecture/job-lifecycle-events.md}.</p>
 */
final class JobLifecycleEventSerializer {

	/** Version of the envelope written by this serializer. */
	static final int SCHEMA_VERSION = 1;

	private final JsonMapper json;

	JobLifecycleEventSerializer(JsonMapper json) {
		this.json = Objects.requireNonNull(json, "json must not be null");
	}

	String serialize(JobLifecycleEvent event) {
		Objects.requireNonNull(event, "event must not be null");
		var envelope = json.createObjectNode();
		envelope.put("schemaVersion", SCHEMA_VERSION);
		envelope.put("eventId", event.eventId().toString());
		envelope.put("eventType", event.type().wireName());
		envelope.put("occurredAt", event.occurredAt().toString());

		var job = envelope.putObject("job");
		job.put("id", event.jobId().value().toString());
		job.put("revision", event.revision());

		if (event.pipelineId() != null) {
			var pipeline = envelope.putObject("pipeline");
			pipeline.put("id", event.pipelineId().toString());
			pipeline.put("stepName", event.stepName());
			pipeline.put("stepPosition", event.stepPosition());
		}
		if (event.runner() != null) {
			var runner = envelope.putObject("runner");
			runner.put("id", event.runner().runnerId());
			runner.put("instanceId", event.runner().instanceId());
		}
		if (event.type() == JobLifecycleEventType.SUCCEEDED || event.type() == JobLifecycleEventType.FAILED) {
			var outcome = envelope.putObject("outcome");
			if (event.failureCode() != null) {
				outcome.put("failureCode", event.failureCode());
			}
			if (event.exitCode() != null) {
				outcome.put("exitCode", event.exitCode());
			}
		}
		return json.writeValueAsString(envelope);
	}
}

package io.zeroyaml.controlplane.events;

/**
 * Job lifecycle transitions published to operational consumers.
 *
 * <p>Only transitions the Control Plane records today are listed. Queueing and
 * cancellation have no production caller yet and are added with the change
 * that introduces one. The declaration order is the lifecycle order, so a
 * consumer can restore it when events for one Job arrive out of order.</p>
 */
public enum JobLifecycleEventType {

	/** The Control Plane durably recorded a new Job for a pipeline step. */
	ACCEPTED("job.accepted"),

	/** A Runner started the Job, as recorded from its status report. */
	STARTED("job.started"),

	/** The Job completed successfully. */
	SUCCEEDED("job.succeeded"),

	/** The Job completed with a failure. */
	FAILED("job.failed");

	private final String wireName;

	JobLifecycleEventType(String wireName) {
		this.wireName = wireName;
	}

	/**
	 * @return the stable name written to the event envelope; it never changes
	 *         within one schema version
	 */
	public String wireName() {
		return wireName;
	}
}

package io.zeroyaml.controlplane.events;

/**
 * Discards every event. It is the default, so the Control Plane runs without
 * Kafka until publication is enabled explicitly.
 */
final class DisabledJobLifecycleEventPublisher implements JobLifecycleEventPublisher {

	@Override
	public void publish(JobLifecycleEvent event) {
		// Publication is disabled; the transition is already recorded in PostgreSQL.
	}
}

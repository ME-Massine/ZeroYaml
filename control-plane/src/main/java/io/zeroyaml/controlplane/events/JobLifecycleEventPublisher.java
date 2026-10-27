package io.zeroyaml.controlplane.events;

/**
 * Hands recorded Job lifecycle transitions to operational consumers.
 *
 * <p>Callers publish only after the transition is committed, so an event never
 * describes state that PostgreSQL does not hold. Publication is a best-effort
 * notification and never part of the transition itself: an implementation
 * returns without waiting for a broker and never throws because delivery
 * failed. An event can therefore be lost, for example when the broker is
 * unreachable or the process stops; it is never published for a transition
 * that was rolled back.</p>
 */
@FunctionalInterface
public interface JobLifecycleEventPublisher {

	/**
	 * Publishes one committed transition without blocking on delivery.
	 *
	 * @param event committed transition
	 */
	void publish(JobLifecycleEvent event);
}

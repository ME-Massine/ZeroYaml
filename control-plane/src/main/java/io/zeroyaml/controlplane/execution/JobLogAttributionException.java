package io.zeroyaml.controlplane.execution;

/** Output may only be attributed to the Job's immutable Runner assignment. */
class JobLogAttributionException extends RuntimeException {
	JobLogAttributionException() { super("log must come from the assigned Runner process"); }
}

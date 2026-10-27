package io.zeroyaml.controlplane.execution;

/** A record conflicts with retained output ordering, ownership, or completion. */
class JobLogSequenceException extends RuntimeException {
	JobLogSequenceException(String message) { super(message); }
}

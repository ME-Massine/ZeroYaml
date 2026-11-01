package io.zeroyaml.controlplane.execution;

/** Recent unfinished output cannot be evicted to admit another Job's logs. */
class JobLogCapacityException extends RuntimeException {
	JobLogCapacityException() { super("Job log retention is occupied by active output"); }
}

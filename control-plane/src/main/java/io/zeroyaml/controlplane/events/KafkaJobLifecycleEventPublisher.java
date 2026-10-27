package io.zeroyaml.controlplane.events;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publishes Job lifecycle events to one Kafka topic, keyed by Job identity.
 *
 * <p>The caller only enqueues the event on {@code sender}, a bounded
 * single-thread executor, so a slow or unreachable broker never delays a
 * webhook acknowledgment or a status report. Even sending can block, because
 * the producer waits for topic metadata for up to {@code max.block.ms}; that
 * wait happens on the sender thread. When the queue is full or closed the
 * event is dropped and logged, and a failed delivery is logged after the
 * producer gives up. Neither case is reported to the caller, whose transition
 * is already committed.</p>
 *
 * <p>Keying by Job identity places every event of one Job on one partition. A
 * single sender thread keeps them in hand-off order, and the idempotent
 * producer keeps that order through retries. Hand-off order can still differ
 * from transition order when two reports for the same Job are reconciled
 * concurrently, so consumers order events of one Job by
 * {@link JobLifecycleEventType} rather than by offset.</p>
 *
 * <p>Log lines carry the event, its type, and the Job identity, never the
 * payload.</p>
 */
final class KafkaJobLifecycleEventPublisher implements JobLifecycleEventPublisher, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(KafkaJobLifecycleEventPublisher.class);

	private final KafkaTemplate<String, String> kafka;
	private final String topic;
	private final JobLifecycleEventSerializer serializer;
	private final ExecutorService sender;
	private final Duration shutdownTimeout;

	KafkaJobLifecycleEventPublisher(
			KafkaTemplate<String, String> kafka,
			String topic,
			JobLifecycleEventSerializer serializer,
			ExecutorService sender,
			Duration shutdownTimeout
	) {
		this.kafka = Objects.requireNonNull(kafka, "kafka must not be null");
		this.topic = Objects.requireNonNull(topic, "topic must not be null");
		this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
		this.sender = Objects.requireNonNull(sender, "sender must not be null");
		this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout must not be null");
	}

	@Override
	public void publish(JobLifecycleEvent event) {
		Objects.requireNonNull(event, "event must not be null");
		try {
			sender.execute(() -> send(event));
		} catch (RejectedExecutionException rejected) {
			log.warn("Dropped {} event {} for job {}: the publication queue is full or closed",
					event.type().wireName(), event.eventId(), event.jobId().value());
		}
	}

	private void send(JobLifecycleEvent event) {
		try {
			kafka.send(topic, event.jobId().value().toString(), serializer.serialize(event))
					.whenComplete((result, failure) -> {
						if (failure != null) {
							logDeliveryFailure(event, failure);
						}
					});
		} catch (RuntimeException failure) {
			logDeliveryFailure(event, failure);
		}
	}

	private static void logDeliveryFailure(JobLifecycleEvent event, Throwable failure) {
		log.warn("Failed to publish {} event {} for job {}: {}",
				event.type().wireName(), event.eventId(), event.jobId().value(), failure.toString());
	}

	/**
	 * Stops accepting events and waits up to the shutdown timeout for queued
	 * events to be handed to the producer. Events still queued after that are
	 * dropped and counted in the log.
	 */
	@Override
	public void close() {
		sender.shutdown();
		try {
			if (!sender.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				var dropped = sender.shutdownNow().size();
				log.warn("Dropped {} queued job lifecycle events at shutdown", dropped);
			}
		} catch (InterruptedException interrupted) {
			sender.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
}

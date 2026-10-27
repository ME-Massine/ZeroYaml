package io.zeroyaml.controlplane.events;

import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.JOB_ID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.mock.MockProducerFactory;

import tools.jackson.databind.json.JsonMapper;

/**
 * Covers the producer side against Kafka's in-memory producer: what is sent,
 * under which key, in which order, and that no delivery failure reaches the
 * caller whose transition is already committed.
 */
class KafkaJobLifecycleEventPublisherTest {

	private static final String TOPIC = "zeroyaml.job-lifecycle.v1";

	private final JsonMapper json = JsonMapper.builder().build();
	private final ExecutorService sender = Executors.newSingleThreadExecutor();

	@AfterEach
	void stopSender() {
		sender.shutdownNow();
	}

	@Test
	void sendsTheSerializedEnvelopeKeyedByJobIdentity() {
		var producer = autoCompletingProducer();
		var event = JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob());

		try (var publisher = publisher(producer)) {
			publisher.publish(event);
		}

		var sent = producer.history();
		assertEquals(1, sent.size());
		assertEquals(TOPIC, sent.get(0).topic());
		assertEquals(JOB_ID.value().toString(), sent.get(0).key());
		assertEquals(new JobLifecycleEventSerializer(json).serialize(event), sent.get(0).value());
	}

	@Test
	void sendsEventsInTheOrderTheyWerePublished() {
		var producer = autoCompletingProducer();
		var events = List.of(
				JobLifecycleEvent.accepted(JobLifecycleEventFixtures.acceptedJob()),
				JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob()),
				JobLifecycleEvent.completed(JobLifecycleEventFixtures.succeededJob(0)));

		try (var publisher = publisher(producer)) {
			events.forEach(publisher::publish);
		}

		assertEquals(
				List.of("job.accepted", "job.started", "job.succeeded"),
				producer.history().stream().map(ProducerRecord::value)
						.map(value -> json.readTree(value).get("eventType").asString()).toList());
	}

	@Test
	void keepsPublishingAfterTheBrokerRejectsADelivery() {
		var producer = producer(false);
		var publisher = publisher(producer);

		publisher.publish(JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob()));
		awaitSent(producer, 1);
		assertTrue(producer.errorNext(new TimeoutException("broker unavailable")));
		publisher.publish(JobLifecycleEvent.completed(JobLifecycleEventFixtures.succeededJob(0)));
		publisher.close();

		assertEquals(2, producer.history().size());
	}

	@Test
	void keepsPublishingAfterTheProducerRefusesASend() {
		var producer = autoCompletingProducer();
		producer.sendException = new KafkaException("producer refused the record");
		var publisher = publisher(producer);

		assertDoesNotThrow(() -> publisher.publish(JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob())));
		awaitIdle();
		producer.sendException = null;
		publisher.publish(JobLifecycleEvent.completed(JobLifecycleEventFixtures.succeededJob(0)));
		publisher.close();

		assertEquals(1, producer.history().size());
		assertEquals("job.succeeded", json.readTree(producer.history().get(0).value()).get("eventType").asString());
	}

	@Test
	void dropsAnEventItCannotQueueWithoutFailingTheCaller() {
		var producer = autoCompletingProducer();
		var publisher = publisher(producer);
		publisher.close();

		assertDoesNotThrow(() -> publisher.publish(JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob())));

		assertTrue(producer.history().isEmpty());
	}

	private KafkaJobLifecycleEventPublisher publisher(MockProducer<String, String> producer) {
		var template = new KafkaTemplate<>(new MockProducerFactory<>(() -> producer));
		return new KafkaJobLifecycleEventPublisher(
				template, TOPIC, new JobLifecycleEventSerializer(json), sender, Duration.ofSeconds(10));
	}

	private static MockProducer<String, String> autoCompletingProducer() {
		return producer(true);
	}

	/**
	 * The template closes a raw producer after every send, while a real producer
	 * factory hands out a shared one whose close is a no-op; this keeps the mock
	 * usable for the next send in the same way.
	 */
	private static MockProducer<String, String> producer(boolean autoComplete) {
		return new MockProducer<>(autoComplete, null, new StringSerializer(), new StringSerializer()) {
			@Override
			public void close() {
			}

			@Override
			public void close(Duration timeout) {
			}
		};
	}

	/** Waits until the sender thread has handed everything queued so far to the producer. */
	private void awaitIdle() {
		assertDoesNotThrow(() -> sender.submit(() -> { }).get());
	}

	private void awaitSent(MockProducer<String, String> producer, int count) {
		awaitIdle();
		assertEquals(count, producer.history().size());
	}
}

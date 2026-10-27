package io.zeroyaml.controlplane.events;

import static io.zeroyaml.controlplane.events.JobLifecycleEventFixtures.JOB_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import tools.jackson.databind.json.JsonMapper;

/**
 * Publishes one Job's lifecycle through a real broker and reads it back as a
 * consumer would. The image tag matches {@code infra/compose.yaml}.
 */
@Testcontainers
class KafkaJobLifecycleEventPublisherIntegrationTest {

	@Container
	static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

	private final JsonMapper json = JsonMapper.builder().build();

	@Test
	void consumerReadsTheLifecycleOfOneJobUnderItsKey() {
		var topic = "zeroyaml.job-lifecycle.v1." + UUID.randomUUID();
		var producerFactory = new DefaultKafkaProducerFactory<String, String>(Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.ACKS_CONFIG, "all"));
		var events = List.of(
				JobLifecycleEvent.accepted(JobLifecycleEventFixtures.acceptedJob()),
				JobLifecycleEvent.started(JobLifecycleEventFixtures.runningJob()),
				JobLifecycleEvent.completed(JobLifecycleEventFixtures.failedJob()));

		try (var publisher = new KafkaJobLifecycleEventPublisher(
				new KafkaTemplate<>(producerFactory), topic, new JobLifecycleEventSerializer(json),
				Executors.newSingleThreadExecutor(), Duration.ofSeconds(30))) {
			events.forEach(publisher::publish);
		} finally {
			// Closing the factory flushes every record the publisher handed over.
			producerFactory.destroy();
		}

		var received = consume(topic, events.size());
		assertEquals(List.of(JOB_ID.value().toString()), received.stream().map(ConsumerRecord::key).distinct().toList());
		assertEquals(
				events.stream().map(event -> event.eventId().toString()).toList(),
				received.stream().map(record -> json.readTree(record.value()).get("eventId").asString()).toList());
		assertEquals(
				List.of("job.accepted", "job.started", "job.failed"),
				received.stream().map(record -> json.readTree(record.value()).get("eventType").asString()).toList());
	}

	private static List<ConsumerRecord<String, String>> consume(String topic, int expected) {
		var received = new ArrayList<ConsumerRecord<String, String>>();
		try (var consumer = new KafkaConsumer<String, String>(Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
				ConsumerConfig.GROUP_ID_CONFIG, "job-lifecycle-test-" + UUID.randomUUID(),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
				new StringDeserializer(), new StringDeserializer())) {
			consumer.subscribe(List.of(topic));
			var deadline = Instant.now().plusSeconds(30);
			while (received.size() < expected && Instant.now().isBefore(deadline)) {
				consumer.poll(Duration.ofMillis(500)).forEach(received::add);
			}
		}
		return received;
	}
}

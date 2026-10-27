package io.zeroyaml.controlplane.events;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import tools.jackson.databind.json.JsonMapper;

/**
 * Selects the {@link JobLifecycleEventPublisher}: Kafka when
 * {@code zeroyaml.events.job-lifecycle.enabled} is {@code true}, otherwise one
 * that discards events. Neither choice makes Kafka a startup dependency,
 * because the producer connects on the first send.
 */
@Configuration
class JobLifecycleEventConfiguration {

	private static final Logger log = LoggerFactory.getLogger(JobLifecycleEventConfiguration.class);

	private static final String ENABLED = "zeroyaml.events.job-lifecycle.enabled";

	@Bean
	@ConditionalOnBooleanProperty(ENABLED)
	KafkaJobLifecycleEventPublisher kafkaJobLifecycleEventPublisher(
			KafkaTemplate<String, String> kafkaTemplate,
			JsonMapper json,
			JobLifecycleEventProperties properties
	) {
		// One thread keeps hand-off order; the bounded queue keeps an unreachable
		// broker from growing memory without limit.
		var sender = new ThreadPoolExecutor(
				1, 1, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(properties.getQueueCapacity()),
				Thread.ofPlatform().name("job-lifecycle-events").daemon().factory());
		log.info("Publishing job lifecycle events to topic {}", properties.getTopic());
		return new KafkaJobLifecycleEventPublisher(
				kafkaTemplate, properties.getTopic(), new JobLifecycleEventSerializer(json), sender,
				properties.getShutdownTimeout());
	}

	@Bean
	@ConditionalOnBooleanProperty(name = ENABLED, havingValue = false, matchIfMissing = true)
	JobLifecycleEventPublisher disabledJobLifecycleEventPublisher() {
		log.info("Job lifecycle event publication is disabled");
		return new DisabledJobLifecycleEventPublisher();
	}
}

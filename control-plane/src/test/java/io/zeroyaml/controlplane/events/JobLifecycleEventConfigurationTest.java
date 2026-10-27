package io.zeroyaml.controlplane.events;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import tools.jackson.databind.json.JsonMapper;

/**
 * Covers publisher selection. Neither context reaches a broker, which shows
 * that enabling publication does not make Kafka a startup dependency.
 */
class JobLifecycleEventConfigurationTest {

	private final ApplicationContextRunner contexts = new ApplicationContextRunner()
			.withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
			.withUserConfiguration(JobLifecycleEventConfiguration.class)
			.withBean(JobLifecycleEventProperties.class)
			.withBean(JsonMapper.class, () -> JsonMapper.builder().build())
			// Nothing listens here; a context that tried to connect would fail to start.
			.withPropertyValues("spring.kafka.bootstrap-servers=localhost:1");

	@Test
	void discardsEventsWhenPublicationIsNotConfigured() {
		contexts.run(context -> {
			assertNull(context.getStartupFailure());
			assertInstanceOf(DisabledJobLifecycleEventPublisher.class, context.getBean(JobLifecycleEventPublisher.class));
		});
	}

	@Test
	void discardsEventsWhenPublicationIsDisabled() {
		contexts.withPropertyValues("zeroyaml.events.job-lifecycle.enabled=false").run(context ->
				assertInstanceOf(DisabledJobLifecycleEventPublisher.class, context.getBean(JobLifecycleEventPublisher.class)));
	}

	@Test
	void publishesToKafkaWhenPublicationIsEnabled() {
		contexts.withPropertyValues("zeroyaml.events.job-lifecycle.enabled=true").run(context -> {
			assertNull(context.getStartupFailure());
			assertInstanceOf(KafkaJobLifecycleEventPublisher.class, context.getBean(JobLifecycleEventPublisher.class));
		});
	}
}

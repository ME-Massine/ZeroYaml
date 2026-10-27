package io.zeroyaml.controlplane.events;

import java.time.Duration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Settings for publishing Job lifecycle events. Publication is off unless
 * enabled, so a Control Plane without Kafka keeps working. Broker connection
 * settings are the standard {@code spring.kafka.*} properties.
 */
@Validated
@ConfigurationProperties(prefix = "zeroyaml.events.job-lifecycle")
public class JobLifecycleEventProperties {

	private boolean enabled = false;

	// The version suffix changes together with the envelope schema version.
	@NotBlank
	private String topic = "zeroyaml.job-lifecycle.v1";

	// Bounds memory while the broker is unreachable; events beyond it are dropped.
	@Min(1)
	@Max(100_000)
	private int queueCapacity = 1_000;

	// How long shutdown waits for queued events to reach the producer.
	@NotNull
	private Duration shutdownTimeout = Duration.ofSeconds(5);

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public Duration getShutdownTimeout() {
		return shutdownTimeout;
	}

	public void setShutdownTimeout(Duration shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}
}

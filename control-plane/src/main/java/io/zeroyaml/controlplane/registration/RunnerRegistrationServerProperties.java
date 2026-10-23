package io.zeroyaml.controlplane.registration;

import java.time.Duration;

import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Externalized configuration for the gRPC endpoint Runners call and for the
 * heartbeat liveness policy the registry enforces. Bean validation and the
 * startup check below make an invalid value fail during application startup
 * instead of silently misjudging Runner liveness.
 *
 * <p>The {@code port} is the single Runner-facing endpoint. Every Control Plane
 * gRPC service a Runner calls is served on it, so the setting keeps its
 * original {@code zeroyaml.registration} prefix rather than changing deployed
 * configuration.</p>
 */
@Validated
@ConfigurationProperties(prefix = "zeroyaml.registration")
public class RunnerRegistrationServerProperties {

	// Bound from application properties or environment variables at application startup.
	@Min(1)
	@Max(65535)
	private int port = 50052;

	/**
	 * Bounds how long a Runner may go without an acknowledged registration or
	 * heartbeat before {@link RunnerLiveness#UNAVAILABLE} is reported. Kept
	 * configurable so tests and local development can use a short timeout
	 * without changing production behavior.
	 */
	private Duration heartbeatTimeout = Duration.ofSeconds(15);

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public Duration getHeartbeatTimeout() {
		return heartbeatTimeout;
	}

	public void setHeartbeatTimeout(Duration heartbeatTimeout) {
		this.heartbeatTimeout = heartbeatTimeout;
	}

	@PostConstruct
	void validate() {
		if (heartbeatTimeout == null || heartbeatTimeout.isZero() || heartbeatTimeout.isNegative()) {
			throw new IllegalStateException(
					"zeroyaml.registration.heartbeat-timeout must be greater than zero, got " + heartbeatTimeout
			);
		}
	}
}

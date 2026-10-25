package io.zeroyaml.controlplane.execution;

import java.nio.file.Files;
import java.nio.file.Path;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Optional log ingress; configuration is all-or-nothing and never falls back to plaintext. */
@ConfigurationProperties(prefix = "zeroyaml.job-logs")
record JobLogServerProperties(Integer port, Path certificate, Path privateKey, Path clientCa) {

	boolean enabled() { return port != null; }

	@PostConstruct
	void validate() {
		if (port == null && certificate == null && privateKey == null && clientCa == null) {
			return;
		}
		if (port == null || port < 1 || port > 65535 || certificate == null || privateKey == null || clientCa == null) {
			throw new IllegalStateException("zeroyaml.job-logs requires port (1-65535), certificate, private-key, and client-ca");
		}
		for (var path : new Path[] {certificate, privateKey, clientCa}) {
			if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
				throw new IllegalStateException("zeroyaml.job-logs TLS files must be readable regular files");
			}
		}
	}
}

package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.catalina.connector.Connector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;

/** Exercises native TLS and plaintext rejection through real servlet connectors. */
class JobLogReadTlsTest {

	@TempDir
	Path directory;

	@Test
	void readsOverTrustedHttpsAndRefusesPlaintextDespiteForgedForwardingHeaders() throws Exception {
		var store = directory.resolve("reader.p12");
		var executable = Path.of(System.getProperty("java.home"), "bin",
				System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool");
		var process = new ProcessBuilder(executable.toString(), "-genkeypair", "-alias", "reader", "-keyalg", "EC",
				"-groupname", "secp256r1", "-dname", "CN=localhost", "-validity", "2", "-ext", "SAN=DNS:localhost",
				"-ext", "EKU=serverAuth", "-storetype", "PKCS12", "-keystore", store.toString(),
				"-storepass", "test-only-password")
				.redirectErrorStream(true).redirectOutput(directory.resolve("keytool.txt").toFile()).start();
		if (!process.waitFor(30, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new IllegalStateException("test certificate generation timed out");
		}
		assertEquals(0, process.exitValue(), "test certificate generation failed");
		var trust = KeyStore.getInstance("PKCS12");
		try (var input = Files.newInputStream(store)) {
			trust.load(input, "test-only-password".toCharArray());
		}
		var trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagers.init(trust);
		var tls = SSLContext.getInstance("TLS");
		tls.init(null, trustManagers.getTrustManagers(), null);

		try (var application = new SpringApplicationBuilder(ReaderApplication.class).run(
				"--server.port=0", "--server.address=127.0.0.1", "--server.ssl.enabled=true",
				"--server.ssl.key-store=" + store.toUri(), "--server.ssl.key-store-type=PKCS12",
				"--server.ssl.key-store-password=test-only-password", "--server.forward-headers-strategy=NONE",
				"--zeroyaml.job-logs.read.token=" + JobLogReadControllerTest.TOKEN);
				var client = HttpClient.newBuilder().sslContext(tls).connectTimeout(Duration.ofSeconds(5)).build()) {
			var job = JobId.newId();
			application.getBean(JobLogStore.class).append(new JobLogRecord(job,
					new RunnerAssignment("runner-1", "instance-1"), 1, Instant.parse("2026-11-01T10:00:00Z"),
					JobLogRecord.Stream.STDOUT, new byte[] { 'x' }, false, false, false));
			int httpsPort = ((ServletWebServerApplicationContext) application).getWebServer().getPort();
			String path = "/api/v1/jobs/" + job.value() + "/logs";
			var secure = HttpRequest.newBuilder(URI.create("https://localhost:" + httpsPort + path))
					.timeout(Duration.ofSeconds(5)).header("Authorization", "Bearer " + JobLogReadControllerTest.TOKEN).build();
			var response = client.send(secure, BodyHandlers.ofString());
			assertEquals(200, response.statusCode());
			assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow());
			assertTrue(response.body().contains(job.value().toString()));
			assertTrue(response.body().contains("eA=="));
			var unauthenticated = HttpRequest.newBuilder(secure.uri()).timeout(Duration.ofSeconds(5)).build();
			assertEquals(401, client.send(unauthenticated, BodyHandlers.discarding()).statusCode());

			int httpPort = application.getBean(Connector.class).getLocalPort();
			var plaintext = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + httpPort + path))
					.timeout(Duration.ofSeconds(5)).header("Authorization", "Bearer " + JobLogReadControllerTest.TOKEN)
					.header("Forwarded", "for=127.0.0.1;proto=https")
					.header("X-Forwarded-Proto", "https").header("X-Forwarded-Ssl", "on").build();
			var refused = client.send(plaintext, BodyHandlers.ofString());
			assertEquals(403, refused.statusCode());
			assertEquals("", refused.body());
			assertTrue(refused.headers().firstValue("Location").isEmpty());
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
	@EnableConfigurationProperties(JobLogReadProperties.class)
	@Import(JobLogReadController.class)
	static class ReaderApplication {
		@Bean
		JobLogStore jobLogStore() {
			return new InMemoryJobLogStore();
		}

		@Bean
		Connector plaintextConnector() {
			var connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
			connector.setPort(0);
			connector.setProperty("address", "127.0.0.1");
			return connector;
		}

		@Bean
		WebServerFactoryCustomizer<TomcatServletWebServerFactory> additionalHttpConnector(Connector connector) {
			// Exercise the guard even if a deployment adds a second, plaintext connector.
			return factory -> factory.addAdditionalConnectors(connector);
		}
	}
}

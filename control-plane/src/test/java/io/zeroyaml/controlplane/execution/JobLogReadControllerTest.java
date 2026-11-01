package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.autoconfigure.ServerProperties;
import org.springframework.boot.web.server.autoconfigure.ServerProperties.ForwardHeadersStrategy;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;

/** Transport security, authorization, status mapping, and byte fidelity of the retained-log reader. */
@WebMvcTest(JobLogReadController.class)
@Import(JobLogReadControllerTest.ReaderTestConfiguration.class)
@TestPropertySource(properties = { "zeroyaml.job-logs.read.token=" + JobLogReadControllerTest.TOKEN,
		"server.ssl.enabled=true", "server.forward-headers-strategy=NONE" })
class JobLogReadControllerTest {
	static final String TOKEN = "zeroyaml-test-job-log-read-token-0001";
	private static final RunnerAssignment RUNNER = new RunnerAssignment("runner-1", "instance-1");
	private static final Instant OBSERVED = Instant.parse("2026-10-31T10:00:00Z");

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private JobLogStore logs;

	@Test
	void returnsRetainedChunksInSequenceOrderWithFinalFlags() throws Exception {
		var job = JobId.newId();
		byte[] raw = { 'a', (byte) 0xff, '\n' };
		logs.append(chunk(job, 1, JobLogRecord.Stream.STDOUT, raw));
		logs.append(chunk(job, 2, JobLogRecord.Stream.STDERR, "warning\n".getBytes(StandardCharsets.UTF_8)));
		logs.append(new JobLogRecord(job, RUNNER, 3, OBSERVED, JobLogRecord.Stream.NONE, new byte[0], true, true, false));
		mockMvc.perform(get(JobLogReadController.PATH, job.value()).secure(true).header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
				.andExpect(status().isOk())
				.andExpect(header().string(HttpHeaders.CACHE_CONTROL, "no-store"))
				.andExpect(jsonPath("$.jobId").value(job.value().toString()))
				.andExpect(jsonPath("$.endOfOutput").value(true))
				.andExpect(jsonPath("$.truncated").value(true))
				.andExpect(jsonPath("$.incomplete").value(false))
				.andExpect(jsonPath("$.records.length()").value(2))
				.andExpect(jsonPath("$.records[0].sequence").value(1))
				.andExpect(jsonPath("$.records[0].stream").value("STDOUT"))
				.andExpect(jsonPath("$.records[0].observedAt").value("2026-10-31T10:00:00Z"))
				.andExpect(jsonPath("$.records[0].content").value(Base64.getEncoder().encodeToString(raw)))
				.andExpect(jsonPath("$.records[1].stream").value("STDERR"));
	}

	@Test
	void leavesCompletenessUnknownWhileOutputHasNoFinalMarker() throws Exception {
		var job = JobId.newId();
		logs.append(chunk(job, 1, JobLogRecord.Stream.STDOUT, new byte[] { 'x' }));
		mockMvc.perform(get(JobLogReadController.PATH, job.value()).secure(true).header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.endOfOutput").value(false))
				.andExpect(jsonPath("$.truncated").isEmpty())
				.andExpect(jsonPath("$.incomplete").isEmpty())
				.andExpect(jsonPath("$.records.length()").value(1));
	}

	@Test
	void refusesMissingMalformedAndWrongTokensBeforeReadingOutput() throws Exception {
		var job = JobId.newId();
		logs.append(chunk(job, 1, JobLogRecord.Stream.STDOUT, "secret".getBytes(StandardCharsets.UTF_8)));
		for (var authorization : new String[] { null, TOKEN, "Basic " + TOKEN, "Bearer ", "Bearer " + TOKEN + "x",
				"Bearer " + TOKEN.substring(1), "Bearer " + TOKEN.toUpperCase(), "Bearer" + TOKEN, "Bearer  " + TOKEN }) {
			var request = get(JobLogReadController.PATH, "not-a-job-id").secure(true);
			if (authorization != null) {
				request.header(HttpHeaders.AUTHORIZATION, authorization);
			}
			mockMvc.perform(request)
					.andExpect(status().isUnauthorized())
					.andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, "Bearer"));
		}
	}

	@Test
	void summarizesRejectedReadsAtMostOncePerInterval() {
		var clock = new SteppingClock(OBSERVED);
		var reader = new JobLogReadController(new InMemoryJobLogStore(), new JobLogReadProperties(TOKEN), secureServer(), clock);
		var logger = (Logger) LoggerFactory.getLogger(JobLogReadController.class);
		var appender = new ListAppender<ILoggingEvent>();
		appender.start();
		logger.addAppender(appender);
		try {
			for (int attempt = 0; attempt < 100; attempt++) {
				assertEquals(HttpStatus.UNAUTHORIZED, reader.read(JobId.newId().value().toString(), null, secureRequest()).getStatusCode());
			}
			assertEquals(1, appender.list.size());
			clock.advance(JobLogReadController.REJECTION_WARNING_INTERVAL);
			assertEquals(HttpStatus.UNAUTHORIZED, reader.read("not-a-job-id", "Bearer wrong", secureRequest()).getStatusCode());
			assertEquals(2, appender.list.size());
			var summary = appender.list.getLast();
			assertEquals(Level.WARN, summary.getLevel());
			assertEquals("Rejected unauthenticated Job log reads: 100 since the previous warning", summary.getFormattedMessage());
			assertFalse(summary.getFormattedMessage().contains("wrong"));
		} finally {
			logger.detachAppender(appender);
			appender.stop();
		}
	}

	@Test
	void matchesTheBearerSchemeCaseInsensitively() throws Exception {
		var job = JobId.newId();
		logs.append(chunk(job, 1, JobLogRecord.Stream.STDOUT, new byte[] { 'x' }));
		for (var scheme : new String[] { "bearer ", "BEARER ", "bEaReR " }) {
			mockMvc.perform(get(JobLogReadController.PATH, job.value()).secure(true).header(HttpHeaders.AUTHORIZATION, scheme + TOKEN))
					.andExpect(status().isOk());
		}
	}

	@Test
	void mapsMalformedAndUnknownJobIdentities() throws Exception {
		mockMvc.perform(get(JobLogReadController.PATH, "not-a-job-id").secure(true).header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
				.andExpect(status().isBadRequest());
		mockMvc.perform(get(JobLogReadController.PATH, JobId.newId().value()).secure(true).header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
				.andExpect(status().isNotFound());
	}

	@Test
	void disabledReaderAnswersNotFoundEvenWithAToken() {
		var logs = new InMemoryJobLogStore();
		var job = JobId.newId();
		logs.append(chunk(job, 1, JobLogRecord.Stream.STDOUT, new byte[] { 'x' }));
		for (var token : new String[] { null, "" }) {
			var disabled = new JobLogReadController(logs, new JobLogReadProperties(token), new ServerProperties());
			assertEquals(HttpStatus.NOT_FOUND,
					disabled.read(job.value().toString(), "Bearer " + TOKEN, new MockHttpServletRequest()).getStatusCode());
		}
	}

	@Test
	void rejectsPlaintextEvenWithValidCredentialsAndForgedProxyHeaders() throws Exception {
		var job = JobId.newId();
		logs.append(chunk(job, 1, JobLogRecord.Stream.STDOUT, new byte[] { 'x' }));
		for (var identity : new String[] { job.value().toString(), "not-a-job-id" }) {
			mockMvc.perform(get(JobLogReadController.PATH, identity)
					.header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN)
					.header("Forwarded", "for=127.0.0.1;proto=https")
					.header("X-Forwarded-Proto", "https").header("X-Forwarded-Ssl", "on"))
					.andExpect(status().isForbidden())
					.andExpect(header().doesNotExist(HttpHeaders.LOCATION))
					.andExpect(header().doesNotExist(HttpHeaders.WWW_AUTHENTICATE))
					.andExpect(result -> assertEquals("", result.getResponse().getContentAsString()));
		}
		var store = mock(JobLogStore.class);
		var reader = new JobLogReadController(store, new JobLogReadProperties(TOKEN), secureServer());
		assertEquals(HttpStatus.FORBIDDEN,
				reader.read(job.value().toString(), "Bearer " + TOKEN, new MockHttpServletRequest()).getStatusCode());
		verifyNoInteractions(store);
	}

	@Test
	void refusesStartupWithoutNativeHttpsOrWithForwardedHeaderTrust() {
		var context = new WebApplicationContextRunner()
				.withUserConfiguration(ReaderTestConfiguration.class, JobLogReadController.class)
				.withPropertyValues("zeroyaml.job-logs.read.token=" + TOKEN);
		context.run(application -> assertStartupFailure(application.getStartupFailure(), "requires HTTPS"));
		context.withPropertyValues("server.ssl.enabled=false", "server.forward-headers-strategy=NONE")
				.run(application -> assertStartupFailure(application.getStartupFailure(), "requires HTTPS"));
		for (var strategy : new String[] { "", "NATIVE", "FRAMEWORK" }) {
			var configured = context.withPropertyValues("server.ssl.enabled=true");
			if (!strategy.isEmpty()) {
				configured = configured.withPropertyValues("server.forward-headers-strategy=" + strategy);
			}
			configured.run(application -> assertStartupFailure(application.getStartupFailure(), "forward-headers-strategy=NONE"));
		}
		context.withPropertyValues("server.ssl.enabled=true", "server.forward-headers-strategy=NONE")
				.run(application -> assertNull(application.getStartupFailure()));
		context.withPropertyValues("zeroyaml.job-logs.read.token=")
				.run(application -> assertNull(application.getStartupFailure()));
	}

	private static void assertStartupFailure(Throwable failure, String message) {
		assertTrue(failure != null, "unsafe reader configuration must fail startup");
		while (failure.getCause() != null) {
			failure = failure.getCause();
		}
		assertTrue(failure.getMessage().contains(message), failure.getMessage());
		assertFalse(failure.getMessage().contains(TOKEN));
	}

	private static ServerProperties secureServer() {
		var server = new ServerProperties();
		server.setSsl(new Ssl());
		server.setForwardHeadersStrategy(ForwardHeadersStrategy.NONE);
		return server;
	}

	private static MockHttpServletRequest secureRequest() {
		var request = new MockHttpServletRequest();
		request.setSecure(true);
		return request;
	}

	@Test
	void validatesConfiguredTokens() {
		new JobLogReadProperties(null).validate();
		new JobLogReadProperties("").validate();
		assertFalse(new JobLogReadProperties("").enabled());
		assertTrue(new JobLogReadProperties(TOKEN).enabled());
		assertThrows(IllegalStateException.class, () -> new JobLogReadProperties("short-token").validate());
		var whitespace = assertThrows(IllegalStateException.class, () -> new JobLogReadProperties(" " + TOKEN).validate());
		assertFalse(whitespace.getMessage().contains(TOKEN));
	}

	@Test
	void refusesAWhitespaceOnlyTokenInsteadOfDisablingTheReader() {
		for (var blank : new String[] { "   ", " ".repeat(JobLogReadProperties.MINIMUM_TOKEN_LENGTH), "\t\n" }) {
			assertTrue(new JobLogReadProperties(blank).enabled());
			var refused = assertThrows(IllegalStateException.class, () -> new JobLogReadProperties(blank).validate());
			assertTrue(refused.getMessage().contains("ZEROYAML_JOB_LOGS_READ_TOKEN"));
		}
	}

	private static JobLogRecord chunk(JobId job, long sequence, JobLogRecord.Stream stream, byte[] content) {
		return new JobLogRecord(job, RUNNER, sequence, OBSERVED, stream, content, false, false, false);
	}

	/** A clock that moves only when the test advances it. */
	private static final class SteppingClock extends Clock {
		private Instant now;

		SteppingClock(Instant start) {
			this.now = start;
		}

		void advance(Duration duration) {
			now = now.plus(duration);
		}

		@Override
		public Instant instant() {
			return now;
		}

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			throw new UnsupportedOperationException("the test clock stays in UTC");
		}
	}

	@TestConfiguration
	@EnableConfigurationProperties({ JobLogReadProperties.class, ServerProperties.class })
	static class ReaderTestConfiguration {
		@Bean
		JobLogStore jobLogStore() {
			return new InMemoryJobLogStore();
		}
	}
}

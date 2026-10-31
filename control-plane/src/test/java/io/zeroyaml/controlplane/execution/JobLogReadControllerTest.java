package io.zeroyaml.controlplane.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import io.zeroyaml.controlplane.domain.job.JobId;
import io.zeroyaml.controlplane.domain.job.RunnerAssignment;

/** Authorization, status mapping, and byte fidelity of the retained-log reader. */
@WebMvcTest(JobLogReadController.class)
@Import(JobLogReadControllerTest.ReaderTestConfiguration.class)
@TestPropertySource(properties = "zeroyaml.job-logs.read.token=" + JobLogReadControllerTest.TOKEN)
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
		mockMvc.perform(get(JobLogReadController.PATH, job.value()).header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
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
		mockMvc.perform(get(JobLogReadController.PATH, job.value()).header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
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
				"Bearer " + TOKEN.substring(1), "bearer " + TOKEN }) {
			var request = get(JobLogReadController.PATH, "not-a-job-id");
			if (authorization != null) {
				request.header(HttpHeaders.AUTHORIZATION, authorization);
			}
			mockMvc.perform(request)
					.andExpect(status().isUnauthorized())
					.andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, "Bearer"));
		}
	}

	@Test
	void mapsMalformedAndUnknownJobIdentities() throws Exception {
		mockMvc.perform(get(JobLogReadController.PATH, "not-a-job-id").header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
				.andExpect(status().isBadRequest());
		mockMvc.perform(get(JobLogReadController.PATH, JobId.newId().value()).header(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN))
				.andExpect(status().isNotFound());
	}

	@Test
	void disabledReaderAnswersNotFoundEvenWithAToken() {
		var logs = new InMemoryJobLogStore();
		var job = JobId.newId();
		logs.append(chunk(job, 1, JobLogRecord.Stream.STDOUT, new byte[] { 'x' }));
		for (var token : new String[] { null, "", "   " }) {
			var disabled = new JobLogReadController(logs, new JobLogReadProperties(token));
			assertEquals(HttpStatus.NOT_FOUND, disabled.read(job.value().toString(), "Bearer " + TOKEN).getStatusCode());
		}
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

	private static JobLogRecord chunk(JobId job, long sequence, JobLogRecord.Stream stream, byte[] content) {
		return new JobLogRecord(job, RUNNER, sequence, OBSERVED, stream, content, false, false, false);
	}

	@TestConfiguration
	@EnableConfigurationProperties(JobLogReadProperties.class)
	static class ReaderTestConfiguration {
		@Bean
		JobLogStore jobLogStore() {
			return new InMemoryJobLogStore();
		}
	}
}

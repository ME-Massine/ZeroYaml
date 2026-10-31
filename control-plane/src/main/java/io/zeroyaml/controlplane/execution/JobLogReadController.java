package io.zeroyaml.controlplane.execution;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import io.zeroyaml.controlplane.domain.job.JobId;

/**
 * Read-only access to retained Job output for operators and tooling.
 *
 * <p>Answers:</p>
 * <ul>
 *   <li>{@code 404 Not Found} for every request while no read token is
 *       configured, so a disabled reader is indistinguishable from an absent one;</li>
 *   <li>{@code 401 Unauthorized} when the {@code Authorization: Bearer} token is
 *       missing or wrong, before the Job identity is even parsed;</li>
 *   <li>{@code 400 Bad Request} for a malformed Job identity;</li>
 *   <li>{@code 404 Not Found} for unknown, evicted, or not yet received output;</li>
 *   <li>{@code 200 OK} with the retained data chunks in sequence order. Content is
 *       raw bytes encoded as base64, so binary output and split UTF-8 survive.</li>
 * </ul>
 *
 * <p>The token is compared through SHA-256 digests in constant time, so neither
 * its content nor its length is revealed by timing. Responses are never cached.
 * Output bytes never enter operational logs; reads are logged with the Job
 * identity only.</p>
 */
@RestController
class JobLogReadController {
	static final String PATH = "/api/v1/jobs/{jobId}/logs";
	private static final String BEARER_PREFIX = "Bearer ";
	private static final Logger log = LoggerFactory.getLogger(JobLogReadController.class);
	private final JobLogStore logs;
	private final byte[] tokenDigest;

	JobLogReadController(JobLogStore logs, JobLogReadProperties properties) {
		this.logs = Objects.requireNonNull(logs);
		this.tokenDigest = properties.enabled() ? sha256(properties.token()) : null;
	}

	@GetMapping(PATH)
	ResponseEntity<JobLogView> read(@PathVariable String jobId,
			@RequestHeader(name = HttpHeaders.AUTHORIZATION, required = false) String authorization) {
		if (tokenDigest == null) {
			return ResponseEntity.notFound().build();
		}
		if (!authorized(authorization)) {
			log.warn("Rejected an unauthenticated Job log read");
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, "Bearer").build();
		}
		JobId id;
		try {
			id = new JobId(UUID.fromString(jobId));
		} catch (IllegalArgumentException exception) {
			return ResponseEntity.badRequest().build();
		}
		var records = logs.snapshot(id);
		if (records.isEmpty()) {
			return ResponseEntity.notFound().build();
		}
		log.info("Served Job log for jobId={} ({} records)", id, records.size());
		return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(JobLogView.of(id, records));
	}

	private boolean authorized(String authorization) {
		if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
			return false;
		}
		return MessageDigest.isEqual(tokenDigest, sha256(authorization.substring(BEARER_PREFIX.length())));
	}

	private static byte[] sha256(String value) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
		} catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException("SHA-256 is required by every Java platform", exception);
		}
	}

	/**
	 * Retained output of one Job. {@code endOfOutput} is false while the Job is
	 * running or when its final marker was lost; {@code truncated} and
	 * {@code incomplete} then stay {@code null} because completeness is unknown.
	 */
	record JobLogView(String jobId, boolean endOfOutput, Boolean truncated, Boolean incomplete, List<Chunk> records) {
		static JobLogView of(JobId jobId, List<JobLogRecord> records) {
			var last = records.getLast();
			var chunks = records.stream().filter(record -> !record.endOfOutput())
					.map(record -> new Chunk(record.sequence(), record.stream().name(), record.observedAt(), record.content()))
					.toList();
			return last.endOfOutput()
					? new JobLogView(jobId.value().toString(), true, last.truncated(), last.incomplete(), chunks)
					: new JobLogView(jobId.value().toString(), false, null, null, chunks);
		}
	}

	/** One data chunk; {@code observedAt} is Runner time of its first byte. */
	record Chunk(long sequence, String stream, Instant observedAt, byte[] content) {
	}
}

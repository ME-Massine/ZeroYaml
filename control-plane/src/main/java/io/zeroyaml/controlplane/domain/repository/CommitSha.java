package io.zeroyaml.controlplane.domain.repository;

import java.util.Locale;
import java.util.regex.Pattern;

/** Validated, canonical Git object identifier. */
public record CommitSha(String value) {
	private static final Pattern COMMIT_SHA_PATTERN = Pattern.compile("(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})");

	public CommitSha {
		if (value == null || !COMMIT_SHA_PATTERN.matcher(value).matches()) {
			throw new IllegalArgumentException("commit SHA must be a 40- or 64-character hexadecimal object ID");
		}
		value = value.toLowerCase(Locale.ROOT);
	}

	@Override
	public String toString() {
		return value;
	}
}

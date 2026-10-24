package io.zeroyaml.controlplane.domain.pipeline.inference;

import java.util.Set;

/**
 * The file listing of one repository revision, as far as pipeline inference needs it.
 *
 * <p>Inference only inspects which files exist, never their content, so a snapshot
 * is a set of repository-relative paths. How the listing is obtained (provider API,
 * checkout) is outside the domain. Construction is the validation point: a path
 * must be relative, use {@code '/'} separators, and contain no empty, {@code "."} or
 * {@code ".."} segment, so a snapshot cannot point outside the repository.</p>
 *
 * @param paths repository-relative file paths, at most {@value #MAX_PATHS}; copied defensively
 */
public record RepositorySnapshot(Set<String> paths) {

	/** Bounds the listing a caller can hand to inference. */
	static final int MAX_PATHS = 100_000;

	public RepositorySnapshot {
		// copyOf rejects a null set and null paths.
		paths = Set.copyOf(paths);
		if (paths.size() > MAX_PATHS) {
			throw new IllegalArgumentException("paths must not contain more than " + MAX_PATHS + " entries");
		}
		paths.forEach(RepositorySnapshot::requireRelativePath);
	}

	boolean contains(String path) {
		return paths.contains(path);
	}

	private static void requireRelativePath(String path) {
		if (path.isBlank() || path.startsWith("/") || path.contains("\\")) {
			throw new IllegalArgumentException("path must be a non-blank relative path using '/': " + path);
		}
		for (var segment : path.split("/", -1)) {
			if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
				throw new IllegalArgumentException("path must not contain empty, '.' or '..' segments: " + path);
			}
		}
	}
}

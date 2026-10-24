package io.zeroyaml.controlplane.domain.pipeline.inference;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import io.zeroyaml.controlplane.domain.job.ExecutionDefinition;
import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.pipeline.Pipeline;
import io.zeroyaml.controlplane.domain.pipeline.PipelineId;
import io.zeroyaml.controlplane.domain.pipeline.PipelineStep;

/**
 * Infers the first supported pipeline from the files at a repository root.
 *
 * <p>Rule: a root {@code pom.xml} is a Maven project and yields two ordered steps,
 * {@code build} (package without tests) then {@code test}. The commands use
 * {@code sh mvnw} when a root {@code mvnw} wrapper exists, because a checkout may
 * not preserve the executable bit, and {@code mvn} otherwise.</p>
 *
 * <p>Inference is a pure function of its arguments: it executes nothing, selects no
 * Runner, and derives the pipeline identity from the source, so the same repository
 * state always yields an equal pipeline. When {@code pom.xml} appears together with
 * another known build file the result is {@link PipelineInference.Ambiguous}; with
 * no supported build file it is {@link PipelineInference.Unsupported}. Only the
 * repository root is inspected.</p>
 */
public final class PipelineInferrer {

	private static final String MAVEN_FILE = "pom.xml";
	private static final String MAVEN_WRAPPER = "mvnw";
	/** Build files of ecosystems this strategy does not support but must not silently ignore. */
	private static final List<String> OTHER_BUILD_FILES = List.of("build.gradle", "build.gradle.kts", "go.mod", "package.json");

	/**
	 * Infers a pipeline for one repository revision.
	 *
	 * @param source repository and immutable revision the pipeline would run against
	 * @param snapshot file listing of that revision
	 * @return the inferred pipeline, or why none was inferred
	 */
	public PipelineInference infer(RepositoryReference source, RepositorySnapshot snapshot) {
		Objects.requireNonNull(source, "source must not be null");
		Objects.requireNonNull(snapshot, "snapshot must not be null");

		var found = new ArrayList<String>();
		if (snapshot.contains(MAVEN_FILE)) {
			found.add(MAVEN_FILE);
		}
		OTHER_BUILD_FILES.stream().filter(snapshot::contains).forEach(found::add);

		if (found.isEmpty()) {
			return new PipelineInference.Unsupported(
					"No supported build file found at the repository root; supported: " + MAVEN_FILE);
		}
		if (found.size() > 1) {
			return new PipelineInference.Ambiguous(found.stream().sorted().toList());
		}
		if (!found.contains(MAVEN_FILE)) {
			return new PipelineInference.Unsupported("Unsupported build file at the repository root: " + found.get(0));
		}
		return new PipelineInference.Inferred(mavenPipeline(source, snapshot.contains(MAVEN_WRAPPER)));
	}

	private static Pipeline mavenPipeline(RepositoryReference source, boolean hasWrapper) {
		var maven = hasWrapper ? List.of("sh", MAVEN_WRAPPER) : List.of("mvn");
		return new Pipeline(deterministicId(source), source, List.of(
				step("build", maven, "--batch-mode", "-DskipTests", "package"),
				step("test", maven, "--batch-mode", "test")));
	}

	private static PipelineStep step(String name, List<String> executable, String... arguments) {
		var command = new ArrayList<>(executable);
		command.addAll(List.of(arguments));
		return new PipelineStep(name, new ExecutionDefinition(command, "."));
	}

	private static PipelineId deterministicId(RepositoryReference source) {
		var seed = source.location() + "@" + source.revision();
		return new PipelineId(UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)));
	}
}

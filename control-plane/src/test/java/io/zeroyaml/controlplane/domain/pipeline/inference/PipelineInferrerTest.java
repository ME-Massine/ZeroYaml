package io.zeroyaml.controlplane.domain.pipeline.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.zeroyaml.controlplane.domain.job.RepositoryReference;
import io.zeroyaml.controlplane.domain.pipeline.PipelineStep;
import io.zeroyaml.controlplane.domain.pipeline.inference.PipelineInference.Ambiguous;
import io.zeroyaml.controlplane.domain.pipeline.inference.PipelineInference.Inferred;
import io.zeroyaml.controlplane.domain.pipeline.inference.PipelineInference.Unsupported;

class PipelineInferrerTest {

	private static final RepositoryReference SOURCE =
			new RepositoryReference(URI.create("https://github.com/acme/app.git"), "abc123");
	private final PipelineInferrer inferrer = new PipelineInferrer();

	private static RepositorySnapshot files(String... paths) {
		return new RepositorySnapshot(Set.of(paths));
	}

	@Test
	void infersOrderedBuildAndTestStepsForMavenWrapperProject() {
		var result = assertInstanceOf(Inferred.class,
				inferrer.infer(SOURCE, files("pom.xml", "mvnw", "src/Main.java")));

		var steps = result.pipeline().steps();
		assertEquals(List.of("build", "test"), steps.stream().map(PipelineStep::name).toList());
		assertEquals(List.of("sh", "mvnw", "--batch-mode", "-DskipTests", "package"),
				steps.get(0).execution().command());
		assertEquals(List.of("sh", "mvnw", "--batch-mode", "test"), steps.get(1).execution().command());
		assertEquals(".", steps.get(0).execution().workingDirectory());
		assertEquals(SOURCE, result.pipeline().source());
	}

	@Test
	void usesInstalledMavenWithoutWrapper() {
		var result = assertInstanceOf(Inferred.class, inferrer.infer(SOURCE, files("pom.xml")));

		assertEquals(List.of("mvn", "--batch-mode", "test"), result.pipeline().steps().get(1).execution().command());
	}

	@Test
	void sameRepositoryStateYieldsEqualPipeline() {
		assertEquals(inferrer.infer(SOURCE, files("pom.xml", "mvnw")), inferrer.infer(SOURCE, files("mvnw", "pom.xml")));
	}

	@Test
	void differentRevisionYieldsDifferentPipelineIdentity() {
		var other = new RepositoryReference(SOURCE.location(), "def456");

		assertNotEquals(inferrer.infer(SOURCE, files("pom.xml")), inferrer.infer(other, files("pom.xml")));
	}

	@Test
	void ignoresBuildFilesBelowTheRoot() {
		assertInstanceOf(Unsupported.class, inferrer.infer(SOURCE, files("service/pom.xml", "README.md")));
	}

	@Test
	void reportsAmbiguityWhenMavenMeetsAnotherBuildSystem() {
		var result = assertInstanceOf(Ambiguous.class,
				inferrer.infer(SOURCE, files("pom.xml", "package.json", "build.gradle")));

		assertEquals(List.of("build.gradle", "package.json", "pom.xml"), result.candidates());
	}

	@Test
	void reportsAmbiguityForTwoUnsupportedBuildSystems() {
		assertInstanceOf(Ambiguous.class, inferrer.infer(SOURCE, files("go.mod", "package.json")));
	}

	@Test
	void reportsUnsupportedForRepositoryWithoutBuildFile() {
		assertInstanceOf(Unsupported.class, inferrer.infer(SOURCE, files("README.md")));
		assertInstanceOf(Unsupported.class, inferrer.infer(SOURCE, files()));
	}

	@Test
	void namesTheUnsupportedBuildFile() {
		var result = assertInstanceOf(Unsupported.class, inferrer.infer(SOURCE, files("go.mod")));

		assertEquals("Unsupported build file at the repository root: go.mod", result.reason());
	}

	@Test
	void rejectsMissingArguments() {
		assertThrows(NullPointerException.class, () -> inferrer.infer(null, files("pom.xml")));
		assertThrows(NullPointerException.class, () -> inferrer.infer(SOURCE, null));
	}

	@ParameterizedTest
	@ValueSource(strings = { "", " ", "/pom.xml", "../pom.xml", "a/../pom.xml", "./pom.xml", "a//pom.xml", "a\\pom.xml", "a/" })
	void rejectsMalformedSnapshotPaths(String path) {
		assertThrows(IllegalArgumentException.class, () -> new RepositorySnapshot(Set.of(path)));
	}

	@Test
	void rejectsNullSnapshotAndOversizedListing() {
		assertThrows(NullPointerException.class, () -> new RepositorySnapshot(null));
		var tooMany = new HashSet<String>();
		for (var i = 0; i <= RepositorySnapshot.MAX_PATHS; i++) {
			tooMany.add("f" + i);
		}
		assertThrows(IllegalArgumentException.class, () -> new RepositorySnapshot(tooMany));
	}

	@Test
	void rejectsInvalidInferenceOutcomes() {
		assertThrows(IllegalArgumentException.class, () -> new Unsupported(" "));
		assertThrows(IllegalArgumentException.class, () -> new Ambiguous(List.of("pom.xml")));
	}
}

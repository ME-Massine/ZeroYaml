package io.zeroyaml.controlplane.domain.pipeline;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import io.zeroyaml.controlplane.domain.job.Job;
import io.zeroyaml.controlplane.domain.job.JobId;

/**
 * Turns a {@link Pipeline} into the executable Jobs that carry it out: one Job
 * per step, in step order.
 *
 * <p>Each Job runs the step's {@link PipelineStep#execution()} against the
 * pipeline's {@link Pipeline#source()}, so the repository location and revision
 * are carried unchanged. Jobs start in
 * {@link io.zeroyaml.controlplane.domain.job.JobStatus#CREATED}; queuing them,
 * selecting a Runner, and dispatching belong to the application boundary.</p>
 *
 * <p>Identity rule: a Job identity is a name-based UUID over the pipeline
 * identity, the step name, the source, and the step's execution (command and
 * working directory), so an identity always implies one Job definition. A
 * repeated webhook delivery or an inference retry for the same plan yields the
 * same identities, which lets the application layer recognize Jobs it already
 * recorded. A changed plan for the same revision yields new identities for the
 * changed steps and never reuses a recorded Job for another definition; Jobs of
 * the earlier plan stay recorded and untouched. Step names are unique within a
 * pipeline, so the identities within one pipeline are distinct.</p>
 *
 * <p>The translation is a pure function of its arguments: it records nothing,
 * selects no Runner, and performs no I/O.</p>
 */
public final class PipelineJobTranslator {

	/**
	 * Creates the Jobs for every step of {@code pipeline}.
	 *
	 * @param pipeline validated pipeline to execute
	 * @param createdAt creation timestamp shared by every Job
	 * @return one Job per step, ordered as {@link Pipeline#steps()}; unmodifiable
	 */
	public List<PipelineJob> translate(Pipeline pipeline, Instant createdAt) {
		Objects.requireNonNull(pipeline, "pipeline must not be null");
		Objects.requireNonNull(createdAt, "createdAt must not be null");

		var jobs = new ArrayList<PipelineJob>(pipeline.steps().size());
		var position = 1;
		for (var step : pipeline.steps()) {
			var job = Job.create(jobIdFor(pipeline, step), pipeline.source(), step.execution(), createdAt);
			jobs.add(new PipelineJob(pipeline.id(), position++, step.name(), job));
		}
		return List.copyOf(jobs);
	}

	private static JobId jobIdFor(Pipeline pipeline, PipelineStep step) {
		var seed = new StringBuilder("pipeline-job:").append(pipeline.id().value());
		append(seed, step.name());
		append(seed, pipeline.source().location().toString());
		append(seed, pipeline.source().revision());
		append(seed, step.execution().workingDirectory());
		step.execution().command().forEach(argument -> append(seed, argument));
		return new JobId(UUID.nameUUIDFromBytes(seed.toString().getBytes(StandardCharsets.UTF_8)));
	}

	// Length prefixes keep distinct inputs from sharing a seed, whatever characters a field contains.
	private static void append(StringBuilder seed, String field) {
		seed.append('|').append(field.length()).append(':').append(field);
	}
}

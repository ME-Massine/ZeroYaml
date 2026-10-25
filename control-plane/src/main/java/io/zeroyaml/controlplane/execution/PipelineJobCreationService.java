package io.zeroyaml.controlplane.execution;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.zeroyaml.controlplane.domain.pipeline.Pipeline;
import io.zeroyaml.controlplane.domain.pipeline.PipelineJobTranslator;

/**
 * Creates and records the executable Jobs for an inferred pipeline.
 *
 * <p>This is the orchestration seam between a decided plan and execution. The
 * domain {@link PipelineJobTranslator} builds one Job per step; this service
 * records them in the {@link JobStore}. It neither selects a Runner nor
 * dispatches anything, and the Jobs stay in
 * {@link io.zeroyaml.controlplane.domain.job.JobStatus#CREATED}.</p>
 *
 * <p>Idempotency: a Job identity is derived from the pipeline identity, the
 * step name, the source, and the step's execution, so an identity always means
 * one Job definition. A repeated webhook delivery or an inference retry for the
 * same plan therefore maps to Jobs that are already recorded; those are
 * returned as recorded and never duplicated or reset, whatever state they have
 * reached. Each Job is recorded through {@link JobStore#recordIfAbsent}, so
 * concurrent calls for the same plan record each Job exactly once, and a call
 * that finds only some Jobs recorded, for example after an earlier call failed
 * part-way, records the missing ones.</p>
 *
 * <p>A changed plan for the same revision, for example after an inference rule
 * changed, yields new identities for the changed steps. Its new Jobs are
 * recorded next to the earlier plan's Jobs, which stay untouched and are not
 * part of the result. Deciding whether the earlier plan's Jobs must be stopped
 * or superseded belongs to the Phase 3 pipeline lifecycle.</p>
 */
@Service
public class PipelineJobCreationService {

	private static final Logger log = LoggerFactory.getLogger(PipelineJobCreationService.class);

	private final JobStore jobs;
	private final Clock clock;
	private final PipelineJobTranslator translator = new PipelineJobTranslator();

	@Autowired
	PipelineJobCreationService(JobStore jobs) {
		this(jobs, Clock.systemUTC());
	}

	/**
	 * Accepts an explicit clock so tests can control creation times. The
	 * registration package already exposes a {@link Clock} bean, so a second one
	 * is not registered here; that would make injection by type ambiguous.
	 */
	PipelineJobCreationService(JobStore jobs, Clock clock) {
		this.jobs = Objects.requireNonNull(jobs, "jobs must not be null");
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
	}

	/**
	 * Records one Job per step of {@code pipeline}, skipping Jobs that are
	 * already recorded.
	 *
	 * @param pipeline validated pipeline to execute
	 * @return a reference to the recorded Job of every step, in pipeline order, and how many were
	 *         new; callers read the Jobs through {@link JobStore#find} and change them only through
	 *         {@link JobStore#update}
	 */
	public PipelineJobCreation createJobs(Pipeline pipeline) {
		Objects.requireNonNull(pipeline, "pipeline must not be null");

		var planned = translator.translate(pipeline, clock.instant());
		var recorded = new ArrayList<PipelineJobReference>(planned.size());
		var createdCount = 0;
		for (var pipelineJob : planned) {
			var job = jobs.recordIfAbsent(pipelineJob.job());
			if (job == pipelineJob.job()) {
				createdCount++;
			}
			recorded.add(new PipelineJobReference(
					pipelineJob.pipelineId(), pipelineJob.position(), pipelineJob.stepName(), job.id()));
		}

		var creation = new PipelineJobCreation(pipeline.id(), recorded, createdCount);
		logCreation(pipeline, creation);
		return creation;
	}

	/**
	 * Records one audit line per call. The repository location is left out
	 * because it can embed credentials; the pipeline identity is enough to
	 * correlate the Jobs with the push that produced them.
	 */
	private static void logCreation(Pipeline pipeline, PipelineJobCreation creation) {
		if (creation.alreadyRecorded()) {
			log.info("Pipeline {}: all {} Jobs were already recorded", pipeline.id().value(), creation.jobs().size());
		} else {
			log.info("Pipeline {}: recorded {} of {} Jobs",
					pipeline.id().value(), creation.createdCount(), creation.jobs().size());
		}
	}
}

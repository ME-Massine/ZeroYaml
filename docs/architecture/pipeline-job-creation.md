# Pipeline Job Creation

A `Pipeline` is a decision; a Runner executes Jobs. Job creation is the
Control Plane seam that turns the first into the second (#23):

```text
PipelineInferrer ──> Pipeline ──> PipelineJobTranslator ──> PipelineJob list ──> PipelineJobCreationService ──> JobStore
     (#22)            (#21)        (domain, pure)          (Job + step link)       (application, idempotent)
```

Runner selection (#24) and `RunJob` dispatch are not part of this seam. The
created Jobs stay in `CREATED`; queuing them, selecting a Runner, and
dispatching belong to the end-to-end coordination work (#30).

## Translation (domain)

`io.zeroyaml.controlplane.domain.pipeline.PipelineJobTranslator` builds one Job
per step and records no state:

| Job field | Source |
| --- | --- |
| `id` | Name-based UUID over the pipeline identity, step name, repository location and revision, working directory, and command arguments (length-prefixed) |
| `repository` | `Pipeline.source()`: repository location and commit revision, unchanged |
| `execution` | `PipelineStep.execution()`: command arguments and working directory, unchanged |
| `createdAt` | The caller's timestamp, shared by every Job of the call |
| `status` | `CREATED` |

Each Job is returned as a `PipelineJob`, which links it to its pipeline, its
step name, and its 1-based position. The Job model itself stays unaware of
pipelines.

- **Order** is the order of `Pipeline.steps()`, never sorted or reordered. It
  does not depend on timestamps, which are equal within one call.
- **Validity** comes from the model: a pipeline with no steps, a duplicate step
  name, an invalid step name, or an empty or blank command cannot be
  constructed, so every step that reaches translation is executable.
- **Identity** is deterministic and covers the Job definition, so an identity
  always means one definition. Step names are unique within a pipeline, so the
  identities in one pipeline are distinct. The same step name in another
  pipeline, or the same step with another command, working directory, or
  source, gives another identity. Length prefixes keep argument boundaries
  (`make test` against `make`, `test`) from sharing an identity.

## Recording and idempotency (application)

`io.zeroyaml.controlplane.execution.PipelineJobCreationService.createJobs`
translates the pipeline and records each Job through `JobStore.recordIfAbsent`,
which atomically records it or returns the Job already recorded under its
identity. That is one store operation per step.

It returns a `PipelineJobCreation` with a `PipelineJobReference` for every
step's recorded Job, in order, and how many of them this call created. A
reference carries the pipeline identity, the step name and position, and the
Job identity, never the Job itself: the recorded Job is a mutable aggregate
owned by the `JobStore`, so callers read it through `JobStore.find` and change
it only through `JobStore.update`, which serializes changes per Job.

| Input | Result |
| --- | --- |
| First pipeline for a revision | Every Job recorded; `createdCount` equals the step count |
| Repeated webhook delivery or inference retry with an equal plan | Nothing recorded; the existing Jobs are returned as recorded, whatever state they reached |
| Concurrent calls with an equal plan | Each Job is recorded exactly once |
| Equal plan after an earlier call failed part-way | Only the missing Jobs are recorded |
| Same revision with a changed plan (changed command, added step) | Jobs with new identities are recorded for the changed steps; unchanged steps map to their recorded Jobs |
| Same revision with a plan that drops a step | The dropped step's Job stays recorded and untouched; it is not part of the result |

A changed plan for an already-processed revision typically follows a change of
inference rules. No recorded Job is ever reused for another definition and no
recorded Job is rewritten, because the identity differs. The earlier plan's
Jobs, including steps the new plan drops, stay recorded in whatever state they
reached.

Each call writes one info log line with the pipeline identity and the created
and total counts. The repository location and revision are not logged: the
location can embed credentials, and the pipeline identity is enough to
correlate the Jobs with the push that produced them.

## Limits

- Jobs are held by the in-memory `JobStore` and lost on restart until durable
  persistence lands (#28). Idempotency therefore holds per Control Plane
  process; a redelivery after a restart creates the Jobs again.
- Memory grows with every distinct plan: at most 50 Jobs per pipeline, never
  evicted before restart. No other store grows with pipelines.
- A changed plan for a revision runs next to the earlier plan's Jobs. Whether
  those must be stopped or superseded, and which plan wins, needs an explicit
  re-run decision that belongs to the Phase 3 pipeline lifecycle.
- A failure part-way through the steps leaves the Jobs recorded so far. A retry
  with the same plan records the rest; it does not roll back.
- The link from a Job back to its step is returned to the caller and can be
  recomputed from the plan and the identities, but it is not stored as such.
  Storing it is part of persistence (#28).
- Nothing calls the service from the webhook path yet. Obtaining the repository
  file listing, calling inference, and handing the Jobs to selection and
  dispatch are #30.

## Out of scope

Runner selection, gRPC dispatch, Docker execution, sequencing the execution of
later steps on earlier results, and pipeline visualization.

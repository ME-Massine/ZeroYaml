# Job and pipeline run persistence

Issue #28 makes PostgreSQL the Control Plane's authoritative Job store. Flyway
`V2__create_pipeline_run_and_job.sql` adds a `pipeline_run` row for a pipeline
identity and repository revision, plus one `job` row for each executable step.
Jobs created outside a pipeline can also be recorded; their pipeline link is
empty until a matching step is recorded.

`PipelineJobCreationService` still derives stable Job identities and uses
`recordIfAbsent`. The database unique keys make redelivery idempotent across
Control Plane restarts. A changed plan for the same revision can add new Jobs;
earlier Jobs retain their definition and lifecycle state, as specified by issue
#23. When unchanged steps move, their stored positions follow the latest
recorded request for each step. The run row groups these Jobs and does not
claim a separate pipeline outcome. Issue #30 decides when to dispatch steps,
and the later pipeline lifecycle work decides how to resolve superseded or
stopped plans.

`JdbcJobStore.update` locks one Job row with `SELECT ... FOR UPDATE` and applies
the core `Job` transition rules inside the same transaction. A rejected change
or thrown callback rolls back. Runner status reports still pass through
`JobExecutionStatusRecorder`, which reconciles duplicate, late, and conflicting
reports before a change is committed. PostgreSQL retains creation, queue,
start, and completion times, the assigned Runner process, and structured
failure code, message, and optional exit code. Timestamp values are truncated
to PostgreSQL microsecond precision when written.

`JobStore.find` looks up a Job by identity. `findByPipeline` requires both the
pipeline identity and repository revision so separate runs cannot mix. Along
with `findByRepository` and `findBySource`, it returns ordered step references
for the first end-to-end scenario; each reference resolves to its current Job
through `find`. These are application queries, not HTTP endpoints. Command arguments
are stored as JSON; logs and command output are not stored in these tables.
Repository locations with URI user info, query, or fragment are refused before
storage because they can carry credentials. The first scenario uses a plain
repository URL.

The migration is additive and does not change `repository_connection`. A
rollback to application code that predates issue #28 leaves the new tables
unused; dropping them would discard run history, so any schema rollback needs
an explicit export or retention decision first. PostgreSQL remains the only
required state service for this feature; Redis and Kafka are not involved.

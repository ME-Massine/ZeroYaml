# Job lifecycle events

Issue #29 publishes a small, versioned stream of Job lifecycle events for
operational consumers such as future observability or audit services.
PostgreSQL stays the source of truth for Job state (see
[Job and pipeline run persistence](./job-persistence.md)). An event tells a
consumer that a transition was committed. It never replaces reading that
state, and the Control Plane never reads events back.

Publication is **off by default**. A Control Plane without Kafka starts,
acknowledges webhooks, and records status reports exactly as before.

## Event types

Only transitions that the Control Plane records today are published:

| `eventType` | Published when | Source of the transition |
| --- | --- | --- |
| `job.accepted` | A pipeline step's Job is recorded for the first time | `PipelineJobCreationService.createJobs` |
| `job.started` | A Runner's report starts a queued Job | `JobExecutionStatusRecorder` |
| `job.succeeded` | A Runner's report completes a Job successfully | `JobExecutionStatusRecorder` |
| `job.failed` | A Runner's report completes a Job with a failure | `JobExecutionStatusRecorder` |

Queueing and cancellation are not published. Nothing in production queues
or cancels a Job yet, so those event types are added together with the code
that performs those transitions. A terminal report for a still-queued Job
(the running report was lost) applies two transitions in one commit and
publishes `job.started` followed by the outcome event.

A duplicate, late, conflicting, or unknown-Job report changes nothing and
publishes nothing. A repeated webhook delivery that finds its Jobs already
recorded publishes nothing either. Replays therefore do not create new events.

## Envelope (schema version 1)

Each Kafka record value is one UTF-8 JSON object:

```json
{
  "schemaVersion": 1,
  "eventId": "4f1d3c1a-8e0b-4a8e-9a57-1b2f0d6c9e21",
  "eventType": "job.failed",
  "occurredAt": "2026-10-26T10:00:35Z",
  "job": { "id": "0f6a3c2e-5b1d-4e8a-9c7f-2d4b6e8a1c3e", "revision": "0123456789abcdef0123456789abcdef01234567" },
  "runner": { "id": "runner-1", "instanceId": "instance-1" },
  "outcome": { "failureCode": "NON_ZERO_EXIT", "exitCode": 2 }
}
```

| Field | Present for | Meaning |
| --- | --- | --- |
| `schemaVersion` | every event | Envelope version, `1` |
| `eventId` | every event | Random UUID, unique per event; use it to drop duplicates |
| `eventType` | every event | One of the types above |
| `occurredAt` | every event | ISO-8601 UTC time of the transition, taken from the recorded Job: creation, start, or completion |
| `job.id` | every event | Job identity, also the record key |
| `job.revision` | every event | Immutable source revision the Job executes |
| `pipeline.id`, `pipeline.stepName`, `pipeline.stepPosition` | `job.accepted` | Pipeline and 1-based step that the Job executes |
| `runner.id`, `runner.instanceId` | `job.started`, `job.succeeded`, `job.failed` | Runner process the Job is assigned to |
| `outcome` | `job.succeeded`, `job.failed` | Always an object, possibly empty |
| `outcome.failureCode` | `job.failed` | Structured failure code, such as `NON_ZERO_EXIT` or `TIMEOUT` |
| `outcome.exitCode` | terminal events, when reported | Command exit code; absent when the command produced none |

The envelope does not include the repository location, which can carry
credentials, the command, the failure message (it comes from execution
output), or logs. A consumer that needs more detail looks the Job up through
the Control Plane. Pipeline membership appears only on `job.accepted`. A
consumer correlates later events with the pipeline through `job.id`.

### Versioning

The topic name carries the major version: `zeroyaml.job-lifecycle.v1`.
Within version 1, the only allowed change is a new optional field, so
consumers must ignore fields they do not recognize. Removing or renaming a
field, changing its meaning, or changing the key requires schema version 2
on a new `...v2` topic. During a migration, both versions can be published.
The envelope is written field by field in `JobLifecycleEventSerializer`, and
`JobLifecycleEventSerializerTest` fixes the exact JSON. A failing test there
means the published contract changed.

## Delivery key and ordering

The record key is the Job identity (`job.id`). All events for one Job go to
one partition, so a consumer can follow a Job's lifecycle by key and join it
to its pipeline through the `job.accepted` event.

Events are queued on one sender thread, and the producer is idempotent (the
Kafka client default), so retries neither reorder nor duplicate records. The
order of Job transitions is serialized by the PostgreSQL row lock, but events
are queued only after that lock is released. When two reports for the same
Job are reconciled concurrently, their events can therefore be queued out of
order. Consumers order one Job's events by lifecycle stage (`accepted`, then
`started`, then the outcome), not by offset.

## Delivery and failure behavior

- **After commit.** `JdbcJobStore` commits each `recordIfAbsent` and
  `update` in its own transaction before returning. Callers publish only after
  that return, so no event can describe a rolled-back transition.
  `JdbcJobStoreTest` checks that each event can already see its transition
  from a different database connection. Events are built while the Job row is
  still locked, so they describe that exact transition and not a later one.
- **Never in the request path.** `publish` only adds the event to a bounded
  in-memory queue (`queue-capacity`, default 1000) and returns. A webhook
  acknowledgment or a status report never waits for Kafka, and a Kafka
  failure never changes or rolls back Job state.
- **Bounded waits.** The sender thread waits at most `max.block.ms` (5 s) for
  metadata or buffer space. Each request is bounded by `request.timeout.ms`
  (10 s), and the whole delivery, including retries, is bounded by
  `delivery.timeout.ms` (30 s). `acks=all` waits for every in-sync replica to
  acknowledge the record.
- **Failures are logged, not retried by the Control Plane.** A full or closed
  queue drops the event with `Dropped <type> event <eventId> for job <jobId>`.
  A delivery that the producer abandons is logged as
  `Failed to publish <type> event <eventId> for job <jobId>: <cause>`. Logs
  never contain the payload.
- **Shutdown.** Closing the publisher stops accepting events and waits up to
  `shutdown-timeout` (default 5 s) for queued events to reach the producer.
  It logs how many events were still queued when that time ran out.

The result is **best-effort, at-most-once publication per committed
transition**. An event can be lost if the broker stays unreachable longer
than the bounds above, if the queue overflows, or if the process stops
between the commit and the send. The lost transition is still recorded in
PostgreSQL. This first deployment makes no exactly-once claim and has no
transactional outbox. If a consumer later needs guaranteed delivery, adding
an outbox table written in the same transaction needs its own issue.

## Configuration

| Property | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `zeroyaml.events.job-lifecycle.enabled` | `ZEROYAML_JOB_EVENTS_ENABLED` | `false` | Publish to Kafka; when false, events are discarded |
| `zeroyaml.events.job-lifecycle.topic` | — | `zeroyaml.job-lifecycle.v1` | Destination topic |
| `zeroyaml.events.job-lifecycle.queue-capacity` | — | `1000` | Events held while the broker is slow; 1 to 100000 |
| `zeroyaml.events.job-lifecycle.shutdown-timeout` | — | `5s` | Drain bound at shutdown |
| `spring.kafka.bootstrap-servers` | `ZEROYAML_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker addresses; the default matches `infra/compose.yaml` |

Enabling publication does not make Kafka a startup dependency. The producer
connects on the first send, and an unreachable broker produces only the
warnings described above. The local broker creates the topic automatically
on the first send, using the broker's defaults. Any other environment
creates the topic with its own partition count and retention before
enabling publication.

## Local verification

Section 9 of the
[local development workflow](../local-development-workflow.md#job-lifecycle-events)
shows how to start Kafka, enable publication, and read the stream with the
console consumer included in the broker image.

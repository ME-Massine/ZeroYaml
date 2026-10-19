# Runner Selection

The Control Plane decides where a queued Job runs; a Runner never selects or
reprioritizes its own work. `RunnerSelector` is the seam for that decision, and
`HealthyRunnerSelector` is the first, deliberately small strategy behind it.

The code lives in `io.zeroyaml.controlplane.scheduling`.

## Eligibility

A Runner is eligible exactly when `RunnerRegistry.availableRunners()` lists it.
The registry owns that policy, so the selector does not re-derive it from
timestamps or flags. A registered Runner is available when all of these hold:

- its liveness is `HEALTHY`: the last acknowledged registration or heartbeat is
  within `zeroyaml.registration.heartbeat-timeout` (default `15s`);
- the state it last reported is `READY`;
- it last reported `accepting_work = true`.

State and `accepting_work` come from the heartbeat, not from registration. A
Runner registers before it knows either value: it becomes `READY` only once the
Control Plane accepted the registration, and it accepts work only if its Docker
daemon also answered at startup. Every heartbeat therefore carries the Runner's
current `status` and `accepting_work`, and the registry replaces the registration
snapshot with them. The Runner sends its first heartbeat immediately after
registration, so it becomes selectable without waiting a heartbeat interval.

Two consequences follow:

- A Runner that is alive but cannot execute, such as one without a Docker
  daemon or one that reports `DRAINING`, is never selected. It cannot keep Jobs away
  from a Runner that can run them, even when its `runnerId` sorts first.
- A Runner that does not report `status` in its heartbeat (one built before the
  field existed) keeps the availability the registry already holds. For such a
  Runner that is the registration snapshot, so it is never selected until it is
  upgraded.

## Selection rule

Among eligible Runners, the one with the lexicographically smallest `runnerId` is
selected. `runnerId` is the registry key and therefore unique, so the order is
total: the same registry state always yields the same Runner, independent of
registration order or the registry's iteration order.

The strategy is stateless. It does not rotate, count load, or reserve the Runner
it returns, so concurrent callers can be handed the same Runner and one Runner
receives all work while it stays available and first in order. Capacity, leases,
and fairness belong to later scheduling work.

## Outcome

`RunnerSelector.select(job)` takes a `QUEUED` Job and returns a
`RunnerSelectionOutcome`. Selecting for a Job in any other state is a caller
error and raises `IllegalStateException`.

`RunnerSelectionOutcome` is a sealed type with two cases, so a dispatch context
exists only where a Runner was selected:

| Case | Meaning | Carries |
| --- | --- | --- |
| `Selected` | An available Runner was chosen. | A `JobDispatchContext` with the Job identity and the Runner's `runnerId` and `instanceId`. |
| `NoAvailableRunner` | No registered Runner is healthy, ready, and accepting work. | The Job identity only. |

`JobDispatchContext` is the decision handed to dispatch: the Job paired with the
Runner process chosen for it. Dispatch reads its target from the context instead
of choosing a Runner itself.

## Enforcing the selection at dispatch

The selected Runner travels with the dispatch. `RunnerClient.dispatchJob(job,
target)` writes the context's Runner into `RunJobRequest.target_runner_id` and
`target_instance_id`, and a Runner process whose own identity differs declines
with `JOB_REJECTION_NOT_TARGET_RUNNER` without starting the Job. The Control
Plane also refuses to record an acceptance that names any other process. See the
[RunJob contract](./run-job-contract.md).

A Job therefore never runs silently on a Runner the Control Plane did not
select, including after a Runner restart that put a new `instanceId` behind a
known address.

## Blocked behavior in the first version

`NoAvailableRunner` is an explicit result, not an exception and not a silent
drop. Selection never modifies the Job: it stays `QUEUED` with no Runner linked,
so the Job aggregate records its assignment only when the selected Runner reports
that it started, as described in the [Core Job Model](./job-model.md).

There is no queue, retry timer, or background loop in this version. Retrying is the
caller's choice: it asks again later, and the next call sees the registry's state
at that time. A Runner that recovers its heartbeat becomes selectable again
without a new registration, as long as its entry was not evicted in the
meantime (see [Limits](#limits)).

## Registry size

Each selection scans the registry once. The registry stays proportional to the
Runners seen recently because every registration first removes the entries that
have been `UNAVAILABLE` for longer than
`zeroyaml.registration.unavailable-retention` (default `10m`). An evicted
`runnerId` is unknown again, so a restarted Runner with a new `instanceId` can
register under it; until then it is an identity conflict. A Runner that keeps
sending heartbeats is never evicted.

## Limits

- The selection is a point-in-time snapshot, not a reservation. A selected Runner
  can stop sending heartbeats or stop accepting work before dispatch, and the
  dispatch then fails through the existing transport-failure or rejection
  handling.
- Availability is as fresh as the last heartbeat. A Runner whose execution slots
  fill up still reports `accepting_work = true`, because that flag covers
  readiness and Docker only; it declines the dispatch with
  `JOB_REJECTION_RUNNER_UNAVAILABLE`, and the stateless selector offers the same
  Runner again. Excluding a Runner after a rejection needs capacity tracking.
- The Runner reports the `status` and `accepting_work` it settled on at startup
  and does not report `DRAINING` when it shuts down; its heartbeat loop simply
  stops. A stopping Runner therefore stays selectable until the heartbeat
  timeout elapses, and a dispatch in that window fails through the existing
  rejection or transport-failure handling.
- The Control Plane still reaches Runners through one configured address
  (`zeroyaml.runner.*`), and the registry stores no per-Runner endpoint. With
  several Runners, a dispatch to a Runner other than the one behind that address
  is declined as not-target rather than delivered. Routing by Runner needs an
  advertised address in the registration contract and is separate work.
- A Runner evicted from the registry while still running, for example after a
  network partition longer than the retention, gets `HEARTBEAT_UNKNOWN_RUNNER`
  and does not re-register on its own yet; it stays unselectable until it is
  restarted.
- Registry state is process-local and in memory, so selection sees only Runners
  registered with this Control Plane instance since it started.
- Nothing yet connects creation, selection, and dispatch end to end. Executable
  Job creation is issue #23 and the end-to-end pipeline scenario is a later Phase 2
  issue.

## Out of scope

Capacity or load scoring, queues and leases, labels or capability matching,
per-Runner routing, autoscaling, Kubernetes integration, and distributed
scheduling.

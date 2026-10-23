# Runner Identity and Registration Contract

The Runner reports facts about its process and execution environment. The
Control Plane owns the registry and makes scheduling decisions.

## Identity fields

| Field | Meaning |
| --- | --- |
| `runner_id` | Stable logical identity configured by the operator. It must remain unchanged across process restarts. |
| `instance_id` | Cryptographically random process identity generated once at startup. A restart receives a new value. |
| `runner_version` | Version of the Runner application. |
| `protocol_version` | Version of the Control Plane-to-Runner protobuf contract. The current value is `runner.v1`. |

The pair `(runner_id, instance_id)` identifies one Runner process. Hostnames,
IP addresses, timestamps, and container IDs are not Runner identity values.

## Capabilities and state

`RunnerCapabilities` contains operating system, architecture, Docker support,
supported executors, and operator-defined labels. A Runner must only report
capabilities that have been verified locally.

`RunnerStatus` currently supports:

- `STARTING`
- `READY`
- `DRAINING`
- `UNAVAILABLE`

`accepting_work` is a Runner-reported fact. The Control Plane must still apply
its own registry, lease, capability, and scheduling rules before assigning work.

The Runner probes its Docker daemon once at startup. Only a daemon that answers
is reported as `docker_available = true` with `supported_executors =
["docker"]`. `GetInfo` reports `accepting_work = true` only when the Runner is
`READY` and Docker is available; otherwise it reports `accepting_work = false`
and every dispatch is rejected as unavailable. The registration request is sent
before the Runner becomes `READY`, so it always carries
`accepting_work = false`. The execution rules are documented in
[`runner-execution-sandbox.md`](./runner-execution-sandbox.md).

## Registration semantics

`RunnerRegistrationService.Register` is the versioned registration boundary
owned by the Control Plane. Persistence, lease expiry, and heartbeat scheduling
are separate follow-up responsibilities.

The Control Plane should handle registration results as follows:

- `REGISTRATION_ACCEPTED`: a new `(runner_id, instance_id)` was registered.
- `REGISTRATION_ALREADY_REGISTERED`: the same pair registered again; the
  operation is idempotent.
- `REGISTRATION_IDENTITY_CONFLICT`: the same `runner_id` is already associated
  with a different active `instance_id`. The Control Plane must not silently
  replace the active process.

After the previous registration lease expires, a new process using the same
`runner_id` may register its new `instance_id`.

## Heartbeat and liveness

`RunnerRegistrationService.Heartbeat` reuses the `(runner_id, instance_id)`
identity pair rather than a new identity shape, so the Control Plane
reconciles a heartbeat against the same registry entry that `Register`
created.

A registered Runner sends a heartbeat at a fixed interval
(`ZEROYAML_RUNNER_HEARTBEAT_INTERVAL`, default `5s`), each attempt bounded by
its own timeout (`ZEROYAML_RUNNER_HEARTBEAT_TIMEOUT`, default `5s`). The loop
only starts once registration accepted the Runner, and it stops when the
Runner shuts down; a failed or declined heartbeat is logged and never stops
the loop or the process.

The Control Plane tracks `lastSeenAt` per registered Runner, advanced by both
registration and every acknowledged heartbeat, and derives liveness from it on
demand rather than through a background sweep:

- `HEALTHY`: a registration or heartbeat was recorded within
  `zeroyaml.registration.heartbeat-timeout` (default `15s`).
- `UNAVAILABLE`: no registration or heartbeat was recorded within the timeout.
  The next acknowledged heartbeat makes the same registry entry `HEALTHY`
  again; recovery never creates a duplicate entry.
- `UNKNOWN`: no Runner has ever registered under that `runner_id`.

A heartbeat for a `runner_id` that never registered, or that is registered
under a different `instance_id`, is answered with
`HEARTBEAT_UNKNOWN_RUNNER` instead of being silently accepted; the Runner
must re-register rather than keep retrying the heartbeat. This is a
last-seen policy, not a distributed failure detector: there is no quorum,
and the Runner never determines its own global scheduling state.

## Compatibility

The protobuf package is `zeroyaml.runner.v1`. New fields must use new field
numbers, and removed field numbers must never be reused. Older clients ignore
unknown fields, while a client that does not understand a required protocol
version must reject the registration with an explicit compatibility error.

Generated Go and Java bindings come from
`proto/runner/v1/runner.proto`; generated files must not be edited manually.

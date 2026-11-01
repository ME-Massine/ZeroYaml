# Runner Docker Execution Sandbox

This document describes how a Runner executes an accepted Job inside Docker
containers. The Control Plane decides which Job runs and where; the Runner owns
the container lifecycle, the workspace, and the process execution.

## Acceptance

A Runner accepts Jobs only when both of these hold at startup:

- the Control Plane accepted its registration, so its status is `READY`;
- its Docker daemon answered `docker version` within five seconds.

A Runner that fails the Docker probe still serves `Ping` and `GetInfo` for
diagnosis, reports `docker_available = false` and `accepting_work = false`, and
rejects every dispatch with `JOB_REJECTION_RUNNER_UNAVAILABLE`. A Runner that
passes reports `docker_available = true` and `supported_executors = ["docker"]`.

After contract validation, `RunJob` maps the Job onto a container execution and
starts it in the background before it acknowledges the dispatch. The
acknowledgment therefore means that an execution was started. Dispatch outcomes
that depend on the sandbox:

| Condition | Answer |
| --- | --- |
| Execution started | `OK`, `JOB_ACCEPTED` |
| Every execution slot is busy (`ZEROYAML_RUNNER_MAX_CONCURRENT_JOBS`) | `OK`, `JOB_REJECTED`, `JOB_REJECTION_RUNNER_UNAVAILABLE` |
| Runner is shutting down | `OK`, `JOB_REJECTED`, `JOB_REJECTION_RUNNER_UNAVAILABLE` |
| Job cannot be mapped to a container on this Runner | gRPC `FAILED_PRECONDITION` |

`FAILED_PRECONDITION` covers a repository scheme the Runner does not fetch, a
`file://` location outside the configured source root, and a revision that is
not a plain commit SHA or reference name. Dispatching the same Job to the same
Runner again cannot succeed.

## Execution inputs

| Input | Source |
| --- | --- |
| Command | `job.execution.command`, used as the exact argument vector: the first element becomes the container entrypoint and the rest its arguments. No shell interprets it. |
| Working directory | `job.execution.working_directory`, resolved below `/workspace` in the container. |
| Repository and revision | `job.repository.location` and `job.repository.revision`. |
| Image | `ZEROYAML_RUNNER_JOB_IMAGE` (default `alpine:3.22`). |
| Timeout | `ZEROYAML_RUNNER_JOB_TIMEOUT` (default `10m`), covering the checkout and the command. |

The `runner.v1` contract does not carry an image or a timeout yet, so both are
Runner configuration for now. They are operator settings, never
repository-specific decisions made in Go. Carrying them in `JobExecution` is a
separate, cross-service contract change.

## Workspace strategy

Each execution gets a random 12-character execution ID and three Docker
resources named after it and labelled `io.zeroyaml.managed=true` and
`io.zeroyaml.execution-id=<id>`:

1. `zeroyaml-<id>-workspace`, a Docker volume mounted at `/workspace`.
2. `zeroyaml-<id>-checkout`, a container from `ZEROYAML_RUNNER_CHECKOUT_IMAGE`
   (default `alpine/git:v2.49.1`) that runs `git init`, a depth-1 `git fetch` of
   the one revision, and a detached checkout into the volume.
3. `zeroyaml-<id>-job`, a container from the Job image that runs the command in
   the resolved working directory.

The location and revision reach the checkout script as positional parameters,
never as script text, and git receives them after `--`. Accepted repository
locations:

- `https://` and `http://`: fetched over the network from inside the checkout
  container.
- `file://`: accepted only when `ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT` is set and
  the path resolves, after following symbolic links, inside that directory. The
  directory is bind-mounted read-only at `/source`. It is disabled by default,
  because mounting an arbitrary host path would let a dispatch read the
  Runner host.

Every other scheme, including `ssh://` and git's `ext::` transport, is refused.

## Resource bounds

Every container runs with:

- `--memory 2GiB` and the same `--memory-swap`, so the limit is not bypassed
  through swap;
- `--pids-limit 512`;
- `--security-opt no-new-privileges`;
- `--pull missing`, so a pinned image is pulled once and then reused.

No container is privileged, receives host environment variables, or mounts the
Docker socket. Containers use Docker's default bridge network, because the
checkout and typical builds need network access.

## Lifecycle and cleanup

The Runner drives the Docker CLI with explicit argument vectors: `volume
create`, then for each container `create`, `start`, `wait`, and `rm --force`,
and finally `volume rm --force`. Containers are removed explicitly rather than
with `--rm`, so `docker wait` can always read the exit code first.

Every resource is removed before the execution returns, whether the command
succeeded, failed, timed out, or was cancelled. Removal runs with its own
30-second context detached from the execution, so a cancelled or timed-out
execution still cleans up. Every failed `create` is followed by a removal by
name, because a create that reports an error never proves that nothing was
created: the daemon may have created the resource and then lost the response,
for example when the CLI was interrupted during a slow image pull or the
connection broke. That removal usually finds nothing, so its failure is logged
at `DEBUG`; a failed removal of a resource that was created is logged at `WARN`
with the execution ID. Neither ever changes the reported result. Leftovers can
be listed with:

```powershell
docker ps --all --filter label=io.zeroyaml.managed=true
docker volume ls --filter label=io.zeroyaml.managed=true
```

## Results and status reporting

Each accepted Job reports `JOB_EXECUTION_RUNNING` when it starts and one
terminal report through `JobExecutionStatusService`:

| Sandbox outcome | Reported state | Failure reason | Exit code |
| --- | --- | --- | --- |
| Command exited 0 | `SUCCEEDED` | - | `0` |
| Command exited non-zero | `FAILED` | `NON_ZERO_EXIT` | command exit code |
| Timeout exceeded | `FAILED` | `TIMEOUT` | absent |
| Runner shutdown | `FAILED` | `CANCELLED` | absent |
| Docker unreachable, checkout failed, or container could not start | `FAILED` | `EXECUTION_ERROR` | absent |

A failure message names the stage that failed and, for Docker errors, the last
512 bytes of the Docker CLI's standard error. It never contains command output
or the repository location. The terminal report is sent even during shutdown,
bounded by `ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT`.

Each execution also writes one `job execution finished` log record with the
`job_id`, `execution_id`, `outcome`, `duration`, and `exit_code`.

## Concurrency and shutdown

At most `ZEROYAML_RUNNER_MAX_CONCURRENT_JOBS` executions (default `1`, maximum
`16`) run at once. Executions run under the Runner process lifetime rather than
under the `RunJob` RPC, because the acknowledgment ends the RPC long before the
command finishes. On `SIGINT` or `SIGTERM` the Runner stops accepting Jobs,
cancels running executions (their containers are force-removed), and waits for
each execution to clean up and send its terminal report before the process
exits.

That wait is bounded. `ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT` is one budget for the
whole shutdown sequence, and it starts when the signal arrives: the RPC drain
and the execution drain share it, so neither Docker cleanup nor a slow status
report can hold the process past the configured deadline. A drain that runs out
of budget logs `runner exited before every job execution finished` with the
number of executions still finishing; their Docker resources carry the
`io.zeroyaml.managed=true` label and the `io.zeroyaml.execution-id` of the
execution, so anything left behind stays attributable and removable.

## Known limits

- A remote location with embedded credentials is visible in the checkout
  container's configuration (`docker inspect`) while that container exists.
- The workspace volume is owned by root. An image whose default user is not root
  cannot write to `/workspace`.
- Command stdout/stderr is captured with bounded, best-effort delivery to the
  Control Plane. See [Runner job logs](./runner-job-logs.md) for ordering,
  truncation, retention, and interruption semantics.
- The sandbox targets local development. Multi-tenant isolation hardening, such
  as user namespaces, seccomp profiles, and network policy, is out of scope.

## Verification

The default `go test ./...` run covers request mapping, Docker argument
construction, lifecycle ordering, cleanup on every outcome, and status mapping
with a scripted Docker CLI. Tests that start real containers are opt-in, because
they need a Docker daemon, a git binary, and the pinned images:

```powershell
Push-Location runner
$env:ZEROYAML_RUNNER_DOCKER_TESTS = '1'
go test -count=1 -run TestDockerExecution ./internal/sandbox/
Remove-Item Env:ZEROYAML_RUNNER_DOCKER_TESTS
Pop-Location
```

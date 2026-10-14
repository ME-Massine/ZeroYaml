# Local End-to-End Development Workflow

This document is the reproducible path from a clean checkout to a verified
local ZeroYAML environment: which services to start and in which order, how to
configure them, how to send the supported signed GitHub `push` delivery, what
each boundary is expected to log and persist, and what to check when a boundary
fails.

It is a runbook. Reference material it would otherwise duplicate stays in
[`DEVELOPER_GUIDE.md`](../DEVELOPER_GUIDE.md) and in
[`docs/architecture/`](./architecture); this document links to it at the step
where it is needed.

Out of scope: production deployment, Kubernetes, and user-facing product
documentation.

## 1. The rule every step follows

> The Control Plane decides and coordinates; the Runner executes.

The Control Plane terminates the GitHub webhook, authenticates the delivery,
owns connected-repository metadata, owns authoritative Job state, and decides
what runs. The Runner executes one dispatched Job in Docker and reports what
that execution did. A Runner never learns scheduling, retry, or pipeline
policy, and the Control Plane never runs a Job command itself. A local
observation that contradicts that split is a defect, not a configuration
question.

## 2. What the documented path covers today

Phase 2 is in delivery, so the push-to-execution chain is wired at both ends
and not yet joined in the middle. The table states which step this document can
reproduce and which it cannot, so missing wiring stays distinguishable from a
broken environment.

| Step | State | Where |
| --- | --- | --- |
| Local infrastructure starts | Wired | [Section 4](#4-startup-order) |
| Control Plane starts, applies migrations, serves HTTP and the Runner-facing gRPC endpoint | Wired | [Section 4](#4-startup-order) |
| Runner starts, probes Docker, registers, heartbeats | Wired | [Section 4](#4-startup-order) |
| Connected repository metadata is persisted | Wired; no HTTP API yet | [Section 6](#6-connect-a-repository) |
| A signed `push` delivery is authenticated and accepted | Wired | [Section 7](#7-trigger-the-supported-push-scenario) |
| An accepted delivery is normalized into an internal event | Not wired — [#20](https://github.com/MohamedMBG/ZeroYaml/issues/20) | [Section 7](#7-trigger-the-supported-push-scenario) |
| A pipeline is inferred for the repository | Not wired — [#21](https://github.com/MohamedMBG/ZeroYaml/issues/21), [#22](https://github.com/MohamedMBG/ZeroYaml/issues/22) | [Section 9](#9-expected-states) |
| Executable Jobs are created and recorded | Not wired — [#23](https://github.com/MohamedMBG/ZeroYaml/issues/23) | [Section 9](#9-expected-states) |
| A healthy Runner is selected and the Job is dispatched | Not wired — [#24](https://github.com/MohamedMBG/ZeroYaml/issues/24) | [Section 9](#9-expected-states) |
| A dispatched Job runs in Docker and reports running and terminal status | Wired | [Section 8](#8-observe-one-job-execution) |
| Job logs are streamed and captured | Not wired — [#26](https://github.com/MohamedMBG/ZeroYaml/issues/26) | [Section 9](#9-expected-states) |
| Job state survives a Control Plane restart | Not wired — [#28](https://github.com/MohamedMBG/ZeroYaml/issues/28) | [Section 9](#9-expected-states) |
| Lifecycle events are published | Not wired — [#29](https://github.com/MohamedMBG/ZeroYaml/issues/29) | [Section 3](#3-prerequisites) |
| One push produces one executed pipeline | Not wired — [#30](https://github.com/MohamedMBG/ZeroYaml/issues/30) | [Section 9](#9-expected-states) |
| An automated test covers the whole chain | Not wired — [#31](https://github.com/MohamedMBG/ZeroYaml/issues/31) | [Section 11](#11-automated-coverage) |

Sections 7 and 8 are therefore two verifiable halves rather than one continuous
run: a delivery stops at the Control Plane audit record, and an execution is
started by dispatching a Job directly to the Runner. Issue
[#30](https://github.com/MohamedMBG/ZeroYaml/issues/30) joins them, and
[#31](https://github.com/MohamedMBG/ZeroYaml/issues/31) adds the automated test
this document must then be checked against.

## 3. Prerequisites

| Tool | Needed for |
| --- | --- |
| Git | Checkout, and the local repository used as a Job source |
| Docker Desktop with Compose | Infrastructure, Control Plane tests, and every Job execution |
| Java 21 | Control Plane |
| Go, version from `runner/go.mod` | Runner |
| `curl` | Sending a local webhook delivery |
| `grpcurl` | Dispatching a Job by hand ([Section 8](#8-observe-one-job-execution)) |
| `perl` with the core `Digest::SHA` module | Signing a local delivery from bash ([Section 7.1](#71-signed-local-delivery)); ships with Linux and macOS |

No host PostgreSQL client is needed: every persistence check runs `psql`
inside the `zeroyaml-postgres` container.

PowerShell blocks are for Windows and run unchanged on Windows PowerShell 5.1
and PowerShell 7. Bash blocks have been executed only under Git Bash on
Windows. They have not been run on Linux or macOS, where `openssl`, `curl`,
`mktemp`, `date`, `sed`, and `grpcurl` can behave differently, so treat them as
unverified on those platforms until a run there is recorded.

Verify the toolchain before starting anything:

```powershell
git --version
java -version
go version
docker --version
docker compose version
```

`infra/compose.yaml` also defines Redis and Kafka. Neither has a consumer yet —
the Control Plane declares no Redis or Kafka dependency — so this workflow
starts PostgreSQL only. Lifecycle event publishing is tracked as
[#29](https://github.com/MohamedMBG/ZeroYaml/issues/29).

## 4. Startup order

The order matters in one direction only: the Control Plane needs PostgreSQL to
start at all, and the Runner needs the Control Plane to accept it into the
registry. A Runner started earlier does not crash — it stays unavailable and
rejects every dispatch until it is restarted after the Control Plane is up,
because startup registration and the Docker probe each run once.

### 4.1 Infrastructure

```powershell
docker compose -f .\infra\compose.yaml up -d postgres
docker compose -f .\infra\compose.yaml ps
```

```bash
docker compose -f ./infra/compose.yaml up -d postgres
docker compose -f ./infra/compose.yaml ps
```

Expect `zeroyaml-postgres` running with `5432` published. Its data lives in the
`postgres-data` volume, so the schema and connected-repository rows survive a
restart of the stack.

### 4.2 Control Plane

Both required secrets come from the environment; read
[Section 5](#5-configuration-and-secret-handling) before running this.

The webhook secret is generated once into a file in the home directory, outside
the checkout, and loaded from there. The signed delivery in
[Section 7.1](#71-signed-local-delivery) is sent from a different shell and
needs the identical value; loading it from the file in both shells shares it
without ever printing it.

```powershell
$secretFile = Join-Path $HOME '.zeroyaml-webhook-secret'
if (-not (Test-Path $secretFile)) {
  $bytes = New-Object byte[] 32
  $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
  $rng.GetBytes($bytes)
  $rng.Dispose()
  [IO.File]::WriteAllText($secretFile, (($bytes | ForEach-Object { $_.ToString('x2') }) -join ''))
}

$env:ZEROYAML_DATABASE_PASSWORD = 'zeroyaml'
$env:ZEROYAML_GITHUB_WEBHOOK_SECRET = [IO.File]::ReadAllText($secretFile)

Push-Location control-plane
.\mvnw.cmd --batch-mode spring-boot:run
Pop-Location
```

```bash
secret_file="$HOME/.zeroyaml-webhook-secret"
[ -f "$secret_file" ] || (umask 077; openssl rand -hex 32 | tr -d '\n' > "$secret_file")

export ZEROYAML_DATABASE_PASSWORD='zeroyaml'
export ZEROYAML_GITHUB_WEBHOOK_SECRET="$(cat "$secret_file")"

cd control-plane && ./mvnw --batch-mode spring-boot:run
```

`RandomNumberGenerator` is a cryptographically secure source on Windows
PowerShell 5.1 and PowerShell 7 alike; `Get-Random` is not, so it is not used
for a secret.

Expected during startup:

- Flyway applies `V1__create_repository_connection.sql` and reports the schema
  version it migrated to, before the service accepts traffic;
- the HTTP port is bound on `8080`, the Spring Boot default, because
  `application.properties` sets no `server.port`;
- the runner-facing gRPC endpoint is bound on `zeroyaml.registration.port`
  (default `50052`). It hosts `RunnerRegistrationService` and
  `JobExecutionStatusService` on one listener, so a Runner is configured with a
  single Control Plane address.

Confirm it is serving:

```powershell
curl.exe -s http://localhost:8080/actuator/health
```

A missing webhook secret or database password stops startup with an actionable
message; both are listed in [Section 10](#10-troubleshooting).

### 4.3 Runner

In a second shell. `ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT` is only needed for the
`file://` Job source used in [Section 8](#8-observe-one-job-execution).

```powershell
New-Item -ItemType Directory -Force -Path 'C:\zeroyaml-sources' | Out-Null
$env:ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT = 'C:\zeroyaml-sources'

Push-Location runner
go run ./cmd/runner
Pop-Location
```

```bash
mkdir -p "$HOME/zeroyaml-sources"
export ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT="$HOME/zeroyaml-sources"

cd runner && go run ./cmd/runner
```

Expected output, in this order:

```text
level=INFO msg="docker daemon available" docker_version=...
Runner local-runner registered with the Control Plane at localhost:50052 - registration <uuid> - result accepted
ZeroYAML Runner gRPC server listening on :50051 - runner local-runner - instance <uuid> - version 0.1.0 - protocol runner.v1
```

A Runner accepts Jobs only when registration was accepted **and** the Docker
daemon answered the startup probe. If either fails, it keeps serving `Ping` and
`GetInfo` for diagnosis but rejects every dispatch with
`JOB_REJECTION_RUNNER_UNAVAILABLE` rather than failing later inside the
sandbox. The heartbeat loop starts only after an accepted registration and then
sends a heartbeat every `ZEROYAML_RUNNER_HEARTBEAT_INTERVAL` (default `5s`);
the Control Plane reports the registry entry `UNAVAILABLE` once
`zeroyaml.registration.heartbeat-timeout` (default `15s`) passes without one.

Each Runner process generates a new instance identifier, and the Control Plane
registry keeps the first instance registered under a `ZEROYAML_RUNNER_ID` for
as long as the Control Plane process lives. A Runner restarted after an
accepted registration is therefore rejected with an identity conflict and stays
unavailable. Restart the Control Plane before restarting such a Runner, or
start the Runner under a different `ZEROYAML_RUNNER_ID`.

`:50051` is the Runner's configurable default, not a fixed port. Override it
with `ZEROYAML_RUNNER_GRPC_ADDRESS` and point the Control Plane at the new
value with `ZEROYAML_RUNNER_PORT`.

## 5. Configuration and secret handling

The complete property and environment-variable tables are in
[`DEVELOPER_GUIDE.md`](../DEVELOPER_GUIDE.md#10-prerequisites). The values this
workflow will not start without:

| Setting | Component | Local value |
| --- | --- | --- |
| `ZEROYAML_GITHUB_WEBHOOK_SECRET` | Control Plane | Generated per developer; at least 16 characters |
| `ZEROYAML_DATABASE_PASSWORD` | Control Plane | `zeroyaml`, matching `infra/compose.yaml` |
| `ZEROYAML_DATABASE_URL` | Control Plane | Default `jdbc:postgresql://localhost:5432/zeroyaml` |
| `ZEROYAML_CONTROLPLANE_ADDRESS` | Runner | Default `localhost:50052` |
| `ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT` | Runner | Absolute directory holding local Job sources; empty disables `file://` |

### Handling the webhook secret safely

- Generate a high-entropy value per developer and per environment. The Control
  Plane refuses to start on a secret shorter than 16 characters, because a
  guessable secret makes signature verification decorative.
- Keep it in the shell environment or an approved secret store. It has no value
  in `application.properties` on purpose, and it must never be committed,
  pasted into an issue or pull request, or written to a tracked file.
- Set the identical value on the GitHub webhook. A mismatch is
  indistinguishable from a forged delivery and is answered `401`.
- Never reuse a shared or production secret for local work, and rotate the
  local value if it was ever echoed into a terminal transcript or a log file.
- The verifier authenticates every delivery against this one configured secret.
  Per-repository secret resolution is not wired: the
  `webhook_secret_reference` column records only the *name* of a secret, never
  its value, and nothing reads it during verification yet.

Startup validation is deliberate. Invalid configuration fails at startup with
an actionable message instead of failing on the first delivery.

## 6. Connect a repository

Issues [#18](https://github.com/MohamedMBG/ZeroYaml/issues/18) and
[#19](https://github.com/MohamedMBG/ZeroYaml/issues/19) delivered the
`RepositoryConnection` aggregate and its PostgreSQL store. No HTTP API exposes
them yet — `GitHubWebhookController` is the Control Plane's only REST
controller — so a local connection record is inserted directly. Webhook
authentication does not depend on this record; it is what later phases read to
decide which repository a delivery belongs to.

```powershell
docker exec -i zeroyaml-postgres psql -U zeroyaml -d zeroyaml -c "INSERT INTO repository_connection (provider, owner, name, default_branch, webhook_secret_reference, status, connected_at, status_changed_at) VALUES ('GITHUB', 'mohamedmbg', 'zeroyaml', 'main', 'local-dev-webhook-secret', 'ACTIVE', now(), now());"
```

The table constraints mirror the domain rules, so a row that contradicts them
is refused: `provider` must be `GITHUB`, `owner` and `name` must be lowercase,
`status` must be one of `PENDING`, `ACTIVE`, `SUSPENDED`, `DISCONNECTED`,
`status_changed_at` must not precede `connected_at`, and one repository can be
connected only once. `webhook_secret_reference` holds a name only; storing a
secret value there would be a security defect.

Read it back:

```powershell
docker exec -i zeroyaml-postgres psql -U zeroyaml -d zeroyaml -c "SELECT provider, owner, name, default_branch, status FROM repository_connection;"
```

The table layout, the duplicate rule, and the locking behavior are documented
in
[`repository-connection-model.md`](./architecture/repository-connection-model.md#persistence).

## 7. Trigger the supported push scenario

`push` is the only supported event. The Control Plane answers `202 Accepted`
for a verified `push`, `200 OK` with outcome `IGNORED` for `ping` and every
other verified event, and rejects an invalid envelope or signature before any
downstream processing. The full validation order and response contract are in
[`github-webhook-ingress.md`](./architecture/github-webhook-ingress.md).

### 7.1 Signed local delivery

This reproduces exactly how GitHub signs a delivery, needs no public tunnel,
and is the form a reviewer can run.

The body is written to a file and the digest is computed over that file's
bytes, then the same file is sent with `--data-binary`. Passing the JSON as a
command-line argument is avoided on purpose: Windows PowerShell 5.1 strips the
embedded double quotes from arguments handed to a native executable, so the
bytes sent would no longer match the bytes signed and the Control Plane would
answer `401`. The secret is loaded from the file created in
[Section 4.2](#42-control-plane), because this shell is not the one that
started the Control Plane.

```powershell
$secretFile = Join-Path $HOME '.zeroyaml-webhook-secret'
$secret = [IO.File]::ReadAllText($secretFile)
$payloadFile = Join-Path ([IO.Path]::GetTempPath()) 'zeroyaml-push.json'
[IO.File]::WriteAllText($payloadFile, '{"ref":"refs/heads/main","after":"0f4b1a1e2c3d4e5f60718293a4b5c6d7e8f90a1b"}')

$hmac = New-Object System.Security.Cryptography.HMACSHA256 -ArgumentList (,[Text.Encoding]::UTF8.GetBytes($secret))
$digest = ($hmac.ComputeHash([IO.File]::ReadAllBytes($payloadFile)) |
  ForEach-Object { $_.ToString('x2') }) -join ''

curl.exe -i -X POST http://localhost:8080/webhooks/github `
  -H "Content-Type: application/json" `
  -H "X-GitHub-Event: push" `
  -H "X-GitHub-Delivery: 72d3162e-cc78-11e3-81ab-4c9367dc0958" `
  -H "X-Hub-Signature-256: sha256=$digest" `
  --data-binary "@$payloadFile"
```

```bash
payload_file="$(mktemp)"
printf '%s' '{"ref":"refs/heads/main","after":"0f4b1a1e2c3d4e5f60718293a4b5c6d7e8f90a1b"}' > "$payload_file"

digest="$(perl -MDigest::SHA=hmac_sha256_hex -e '
  local $/;
  open my $key, "<:raw", $ARGV[0] or die "secret file: $!";
  open my $body, "<:raw", $ARGV[1] or die "payload file: $!";
  print hmac_sha256_hex(scalar <$body>, scalar <$key>);
' "$HOME/.zeroyaml-webhook-secret" "$payload_file")"

curl -i -X POST http://localhost:8080/webhooks/github \
  -H "Content-Type: application/json" \
  -H "X-GitHub-Event: push" \
  -H "X-GitHub-Delivery: 72d3162e-cc78-11e3-81ab-4c9367dc0958" \
  -H "X-Hub-Signature-256: sha256=$digest" \
  --data-binary "@$payload_file"
```

The bash block hands `perl` the two file paths and lets it read the key itself.
`openssl dgst -hmac "$secret"` is avoided on purpose: it puts the secret on the
command line, where any local user can read it from the process list, and
`openssl` offers no way to read an HMAC key from a file.

Expected response:

```text
HTTP/1.1 202

{"deliveryId":"72d3162e-cc78-11e3-81ab-4c9367dc0958","event":"push","outcome":"ACCEPTED","message":"delivery accepted for processing"}
```

Expected Control Plane log lines, in this order:

```text
INFO  .g.w.LoggingGitHubWebhookDeliveryHandler : GitHub delivery 72d3162e-... for event push recorded without further processing (<n> payload bytes)
INFO  i.z.c.g.webhook.GitHubWebhookController  : Accepted GitHub delivery 72d3162e-... for event push (<n> payload bytes)
```

The controller logs `Accepted` only after the delivery handler returns, so the
handler line comes first and is the current end of the chain. The delivery handler writes an
audit record and starts no pipeline work, which is the expected behavior until
[#20](https://github.com/MohamedMBG/ZeroYaml/issues/20) normalizes accepted
deliveries. Neither line contains the payload or the signature.

Each negative check below proves the authentication gate rather than the happy
path, so each is worth running once:

| Change to the request | Expected |
| --- | --- |
| Corrupt one character of the digest | `401`, outcome `REJECTED`, `WARN` log, no handler line |
| Drop the `X-Hub-Signature-256` header | `401`, outcome `REJECTED` |
| `X-GitHub-Event: ping`, same body and digest | `200`, outcome `IGNORED` |
| `X-GitHub-Event: pull_request`, same body and digest | `200`, outcome `IGNORED` |
| Drop the `X-GitHub-Delivery` header | `400`, outcome `REJECTED` |
| `Content-Type: text/plain` | `415`, outcome `REJECTED` |
| Body above `zeroyaml.github.webhook.max-payload-size` | `413`, outcome `REJECTED` |

The signature covers the body bytes only, so changing a header never requires a
new digest. An unverified delivery is rejected before the event allow-list is
consulted, so an unauthenticated caller cannot learn which events the endpoint
acts on.

### 7.2 Delivery from GitHub itself

> Not executed end to end; verify when
> [#31](https://github.com/MohamedMBG/ZeroYaml/issues/31) lands. Unlike
> [Section 7.1](#71-signed-local-delivery), this procedure has not been run
> against a real repository webhook.

To drive the same endpoint from a real repository, expose port `8080` through a
tunnel of your choice and configure the repository webhook with the tunnel URL
plus `/webhooks/github`, content type `application/json`, the same secret as
`ZEROYAML_GITHUB_WEBHOOK_SECRET`, and the `push` event only. GitHub sends one
`ping` when the webhook is created: a `200` with outcome `IGNORED` confirms the
secret matches. GitHub's **Recent Deliveries** view then shows each push with
the Control Plane's response body, and any delivery can be redelivered from
there instead of pushing again.

## 8. Observe one job execution

Nothing in the Control Plane creates or dispatches a Job yet
([#23](https://github.com/MohamedMBG/ZeroYaml/issues/23),
[#24](https://github.com/MohamedMBG/ZeroYaml/issues/24)), so the execution half
is exercised by dispatching one `RunJob` by hand. This verifies the Runner
sandbox, the status-report contract, and the Control Plane's reconciliation of
a report — the parts [#30](https://github.com/MohamedMBG/ZeroYaml/issues/30)
will drive from a push.

Prepare a source repository below `ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT`. Start in
the repository root: both blocks return there, and the dispatch that follows
resolves `-import-path proto` against it.

```powershell
New-Item -ItemType Directory -Force -Path C:\zeroyaml-sources\demo | Out-Null
Push-Location C:\zeroyaml-sources\demo
git init --initial-branch=main
'hello' | Out-File -Encoding ascii greeting.txt
git add greeting.txt
git commit -m "add greeting"
$revision = git rev-parse --verify HEAD
Pop-Location
if (-not $revision) { throw 'No commit exists in the demo repository. Set git user.name and user.email, then run this block again.' }
```

```bash
mkdir -p "$HOME/zeroyaml-sources/demo"
cd "$HOME/zeroyaml-sources/demo"
git init --initial-branch=main
echo 'hello' > greeting.txt
git add greeting.txt
git commit -m "add greeting"
revision="$(git rev-parse --verify HEAD)"
cd -
[ -n "$revision" ] || echo 'No commit exists in the demo repository. Set git user.name and user.email, then run this block again.' >&2
```

Both blocks can be run again: on a later run `git commit` reports that there is
nothing to commit and the existing commit is reused. `--verify` matters when no
commit was ever created, for example because `user.name` and `user.email` are
not configured: a plain `git rev-parse HEAD` then prints the literal `HEAD`,
which the Runner accepts as a revision and fails only later, inside the
checkout, as `execution_error`.

Run the dispatch below in the same shell, PowerShell or bash, and from the
repository root. `$revision` exists only in the shell that prepared the
repository, and an empty or null revision is refused with `INVALID_ARGUMENT`;
from any other directory `grpcurl` cannot find `runner/v1/runner.proto`.

Dispatch it. The Runner registers no gRPC reflection service, so `grpcurl` is
pointed at the contract in `proto/`:

```powershell
$request = @{
  protocol_version = 'runner.v1'
  job = @{
    job_id     = [guid]::NewGuid().ToString()
    repository = @{ location = 'file:///C:/zeroyaml-sources/demo'; revision = $revision }
    execution  = @{ command = @('sh', '-c', 'cat greeting.txt'); working_directory = '.' }
  }
} | ConvertTo-Json -Depth 5

$request | grpcurl -plaintext -import-path proto -proto runner/v1/runner.proto `
  -d '@' localhost:50051 zeroyaml.runner.v1.RunnerService/RunJob
```

The `@` is quoted because PowerShell rejects a bare `@` argument with a parser
error.

```bash
grpcurl -plaintext -import-path proto -proto runner/v1/runner.proto -d @ \
  localhost:50051 zeroyaml.runner.v1.RunnerService/RunJob <<JSON
{
  "protocol_version": "runner.v1",
  "job": {
    "job_id": "$(uuidgen)",
    "repository": { "location": "file://$HOME/zeroyaml-sources/demo", "revision": "$revision" },
    "execution": { "command": ["sh", "-c", "cat greeting.txt"], "working_directory": "." }
  }
}
JSON
```

Expected acknowledgment. It reports acceptance only, never the outcome, because
a dispatch must not block on job duration:

```json
{
  "jobId": "<uuid>",
  "acceptance": "JOB_ACCEPTED",
  "message": "job accepted by the runner",
  "runnerId": "local-runner",
  "instanceId": "<uuid>"
}
```

Expected Runner records: the dispatch acknowledgment, a running status report,
the execution result, and one terminal report. The dispatch acknowledgment and
status-report acknowledgments omit the repository location, revision, command
arguments, and failure message. The execution-result record includes a
`message` field when execution fails. The dispatch acknowledgment and running
status report can appear in either order because execution starts in a separate
goroutine before the dispatch acknowledgment is logged.

```text
level=INFO msg="run job acknowledged" job_id=<uuid> protocol_version=runner.v1 runner_id=local-runner instance_id=<uuid> acceptance=JOB_ACCEPTED
level=WARN msg="job status report acknowledged" job_id=<uuid> reported_state=running runner_id=local-runner instance_id=<uuid> decision=unknown_job job_state=unspecified
level=INFO msg="job execution finished" job_id=<uuid> execution_id=<id> outcome=succeeded duration=<d> exit_code=0
level=WARN msg="job status report acknowledged" job_id=<uuid> reported_state=succeeded runner_id=local-runner instance_id=<uuid> decision=unknown_job job_state=unspecified
```

Expected Control Plane records:

```text
WARN  i.z.c.e.JobExecutionStatusRecorder : Job <uuid> reported RUNNING by runner local-runner instance <uuid> was refused: ...
WARN  i.z.c.e.JobExecutionStatusRecorder : Job <uuid> reported SUCCEEDED by runner local-runner instance <uuid> was refused: ...
```

`decision=unknown_job` is the correct answer here, not a defect. The Control
Plane owns authoritative Job state and never invents a Job from a report, and a
hand-dispatched Job was never recorded because nothing creates Jobs yet. Once
[#23](https://github.com/MohamedMBG/ZeroYaml/issues/23) records created Jobs,
the same two reports are answered `applied` with `job_state` `running` and then
`succeeded`.

Docker checks for the same execution:

```powershell
# While the job runs: the checkout container, then the job container.
docker ps --filter label=io.zeroyaml.managed=true

# After it ends, on any outcome: nothing is left behind.
docker ps -a --filter label=io.zeroyaml.managed=true
docker volume ls --filter label=io.zeroyaml.managed=true
```

Both post-run commands are expected to list nothing. Every container and the
workspace volume are removed whether the command succeeded, failed, timed out,
or was cancelled. The workspace strategy, resource bounds, result mapping, and
known limits are documented in
[`runner-execution-sandbox.md`](./architecture/runner-execution-sandbox.md).

For the failure path, dispatch `sh -c 'exit 3'`. The terminal report then
carries state `failed` with reason `non_zero_exit` and exit code `3`, and the
Runner still removes every resource. The Runner's `job execution finished`
WARN record includes the execution failure `message` alongside the outcome and
exit code; status-report acknowledgment records omit that message.

## 9. Expected states

### Job lifecycle

The Control Plane owns these states
([`JobStatus`](../control-plane/src/main/java/io/zeroyaml/controlplane/domain/job/JobStatus.java),
mirrored to the Runner as `JobLifecycleState`):

| State | Meaning | Reached by | Observable today |
| --- | --- | --- | --- |
| `CREATED` | The Job exists and has not been offered to a Runner | Job creation from a pipeline | No — [#23](https://github.com/MohamedMBG/ZeroYaml/issues/23) |
| `QUEUED` | The Job is waiting for a Runner | A Control Plane scheduling decision a Runner never observes or reports | No — [#24](https://github.com/MohamedMBG/ZeroYaml/issues/24) |
| `RUNNING` | The Runner reported `JOB_EXECUTION_RUNNING` | An accepted `RunJob`, then a running report | The report is sent and reconciled ([Section 8](#8-observe-one-job-execution)); it is applied only once Jobs are recorded |
| `SUCCEEDED` | The command exited `0` | Terminal report `JOB_EXECUTION_SUCCEEDED` | As above |
| `FAILED` | Non-zero exit, timeout, cancellation, or execution error | Terminal report `JOB_EXECUTION_FAILED` with a failure reason | As above |
| `CANCELLED` | The Control Plane stopped the Job | A Control Plane decision. A Runner reports a cancelled execution as `FAILED` with reason `cancelled` and never reports `CANCELLED` | No |

A report is applied only when it advances the Job. A duplicate, late, or
contradicting report is answered `duplicate`, `unknown_job`, or `conflict` and
leaves state untouched, which is what makes the call safe to repeat. A terminal
report repeats the execution start time, so a lost running report cannot leave
a finished execution without a Job outcome. The full reconciliation and failure
rules are in
[`job-status-contract.md`](./architecture/job-status-contract.md).

### Runner liveness

| State | Meaning |
| --- | --- |
| `HEALTHY` | A registration or heartbeat was acknowledged within `zeroyaml.registration.heartbeat-timeout` |
| `UNAVAILABLE` | That window passed without one |
| `UNKNOWN` | No Runner ever registered under this `runner_id` in the life of this Control Plane process |

Liveness is derived on demand from the last acknowledged contact. There is no
background sweep and no distributed failure detector.

`UNKNOWN` is the state of a Runner that started before the Control Plane, or
whose registration never reached it, and of every Runner after a Control Plane
restart until it registers again. A Runner rejected with an identity conflict
is different: its `runner_id` has an entry, owned by the earlier instance, so
the liveness reported is that instance's.

Liveness cannot be read from a running Control Plane yet. Nothing outside the
registry calls `RunnerRegistry.livenessOf` — no HTTP endpoint, log line, or
metric reports it — until Runner selection
([#24](https://github.com/MohamedMBG/ZeroYaml/issues/24)) consumes it. Locally
it is inferred from the Runner side: an accepted registration line followed by
no heartbeat warnings means `HEALTHY`, and the derivation itself is covered by
the Control Plane `registration` tests ([Section 11](#11-automated-coverage)).

### Persistence checks

| Check | Command | Expected |
| --- | --- | --- |
| Schema applied | `docker exec -i zeroyaml-postgres psql -U zeroyaml -d zeroyaml -c "\dt"` | `repository_connection` and Flyway's `flyway_schema_history` |
| Migration recorded | `... -c "SELECT version, description, success FROM flyway_schema_history;"` | `1`, `success = t` |
| Connection row | `... -c "SELECT provider, owner, name, status FROM repository_connection;"` | The row from [Section 6](#6-connect-a-repository) |
| Connection survives a restart | `docker compose -f ./infra/compose.yaml down`, then `up -d postgres`, then repeat the row check | The row is still present, because `down` without `-v` removes the container and keeps the `postgres-data` volume. Not executed end to end; verify when [#31](https://github.com/MohamedMBG/ZeroYaml/issues/31) lands |
| Job state survives a restart | — | Not expected. Jobs are held in a process-local store and are lost on restart until [#28](https://github.com/MohamedMBG/ZeroYaml/issues/28) |

The restart check recreates PostgreSQL, not the Control Plane, because the row
lives in the database and a Control Plane restart cannot lose it. Database
calls made while PostgreSQL is down fail until it is back. Restarting the
Control Plane is not part of this check and has a side effect: it empties the
in-memory Runner registry, so the Runner from
[Section 4.3](#43-runner) is answered `HEARTBEAT_UNKNOWN_RUNNER` from then on
and must be restarted to register again.

Captured Job logs have no persistence check yet; log streaming is
[#26](https://github.com/MohamedMBG/ZeroYaml/issues/26). Until then, command
output is visible only in the Job container's own output.

## 10. Troubleshooting

### GitHub webhook

| Symptom | Cause | Action |
| --- | --- | --- |
| Startup fails with `zeroyaml.github.webhook.secret must be set to at least 16 characters` | The secret is missing or too short | Set `ZEROYAML_GITHUB_WEBHOOK_SECRET` in the shell that starts the Control Plane; it is never read from a tracked file |
| `401`, outcome `REJECTED`, no handler log line | The signature is missing, malformed, or was computed with a different secret | Recompute the digest over the exact bytes sent, with no trailing newline, and confirm the GitHub webhook secret matches the environment value |
| `400`, outcome `REJECTED` | `X-GitHub-Event` or `X-GitHub-Delivery` is missing, or the body is not a JSON object | Send both headers and a JSON object body |
| `415` | The content type is not JSON | Use `Content-Type: application/json`, and set the same on the GitHub webhook |
| `413` | The payload is above `zeroyaml.github.webhook.max-payload-size` | Raise the limit, up to GitHub's 25 MB cap, or send a smaller payload |
| `200`, outcome `IGNORED`, for a push | `X-GitHub-Event` is not `push` | `push` is the only supported event; `ping` and all others are ignored by design |
| `202` but no pipeline runs | Expected today | The delivery handler only writes an audit line until [#20](https://github.com/MohamedMBG/ZeroYaml/issues/20) |
| GitHub reports a delivery error while local `curl` works | The tunnel is down or the URL is wrong | Fix the tunnel, then redeliver from **Recent Deliveries** rather than pushing again |

### gRPC

| Symptom | Cause | Action |
| --- | --- | --- |
| `Runner registration with the Control Plane at localhost:50052 failed - runner local-runner stays unavailable` | The Control Plane is not started, or listens elsewhere | Start the Control Plane first, or set `ZEROYAML_CONTROLPLANE_ADDRESS` to its `zeroyaml.registration.port` |
| The same message ending in `DeadlineExceeded` while the Control Plane is up and listening | Resolving `localhost` took longer than `ZEROYAML_RUNNER_REGISTRATION_TIMEOUT` (default `5s`); observed on Windows | Set `ZEROYAML_CONTROLPLANE_ADDRESS` to `127.0.0.1:50052`, or raise the timeout |
| `rejected registration for runner local-runner - identity conflict` | This `ZEROYAML_RUNNER_ID` is already registered under a different instance: another Runner is running, or this Runner was restarted after an accepted registration | Stop the other Runner if there is one, then restart the Control Plane to clear its in-memory registry, or give this Runner a distinct `ZEROYAML_RUNNER_ID` |
| Every dispatch answered `JOB_REJECTION_RUNNER_UNAVAILABLE`, message `runner status is ... and it is not accepting work` | Registration was not accepted, or Docker was unavailable at startup | Fix the cause, then restart the Runner; both checks run once at startup. If the earlier registration was accepted, restart the Control Plane first to avoid an identity conflict |
| A dispatch answered `JOB_REJECTION_RUNNER_UNAVAILABLE`, message `runner is already running its maximum number of concurrent jobs` | Every execution slot is in use; `ZEROYAML_RUNNER_MAX_CONCURRENT_JOBS` defaults to `1` | Wait for the running Job's terminal report and dispatch again, or raise the limit. Do not restart the Runner: that cancels the healthy running Job |
| A dispatch answered `JOB_REJECTION_RUNNER_UNAVAILABLE`, message `runner is shutting down and accepts no new jobs` | The Runner received `SIGINT` or `SIGTERM` and is draining | Dispatch again once the Runner has been started again |
| Dispatch answered `JOB_REJECTION_UNSUPPORTED_PROTOCOL_VERSION` | `protocol_version` is not `runner.v1` | Send the version declared in `proto/runner/v1/runner.proto` |
| Dispatch fails with `INVALID_ARGUMENT` and no response body | A contract violation, such as a missing job identity or an empty command | Fix the request; a caller defect is deliberately not answered with a rejection |
| Heartbeats answered `HEARTBEAT_UNKNOWN_RUNNER` | The Control Plane restarted and lost its in-memory registry | Restart the Runner so it registers again |
| Control Plane startup fails with `Failed to start the runner-facing gRPC server on port 50052` | The port is already bound | Stop the other process, or change `zeroyaml.registration.port` and `ZEROYAML_CONTROLPLANE_ADDRESS` together |
| `grpcurl` reports `server does not support the reflection API` | The Runner registers no reflection service | Pass `-import-path proto -proto runner/v1/runner.proto` |

### Docker

| Symptom | Cause | Action |
| --- | --- | --- |
| `docker daemon unavailable - runner will not accept jobs` | The daemon is not running, or did not answer within 5 s | Start Docker Desktop, confirm `docker version`, then restart the Control Plane and the Runner; the Runner's registration is accepted even when the probe fails, so restarting it alone ends in an identity conflict |
| Terminal report with reason `execution_error` | The workspace or the executor could not be prepared, for example a missing image | Pre-pull `ZEROYAML_RUNNER_JOB_IMAGE` and `ZEROYAML_RUNNER_CHECKOUT_IMAGE` |
| Dispatch fails with `FAILED_PRECONDITION`, naming the scheme | The repository scheme is not `https`, `http`, or `file` | Use a supported scheme; other git transports can run arbitrary commands and are refused |
| Dispatch fails with `FAILED_PRECONDITION`: `file repository locations are disabled on this runner` | `ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT` is unset | Set it to an existing absolute directory and restart the Runner |
| Dispatch fails with `FAILED_PRECONDITION`: `file repository location is outside the configured local source root` | The path escapes the root, including through a symlink | Move the source below the root; containment is checked after symlinks are resolved |
| Terminal report with reason `timeout` | The execution exceeded `ZEROYAML_RUNNER_JOB_TIMEOUT` | Raise the timeout or shorten the command; the checkout counts toward the same budget |
| Containers or volumes left behind after a crash | The Runner exited before cleanup finished | Remove them by label: `docker rm -f $(docker ps -aq --filter label=io.zeroyaml.managed=true)`, then `docker volume rm $(docker volume ls -q --filter label=io.zeroyaml.managed=true)`. `docker volume prune` with the same filter is not enough: workspace volumes are named, and since Docker Engine 23 `prune` removes only anonymous volumes unless `--all` is passed |
| `go test ./...` fails in `internal/sandbox` | Container tests were opted into without a usable daemon | Unset `ZEROYAML_RUNNER_DOCKER_TESTS`, or start Docker |
| Control Plane tests cannot start a container | Testcontainers needs a reachable daemon | Start Docker; these tests use a disposable `postgres:16-alpine` and never the compose database |

### PostgreSQL

| Symptom | Cause | Action |
| --- | --- | --- |
| Startup fails with `no password was provided` | `ZEROYAML_DATABASE_PASSWORD` is unset | Export it in the shell that starts the Control Plane |
| Startup fails with a refused connection after about 5 s | The database is unreachable; connection acquisition is bounded at 5 s on purpose | `docker compose -f ./infra/compose.yaml up -d postgres`, then confirm `5432` is published |
| `password authentication failed for user "zeroyaml"` | The password differs from the compose value | Align the environment value with `infra/compose.yaml`, or point `ZEROYAML_DATABASE_URL` at the right database |
| Startup fails with a Flyway validation error | The volume holds a schema from an edited migration | Applied migrations are immutable: add a new versioned migration, or discard local data with `docker compose -f ./infra/compose.yaml down -v` |
| An insert is rejected by a check constraint | The row contradicts the domain rules | Use lowercase `owner` and `name`, provider `GITHUB`, a valid status, and `status_changed_at >= connected_at` |
| An insert is rejected by `repository_connection_identity_key` | The repository is already connected | Update the existing row; one repository can be connected only once |
| Requests fail after about 5 s | The statement timeout is bounded at 5 s | Investigate the slow statement; an unreachable or stalled database fails fast by design |

## 11. Automated coverage

Run both suites from the repository root before proposing a change:

```powershell
Push-Location control-plane
.\mvnw.cmd --batch-mode verify
Pop-Location

Push-Location runner
go test ./...
Pop-Location
```

Container-backed Runner tests are opt-in, because they need a daemon, `git`,
and the configured images:

```powershell
Push-Location runner
$env:ZEROYAML_RUNNER_DOCKER_TESTS = '1'
go test -count=1 -run TestDockerExecution ./internal/sandbox/
Remove-Item Env:ZEROYAML_RUNNER_DOCKER_TESTS
Pop-Location
```

What the suites already cover, per step of this document:

| Step | Covered by |
| --- | --- |
| Webhook envelope, signature, event allow-list, response contract | Control Plane `github.webhook` tests |
| Repository connection persistence and constraints | Control Plane `connection` tests, against a Testcontainers PostgreSQL |
| Registration, heartbeat, and derived liveness | Control Plane `registration` tests; Runner `registrationclient` and `heartbeat` tests |
| `RunJob` validation, acceptance, and rejection | Runner `grpcserver` tests, including over the transport |
| Container execution, cleanup, and result mapping | Runner `sandbox` tests; `TestDockerExecution*` for real containers |
| Status reporting and reconciliation | Runner `jobstatus` tests; Control Plane `execution` tests |
| A push through to an execution | Not covered — [#31](https://github.com/MohamedMBG/ZeroYaml/issues/31) |

No automated test spans a push to an execution yet. The two halves in Sections
7 and 8 were each executed by hand on Windows on 2026-10-11, and nothing has
run them as one continuous push-to-execution scenario. When
[#31](https://github.com/MohamedMBG/ZeroYaml/issues/31) lands, this document
must be checked against that test and corrected wherever the two disagree; the
test is authoritative.

## 12. Shut down

```powershell
# Stop the Runner, then the Control Plane: Ctrl+C in each shell.
docker compose -f .\infra\compose.yaml down
```

The Runner stops on `SIGINT` or `SIGTERM` by closing its listener, draining
in-flight RPCs, and cancelling running executions, all inside one
`ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT` budget measured from the signal. If that
budget runs out it logs `runner exited before every job execution finished` and
exits, leaving labelled Docker resources to be removed as described under
[Docker](#docker).

`down` keeps the `postgres-data` volume, so connected-repository rows survive.
Add `-v` to discard the local database as well.

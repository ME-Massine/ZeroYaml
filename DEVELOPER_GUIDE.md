# ZeroYAML — Developer Guide

Welcome to **ZeroYAML**.

This document is the first thing new contributors should read before changing code. Its purpose is to explain the repository structure, what each folder/file is responsible for, and how the main parts of the system fit together.

> **Core idea:** the **Control Plane decides and coordinates**; the **Runner executes**.

---

## 1. What is ZeroYAML?

ZeroYAML is a developer platform that aims to turn a connected Git repository into an executable CI/CD pipeline without requiring developers to maintain large YAML workflow files.

At a high level:

```text
GitHub Repository
       |
       | webhook / API events
       v
+----------------------+
|    CONTROL PLANE     |
|  Java + Spring Boot  |
|                      |
| Decides what runs,   |
| when it runs, and    |
| where it runs.       |
+----------+-----------+
           |
           | gRPC
           v
+----------------------+
|        RUNNER        |
|          Go          |
|                      |
| Executes jobs in     |
| isolated containers. |
+----------+-----------+
           |
           v
         Docker
```

Supporting infrastructure will include PostgreSQL, Redis, and Kafka.

---

## 2. Repository Structure

Current structure:

```text
ZeroYAML/
|
|-- control-plane/          # Java/Spring Boot control plane
|-- runner/                 # Go execution runner
|-- infra/                  # Local infrastructure and Docker configuration
|-- docs/                   # Architecture and engineering documentation
|   `-- architecture/
|
|-- .gitignore              # Files Git must not track
`-- README.md               # Main project introduction
```

Each top-level directory represents a different responsibility. Avoid mixing responsibilities between them.

---

# 3. `control-plane/`

```text
control-plane/
```

The Control Plane is the **brain of ZeroYAML**.

It will eventually be responsible for:

- receiving API requests;
- receiving GitHub webhooks;
- understanding pipeline definitions;
- coordinating pipeline execution;
- scheduling jobs;
- keeping execution state;
- selecting runners;
- communicating with runners;
- storing persistent application data;
- publishing and consuming system events.

The Control Plane **does not execute user builds directly**. Execution belongs to the Runner.

Technology:

- Java 21
- Spring Boot
- Maven

---

## 3.1 `control-plane/pom.xml`

```text
control-plane/
`-- pom.xml
```

This is the Maven project configuration.

It defines things such as:

- project metadata;
- Java version;
- Spring Boot version;
- dependencies;
- Maven plugins;
- build configuration;
- testing configuration.

Think of it roughly as:

```text
Java       -> pom.xml
Node.js    -> package.json
Go         -> go.mod
```

When a Java dependency is added to the Control Plane, `pom.xml` is usually where it is declared.

---

## 3.2 `control-plane/src/main/java/`

```text
control-plane/
`-- src/
    `-- main/
        `-- java/
            `-- io/
                `-- zeroyaml/
                    `-- controlplane/
```

This folder contains the **actual Java source code** for the Control Plane.

Our base Java package is:

```text
io.zeroyaml.controlplane
```

As the project grows, this package will be divided into modules such as:

```text
io.zeroyaml.controlplane
|
|-- api/
|-- github/
|-- pipeline/
|-- scheduler/
|-- execution/
|-- runner/
|-- security/
|-- persistence/
`-- config/
```

Do not create random classes directly in the root package when a clear module exists.

---

## 3.3 `ControlPlaneApplication.java`

```text
src/main/java/io/zeroyaml/controlplane/
`-- ControlPlaneApplication.java
```

This is the **entry point of the Spring Boot application**.

It contains the Java `main()` method that starts the Control Plane.

Conceptually:

```text
ControlPlaneApplication.main()
          |
          v
Spring Boot starts
          |
          v
Spring discovers application components
          |
          v
HTTP server and application context start
```

This file should remain small. Business logic should not be placed here.

---

## 3.4 `control-plane/src/main/resources/`

```text
src/main/resources/
|
|-- application.properties
|-- static/
`-- templates/
```

This directory contains resources and application configuration that are not Java source files.

### `application.properties`

Spring Boot configuration lives here.

Examples of things that may eventually be configured here:

- application name;
- HTTP port;
- database connection;
- Kafka;
- Redis;
- logging;
- feature configuration.

Secrets should **not** be hard-coded here. Sensitive values should come from environment variables or a secret-management system.

### `static/`

Spring can serve static files from this directory.

ZeroYAML's Control Plane is primarily an API, so this folder may remain unused.

### `templates/`

Spring can render server-side HTML templates from this directory.

ZeroYAML currently does not depend on server-side HTML rendering, so this folder may also remain unused.

---

## 3.5 `control-plane/src/test/`

```text
control-plane/
`-- src/
    `-- test/
        `-- java/
```

All automated Java tests belong here.

The generated project currently contains:

```text
ControlPlaneApplicationTests.java
```

Its first responsibility is simply verifying that the Spring application context can start.

As ZeroYAML grows, tests should cover components such as:

```text
PipelineServiceTest
WebhookServiceTest
SchedulerTest
RunnerRegistryTest
ExecutionServiceTest
```

Run Java tests with:

```powershell
.\control-plane\mvnw.cmd test
```

---

## 3.6 `.mvn/`

```text
control-plane/
`-- .mvn/
    `-- wrapper/
        `-- maven-wrapper.properties
```

This belongs to the **Maven Wrapper**.

The wrapper lets contributors use the Maven configuration expected by the project instead of relying entirely on a globally installed Maven version.

Windows:

```powershell
.\control-plane\mvnw.cmd test
```

Linux/macOS:

```bash
./control-plane/mvnw test
```

---

## 3.7 `mvnw` and `mvnw.cmd`

```text
mvnw
mvnw.cmd
```

Both files launch the Maven Wrapper.

- `mvnw` -> Linux/macOS
- `mvnw.cmd` -> Windows

Do not delete them.

---

## 3.8 `control-plane/.gitignore`

This `.gitignore` is specific to the Java/Spring Boot project.

It prevents generated or local files such as Maven build output or IDE metadata from being committed.

The repository also has a root `.gitignore`.

---

## 3.9 `control-plane/.gitattributes`

This file helps Git treat files consistently across operating systems.

One important use is normalizing line endings between:

- Windows;
- Linux;
- macOS.

Normally this file should not require frequent changes.

---

## 3.10 `control-plane/HELP.md`

This file is generated by Spring Initializr.

It contains links and basic information about the generated Spring Boot project.

It is not application code and may eventually be removed once the ZeroYAML documentation fully replaces it.

---

# 4. `runner/`

```text
runner/
```

The Runner is the **execution engine**.

Technology:

- Go
- Docker
- gRPC

The Runner will eventually:

1. register itself with the Control Plane;
2. receive execution requests;
3. prepare an isolated execution environment;
4. start Docker containers;
5. execute build/test/deployment commands;
6. stream logs and execution state;
7. return the final result to the Control Plane.

Example:

```text
Control Plane
     |
     | Run job #123
     v
   Runner
     |
     | create isolated container
     v
   Docker
     |
     | mvn test / npm test / go test ...
     v
Execution result
```

The Runner should focus on **safe, reliable execution**. Business decisions about which pipeline should run belong in the Control Plane.

The Go project will later contain its own `go.mod`, entry point, internal packages, executor code, and gRPC implementation.

---

# 5. `infra/`

```text
infra/
```

Infrastructure configuration belongs here.

This directory will contain the services required to run ZeroYAML locally and eventually deployment-related infrastructure.

Expected local services include:

- PostgreSQL;
- Redis;
- Kafka.

A future structure may look like:

```text
infra/
|
|-- compose.yaml
|-- postgres/
|-- kafka/
`-- monitoring/
```

The intention is that developers should not manually install every infrastructure service.

Eventually a command such as:

```powershell
docker compose -f .\infra\compose.yaml up -d
```

should start the required local environment.

---

# 6. `docs/`

```text
docs/
```

Long-form technical documentation belongs here.

The [Local End-to-End Development Workflow](./docs/local-development-workflow.md)
is the task-ordered runbook for this guide: it sequences the startup of
infrastructure, Control Plane, and Runner, reproduces the supported signed
GitHub `push` scenario, states the expected states and persistence checks, and
collects troubleshooting for the webhook, gRPC, Docker, and PostgreSQL
boundaries. This guide stays the reference for individual settings and
contracts.

Code explains **how the current implementation works**.

Documentation should also explain **why architectural decisions were made**.

---

## 6.1 `docs/architecture/`

```text
docs/
`-- architecture/
```

This directory is for system architecture documentation.

Start with the [Phase 1 architecture and operating contract](./docs/architecture/phase-1-overview.md),
then use the focused [Runner identity and registration contract](./docs/architecture/runner-identity.md),
[Core Job Model](./docs/architecture/job-model.md),
[RunJob dispatch contract](./docs/architecture/run-job-contract.md), and
[Job status reporting contract](./docs/architecture/job-status-contract.md)
references for the implemented foundation and its explicit integration limits. The
[Repository Connection Model](./docs/architecture/repository-connection-model.md)
reference covers how the Control Plane identifies connected repositories and
references webhook secrets without storing them.

Examples:

```text
architecture/
|
|-- overview.md
|-- control-plane.md
|-- runner.md
|-- execution-flow.md
`-- event-flow.md
```

When a major architectural decision is made, document it instead of leaving the reasoning only in chat messages or developer memory.

---

# 7. Root `.gitignore`

```text
ZeroYAML/
`-- .gitignore
```

The root `.gitignore` applies to the whole repository.

Because ZeroYAML contains multiple technologies, it ignores files from several ecosystems.

Examples:

```text
**/target/        # Java/Maven build output
runner/bin/       # Go binaries
.env              # local environment variables
.idea/            # IntelliJ metadata
.vscode/          # VS Code workspace metadata
*.log             # logs
```

Never commit secrets, tokens, passwords, or private `.env` files.

---

# 8. Root `README.md`

```text
ZeroYAML/
`-- README.md
```

This is the public front page of the repository.

It should answer questions such as:

- What is ZeroYAML?
- What problem does it solve?
- How is it architected?
- How do I run it?
- What stage is the project currently in?
- Where should contributors start?

Keep the root README concise enough that a new developer can understand the project quickly.

This Developer Guide can contain the deeper repository explanation.

---

# 9. Architecture Rule to Remember

The most important separation in ZeroYAML is:

```text
CONTROL PLANE
    |
    | decides + coordinates
    |
    v
RUNNER
    |
    | executes
    |
    v
DOCKER
```

### Control Plane

Answers:

> What should run?

> When should it run?

> Which runner should execute it?

> What is the current state?

### Runner

Answers:

> How do I execute this job safely?

> How do I isolate it?

> What logs/results should I return?

Do not move execution responsibilities into the Control Plane merely because it is easier in the short term.

---

# 10. Prerequisites

Before working on ZeroYAML, install:

### Required for all contributors

- Git
- Docker Desktop
- Docker Compose

### Control Plane development

- Java 21

### Control Plane Runner connection

The Control Plane generates its Java gRPC client from `proto/runner/v1/runner.proto` during the Maven build. The local Runner connection uses these defaults:

| Property | Default | Purpose |
| --- | --- | --- |
| `zeroyaml.runner.host` | `localhost` | Runner host name |
| `zeroyaml.runner.port` | `50051` | Runner gRPC port |
| `zeroyaml.runner.ping-deadline` | `2s` | Ping RPC deadline |
| `zeroyaml.runner.dispatch-deadline` | `5s` | `RunJob` RPC deadline |

Override them with Spring Boot environment variables such as `ZEROYAML_RUNNER_HOST`, `ZEROYAML_RUNNER_PORT`, `ZEROYAML_RUNNER_PING_DEADLINE`, and `ZEROYAML_RUNNER_DISPATCH_DEADLINE`; do not hardcode deployment endpoints in Java code.

`RunJob` acknowledges a dispatch rather than waiting for execution, so the
dispatch deadline bounds the acknowledgment only. The dispatch contract, its
request and response fields, and its failure semantics are documented in
[`docs/architecture/run-job-contract.md`](./docs/architecture/run-job-contract.md).
A Runner accepts a dispatch only when it is registered, its Docker daemon is
reachable, and it has a free execution slot; see
[Runner Docker execution](#runner-docker-execution).

### Runner-facing gRPC endpoint

The Control Plane serves every gRPC service a Runner calls on one endpoint, so a
Runner is configured with a single Control Plane address:

| Property | Default | Purpose |
| --- | --- | --- |
| `zeroyaml.registration.port` | `50052` | Port for `RunnerRegistrationService` and `JobExecutionStatusService` |
| `zeroyaml.registration.heartbeat-timeout` | `15s` | Time without a heartbeat before a Runner is reported `UNAVAILABLE` |

The setting keeps its `zeroyaml.registration` prefix so deployed configuration
and Runner environment variables stay valid as services are added to the
endpoint.

`JobExecutionStatusService.ReportJobStatus` is how a Runner reports what one
execution did. The Control Plane owns authoritative Job state: it validates
every transition, applies a report only when it advances the Job, and answers a
duplicate, late, or contradicting report explicitly instead of changing state.
A terminal report repeats the execution start time, so a lost running report
cannot leave a finished execution without a Job outcome. The states, the exit
code and failure reasons, and the full reconciliation and failure rules are
documented in
[`docs/architecture/job-status-contract.md`](./docs/architecture/job-status-contract.md).

### GitHub webhook endpoint

The Control Plane receives GitHub webhook deliveries on
`POST /webhooks/github` on its HTTP port (default `8080`). Configure the GitHub
webhook with content type `application/json` and a secret.

| Property | Default | Purpose |
| --- | --- | --- |
| `zeroyaml.github.webhook.max-payload-size` | `5MB` | Largest accepted payload, at most GitHub's 25 MB cap |
| `zeroyaml.github.webhook.secret` | none | Shared secret the delivery signature is verified against |

The secret is required and has no default, so the Control Plane does not start
without it. Supply it from the environment and set the same value on the GitHub
webhook:

```powershell
$env:ZEROYAML_GITHUB_WEBHOOK_SECRET = (openssl rand -hex 32)
```

`push` deliveries that pass envelope validation and carry a valid
`X-Hub-Signature-256` HMAC are answered `202 Accepted` and handed to the
downstream delivery handler with the raw body and GitHub delivery headers.
`ping` and every other verified event are answered `200 OK` with outcome
`IGNORED` and start no work. A missing or malformed `X-GitHub-Event` or
`X-GitHub-Delivery` header, a body that is not a JSON object, a non-JSON content
type, or an oversized payload is rejected with `400`, `415`, or `413`, and a
missing, malformed, or non-matching signature is rejected with `401` before any
downstream processing.

Send a local test delivery, signed the way GitHub signs one:

```powershell
$payload = '{"ref":"refs/heads/main"}'
$hmac = [System.Security.Cryptography.HMACSHA256]::new(
  [Text.Encoding]::UTF8.GetBytes($env:ZEROYAML_GITHUB_WEBHOOK_SECRET))
$digest = ($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($payload)) |
  ForEach-Object { $_.ToString('x2') }) -join ''

curl.exe -i -X POST http://localhost:8080/webhooks/github `
  -H "Content-Type: application/json" `
  -H "X-GitHub-Event: push" `
  -H "X-GitHub-Delivery: 72d3162e-cc78-11e3-81ab-4c9367dc0958" `
  -H "X-Hub-Signature-256: sha256=$digest" `
  --data $payload
```

The validation order, the signature rules, the response body, and the hand-off
contract are documented in
[`docs/architecture/github-webhook-ingress.md`](./docs/architecture/github-webhook-ingress.md).

### Control Plane database

The Control Plane stores connected repository metadata in PostgreSQL. Flyway
applies the migrations in `control-plane/src/main/resources/db/migration` at
startup, so the schema is always created or upgraded before the service
accepts traffic.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ZEROYAML_DATABASE_URL` | `jdbc:postgresql://localhost:5432/zeroyaml` | JDBC URL of the Control Plane database |
| `ZEROYAML_DATABASE_USERNAME` | `zeroyaml` | Database user |
| `ZEROYAML_DATABASE_PASSWORD` | none | Database password; never stored in a tracked file |

The defaults match the `postgres` service in `infra/compose.yaml`. The
password has no default, so a Control Plane started without it stops during
startup with `no password was provided`. For local development:

```powershell
docker compose -f .\infra\compose.yaml up -d postgres
$env:ZEROYAML_DATABASE_PASSWORD = 'zeroyaml'
```

Obtaining a connection is bounded at 5 seconds and each statement at 5
seconds, so an unreachable database fails startup and requests instead of
hanging them.

The table layout, the duplicate rule, and the locking behavior are documented
in
[`docs/architecture/repository-connection-model.md`](./docs/architecture/repository-connection-model.md#persistence).

### Runner development

- Go
- Docker

The Runner reads startup configuration from environment variables and keeps safe local defaults:

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ZEROYAML_RUNNER_GRPC_ADDRESS` | `:50051` | TCP address used by the Runner gRPC server |
| `ZEROYAML_RUNNER_ID` | `local-runner` | Stable logical Runner identity returned by `GetInfo` and shown in startup logs |
| `ZEROYAML_RUNNER_VERSION` | `0.1.0` | Runner version returned by `RunnerService.Ping` |
| `ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT` | `5s` | Bound on the whole shutdown: draining in-flight RPCs and running Job executions |
| `ZEROYAML_CONTROLPLANE_ADDRESS` | `localhost:50052` | Control Plane gRPC endpoint for registration, heartbeats, and job status reports |
| `ZEROYAML_RUNNER_REGISTRATION_TIMEOUT` | `5s` | Bound on the single startup registration attempt |
| `ZEROYAML_RUNNER_HEARTBEAT_INTERVAL` | `5s` | Interval between heartbeats sent by a registered Runner |
| `ZEROYAML_RUNNER_HEARTBEAT_TIMEOUT` | `5s` | Bound on each individual heartbeat attempt |
| `ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT` | `5s` | Bound on each job status report sent to the Control Plane |
| `ZEROYAML_RUNNER_JOB_IMAGE` | `alpine:3.22` | Docker image that runs every Job command |
| `ZEROYAML_RUNNER_CHECKOUT_IMAGE` | `alpine/git:v2.49.1` | Docker image, with `sh` and `git`, that fetches the Job repository |
| `ZEROYAML_RUNNER_JOB_TIMEOUT` | `10m` | Bound on one execution, including the source checkout |
| `ZEROYAML_RUNNER_MAX_CONCURRENT_JOBS` | `1` | Executions run at once, from `1` to `16` |
| `ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT` | empty | Absolute host directory below which `file://` repository locations are accepted; empty disables them |

`ZEROYAML_RUNNER_GRPC_ADDRESS` and `ZEROYAML_CONTROLPLANE_ADDRESS` must use host-and-port syntax such as `:50051` or `127.0.0.1:50051`. Invalid values fail startup with an actionable configuration error.

`ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT`, `ZEROYAML_RUNNER_REGISTRATION_TIMEOUT`, `ZEROYAML_RUNNER_HEARTBEAT_INTERVAL`, `ZEROYAML_RUNNER_HEARTBEAT_TIMEOUT`, `ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT`, and `ZEROYAML_RUNNER_JOB_TIMEOUT` must each be a positive Go duration such as `5s` or `500ms`. Keep the shutdown timeout below the stop grace period of the process supervisor, otherwise the supervisor kills the Runner before draining completes.

The heartbeat loop only starts once startup registration reports the Runner
`ready`; a Runner the Control Plane never accepted has nothing to keep alive on
the registry side. A failed or declined heartbeat is logged and never stops the
loop or the process — only Runner shutdown does. See
[Heartbeat and liveness](./docs/architecture/runner-identity.md#heartbeat-and-liveness)
for the Control Plane side of the contract.

The Runner also creates an in-memory `instance_id` once per process start. The
same `runner_id` can therefore identify a logical Runner across restarts while
the Control Plane can distinguish each process instance. The identity and
capability contract is documented in
[`docs/architecture/runner-identity.md`](./docs/architecture/runner-identity.md).

Each `RunJob` dispatch writes one structured log record to standard error, for
example:

```text
time=2024-01-01T00:00:00.000Z level=INFO msg="run job acknowledged" job_id=0f4b1a1e-... protocol_version=runner.v1 runner_id=local-runner instance_id=... acceptance=JOB_REJECTED rejection_reason=JOB_REJECTION_RUNNER_UNAVAILABLE
```

Invalid requests are logged at `WARN` with the `grpc_code` and validation
`error`, and a request whose caller already cancelled or exceeded its deadline
is logged as abandoned and answered with `CANCELLED` or `DEADLINE_EXCEEDED`
instead of an acknowledgment. Records never contain the repository location,
revision, or command arguments, and caller-supplied identifiers are cut to
128 bytes with a trailing `...(truncated)` marker, so a truncated field is up
to 143 bytes long.

### Runner execution status reporting

`runner/internal/jobstatus` is the Runner side of
[`JobExecutionStatusService`](./docs/architecture/job-status-contract.md). It
builds one observation with `jobstatus.Running`, `jobstatus.Succeeded`, or
`jobstatus.Failed`, and `Reporter.Report` sends it to
`ZEROYAML_CONTROLPLANE_ADDRESS`, bounded by the timeout the Reporter was created
with. A failure diagnostic is cut to 1024 bytes before it is sent, because
dropping a terminal report over an oversized message would leave the Job without
an outcome.

Each attempt writes one structured log record, for example:

```text
time=2024-01-01T00:00:00.000Z level=INFO msg="job status report acknowledged" job_id=0f4b1a1e-... reported_state=succeeded runner_id=local-runner instance_id=... decision=applied job_state=succeeded
```

A report the Control Plane did not apply is logged at `WARN` with its
`decision`, because it means the Job already moved on or this process is not the
one executing it. Records never contain the failure message, which is derived
from execution output.

The execution dispatcher (`runner/internal/jobexecution`) sends a running
report when an accepted Job starts and one terminal report when it ends.

### Runner Docker execution

`runner/internal/sandbox` runs every accepted Job in Docker; the Runner never
executes a Job command on its host. At startup the Runner probes the Docker
daemon with `docker version`. If the daemon does not answer, the Runner keeps
serving `Ping` and `GetInfo` but rejects every dispatch as unavailable.

Each execution creates a workspace volume, fetches the dispatched revision into
it with a checkout container, runs the command as the entrypoint of a Job
container in `/workspace/<working_directory>`, and then removes every container
and the volume, whether the command succeeded, failed, timed out, or was
cancelled. Resources carry the `io.zeroyaml.managed=true` label. Repository
locations must use `https://`, `http://`, or, when
`ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT` is set, `file://` below that directory.

The workspace strategy, resource bounds, result mapping, and known limits are
documented in
[`docs/architecture/runner-execution-sandbox.md`](./docs/architecture/runner-execution-sandbox.md).
Tests that start real containers are opt-in:

```powershell
Push-Location runner
$env:ZEROYAML_RUNNER_DOCKER_TESTS = '1'
go test -count=1 -run TestDockerExecution ./internal/sandbox/
Remove-Item Env:ZEROYAML_RUNNER_DOCKER_TESTS
Pop-Location
```

The Go protobuf bindings can be regenerated from the repository root after
installing the pinned `protoc-gen-go` and `protoc-gen-go-grpc` tool versions:

```powershell
protoc -I proto `
  --go_out=runner/gen `
  --go_opt=paths=source_relative `
  --go-grpc_out=runner/gen `
  --go-grpc_opt=paths=source_relative `
  proto/runner/v1/runner.proto
```

The Java bindings are regenerated by the Maven protobuf plugin during
`control-plane\mvnw.cmd verify`.

### Runner shutdown

The Runner stops on `SIGINT` (local `Ctrl+C`) and on `SIGTERM` (containers and service managers). On either signal it:

1. closes the gRPC listener, so no new connection or RPC is accepted;
2. waits for in-flight RPCs to finish, bounded by `ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT`;
3. cancels the remaining RPCs when that timeout expires, so the process always terminates;
4. cancels running Job executions, which force-removes their containers, and waits for each to send its terminal status report.

`ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT` is one budget for the whole sequence, measured from the signal, so the RPC drain and the execution drain cannot add up past it. When the budget runs out with executions still finishing, the Runner logs `runner exited before every job execution finished` with their number and exits; their Docker resources stay labelled `io.zeroyaml.managed=true` and can be removed manually.

Verify it locally:

```powershell
Push-Location runner
go run ./cmd/runner
# Press Ctrl+C and confirm the shutdown log line appears and the process exits.
Pop-Location
```

### Frontend development

When the frontend is introduced:

- Node.js
- pnpm

Developers working only on one subsystem do not necessarily need every language toolchain immediately, but integration work may require multiple toolchains.

---

# 11. First Commands for a New Developer

Clone the repository:

```powershell
git clone https://github.com/MohamedMBG/ZeroYAML.git
cd ZeroYAML
```

Check Java:

```powershell
java -version
```

Check Go:

```powershell
go version
```

Check Docker:

```powershell
docker --version
docker compose version
```

Run Control Plane tests:

```powershell
.\control-plane\mvnw.cmd test
```

The database tests and the application context test start a disposable
`postgres:16-alpine` container through Testcontainers, so Docker must be
running. They never use the `infra/compose.yaml` database.

When the Runner is initialized:

```powershell
Push-Location runner
go test ./...
Pop-Location
```

To generate the Control Plane gRPC client and verify its Ping integration path, run:

```powershell
.\control-plane\mvnw.cmd test
```

The focused Ping test uses an in-process Runner and does not require a separately running Runner process.

Then bring the platform up and verify the available webhook and Runner
execution paths separately by following the
[Local End-to-End Development Workflow](./docs/local-development-workflow.md).
It also records which steps of the push-to-execution chain are wired today and
which remain open Phase 2 work.

---

# 12. Before Creating New Code

Before adding a class, package, service, or infrastructure component, ask:

1. Does this belong to the Control Plane or the Runner?
2. Does a module for this responsibility already exist?
3. Am I introducing business logic into an entry-point/configuration class?
4. Am I committing generated files or secrets?
5. Does this change affect an architectural contract that should be documented?

Prefer clear boundaries over shortcuts.

---

# 13. Git Workflow

Do not work directly on the main branch.

Use feature branches, for example:

```text
feat/phase-1-foundation
feat/grpc-runner-contract
feat/github-webhooks
fix/runner-heartbeat
docs/architecture-overview
```

Before committing:

```powershell
git status
```

Run the relevant tests.

Then use clear commit messages:

```text
feat: add runner registration contract
fix: handle invalid webhook signature
test: add scheduler unit tests
docs: document execution lifecycle
chore: configure local kafka service
```

---

# 14. Current Project Phase

ZeroYAML is currently in **Phase 1 — Foundation**.

The immediate goal is to establish:

- repository structure;
- Java Control Plane;
- Go Runner;
- local infrastructure;
- architecture documentation;
- communication contracts;
- reliable development workflow.

Do not prematurely build advanced pipeline features before the foundation and service boundaries are stable.

---

# 15. If You Are New to the Project

Read these in order:

```text
1. Root README.md
2. This Developer Guide
3. docs/architecture/
4. The module you are going to work on
5. Related tests
```

If you still do not understand where a change belongs, discuss the architecture before implementing it.

---

## Final Mental Model

```text
                    ZeroYAML

                  GitHub / API
                       |
                       v
              +----------------+
              | CONTROL PLANE  |
              | Java / Spring  |
              +-------+--------+
                      |
                     gRPC
                      |
                      v
              +----------------+
              |     RUNNER     |
              |       Go       |
              +-------+--------+
                      |
                    Docker
                      |
                      v
                User workload


        PostgreSQL / Redis / Kafka
                  support
             the platform
```

**Control Plane = decide and coordinate.**

**Runner = execute.**

Keep that distinction clear and the rest of the architecture becomes much easier to reason about.

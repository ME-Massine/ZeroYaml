# ZeroYAML

ZeroYAML is a developer platform that turns a connected Git repository
into an executable CI/CD pipeline without requiring developers to write
and maintain large YAML workflow files.


## New contributor?

If this is your first time working on ZeroYAML, read:

[Developer Guide](./DEVELOPER_GUIDE.md)

It explains the repository structure, architecture, prerequisites,
development workflow, and the purpose of the main files and folders.

To bring the platform up locally and verify the available webhook and Runner
execution paths separately, follow:

[Local End-to-End Development Workflow](./docs/local-development-workflow.md)

It gives the startup order for infrastructure, Control Plane, and Runner, the
required configuration and safe webhook secret handling, the supported signed
GitHub `push` scenario, the expected states and persistence checks, and
troubleshooting for the webhook, gRPC, Docker, and PostgreSQL boundaries.


## Architecture

ZeroYAML is divided into two main planes:

### Control Plane

Responsible for:

- API
- GitHub integration
- webhook processing
- pipeline orchestration
- scheduling
- execution coordination

Technology:

- Java 21
- Spring Boot

### Execution Plane

Responsible for executing pipeline jobs in isolated environments.

Technology:

- Go
- Docker
- gRPC

Runner job output can be delivered to the Control Plane through a dedicated
mTLS endpoint with bounded capture and explicit completion metadata. See
[Runner job logs](./docs/architecture/runner-job-logs.md) for configuration,
ordering, and retention limits.

### Infrastructure

The local development environment contains:

- PostgreSQL
- Redis
- Kafka

## Runner log configuration

Runner log delivery is disabled by default. Enabling it through the
`ZEROYAML_JOB_LOGS_*` settings overrides the daemon default log driver for Job
containers with bounded `local` logging so Engine log following remains readable
and bounded. Disabled delivery preserves the daemon log driver and starts no
log follower.

## Repository structure

```text
ZeroYAML/
├── control-plane/
├── runner/
├── infra/
└── docs/
    └── architecture/
# Minimal Pipeline Model

The Control Plane owns `Pipeline`. A pipeline is the ordered plan ZeroYAML
decided to run for one repository revision. It sits between repository
inference and executable Jobs:

```text
repository inference ──> Pipeline ──> one Job per step ──> Runner
        (#22)             (#21)            (#23)
```

The model is deliberately narrow. ZeroYAML infers useful execution plans; it
does not reimplement a general workflow language. The model has no triggers,
matrices, reusable workflows, secret definitions, conditions, or deployment
graphs, and it carries no provider payloads or Runner transport types. The
types live in `io.zeroyaml.controlplane.domain.pipeline`.

## Model contents

| Concept | Representation |
| --- | --- |
| Identity | `PipelineId`, backed by a UUID |
| Source context | The Job model's `RepositoryReference`: provider-neutral URI plus immutable revision |
| Steps | Ordered `List<PipelineStep>`, at least one and at most 50 |
| Step identity | `PipelineStep.name`, unique within the pipeline |
| Step execution | The Job model's `ExecutionDefinition`: argument list plus working directory |
| Outcome | `PipelineOutcome`: a `PipelineStatus` and, for a failed pipeline, a `PipelineFailure` |

A step reuses `ExecutionDefinition` and a pipeline reuses `RepositoryReference`,
so a step becomes a Job without translation: the pipeline's `source()` plus the
step's `execution()` are exactly the inputs of `Job.create`. The command stays an
argument list rather than a shell script, as in the Job model.

Environment and input metadata are omitted because neither `ExecutionDefinition`
nor the current `RunJob` contract carries them. Adding them requires a separate
cross-service contract change.

## Validation rules

Construction is the validation point, and an invalid value is rejected with
`IllegalArgumentException` (`NullPointerException` for a missing required
object). An existing `Pipeline` is therefore always executable.

- A pipeline must have an identity, a source, and at least one step.
- A pipeline must not have more than 50 steps, which keeps the Jobs created from
  it finite.
- A pipeline must not contain a missing step (`NullPointerException`).
- Step names must be unique. The rejection message names the duplicate.
- A step name is 1 to 64 characters of lowercase letters, digits, `.`, `_`, and
  `-`, starting and ending with a letter or digit. Names are never trimmed or
  case-folded, so `unit-test` and `unit_test` are different steps.
- A step's command and working directory follow the Job model's
  `ExecutionDefinition` rules.

## Ordering and identity

The plan orders steps one after another as supplied; executing them in that
order belongs to the application boundary, not this model. The order is
preserved exactly: it is never sorted by name and never reordered. The supplied
list is copied, so a later change to it cannot alter the pipeline, and the
exposed list is unmodifiable. A pipeline is referenced by its `PipelineId` and a
step by its name within that pipeline. `Pipeline` is an immutable value, so two
instances are equal only when identity, source, and steps all match; code that
keys on identity alone must use `PipelineId`.

## Status and failure

`PipelineStatus` is `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, or
`CANCELLED`. `CANCELLED` mirrors the terminal Job state, so a step whose Job was
cancelled maps to a pipeline state without a fabricated failure.
`PipelineFailure` records the name of the failed step and reuses `JobFailure`
for its stable failure code, safe diagnostic message, and optional exit code. An
absent exit code stays absent rather than becoming a successful zero. The step
name follows the same format as `PipelineStep`.

`PipelineOutcome` owns the consistency between a pipeline, its status, and its
failure: a failure is present exactly when the status is `FAILED`, and it must
name a step of that pipeline. Any other combination is rejected with
`IllegalArgumentException`. These are value types only. The rules that move a
pipeline between states belong to the Phase 3 pipeline lifecycle work.

## Out of scope

Repository inspection and inference rules (#22), creating and dispatching Jobs
from a pipeline and correlating each Job to a step (#23), persistence (#28),
Docker-specific details, and compatibility with any workflow YAML format.

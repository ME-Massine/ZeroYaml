# Pipeline Inference

`PipelineInferrer` turns the files at a repository root into a `Pipeline` (see
[`pipeline-model.md`](./pipeline-model.md)). It lives in
`io.zeroyaml.controlplane.domain.pipeline.inference`.

```text
RepositoryReference + RepositorySnapshot ──> PipelineInferrer ──> PipelineInference
   (source, revision)     (file listing)         (pure)           Inferred | Unsupported | Ambiguous
```

## Inputs

- `RepositoryReference`: the repository location and immutable revision the
  pipeline would run against. The caller builds it from the connected repository
  and the normalized push event (#20).
- `RepositorySnapshot`: the set of repository-relative file paths at that
  revision. Inference checks which files exist, never their content. Obtaining the
  listing is a separate concern, so the domain has no provider or network
  dependency. A path must be relative, use `/`, and contain no empty, `.`, or `..`
  segment; a snapshot holds at most 100,000 paths.

## Rules (first supported scenario)

Only the repository root is inspected.

| Root files | Result |
| --- | --- |
| `pom.xml` only (plus any other files) | `Inferred`: `build` then `test` |
| `pom.xml` and one of `build.gradle`, `build.gradle.kts`, `go.mod`, `package.json` | `Ambiguous`, listing the sorted candidates |
| two or more of the non-Maven build files | `Ambiguous` |
| exactly one non-Maven build file | `Unsupported`, naming the file |
| none of them | `Unsupported` |

Inferred steps, run in the working directory `.`:

1. `build`: `sh mvnw --batch-mode -DskipTests package`
2. `test`: `sh mvnw --batch-mode test`

When a root `mvnw` exists the command is `sh mvnw ...` because a checkout may not
preserve the executable bit; without it the command is `mvn ...`, which requires
Maven in the Job image.

## Guarantees

- Deterministic: the same source and snapshot give an equal `Pipeline`. The
  `PipelineId` is a name-based UUID over `location@revision`, not a random value.
- Side-effect free: nothing is executed and no Runner is chosen or consulted.
- Explicit: an unrecognized or conflicting repository returns `Unsupported` or
  `Ambiguous`; inference never picks one silently.

## Out of scope

Other ecosystems (separate issues), content-based detection, nested modules,
fetching the file listing, creating Jobs (#23, see
[`pipeline-job-creation.md`](./pipeline-job-creation.md)), and persistence (#28).

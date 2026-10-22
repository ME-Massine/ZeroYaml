# ZeroYAML Agent Working Agreement

This file defines the rules for every coding agent working in this repository. It applies to the entire repository unless a more specific `AGENTS.md` exists in a subdirectory.

The goal is to keep ZeroYAML professional, scalable, understandable, secure, and easy for multiple developers to change safely.

## 1. Non-negotiable rules

1. Never push code or tags to any remote.
2. Never create, update, approve, merge, or close a pull request. The user performs all publishing and pull-request actions.
3. Never run `git add`, `git commit`, `git push`, `git tag`, `gh pr create`, `gh pr merge`, or equivalent publishing commands.
4. Never push directly to `main`. All changes reach `main` through a reviewed pull request.
5. Never use first-person language to describe agent activity. Avoid `I`, `me`, `my`, `we`, and `our` in responses, documentation, code comments, commits, and proposed pull-request text. Use objective wording such as `Implemented`, `Verified`, or `The change adds`.
6. Never add agent names, generated-by notices, co-author entries, or other agent attribution to source files, commits, documentation, or pull requests.
7. Never expose, print, store, or commit secrets, tokens, credentials, private keys, or sensitive environment values.
8. Never overwrite, delete, stage, or reformat unrelated user changes. Treat every pre-existing modification and untracked file as user-owned.
9. Never bypass tests, required checks, reviews, branch protection, the phase gate, or the progress-file requirement.
10. Never begin work for a later phase while the current phase remains open.

Agents may inspect the repository, edit files locally, run formatters, run tests, and provide a verified handoff. Publishing remains the user's responsibility in every case.

## 2. Required reading before changing code

Before implementation begins, read the relevant parts of:

- `README.md` for the product and repository overview.
- `DEVELOPER_GUIDE.md` for architecture and local-development guidance.
- `docs/project-progress.md` for the active phase, current state, next steps, and blockers.
- The target GitHub issue, including its phase label, dependencies, scope, and acceptance criteria.
- Existing code and tests in the affected package.

Do not assume that an issue is ready only because it is open. Confirm that:

- its `phase:N` label matches `Current phase: N` in `docs/project-progress.md`;
- prerequisite issues are complete;
- the work does not overlap another active issue or local change;
- acceptance criteria are concrete enough to verify.

If a requirement is ambiguous, continue only with a small, reversible assumption that does not change product scope. State the assumption in the handoff.

## 3. Phase and issue workflow

ZeroYAML is delivered sequentially by phase.

1. Work only on issues assigned to the active phase.
2. Keep future-phase issues in the backlog until the current phase is complete.
3. Prefer one issue per branch and one issue per pull request.
4. Keep the change limited to the issue's acceptance criteria. Create or recommend a separate issue for unrelated work.
5. Update `docs/project-progress.md` in every pull request.
6. Do not advance `Current phase` until every issue in the current phase and its parent phase issue are closed.
7. Advance only one phase at a time; phases must never be skipped.
8. A work item becomes `Done` only after its pull request is merged and the acceptance criteria are satisfied.

Suggested branch names:

- `feat/issue-<number>-<short-description>`
- `fix/issue-<number>-<short-description>`
- `docs/issue-<number>-<short-description>`
- `ci/issue-<number>-<short-description>`

## 4. Architecture boundaries

Preserve the central ZeroYAML rule:

> The Control Plane decides and coordinates; the Runner executes.

### Control Plane

- Java 21 and Spring Boot code belongs in `control-plane/`.
- Keep business rules independent from web, database, messaging, and gRPC transport details.
- Put external-system communication behind clear application-facing interfaces.
- Use constructor injection and explicit configuration instead of global state or hidden dependencies.
- Validate configuration during startup and fail with actionable error messages.
- Apply timeouts or deadlines to network calls and represent failures explicitly.

### Runner

- Go execution code belongs in `runner/`.
- Keep executable wiring in `cmd/` and reusable implementation details in `internal/`.
- Pass `context.Context` through operations that perform I/O, wait, or may be cancelled.
- Keep job execution isolated from Control Plane policy decisions.
- Release processes, containers, streams, files, and network resources deterministically.

### Shared protocol

- `proto/` is the source of truth for the Control Plane-to-Runner contract.
- Never edit generated protobuf or gRPC files manually.
- Preserve field numbers and service compatibility. Never reuse a removed protobuf field number.
- Treat contract changes as cross-service changes and test both Java and Go consumers.

### Infrastructure

- Local infrastructure belongs in `infra/`.
- Keep environment-specific values outside source code.
- Pin intentional dependency and image versions; do not silently introduce floating production dependencies.

## 5. Professional and scalable implementation standards

- Prefer small, cohesive classes, packages, and functions with one clear responsibility.
- Use descriptive domain names. Avoid vague names such as `Helper`, `Manager`, `Utils`, `Data`, or `Thing` unless the responsibility is genuinely precise.
- Keep APIs minimal. Do not make classes, methods, fields, or packages public without a consumer that requires it.
- Separate domain decisions from framework and transport code.
- Avoid duplicated logic; extract a shared abstraction only after the common responsibility is clear.
- Do not add speculative abstractions, dependencies, configuration, or features outside the issue scope.
- Prefer immutable values and explicit inputs and outputs.
- Validate data at system boundaries.
- Handle expected failures explicitly. Never swallow exceptions or ignore returned errors.
- Log useful operational context without secrets or full sensitive payloads.
- Remove dead code, commented-out code, temporary debug output, and unused configuration before handoff.
- Every remaining `TODO` or `FIXME` must reference a tracked issue and explain the missing behavior.
- Preserve backward compatibility unless the issue explicitly approves a breaking change and documents migration steps.
- Introduce database changes through versioned migrations that support safe rollout and rollback planning.
- Treat REST, event, and gRPC schemas as versioned contracts; document compatibility impact when they change.
- Keep dependency changes minimal, justified, compatible, and covered by build verification.
- Use the repository's existing style. Avoid broad formatting changes in feature pull requests.

## 6. Comments and documentation

Code must be understandable without reconstructing hidden assumptions.

- Add Javadoc or Go documentation to public APIs and important architectural boundaries.
- Comment why a non-obvious decision, invariant, timeout, compatibility rule, or failure mapping exists.
- Document concurrency, lifecycle, security, retry, and ownership behavior where relevant.
- Do not comment obvious syntax or repeat what the code already says.
- Keep comments accurate when behavior changes. Stale comments are defects.
- Use examples when configuration or an API is easy to misuse.
- Update `README.md`, `DEVELOPER_GUIDE.md`, and architecture documentation when their behavior or setup instructions change.
- Update `docs/project-progress.md` with truthful project state; do not claim local, unmerged work is released or completed on `main`.

## 7. Testing and verification

Every behavior change requires tests at the lowest useful level. Add integration tests when behavior crosses a process, transport, persistence, or framework boundary.

Tests must cover:

- the successful path;
- relevant validation and boundary cases;
- expected failure behavior;
- regressions fixed by the change.

Tests must be deterministic and must not depend on execution order, public services, developer-specific paths, or arbitrary sleeps.

Run the relevant commands from the repository root before handoff.

### Control Plane

```powershell
Push-Location control-plane
.\mvnw.cmd --batch-mode verify
Pop-Location
```

### Runner

```powershell
Push-Location runner
gofmt -w <changed-go-files>
go test ./...
Pop-Location
```

### Repository checks

```powershell
git diff --check
git status --short
```

Run both service test suites when changing `proto/`, shared behavior, build configuration, or CI. If a required command cannot run, report the exact command, error, and unverified risk. Never report a test as passing unless it completed successfully.

## 8. Security and reliability

- Use least privilege for credentials, permissions, APIs, containers, and CI jobs.
- Keep secrets in environment variables or an approved secret store, never in tracked files.
- Sanitize and validate all external input, including webhook payloads, repository metadata, job definitions, and runner messages.
- Verify webhook signatures before trusting events.
- Apply authentication and authorization at trust boundaries.
- Use bounded retries with backoff only for operations that are safe to retry.
- Make retryable operations idempotent where practical.
- Set timeouts for network and external-process operations.
- Avoid unbounded queues, goroutines, thread pools, payloads, logs, and in-memory collections.
- Preserve audit-relevant identifiers in logs while excluding credentials and sensitive content.
- Do not weaken security or reliability controls to make a test pass.

## 9. Git and pull-request standards

The user performs all Git publishing steps. At handoff, provide exact PowerShell commands tailored to the current branch and changed files.

The proposed commit should follow Conventional Commits, for example:

```text
feat(runner): connect control plane over grpc
fix(webhook): reject invalid signatures
docs(progress): update phase 1 state
ci(gate): enforce phase issue references
```

Every proposed pull request must:

- target `main` from a non-protected task branch;
- contain a focused change for the referenced issue;
- use `Closes #<issue-number>` for the issue it completes;
- update `docs/project-progress.md`;
- explain the behavior change and important design decisions;
- list the exact validation commands and results;
- identify risks, compatibility concerns, and follow-up work;
- pass `runner`, `control-plane`, `progress-file`, and `phase-gate` checks;
- receive one approving review and resolve all review threads before merge;
- use squash merge unless the repository policy changes.

Never recommend bypassing these requirements. An administrative bypass is for exceptional repository recovery, not normal delivery.

## 10. Required handoff format

Every completed task must end with:

1. A concise outcome summary.
2. The files changed and why.
3. Tests and checks run, including pass/fail status.
4. Remaining risks, assumptions, or follow-up work.
5. Confirmation that nothing was pushed and no pull request was created.
6. Exact commands for the user to review, stage, commit, push, and open the pull request.

Use explicit paths in staging commands. Never suggest `git add .`, because it can include unrelated user files.

Tailor this PowerShell template to the actual issue, branch, files, and test results:

```powershell
# Review the local work first.
git status --short
git diff --check
git diff -- <explicit-file-1> <explicit-file-2>

# Stage only files that belong to the issue.
git add <explicit-file-1> <explicit-file-2> docs/project-progress.md

# Create the user's commit.
git commit -m "<type>(<scope>): <concise-description>"

# Publish the task branch.
git push -u origin <task-branch>

# Open the pull request with an issue-closing reference.
$prBody = @'
## Summary

<what changed and why>

## Issue tracking

- Issue reference: Closes #<issue-number>
- This change belongs to the current active phase.

## Project progress

- Updated `docs/project-progress.md` with the current project state.

## Validation

- `<exact command>` — passed

## Risks and follow-up

- <known risks or `None`>
'@

gh pr create --repo MohamedMBG/ZeroYaml --base main --head <task-branch> --title "<pull-request-title>" --body $prBody
```

Placeholders must be replaced with real values before presenting commands. The agent must not execute any command from the staging, commit, push, or pull-request sections.

## 11. Definition of done

Local implementation is ready for user review only when:

- the issue acceptance criteria are satisfied;
- architecture boundaries remain intact;
- code is professional, scalable, focused, and appropriately documented;
- tests cover success and important failure paths;
- relevant local checks pass;
- `docs/project-progress.md` accurately reflects the proposed change;
- unrelated user work remains untouched;
- no secrets or generated artifacts are included;
- a clear handoff and exact user-run publishing commands are provided.

Merge completion is reached only after the pull request passes all required checks, receives one approval, resolves review feedback, and is merged by an authorized human.

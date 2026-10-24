# Runner job logs

The Runner captures Job container stdout and stderr. The Control Plane owns
retention and exposure; receiving output never changes Job lifecycle state.
Checkout output is excluded because Git diagnostics can expose repository
credentials. No host environment variables are added to Job containers.

## Capture and delivery

The sandbox creates a non-TTY Job container, starts it, follows `docker logs
--follow`, reads the exit code with `docker wait`, and removes the container.
Following reads retained output even when the command exited before the CLI
attached. Docker demultiplexes stdout and stderr into separate CLI pipes.

`runner/internal/joblogs.Capture` serializes pipe observations and emits chunks
of at most 4096 raw bytes. Partial lines, binary output, and split UTF-8 sequences
are preserved. Sequence starts at 1 and increases across both streams.
`observed_at` is the Runner's UTC observation time. Sequence determines order;
timestamps may move backwards if the host clock changes. Bytes within each
stream retain their order, but concurrent OS pipes cannot reconstruct exact
cross-stream command write order. Consumers concatenate each stream's bytes
before decoding text.

Each chunk travels synchronously over the additive
`JobLogService.AppendJobLog` unary RPC with protocol, Job, Runner, and instance
identity. Each call has a deadline from
`ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT` and respects the execution deadline.
Slow calls apply bounded backpressure without a background queue. No retries
occur. A delivery error suppresses subsequent data calls, drains remaining
output, and still attempts an incomplete final marker. Log delivery failures
never replace the command's exit code or fail an otherwise successful Job.
Older Control Planes return `UNIMPLEMENTED`; the Runner continues execution.
Existing RPC messages and field numbers are unchanged.

After cleanup, the sandbox attempts one empty `end_of_output` marker, with
no stream, before the dispatcher reports terminal execution status:

| Marker | Meaning |
| --- | --- |
| `truncated` | Capture exceeded 1 MiB or 256 data chunks; excess bytes were drained. |
| `incomplete` | Capture/delivery failed, or execution was cancelled, timed out, or could not execute. |
| No final marker | Output completeness is unknown; the Runner or transport may have stopped. |

Non-zero command exit alone does not make output incomplete. An empty command
output still has a final marker. Final delivery uses a detached context with
the same call timeout so cancellation can still be reported; the existing
process shutdown budget continues to bound the overall drain.

## Resource and retention bounds

Capture sends at most 256 data chunks plus one final marker, keeping only the
current chunk and the fixed CLI pipe buffers in Runner memory. The combined
stdout/stderr byte budget is 1 MiB. Very small writes can exhaust the chunk
budget first. Docker's Job log driver is explicitly `local`, with `max-size=10m`
and `max-file=1` (compression disabled, as required for a single file); its
buffer is deleted with the container. A command that emits
more than that buffer before following starts may lose its earliest bytes;
capture retains the first bytes observed by the follower and marks budget
overflow as truncated.

The Control Plane checks field bounds (including a 256-character maximum per
Runner identity field), protobuf timestamp validity, the
existing Job's assigned Runner process, and sequence continuity. The running
status report normally establishes that assignment before output starts. If
that report was lost and no assignment exists, logs are refused; a later
terminal status report may still recover Job state. Unknown Jobs return
`NOT_FOUND`; malformed records return `INVALID_ARGUMENT`; wrong assignment,
sequence conflicts, and post-final records return `FAILED_PRECONDITION`.
An incomplete final marker can skip missing sequence numbers. Duplicates are
refused rather than retained twice. Receiving logs for a terminal Job is valid,
because status and output are separate observations.

`JobLogStore` is the application-facing retention boundary. The in-memory
implementation keeps at most 32 Jobs, each bounded to 1 MiB and 257 records,
and evicts the oldest admitted Job as a unit, including unfinished output.
`snapshot(JobId)` returns immutable ordered records with defensive byte copies;
an empty list means unknown or evicted output. An evicted active sender's next
data chunk is refused, and its incomplete final marker can be retained as a
new partial log. Restart loses every record. Durable storage, REST exposure,
search, and a browser viewer are outside this change.

Output can contain secrets deliberately printed by user commands. No automatic
redaction is claimed. Payloads are carried only in the log contract and never
copied into execution status diagnostics or service operational logs. Remote
delivery-error descriptions are also excluded from operational logs.

## Verification

The normal Runner suite covers both streams, raw bytes, deterministic sequence
context, concurrent writes, empty output, large output, tiny-write limits,
failed execution, cancellation, capture errors, gRPC deadlines, and payload
exclusion from operational logs. Control Plane transport tests cover byte
fidelity, assignment validation, sequence rules, timestamp and payload bounds,
the 1 MiB limit, FIFO eviction, immutable snapshots, and failed Jobs.

The isolated real-Docker test needs the Docker CLI, a running daemon, and
`alpine:3.22`. It exercises the Job container directly without checkout:

```powershell
Push-Location runner
$env:ZEROYAML_RUNNER_DOCKER_TESTS = '1'
go test -count=1 -run '^TestDockerExecutionLogCapture$' ./internal/sandbox/
Remove-Item Env:ZEROYAML_RUNNER_DOCKER_TESTS
Pop-Location
```

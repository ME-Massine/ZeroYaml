# Runner job logs

The Runner captures Job container stdout and stderr. The Control Plane owns
retention and exposure; receiving output never changes Job lifecycle state.
Checkout output is excluded because Git diagnostics can expose repository
credentials. No host environment variables are added to Job containers.

## Capture and delivery

The sandbox creates a non-TTY Job container, starts it, runs `docker wait`
alongside `docker logs --follow`, and removes the container after both finish.
An exit observed before the execution deadline remains the command outcome
even if log following is interrupted by that deadline.
Following reads retained output even when the command exited before the CLI
attached. Docker demultiplexes stdout and stderr into separate CLI pipes.

`runner/internal/joblogs.Capture` serializes pipe observations and emits chunks
of at most 4096 raw bytes. Partial lines, binary output, and split UTF-8 sequences
are preserved. Sequence starts at 1 and increases across both streams.
`observed_at` is the Runner's UTC observation time. Sequence determines order;
timestamps may precede Control Plane Job creation because the two hosts have
independent clocks, and may move backwards if the Runner clock changes. Bytes
within each stream retain their order, but concurrent OS pipes cannot reconstruct exact
cross-stream command write order. Consumers concatenate each stream's bytes
before decoding text.

Each chunk travels synchronously over the additive
`JobLogService.AppendJobLog` unary RPC with protocol, Job, Runner, and instance
identity on a dedicated mTLS listener. One Reporter owns a reusable,
concurrency-safe connection for all Jobs and closes it after execution drain.
Each call has a deadline from
`ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT` and respects the execution deadline.
Slow calls apply bounded backpressure without a background queue. No retries
are configured. A failed chunk is not retried. Later chunks are still sent after
one failure; after a second data delivery failure for the same Job, the Runner
drains remaining output without further data RPCs so a stalled ingress cannot
consume the execution deadline through repeated timeouts. The final marker is
still attempted.
The final marker sets both `incomplete` and `truncated` when a data delivery was
not acknowledged; that chunk may have been retained if only its response was
lost. Sequence gaps describe missing observations without poisoning later calls.
Log delivery failures never replace the command's exit code or fail an otherwise successful Job.
Existing registration/status RPCs and field numbers are unchanged. The new
stream names are `JOB_LOG_STREAM_STDOUT` and `JOB_LOG_STREAM_STDERR` (numeric
values 1 and 2); generated Go and Java consumers use those names. No released
consumer uses the previous names from this unmerged PR.

After cleanup, the sandbox attempts one empty `end_of_output` marker, with
no stream, before the dispatcher reports terminal execution status:

| Marker | Meaning |
| --- | --- |
| `truncated` | Capture exceeded 1 MiB or 256 chunks, or a data delivery was not acknowledged. |
| `incomplete` | Capture/delivery failed, or execution was cancelled, timed out, or could not execute. |
| No final marker | Output completeness is unknown; the Runner or transport may have stopped. |

Non-zero command exit alone does not make output incomplete. An empty command
output still has a final marker. Final delivery uses a detached context with
the same call timeout so cancellation can still be reported; the existing
process shutdown budget continues to bound the overall drain. Capture state
locks are never held over network I/O. Closing cancels an active data call,
waits for its completion under the caller's context, then attempts the final
marker; marking an output-source error does not wait for delivery.

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
existing Job's assigned Runner process, and strictly increasing sequences. The running
status report normally establishes that assignment before output starts. If
that report was lost and no assignment exists, logs are refused; a later
terminal status report may still recover Job state. Unknown Jobs return
`NOT_FOUND`; malformed records return `INVALID_ARGUMENT`; wrong assignment,
sequence conflicts, and post-final records return `FAILED_PRECONDITION`.
Data chunks may skip missing sequence numbers after a delivery failure; a final
marker following any gap must be incomplete. Duplicate and decreasing sequences
are refused rather than retained twice. Receiving logs for a terminal Job is valid,
because status and output are separate observations.

`JobLogStore` is the application-facing retention boundary. The in-memory
implementation keeps at most 32 Jobs, each bounded to 1 MiB and 257 records,
and evicts only the oldest admitted Job with a retained final marker. Active
output is never evicted. If all 32 slots lack final markers, a new Job's chunks
and final marker return `RESOURCE_EXHAUSTED` without changing existing output.
Once a slot is available, later chunks from that sender can be admitted with
their original sequences and an incomplete end. A sender that disappears without
a final marker keeps its slot until restart; no background expiry is implemented.
`snapshot(JobId)` returns immutable ordered records with defensive byte copies;
an empty list means unknown or evicted output. Restart loses every record. Durable storage, REST exposure,
search, and a browser viewer are outside this change.

Output can contain secrets deliberately printed by user commands. No automatic
redaction is claimed. Payloads are carried only in the log contract and never
copied into execution status diagnostics or service operational logs. Remote
delivery-error descriptions are also excluded from operational logs.

## Authentication and configuration

Log delivery is disabled by default on both services. The log service is never
registered on the existing plaintext registration/status listener. The separate
listener requires mutual TLS using gRPC transport credentials
([gRPC authentication guide](https://grpc.io/docs/guides/auth/)); there is no
insecure mode. A partial configuration or unreadable/invalid certificate fails
startup rather than silently downgrading delivery. A Runner with delivery disabled
still drains command output and reports execution status.

Provision certificates outside the repository through the deployment's PKI:

- The server certificate must match the host in the Runner's log address and be
  trusted by the configured server CA.
- Each Runner client certificate must be trusted by the Control Plane client CA,
  permit client authentication, and carry exactly one Runner URI SAN of the form
  `urn:zeroyaml:runner:<runner_id>`, for example `urn:zeroyaml:runner:local-runner`.
  The URI suffix must exactly match the configured Runner ID. Certificates without
  that identity are rejected, regardless of their subject/common name.
- The authenticated Runner ID must match the request's Runner ID. The Job must
  also be assigned to that exact `(runner_id, instance_id)` pair. A certificate
  authorizes its Runner ID across process restarts; instance attribution remains
  checked against the authoritative assignment. Protect its private key accordingly.
- Keep private keys out of Job containers, source control, and operational logs.
  Rotate certificates by provisioning replacement files and restarting the service.
  Changes to registration/status authentication remain outside this log-ingress fix.

| Service | Setting | Purpose |
| --- | --- | --- |
| Control Plane | `zeroyaml.job-logs.port` / `ZEROYAML_JOBLOGS_PORT` | Dedicated log port, for example 50053; no default. |
| Control Plane | `zeroyaml.job-logs.certificate` / `ZEROYAML_JOBLOGS_CERTIFICATE` | PEM server certificate chain path. |
| Control Plane | `zeroyaml.job-logs.private-key` / `ZEROYAML_JOBLOGS_PRIVATEKEY` | PEM server private key path. |
| Control Plane | `zeroyaml.job-logs.client-ca` / `ZEROYAML_JOBLOGS_CLIENTCA` | PEM CA bundle for Runner client certificates. |
| Runner | `ZEROYAML_JOB_LOGS_ADDRESS` | Dedicated log host:port matching the server certificate; no default. |
| Runner | `ZEROYAML_JOB_LOGS_CERTIFICATE` | PEM Runner client certificate chain path. |
| Runner | `ZEROYAML_JOB_LOGS_PRIVATE_KEY` | PEM Runner private key path. |
| Runner | `ZEROYAML_JOB_LOGS_CA` | PEM CA bundle for server verification. |

The server limits incoming log messages to 8192 bytes (including metadata fields
in the protobuf record) and TLS handshakes to five seconds. Invalid or missing
client credentials fail the handshake. A verified certificate without the Runner
URI SAN returns `UNAUTHENTICATED`; a different authenticated Runner ID returns
`PERMISSION_DENIED`, without retaining content. Payload validation and assignment
checks still apply after authentication.

## Verification

The normal Runner suite covers both streams, raw bytes, deterministic sequence
context, concurrent writes, empty output, large output, tiny-write limits,
failed execution, cancellation, capture errors, gRPC deadlines, and payload
exclusion from operational logs. Control Plane transport tests cover byte
fidelity, assignment validation, sequence rules, timestamp and payload bounds,
the 1 MiB limit, completed-output eviction, active-retention saturation,
immutable snapshots, and failed Jobs. Regression tests also cover independent
host clocks, sequence gaps after loss, cancellation of blocked delivery,
connection reuse, TLS trust, and forged certificate/request identities. Test
certificates and keys are generated in temporary storage; none are checked in.

The isolated real-Docker test needs the Docker CLI, a running daemon, and
`alpine:3.22`. It exercises the Job container directly without checkout:

```powershell
Push-Location runner
$env:ZEROYAML_RUNNER_DOCKER_TESTS = '1'
go test -count=1 -run '^TestDockerExecutionLogCapture$' ./internal/sandbox/
Remove-Item Env:ZEROYAML_RUNNER_DOCKER_TESTS
Pop-Location
```

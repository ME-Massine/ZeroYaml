# Runner job logs

The Runner captures Job container stdout and stderr. The Control Plane owns
retention and exposure; receiving output never changes Job lifecycle state.
Checkout output is excluded because Git diagnostics can expose repository
credentials. No host environment variables are added to Job containers.

## Capture and delivery

The sandbox creates a non-TTY Job container, starts it, runs `docker wait`
alongside an Engine follow-logs request through `docker system dial-stdio`, and
removes the container after both finish. The CLI supplies the configured daemon
transport, including Windows named pipes, without adding a Docker SDK.
An exit observed before the execution deadline remains the command outcome
even if log following is interrupted by that deadline.
Following reads retained output even when the command exited before the CLI
attached. The Runner reads the HTTP response and decodes non-TTY Engine frames
into stdout and stderr. Docker CLI stderr is discarded separately, so a CLI
diagnostic cannot be attributed to the Job. Container bytes are forwarded as
frames arrive, including stderr written before a
cancelled follower exits. A follower error marks output incomplete without
copying pipe contents into execution diagnostics.

`runner/internal/joblogs.Capture` serializes pipe observations and emits chunks
of at most 4096 raw bytes. One pending buffer coalesces writes and is sent when
it is full, when stdout/stderr switches, when a write arrives after the pending
bytes have waited one minute, or when capture closes. Close flushes pending
bytes before the final marker using its detached context. No timer or
background goroutine flushes idle output, so bytes followed by silence wait
for the next write or the end of output. The one-minute age stays well below
the Control Plane's five-minute idle eviction, so slow but live output keeps
its retained prefix, while 256 chunks still cover more than four hours of
one-line-per-minute output.
Partial lines, binary output, and split UTF-8 sequences
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
Once the command has exited, the follower gets a fixed ten-second drain bound.
Slow but successful delivery would otherwise hold the container, the
concurrency slot, and the terminal status report until the execution deadline.
When the bound expires, the sandbox cancels in-flight and later data delivery,
stops the follower, and marks the output `incomplete`; pending bytes and the
final marker are still sent by Close.
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
the same call timeout so cancellation can still be reported. When dispatch is
cancelled, the whole close (pending flush and final marker) is bounded by one
second or a fifth of `ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT`, whichever is shorter,
to reserve shutdown time for terminal status reporting after cleanup.
The process shutdown budget still bounds the overall drain. Capture state
locks are never held over network I/O. Closing cancels an active data call,
waits for its completion under the caller's context, then attempts the final
marker; marking an output-source error does not wait for delivery.

## Resource and retention bounds

Capture sends at most 256 data chunks plus one final marker, keeping only the
current chunk, an eight-byte frame header, and a fixed 4096-byte frame buffer in
Runner memory, alongside fixed CLI pipe buffers. The combined
stdout/stderr byte budget is 1 MiB. Frequent stream switches can exhaust the
chunk budget first; short writes on one stream share a chunk. When delivery is
enabled, the Runner overrides the daemon default with the `local` Job log driver
to provide readable, bounded Engine logs, with `max-size=10m`
and `max-file=1` (compression disabled, as required for a single file); its
buffer is deleted with the container. A command that emits
more than that buffer before following starts may lose its earliest bytes;
capture retains the first bytes observed by the follower and marks budget
overflow as truncated.

Rotation while following is also a best-effort boundary: a follower delayed by
delivery backpressure may fall behind the retained file. The Engine stream has
no source byte offsets, so capture cannot detect a daemon-side gap or mark it
`incomplete`/`truncated` unless a separate capture limit or error occurs. Those
flags describe observed capture and delivery, not guaranteed lossless Docker
retention. `TestDockerLogRotationWithBlockedFollower` pauses the reader before
releasing a container to emit 32 MiB through the production `local` options,
waits for exit, then resumes the reader. On Docker Engine 29.8.2 (2026-10-29),
all 33,554,442 generated bytes were observed with no follower error: the proposed
mid-stream loss was not reproduced. This result does not guarantee behavior
across daemon versions or other backpressure schedules.

The Control Plane checks field bounds (including a 256-character maximum per
Runner identity field), protobuf timestamp validity, the
existing Job's assigned Runner process, and strictly increasing sequences. The running
status report normally establishes that assignment before output starts. If
that report was lost and no assignment exists, logs are refused; a later
terminal status report may still recover Job state. Admission uses a read-only
`JobStore.find`: the assignment never changes once set. Later chunks and the final
marker validate against the retained Runner identity without a PostgreSQL read.
The retained check and append are atomic with eviction; an absent or evicted
stream must pass authoritative attribution again before admission. Concurrent
first records may each perform a lookup, but sequence validation still admits
each observation at most once. No database lookup holds the retention lock, and
log ingress never locks or updates the PostgreSQL Job row. Unknown Jobs return
`NOT_FOUND`; malformed records return `INVALID_ARGUMENT`; wrong assignment,
sequence conflicts, and post-final records return `FAILED_PRECONDITION`.
Unexpected storage failures return `INTERNAL`, without exposing storage details
to the caller; the server logs the exception and parsed Job identity at error
level without logging chunk content.
Data chunks may skip missing sequence numbers after a delivery failure; a final
marker following any gap is retained as incomplete, even if the sender reports
complete delivery: acknowledged chunks may have been evicted locally. The
server preserves the sender's sequence, timestamp, and truncation flag.
Duplicate and decreasing sequences are refused rather than retained twice.
A retained stream still accepts its remaining records after the Job reaches a
terminal state, because status and output are separate observations. A record
that needs admission (no retained stream, or an evicted one) for a terminal Job
returns `FAILED_PRECONDITION`: the Runner sends its final marker before the
terminal status report, so a legitimate record never needs admission after
completion, and an evicted finished log cannot be reopened as a new fragment.

`JobLogStore` is the application-facing retention boundary. The in-memory
implementation keeps at most 32 Jobs, each bounded to 1 MiB and 257 records.
At capacity it evicts the oldest admitted Job whose output has a final marker
or has received no records for at least five minutes. The idle interval uses
Control Plane receipt time, independent of Runner timestamps. Successful
appends refresh it; snapshots and rejected records do not. Recent active output
is preserved. If all 32 slots have recent unfinished output, a new Job's chunks
and final marker return `RESOURCE_EXHAUSTED` without changing existing output.
Once a slot is available, later chunks from that sender can be admitted with
their original sequences; the store marks the final record incomplete when
earlier chunks are absent, including when only the final marker returns.
Missing final markers therefore
cannot occupy capacity indefinitely. Eviction happens only when admitting a new
Job at capacity, with no background sweep. A running Job silent for five minutes
can also lose its earlier output; late records may form a new retained fragment,
and the missing initial sequences signal that earlier observations are absent.
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
keeps the daemon default log driver, starts no log follower, and still reports
execution status.

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
failed execution, cancellation, capture errors, gRPC deadlines, age-based
flushing of slow output, the drain bound after command exit, the shutdown-derived
close bound, and payload exclusion from operational logs. Control Plane transport tests cover byte
fidelity, assignment validation, sequence rules, timestamp and payload bounds,
the 1 MiB limit, completed-output and idle-stream eviction, active-retention saturation,
immutable snapshots, retained output of failed Jobs, and refused admission for
terminal Jobs, including after a finished stream was evicted. Regression tests also cover independent
host clocks, sequence gaps after loss, cancellation of blocked delivery,
connection reuse, TLS trust, forged certificate/request identities, read-only
PostgreSQL attribution during a locked lifecycle update, and storage failures. Test
certificates and keys are generated in temporary storage; none are checked in.

The isolated real-Docker test needs the Docker CLI, a running daemon, and
`alpine:3.22`. It exercises the Job container directly without checkout:

```powershell
Push-Location runner
$env:ZEROYAML_RUNNER_DOCKER_TESTS = '1'
go test -count=1 -run '^TestDockerExecutionLogCapture$' ./internal/sandbox/
go test -count=1 -v -run '^TestDockerLogRotationWithBlockedFollower$' ./internal/sandbox/
Remove-Item Env:ZEROYAML_RUNNER_DOCKER_TESTS
Pop-Location
```

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
A follower process failure after a whole Engine frame also marks capture
incomplete. On ordinary end of output, the Runner closes the request pipe and
checks the CLI's exit status instead of killing it and hiding that status. The
CLI has at most five seconds to exit, within the follower's existing drain
bound. Malformed output, cancellation, and the capture limit still stop it
immediately; reaching the capture limit alone means truncated output, not a
capture failure. CLI diagnostics never enter the returned capture error.

`runner/internal/joblogs.Capture` serializes pipe observations and emits chunks
of at most 4096 raw bytes. Each stream coalesces writes into its own pending
chunk, which is sent when it is full, when any write arrives after it has
waited one minute, or when capture closes. A stream switch does not send a
partial chunk: interleaved line output, such as `set -x` traces or Maven
warnings on stderr between stdout lines, would otherwise spend one chunk per
line and exhaust the 256-chunk budget after a few KiB. Close flushes pending
chunks oldest first byte first, before the final marker, using its detached
context. No timer or background goroutine flushes idle output, so bytes
followed by silence wait for the next write or the end of output. The
one-minute age stays well below the Control Plane's five-minute idle eviction,
so output that keeps arriving at least every few minutes keeps its retained
prefix, while 256 chunks still cover more than two hours of
one-line-per-minute output on both streams. Aged output is sent only on the
next write, so a Job silent for longer than five minutes may still lose its
retained prefix under capacity pressure.
Partial lines, binary output, and split UTF-8 sequences
are preserved. Sequence starts at 1 and increases across both streams in send
order. `observed_at` is the Runner's UTC observation time of a chunk's first
byte. Timestamps may precede Control Plane Job creation because the two hosts
have independent clocks, and may move backwards if the Runner clock changes.
Bytes within each stream retain their order. Across streams, order is kept at
chunk granularity only: a full chunk of one stream can be sent before the
other stream's older, still-filling chunk, and lines of the two streams inside
overlapping chunks are not interleaved. Concurrent OS pipes could not
reconstruct exact cross-stream write order either. Consumers concatenate each
stream's bytes before decoding text.

Each chunk travels synchronously over the additive
`JobLogService.AppendJobLog` unary RPC with protocol, Job, Runner, and instance
identity on a dedicated mTLS listener. One Reporter owns a reusable,
concurrency-safe connection for all Jobs and closes it after execution drain.
Each call has a deadline from
`ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT` and respects the execution deadline.
Slow calls apply bounded backpressure without a background queue. No retries
are configured. A failed chunk is not retried. Later chunks are still sent after
a failure, and every acknowledged chunk resets the failure count. After two
consecutive data delivery failures for the same Job, data delivery pauses for
30 seconds: output observed meanwhile, including the other stream's pending
chunk, is dropped without a data RPC and the log is marked `truncated`. A
stream that lost output during the pause sends its pending chunk before its
next bytes start a new one, so no record joins output from before and after
the gap. The first chunk after the pause is a probe. Its success resumes delivery; its
failure pauses delivery again. A stalled ingress therefore costs at most one
deadline-bounded call per 30 seconds instead of consuming the execution
deadline one chunk at a time, while a transient refusal, such as
`RESOURCE_EXHAUSTED` during a retention burst or a short network interruption,
loses only the output of one pause. A send cancelled by the Runner itself (the
drain bound, a Job timeout, or a cancellation) is lost but not counted as a
failure, because it says nothing about the ingress; otherwise it could start a
pause that drops the pending chunks Close is meant to send. Such a send is
logged at debug level only, not as a delivery failure. A call that exceeds
its own deadline still counts and still warns. Sequence numbers are assigned only to sent
chunks, so drops leave no sequence gaps. Once the Job container start was
attempted, the final marker is always attempted.
Once the command has exited, the follower gets a fixed drain bound of 35
seconds. Delivery sends one chunk at a time; the bound fits a full 1 MiB backlog
at about 136 ms per acknowledgment. Following stops as soon as the 1 MiB or
256-chunk budget discards output, so the bound only matters while delivery is
slow. Slow but successful delivery would otherwise hold the container, the
concurrency slot, and the terminal status report until the execution deadline.
When the bound expires, the sandbox cancels in-flight and later data delivery,
stops the follower, and marks the output `incomplete`; pending bytes and the
final marker are still sent by Close. When `docker wait` fails without an
observed exit, the container may still be running, so the follower is stopped
the same way at once instead of waiting for the drain bound; the wait error is
the reported execution failure.
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
| `incomplete` | Capture/delivery failed, or execution was cancelled, timed out, or could not execute after the Job container start was attempted. |
| No final marker | The Job container was never started, or output completeness is unknown because the Runner or transport stopped. |

Non-zero command exit alone does not make output incomplete. An empty command
output still has a final marker. When execution ends before the Job container
start is attempted (workspace creation, checkout, or Job container creation
failed, or the execution was stopped first), no command output can exist, so no
record is sent: the reader answers `404` and no retention slot is used. The
terminal status report carries the failure. A failed start still gets a final
marker, because the command may have run. Final delivery uses a detached context, so
cancellation can still be reported, and every call keeps its own timeout. The
whole close (at most the two pending chunks and the final marker) has one
overall bound of five seconds, including after a Job timeout; without it, a slow
ingress could delay the terminal status report by three call timeouts. The
pending chunks may use only the first half of the remaining close bound, so one
slow chunk send cannot leave the final marker without time to be delivered; a
chunk cut off by that share is lost, and the marker reports the output as
incomplete and truncated. The marker stays last because the Control Plane
refuses records after a final marker. Output delivery after command exit
therefore takes at most 35 + 5 = 40 seconds, excluding container cleanup.
When dispatch is cancelled, the close is instead bounded by one second or a
fifth of `ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT`, whichever is shorter, to reserve
shutdown time for terminal status reporting after cleanup.
If shutdown starts during an existing close, pending sends are cancelled
immediately and the remaining close is capped by that shorter bound. The final
marker keeps the remaining window; a cancelled pending chunk marks it
incomplete and truncated. If the normal close deadline is earlier, it still
wins. The shutdown watcher and its timer are released when close finishes.
The process shutdown budget still bounds the overall drain. Capture state
locks are never held over network I/O. Closing cancels an active data call,
waits for its completion under the caller's context, then attempts the final
marker; marking an output-source error does not wait for delivery.

## Resource and retention bounds

Capture sends at most 256 data chunks plus one final marker, keeping only one
pending chunk per stream, an eight-byte frame header, and a fixed 4096-byte
frame buffer in Runner memory, alongside fixed CLI pipe buffers. A chunk counts
against the budget when it starts, so both streams' pending chunks always fit.
The combined stdout/stderr byte budget is 1 MiB. Short writes and stream
switches share chunks; only age-based flushes of slow output spend partial
chunks. Once the budget discards output, the capture writer returns
`joblogs.ErrOutputLimit` and the follower stops reading, so output beyond the
budget costs no further Engine stream reads or delivery handshakes. The
container writes to its log driver rather than to the follower, so stopping
never blocks the command. When delivery is
enabled, the Runner overrides the daemon default with the `local` Job log driver
to provide readable, bounded Engine logs, with `max-size=10m`
and `max-file=2`, compression disabled; its files are deleted with the
container, and use at most 20 MB of daemon disk per Job. The second file keeps
the rotated file readable for a follower held back by delivery backpressure; a
single file is replaced in place on rotation. A command that emits more than
that buffer before following starts may lose its earliest bytes. Capture marks
only its own budget overflow and delivery loss as `truncated`; such a
daemon-side loss is not detected or flagged.

Rotation while following is therefore a best-effort boundary: a follower delayed by
delivery backpressure may still fall behind the retained files. The Engine stream has
no source byte offsets, so capture cannot detect a daemon-side gap or mark it
`incomplete`/`truncated` unless a separate capture limit or error occurs. Those
flags describe observed capture and delivery, not guaranteed lossless Docker
retention. Because following stops once the 1 MiB budget is spent, a loss can
only affect output the follower had not read by then. `TestDockerLogRotationWithBlockedFollower` pauses the reader before
releasing a container to emit 32 MiB through the production `local` options,
waits for exit, then resumes the reader. On Docker Engine 29.8.2, all
33,554,442 generated bytes were observed with no follower error, both with a
single file (2026-10-29) and with the current two files (2026-11-01): the
proposed mid-stream loss was not reproduced. This result does not guarantee
behavior across daemon versions or other backpressure schedules. The fixture
now fails on any byte-count mismatch, so a future loss in this tested schedule
cannot silently pass as a successful measurement.

The Control Plane checks field bounds (including a 256-character maximum per
Runner identity field), protobuf timestamp validity, the
existing Job's assigned Runner process, and strictly increasing sequences. The running
status report normally establishes that assignment before output starts. If
that report was lost and no assignment exists, logs are refused; a later
terminal status report may still recover Job state. Admission uses a read-only
`JobStore.find`: the assignment never changes once set. Later chunks and the final
marker validate against the retained Runner identity without a PostgreSQL read.
Admission checks retention capacity before that lookup, so overload never costs
a database read per refused record. At full retention, a record that needs
admission therefore returns `RESOURCE_EXHAUSTED` even if it would otherwise be
refused for its attribution or a terminal Job; the capacity check is advisory
and admission rechecks it under the retention lock.
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
an empty list means unknown or evicted output. Restart loses every record. Durable storage,
search, and a browser viewer are outside this change.

## Reading retained output

`GET /api/v1/jobs/{jobId}/logs` returns the retained output of one Job through
`JobLogStore.snapshot`. It is disabled until `zeroyaml.job-logs.read.token`
(`ZEROYAML_JOB_LOGS_READ_TOKEN`) is set to a token of at least 32 characters;
while it is unset or empty, every request answers `404`, like an absent
endpoint. A token that is only whitespace, or that has surrounding whitespace,
fails startup instead, because it usually means a secret rendered empty. Logs can
contain secrets printed by user commands, so reading always requires
`Authorization: Bearer <token>`. The scheme name is case-insensitive
(RFC 7235); the token is not. The token check runs before the Job identity is
parsed and compares SHA-256 digests in constant time. One shared token is the
smallest viable control while the Control Plane has no user authentication;
anyone holding it can read every retained log, so store and rotate it like the
webhook secret. Rotating it means restarting with a new value.

| Status | Meaning |
| --- | --- |
| `200` | Retained data chunks in sequence order, `Cache-Control: no-store`. |
| `400` | The Job identity is not a UUID. |
| `401` | Missing, malformed, or wrong bearer token (`WWW-Authenticate: Bearer`). |
| `404` | Reader disabled, or output unknown, evicted, or not yet received. |

```json
{
  "jobId": "1b4e28ba-2fa1-11d2-883f-0016d3cca427",
  "endOfOutput": true,
  "truncated": false,
  "incomplete": false,
  "records": [
    { "sequence": 1, "stream": "STDOUT", "observedAt": "2026-10-31T10:00:00Z", "content": "aGVsbG8K" }
  ]
}
```

`content` is base64 of the raw bytes, so binary output and split UTF-8 survive;
concatenate each stream's decoded bytes before decoding text. `endOfOutput` is
false while the Job runs or when its final marker was lost; `truncated` and
`incomplete` are then `null` because completeness is unknown. A response holds at
most 1 MiB of output. Reads are logged with the Job identity and record count,
never with content. Rejected reads are counted and logged as one warning at
most once per minute, so unauthenticated callers cannot grow the operational
log without bound; the warning carries the number of rejections since the
previous one and never the presented credential.

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

Known gap until dispatch binds the assignment (#30): the assignment that log
attribution checks is set by the plaintext, unauthenticated `ReportJobStatus`
RPC. Any holder of a valid Runner certificate can report `RUNNING` for another
`QUEUED` Job under its own Runner ID, then write that Job's log, which the read
endpoint serves as genuine, or occupy its retention slot so the real Runner's
chunks are refused with `FAILED_PRECONDITION`. A stricter certificate check on
the log listener alone cannot close this. The status RPC must be authenticated,
or the log writer must be checked against the dispatch record.

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
| Control Plane | `zeroyaml.job-logs.read.token` / `ZEROYAML_JOB_LOGS_READ_TOKEN` | Bearer token for the read endpoint, at least 32 characters; unset or empty disables reading, whitespace fails startup. |

The server limits incoming log messages to 8192 bytes (including metadata fields
in the protobuf record) and TLS handshakes to five seconds. Calls run on a fixed
pool of 8 worker threads instead of grpc-java's unbounded cached executor: the
retention lock and the Job lookup serialize most work, so more threads would
only wait. At most 64 calls run at once, twice the 32 retained Jobs, because each
Runner sends one call at a time per Job. Further calls are answered
`RESOURCE_EXHAUSTED` immediately, before the certificate identity check, and the Runner treats
them like any other lost chunk. One connection may open at most 8 concurrent
streams; a Runner's further calls wait on its side under HTTP/2 flow control.
The call limit itself runs on the pool, so a refused call still takes a queue
slot for its start task. grpc-java serializes each call's events into at most
one queued task, plus one cancellation task when the call fails. The work queue
of 512 tasks therefore holds two tasks for each of the 64 admitted calls plus
the start tasks of 48 connections at their stream cap. A task beyond that is
rejected: grpc-java discards it and resets the stream instead of delivering
`UNAVAILABLE`, so the call never sees its close or cancel events. The call
limit therefore also releases each permit when the transport closes the
stream, through a stream tracer that does not depend on the pool, and a
rejected task cannot leak a permit. A caller-runs policy is deliberately not
used, because the caller is the Netty event loop. The Runner-facing
registration/status server still uses the default executor; bounding it is
tracked by #280. Invalid or missing
client credentials fail the handshake. A verified certificate without the Runner
URI SAN returns `UNAUTHENTICATED`; a different authenticated Runner ID returns
`PERMISSION_DENIED`, without retaining content. Payload validation and assignment
checks still apply after authentication.

## Verification

The normal Runner suite covers both streams, raw bytes, deterministic sequence
context, concurrent writes, empty output, large output, interleaved line output
within the chunk budget, the chunk-count limit, failed execution, cancellation,
capture errors, failure-count reset, the delivery cooldown and its recovery
probe, separate chunks on each side of a cooldown gap, locally cancelled sends
that do not start a cooldown or log a delivery warning, deadline stops
without a capture warning, gRPC
deadlines, age-based flushing of slow output, the drain bound
after command exit, a failed `docker wait` that stops the follower without
waiting for that bound, the follower stopping once the capture budget is spent,
no final marker before the Job container starts, an allocation-free write into
a pending chunk, the close bound on completed, timed-out, and cancelled
executions, the final-marker reserve when a pending chunk send stalls, log endpoint validation at configuration load, and payload exclusion
from operational logs. Control Plane transport tests cover byte
fidelity, assignment validation, sequence rules, timestamp and payload bounds,
the 1 MiB limit, completed-output and idle-stream eviction, active-retention
saturation without a Job lookup, shared protocol and timestamp validation, the
concurrent call limit and permit release, including after executor rejection,
the authenticated read endpoint with case-insensitive scheme matching,
whitespace-token refusal, and rate-limited rejection warnings,
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

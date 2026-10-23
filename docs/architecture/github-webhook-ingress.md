# GitHub Webhook Ingress

The Control Plane receives GitHub webhook deliveries on one HTTP endpoint. The
endpoint is an ingress adapter: it checks the delivery envelope, authenticates
the delivery against the configured webhook secret, applies the supported event
allow-list, and hands verified deliveries to a downstream handler. It does not
parse payloads or start pipeline work.

## Route

```text
POST /webhooks/github
```

The route is served on the Control Plane HTTP port (Spring Boot `server.port`,
default `8080`). In GitHub, set the webhook **Payload URL** to
`https://<control-plane-host>/webhooks/github`, the **Content type** to
`application/json`, and the **Secret** to the same value the Control Plane
reads from `ZEROYAML_GITHUB_WEBHOOK_SECRET`. Form-encoded deliveries are
rejected.

## Envelope validation

Checks run in this order, and the first failure decides the answer:

| Check | Failure |
| --- | --- |
| `X-GitHub-Event` is present and a lower-case event name (`[a-z][a-z_]{0,63}`) | `400` |
| `X-GitHub-Delivery` is present and a delivery identifier (`[A-Za-z0-9-]{1,64}`) | `400` |
| `Content-Type` is `application/json`, with or without parameters | `415` |
| Declared and actual body size are within `zeroyaml.github.webhook.max-payload-size` | `413` |
| The first non-whitespace byte of the body opens a JSON object | `400` |

The body is read at most one byte past the limit, so an oversized or chunked
request cannot claim more memory than the limit allows. Full JSON parsing is
left to event normalization.

## Signature verification

Envelope validation only proves the request is shaped like a delivery. The
`X-Hub-Signature-256` check proves it came from GitHub, and it is the trust
boundary of the endpoint: a delivery that fails it creates no internal event
and no job.

| Check | Failure |
| --- | --- |
| `X-Hub-Signature-256` is present | `401` |
| The header reads `sha256=` followed by 64 hexadecimal characters | `401` |
| HMAC-SHA256 of the raw body under the configured secret equals the header digest | `401` |

Details that matter:

- the HMAC is computed over the request body byte for byte, because
  re-serializing a parsed payload would change the bytes that were signed;
- the digest is compared with `MessageDigest.isEqual`, which examines every
  byte of two equal-length digests. A short-circuiting comparison would reveal
  how many leading bytes of a guess were correct, which is enough to forge a
  signature one byte at a time;
- the digest is accepted in upper or lower case, because hexadecimal is
  case-insensitive; the `sha256=` prefix is not;
- the legacy SHA-1 `X-Hub-Signature` header is ignored. Honouring it would let
  a caller downgrade the delivery to the weaker digest;
- verification runs **before** the event allow-list, so the `200 OK` answers
  below cannot be used by an unauthenticated caller to discover which events
  the Control Plane acts on.

## Event allow-list

Reached only by a verified delivery.

| Event | Status | Outcome | Downstream handler called |
| --- | --- | --- | --- |
| `push` | `202 Accepted` | `ACCEPTED` | yes |
| `ping` | `200 OK` | `IGNORED` | no |
| any other event | `200 OK` | `IGNORED` | no |

GitHub sends `ping` once when a webhook is created. Unsupported events are
answered with a `2xx` status because a well-formed delivery the Control Plane
chooses not to act on is not a failure; an error status would mark it as failed
in GitHub's **Recent Deliveries** view.

## Response body

Every answer, including rejections, has the same shape:

```json
{
  "deliveryId": "72d3162e-cc78-11e3-81ab-4c9367dc0958",
  "event": "push",
  "outcome": "ACCEPTED",
  "message": "delivery accepted for processing"
}
```

`deliveryId` and `event` are `null` when their header failed validation.
Messages never echo header values, signature material, or payload content.

## Downstream hand-off

A verified delivery with a supported event is passed to
`GitHubWebhookDeliveryHandler` as a `GitHubWebhookDelivery`, which keeps:

- the raw body byte-for-byte, which is exactly what the signature covered;
- `X-GitHub-Delivery` and `X-GitHub-Event`;
- `X-Hub-Signature-256`, already verified by the time the handler runs;
- `X-GitHub-Hook-ID`, `X-GitHub-Hook-Installation-Target-Type`, and
  `X-GitHub-Hook-Installation-Target-ID` when present. Optional headers longer
  than 256 characters are dropped.

The handler runs on the request thread before GitHub receives its answer, so an
implementation must return quickly. It must not start pipeline work
synchronously.

## Push event normalization

The accepted `push` handler parses the already-verified raw body into a
`RepositoryPushEvent` for branch pushes that point to a commit. The event
contains:

- repository identity from `repository.owner.login` and `repository.name`;
- a validated branch name from `ref`, after removing the `refs/heads/` prefix;
- the commit SHA from `after`;
- the GitHub delivery identifier from `X-GitHub-Delivery`;
- the tip commit timestamp from `head_commit.timestamp`.

The field mapping follows GitHub's [push webhook payload](https://docs.github.com/en/webhooks/webhook-events-and-payloads#push).

The event time is the timestamp supplied for the tip commit, not the Control
Plane's receipt time. This makes the normalized value stable when GitHub
redelivers the same delivery. Required fields are validated at the webhook
boundary; a malformed supported push receives `400 Bad Request` with a
field-specific message that does not echo payload data.

Valid tag pushes and branch deletions are acknowledged but do not produce a
build event because they do not identify a buildable branch commit. Other refs
are rejected with an actionable validation message.

Normalization is a pure mapping. The same push body and delivery ID produce an
equal event, and the delivery ID remains the idempotency key for downstream
consumers. This boundary does not suppress repeated HTTP deliveries or persist
processed IDs; durable duplicate suppression is tracked separately in #203.

## Configuration

| Property | Default | Purpose |
| --- | --- | --- |
| `zeroyaml.github.webhook.max-payload-size` | `5MB` | Largest accepted payload, between 1 byte and GitHub's 25 MB cap |
| `zeroyaml.github.webhook.secret` | none | Shared secret the delivery signature is verified against |

Override them with `ZEROYAML_GITHUB_WEBHOOK_MAX_PAYLOAD_SIZE` and
`ZEROYAML_GITHUB_WEBHOOK_SECRET`. A payload size outside the range fails
startup, and so does a secret that is missing or shorter than 16 characters:
the endpoint must never run without a key to authenticate deliveries with. The
secret has no default and no value in any tracked file; use a high-entropy
random value, for example `openssl rand -hex 32`.

## Logging

Each delivery writes one log record with the delivery identifier, the event,
and the outcome. Accepted deliveries add the payload size, and rejections add
the status and reason. Records never contain the payload, the signature, or the
secret.

## Current limits

- A repeated verified delivery is normalized to the same event when its body
  and delivery ID match. Redeliveries are not suppressed at ingress; recording
  delivery identifiers under a unique constraint is tracked as #203.
- One secret is configured for the whole Control Plane. Per-repository secrets
  and secret rotation are not supported yet.

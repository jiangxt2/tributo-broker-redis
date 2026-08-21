# Operations guide

## Runtime topology

The v0.1 topology has one business Ray Job per Redis task:

```text
Redis task Stream
  -> provider consume loop
  -> deterministic RayJobSubmission
  -> provider execution-driver Ray Job
  -> Tributo in-process training or batch-inference API
  -> Bundle or ResultSink
  -> provider event Stream
```

Operation mappings only validate and prepare driver input. They do not call a
training or inference submission API, so inference cannot create a nested Ray
Job.

## Admission and ACK

The consumer validates protocol, outer identity, supported operation/profile,
and queued cancellation before admission. With durability enabled it records
the operation identity, request digest, deterministic `submission_id`, and
deadline in Redis before acknowledging admission. It then publishes the
profile-appropriate queued/accepted event and calls `XACK`.

`ACK` means Ray accepted the execution, not that execution completed. An
ambiguous Ray response is queried by the same `submission_id`; if admission
cannot be confirmed, the Redis delivery remains pending. An invalid or
unsupported request publishes a sanitized `FAILED` event and is ACKed to avoid
a poison loop.

Nonterminal `PHASE`, `LOG`, `METRICS`, and `PROGRESS` events remain best effort.
Terminal events are staged as Redis candidates before publication and guarded
by a single-key Lua operation: one terminal wins globally and events after a
terminal are rejected. The supervisor replays staged candidates and reconciles
active Ray Jobs after process restart. This is a scoped terminal ledger, not a
general transactional outbox for every event.

New consumer groups start at `0-0`, so tasks already present in a newly
configured Stream are eligible for delivery. `block_ms` must be a positive,
finite timeout; explicit zero-timeout polls are issued without Redis `BLOCK`
and are therefore nonblocking.

## Cancellation

Each operation has an independent cancel key prefix.

- Before admission, an existing cancel key produces `CANCELLED` and ACK; no
  Ray Job is created. A failed cancel-key check is fail-closed and leaves the
  delivery pending.
- After admission, the durable supervisor calls `stop_job(submission_id)` and
  retries when Ray has not accepted the stop. It publishes `CANCELLED` only
  after Ray reports `STOPPED`.
- If Ray already reports `SUCCEEDED` or `FAILED`, a later cancel key has no
  effect.
- Worker execution also checks the same Redis cancel identity at training
  boundaries and XGBoost rounds; wrapped Ray cancellation failures are mapped
  back to `CANCELLED`.

Active operations and terminal candidates are Redis-backed. Restart recovery,
deadline enforcement, terminal redelivery, and cooperative worker cancellation
are part of the durable profile. The terminal guard resolves completion versus
cancellation races without emitting two terminal events.

## Credentials

Tasks may include only a reference such as `env:OBJECT_STORE_PROFILE` or
`mount:/var/run/secrets/object-store`. The execution driver verifies the
reference inside its pre-provisioned Ray runtime. It does not copy the value
into Ray metadata or request-derived runtime env.

Credential values are also rejected when embedded as URI user information or
sensitive query parameters. Arbitrary opaque values cannot be classified as
secrets generically; integrations must use `credential_ref` rather than
placing credential material in operation configuration maps.
`credential_ref` is accepted only as the direct `spec.credential_ref` field;
nested lookalike fields are rejected.

If execution succeeds but the `COMPLETED` event cannot be encoded or
published, the driver classifies the notification failure as
`TERMINAL_EVENT_PUBLICATION_FAILED`, not as an execution failure. Event
delivery remains best effort.

Redis URLs must be credential-free. Standalone, Sentinel, and Cluster
credentials may be referenced only by configured environment-variable names;
Sentinel and Redis master credentials remain separate. TLS certificate and key
settings must reference absolute mounted paths. Credential values are resolved
only in the process constructing the client and are never serialized into the
driver descriptor.

## Observability

Use the channel's configured outer identity field (default `operation_id`) to
route task and event entries, and use `submission_id` to query Ray Jobs. Event
JSON retains canonical `operation_id` and includes public protocol identity, event ID,
timestamp, operation/run/attempt identity, optional `ray_job_id`, phase, and a
bounded redacted payload.

Expected training events include `PHASE`, `LOG`, `METRICS`, and a terminal
event. Expected inference events include `PHASE`, `LOG`, `PROGRESS`, and a
terminal event. `COMPLETED` carries a credential-free Bundle or ResultSink
reference.

## Alpha failure boundary

The durable profile persists active-scan cursors, reclaims pending deliveries,
reconstructs Redis clients after standalone/Sentinel/Cluster failover, and
reconciles terminal state across Provider restarts. It guarantees terminal
uniqueness for one operation identity; it does not promise general exactly-once
task side effects or DLQ processing.

## Validation

Run configuration validation before starting the service:

```bash
tributo-broker-redis validate --config /etc/tributo/redis-provider.json
tributo-broker-redis validate --config /etc/tributo/redis-provider.json --check-connectivity
```

Configuration validation requires a Core source root usable by Ray runtime
packaging and an explicit provider driver distribution before the consumer
group is opened or any task is read. Compatibility with the Core
`submit_ray_job()` runtime-env extension contract is verified by the pinned
Core baseline, static typing, wheel contract, and Redis/Ray integration matrix.

Start the provider-owned loop under a process supervisor:

```bash
tributo-broker-redis consume --config /etc/tributo/redis-provider.json
```

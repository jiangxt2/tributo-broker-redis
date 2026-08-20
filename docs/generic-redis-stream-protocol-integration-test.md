# Generic Redis Streams integration contract

This document defines the public behavioral fixture for v0.1. It contains no
product-specific key names, repository paths, deployment profiles, or private
protocol compatibility claims.

## Task envelope

Training and batch inference use independent configured Streams. Each Redis
entry contains:

```text
operation_id = <non-empty public operation identity, default field name>
payload      = <UTF-8 JSON object>
```

`outer_identity_field` may select another generic field name per channel. The
same configured name is used in task and event entries; the JSON protocol keeps
the canonical `operation_id` property.

The payload is a closed `tributo-generic-v1` request with protocol version
`1.0`. Its `operation_id` must equal the outer field. Its `operation_type`
must match the selected Stream. Unknown fields, plaintext credential fields,
credential-bearing URI user information or sensitive query parameters,
unsupported operations, algorithms, and execution profiles fail closed.

## Event envelope

Events are appended to `{event_stream_prefix}:{operation_id}` with the
configured outer identity field and `payload`. The JSON payload includes:

- protocol profile and version;
- stable operation, run, attempt, and Ray submission identity;
- event ID and timestamp;
- `PHASE`, `LOG`, `METRICS`, `PROGRESS`, `ACCEPTED`, `COMPLETED`, `FAILED`, or
  `CANCELLED` event type;
- bounded, recursively redacted event data.

`COMPLETED` contains a structured Tributo result and a credential-free Bundle
or ResultSink reference. The event Stream is a best-effort notification path,
not a durable result store. A nonterminal publication failure cannot turn a
successful workload into `EXECUTION_FAILED`; the driver makes at most one
immediate best-effort retry.

## Execution fixtures

The release integration matrix uses one already supported XGBoost training
algorithm and the Bundle-backed inference profile. It proves:

| Fixture | Expected path |
| --- | --- |
| single-worker training | task -> one driver Ray Job -> one Ray Train worker -> Bundle -> events |
| distributed training | task -> one driver Ray Job -> multiple Ray Train workers -> Bundle -> events |
| single-worker batch inference | task -> one driver Ray Job -> Ray Data concurrency 1 -> Parquet receipt -> events |
| distributed batch inference | task -> one driver Ray Job -> Ray Data concurrency 2 -> Parquet receipt -> events |
| queued cancel | cancel key exists -> no Ray submission -> `CANCELLED` -> ACK |
| running cancel | cancel key -> `stop_job(submission_id)` -> Ray `STOPPED` -> `CANCELLED` |
| representative failures | admitted training and inference business failures -> sanitized `FAILED` |
| credential boundary | pre-provisioned reference resolves and secret value is absent from task, metadata, logs, events, and result references |

The matrix deliberately does not multiply every failure and cancellation case
across all four profiles.

The cancel watcher checks the latest terminal event before stopping a Ray Job.
Without a durable result ledger, v0.1 cannot atomically arbitrate a stop that
races with artifact persistence and terminal-event publication. A durable
Bundle or ResultSink receipt remains the source of truth in that narrow race.

## Packaging fixtures

The integration entry point builds and installs the current Tributo Core wheel
and provider wheel. It verifies provider discovery through `tributo.brokers`,
Broker API version 1, `RayJobSubmission` identity, the real Core
`extra_py_modules`/`runtime_pip_packages` call contract, and a separate
Core-only environment without redis-py. Docker Ray uses the Core-owned
`tributo-runtime-full` image and rejects missing or inconsistent version, Ray,
or manifest-attestation labels before starting the matrix. Both training
Bundles and both ResultSink outputs are opened and validated.

## Excluded reliability claims

The fixture does not validate Redis reconnect, ACK loss, consumer crash,
pending cursor persistence, DLQ, Sentinel, Cluster, HA, cross-restart
deduplication, or durable terminal events. These are later provider-level
reliability enhancements rather than Core Broker API requirements.

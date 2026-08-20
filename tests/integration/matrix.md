# Redis and Ray integration test matrix

This ledger applies to branch `feat/generic-training-inference` and Tributo
Core `master@8a3bb4d`. The earlier three-test Docker result was stale because
production validation, protocol fields, and the Docker image contract changed
during review fixes. Attempt 1 and its approved rerun showed that the first
post-training `METRICS` publication can fail while the immediately following,
larger PHASE and `COMPLETED` publications succeed. The final fix adds one
immediate best-effort retry; that explicitly approved complete run passed as
recorded below. A later static review changed the provider driver identity
handoff, closed-envelope validation, failure identity, terminal publication
classification, and cancellation arbitration. Those changes passed the short
contract suite and the final explicitly approved Docker/Ray matrix run recorded
below.

| Suite | Coverage | Reason | Result |
| --- | --- | --- | --- |
| Attempt 1: wheel contract | Core 1.0.0 at `8a3bb4d` (including runtime-env extension `31d790d`) + provider wheel discovery, API v1 identity, and runtime-env extension call | cross-project packaging boundary | Passed: 2 tests |
| Attempt 1: Core-only import | Core wheel installs and imports without redis-py | optional dependency boundary | Passed |
| Attempt 1: attested Ray image preflight | Core-owned `tributo-runtime-full` title, Core/Ray versions, manifest label, and exact local image ID | reproducible Docker/Ray test boundary | Passed: image ID `sha256:5a8abda6619ecca1165a5cd36a1aa1d1cf99f8c2fe2c464625d90a3646727aa0`; evidence in `.runtime/ray-image.log` |
| Attempt 1: Standalone Redis and Docker Ray | four training/inference profiles, both Bundles and ResultSinks readable, configured identity alias, queued/running cancel, representative failures, events, credential redaction | v0.1 release gate | Failed: 1 failed, 2 passed in 102.33 seconds; training succeeded and emitted `COMPLETED`, but `METRICS` was absent |
| Attempt 2 after JSON-safe metrics conversion | Same complete wheel, isolation, image, four-path, failure, cancellation, event, and credential matrix | distinguish serialization from the first post-training Redis write | Failed at the same assertion: 1 failed, 2 passed in 91.71 seconds; JSON-safe conversion did not change the outcome, and the larger `COMPLETED` event still succeeded |
| Final rerun after one immediate best-effort retry | Same complete wheel, isolation, image, four-path, failure, cancellation, event, and credential matrix | execution driver changed after attempt 2 to tolerate one transient nonterminal `XADD` failure | Passed: wheel contract 2 tests; Core-only import; image preflight; runtime matrix 3 tests in 167.07 seconds. Logs: `.runtime/wheel-discovery.log`, `.runtime/ray-image.log`, `.runtime/standalone.log`, and `.runtime/docker.log` |
| Post-review final run | Current provider code + Core `8a3bb4d`; same complete wheel, isolation, image, four-path, failure, cancellation, event, and credential matrix | production runtime and driver identity/event/cancel boundaries changed after the prior pass | Passed: wheel contract 2 tests; Core-only import; image preflight; runtime matrix 3 tests in 172.78 seconds. Logs: `.runtime/wheel-discovery.log`, `.runtime/ray-image.log`, `.runtime/standalone.log`, and `.runtime/docker.log` |

Sentinel, Cluster, reconnect, ACK-loss recovery, DLQ, HA, and cross-restart
idempotency are intentionally outside this Alpha matrix.

The v0.1 stop path checks the latest terminal event before publishing
`CANCELLED`, but it has no durable arbitration between artifact persistence
and terminal-event publication. This narrow race remains a documented Alpha
limitation rather than an exactly-once claim.

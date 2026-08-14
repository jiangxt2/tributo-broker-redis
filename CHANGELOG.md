# Changelog

## Unreleased

- Initial standalone Redis Streams Broker API v1 provider.
- Standalone, Sentinel, and Cluster Redis topology support with Consumer Group
  pending recovery and deterministic Ray submission reconciliation.
- Wheel-only Provider discovery, Ray worker wheel injection, and JSON-safe
  cooperative cancellation support.
- Fail-open Redis outage handling, bounded event publication retries, invalid
  message acknowledgement semantics, and lifecycle metrics replay.

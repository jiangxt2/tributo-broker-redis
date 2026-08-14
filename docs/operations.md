# Operations

## Failure boundaries

Redis is an optional Tributo control-plane dependency. A Redis outage must
not change ordinary Tributo training, inference, data access, or CLI behavior.
The provider follows these boundaries:

- discovery does not connect to Redis;
- `validate --check-connectivity` is the only explicit CLI probe;
- consumer startup and polling failures are contained by Core
  `BrokerRunner`, which enters `DEGRADED`/`RECONNECTING` and applies bounded
  backoff;
- event publication is best effort with bounded retries and rate-limited
  warnings;
- a completed Ray Job is not failed because its reporter cannot publish;
- invalid messages are removed from the task stream after best-effort FAILED
  reporting, even when the invalid-event stream is unavailable;
- temporary task submission failures remain pending for later recovery.

## Redis topology

Standalone uses `url` or `host`/`port` and an optional logical `db`.
Sentinel uses `sentinel_hosts`, `sentinel_service`, and `db`. If Sentinel
announces an internal address behind NAT, use `sentinel_address_map` to map
each advertised `host:port` to a client-reachable address. The optional
`sentinel_force_master_ip` setting is supported by the redis-py range declared
by this package.
Cluster uses `cluster_startup_nodes` and requires `db: 0`. If Redis announces
internal node hosts, use `cluster_address_remap_host` to map them to the
client-reachable host.

Use unique stream keys and consumer groups per environment. Do not reuse a
fixed consumer name across independent provider processes unless the shared
consumer identity is intentional. The default name is unique per process.

## Pending recovery

The provider uses `XAUTOCLAIM` after `claim_idle_ms`. The Redis-returned cursor
is retained between recovery rounds; when Redis returns `0-0`, the next round
starts a new scan. Increase `claim_count` for large PELs while balancing
recovery work against normal task polling.

## Secret handling

Only environment variable names are serialized in provider configuration.
Passwords must be injected into the process or Ray runtime environment under
the configured name. Never put a password in a Redis URL, JSON config, task
payload, event, exception, or log message.

## Observability

Alert on repeated `DEGRADED`/`RECONNECTING` transitions, pending-message growth,
and reporter warning logs. Event consumers should deduplicate using the
business job identity and event payload because Redis Streams delivery is
at-least-once.

<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query RabbitMQ Queues, Topology and Streams With SQL in DuckDB

> **vgi-rabbitmq** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-rabbitmq/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-rabbitmq/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

The RabbitMQ management UI shows one broker one page at a time. This worker turns the
**RabbitMQ Management HTTP API** into SQL tables in DuckDB: queue depth and consumers,
the exchange-to-queue routing graph, connections, channels, users and permissions,
policies, shovels and federation upstreams (credentials masked), node alarms and health
checks. Join them with each other and with anything else DuckDB reads, and read the
messages in a **stream without consuming them**.

It is **read-only**. Every management function is a `GET`. The one function that touches
messages in a classic or quorum queue, `peek_messages`, re-queues what it fetches and
refuses to run unless you pass `peek_requeue := true` (see [Reading messages](#reading-messages)).

## Quick start

```sql
INSTALL vgi FROM community;     -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository
LOAD vgi;
ATTACH 'rabbitmq' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-rabbitmq@v0.1.2');

CREATE SECRET (TYPE rabbitmq, MANAGEMENT_URL 'http://localhost:15672',
               USERNAME 'monitor', PASSWORD '...',
               AMQP_URL 'amqp://monitor:...@localhost:5672/');

-- Queues with a backlog and nobody consuming them.
SELECT vhost, name, type, messages_ready, messages_unacknowledged
FROM rabbitmq.main.queues()
WHERE consumers = 0 AND messages_ready > 0
ORDER BY messages_ready DESC;
```

The secret type is registered by the worker, so run `CREATE SECRET` after `ATTACH`.

<details><summary>Other ways to run it</summary>

- **DuckDB 2.0**:
  `SET allow_extension_repositories = 'allowed'; CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'; INSTALL vgi FROM query_farm; LOAD vgi FROM query_farm;`
  then attach as above.
- **Docker** (HTTP): `docker run -p 8000:8000 ghcr.io/query-farm/vgi-rabbitmq:0.1.2`, then
  `ATTACH 'rabbitmq' (TYPE vgi, LOCATION 'http://localhost:8000')`.
- **Go**: `go install github.com/Query-farm/vgi-rabbitmq/cmd/vgi-rabbitmq-worker@v0.1.2`,
  then attach the binary by absolute path.
- **From source**: `make build` (Go 1.26+, pure Go, no cgo), then
  `ATTACH 'rabbitmq' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-rabbitmq-worker')`.

</details>

## Secrets and configuration

Every function that talks to the broker reads a DuckDB secret of type `rabbitmq`:

| Field | Meaning |
|---|---|
| `MANAGEMENT_URL` | Management plugin root: `http://rabbit:15672`, or `https://ops.example.com/rabbitmq` behind a path prefix. `/api/...` is appended. |
| `USERNAME` / `PASSWORD` | Basic auth. A `monitoring` user can read most listings; `users`, `permissions`, `topic_permissions` and `definitions_summary` need `administrator`. Password redacted. |
| `BEARER_TOKEN` | OAuth 2 access token (rabbitmq_auth_backend_oauth2), sent as `Authorization: Bearer …`; for AMQP it is sent as the password. Redacted. |
| `AMQP_URL` | AMQP 0-9-1 URL for `stream_messages`, e.g. `amqp://user:pass@rabbit:5672/` or `amqps://…`. The vhost argument overrides its path; without user info, `USERNAME`/`PASSWORD` (or the token) are used. Redacted. |
| `TLS_CA_CERT` | PEM CA bundle for HTTPS and AMQPS. |
| `TLS_CLIENT_CERT` / `TLS_CLIENT_KEY` | PEM client certificate and key for mutual TLS (key redacted). |
| `TLS_SERVER_NAME` | Name to verify instead of the URL host. |
| `TLS_SKIP_VERIFY` | `'true'` disables certificate verification (testing only). |

```sql
-- Two brokers; pick one per call with secret_name.
CREATE SECRET prod (TYPE rabbitmq, MANAGEMENT_URL 'https://rabbit-prod:15671', BEARER_TOKEN '...',
                    TLS_CA_CERT '-----BEGIN CERTIFICATE-----...');
CREATE SECRET staging (TYPE rabbitmq, MANAGEMENT_URL 'http://rabbit-staging:15672', USERNAME 'monitor', PASSWORD '...');

SELECT * FROM rabbitmq.main.overview(secret_name := 'staging');
```

Credentials never enter table-function state or logs. Requests that get **429, 502, 503 or
504** are retried with exponential backoff (5 attempts), honouring `Retry-After`. A health
check's own `503 {"status":"failed"}` is an answer, not an outage: it becomes a row with
`ok = false`. Other errors carry the HTTP status and the broker's `error`/`reason`.

## What's inside

All objects are in `rabbitmq.main`. Each table function takes `secret_name := '...'`.

| Function | What it returns |
|---|---|
| `overview()` | One row: cluster name, versions, message totals, rates, object counts, listeners. |
| `nodes()` | Per node: memory / disk usage and **alarms**, file descriptors, sockets, processes, **partitions**, maintenance mode, plugins. |
| `vhosts()` | Name, description, tags, default queue type, per-node state, message totals. |
| `queues(vhost, name, use_regex, page_size)` | Type (classic/quorum/stream), state, node/members, policy, operator policy, effective policy, arguments, `messages_ready` / `messages_unacknowledged`, bytes, consumers, consumer capacity, rates, head-message and stream first timestamps, idle_since, delivery_limit. |
| `exchanges(vhost, name, use_regex, page_size)` | Type, flags, arguments, policy, publish in/out counts and rates. |
| `bindings(vhost)` | The routing graph: source exchange → destination queue/exchange, routing key, arguments. |
| `connections(vhost, name, use_regex, page_size)` | User, state (blocked by an alarm?), protocol, TLS, peer, client name/product/version, heartbeat, traffic. |
| `channels(vhost, name, use_regex, page_size)` | Prefetch, consumers, unacknowledged / unconfirmed / uncommitted messages, confirm/tx mode, rates. |
| `consumers(vhost)` | Queue, consumer tag, channel, ack mode, prefetch, exclusive, active (single active consumer), arguments. |
| `users()` | Name, tags, has_password, hashing algorithm, limits. **Password hashes are never returned.** |
| `permissions(vhost)` / `topic_permissions(vhost)` | Configure/write/read regexes; routing-key regexes per topic exchange. |
| `policies(vhost)` / `operator_policies(vhost)` | Pattern, apply-to, priority, definition. |
| `parameters(component, vhost)` | Runtime parameters (shovels, federation upstreams, limits) as JSON, **credentials masked**. |
| `global_parameters()` | Cluster-wide parameters (cluster_name, ...), masked. |
| `shovels(vhost)` | Shovel status: state, reason, node, since, endpoints (masked). Needs `rabbitmq_shovel_management`. |
| `definitions_summary(vhost)` | Object counts per section of the definitions export (only counts leave the worker). |
| `health_checks(cert_within, cert_unit)` | One row per health check: alarms, local alarms, certificate expiry, quorum-critical, virtual hosts, in-service, connection limit, ready-to-serve. |
| `feature_flags()` | State and stability of every feature flag (upgrade readiness). |
| `deprecated_features()` | Deprecation phase and whether the feature is **in use**. |
| `stream_messages(vhost, stream, start_offset, start_timestamp, max_messages, batch_size, idle_timeout_ms)` | Messages of a stream, non-destructive (AMQP). |
| `peek_messages(vhost, queue, count, peek_requeue, truncate)` | Messages at the head of a classic/quorum queue; **re-queues them; opt-in only**. |
| `topic_matches(pattern, routing_key)` | `BOOLEAN`: does a topic binding key (`*`, `#`) match a routing key? Offline. |
| `mask_uri(text)` | Masks passwords in URIs and password-like query parameters. Offline. |

Maps (`arguments`, `effective_policy_definition`, `definition`, `limits`, `headers`,
`cluster_state`) are `MAP(VARCHAR, VARCHAR)` with values as text, so
`arguments['x-queue-type']` and `definition['message-ttl']` just work. Rates are messages
(or bytes) per second from the broker's last sample; they are `NULL` when the broker
collects no stats for the object.

**Credential masking.** `parameters`, `global_parameters` and `shovels` replace the password
in every `scheme://user:password@host` URI with `****`, the value of `password`/`token`/
`secret`-style query parameters, and any JSON key whose name contains `password`,
`secret`, `token`, `credential` or `private_key`. `mask_uri` applies the same rules to any
text.

## What pushes down

The Management API's own filters are exposed as arguments, so they shrink the request,
not just the result:

| Argument | Becomes |
|---|---|
| `vhost := 'shop'` | the vhost path segment: `/api/queues/shop`, `/api/vhosts/shop/connections`, `/api/policies/shop`, `/api/definitions/shop`, ... (`'/'` is sent as `%2F`) |
| `name := 'orders'` | `?name=orders`: a case-insensitive substring match |
| `name := '^orders\.', use_regex := true` | `?name=^orders\.&use_regex=true`: a regular expression |
| `page_size := 100` | `?page=N&page_size=100` on queues, exchanges, connections and channels |
| `component := 'shovel'` | `/api/parameters/shovel` |

Listings are **paginated with an externalized cursor**: the next page number lives in the
table-function state, one page per batch, so over the HTTP transport each page can be
served by a different request or worker process. A `WHERE` clause is evaluated by DuckDB;
it is not translated into API filters, so use the arguments for large brokers.

## Reading messages

### Streams: `stream_messages` (non-destructive)

A stream is an append-only log, so reading it never removes, reorders or redelivers
anything. `stream_messages` subscribes over AMQP 0-9-1 with `x-stream-offset` and closes
the subscription when done:

```sql
SELECT sequence, key, payload_text::JSON->>'order_id' AS order_id, headers['tenant'] AS tenant
FROM rabbitmq.main.stream_messages('shop', 'events', start_offset := 'first')
ORDER BY sequence;
```

- `start_offset`: `'first'` (default, oldest retained), `'last'` (start of the last chunk),
  `'next'` (only messages published from now on) or a numeric offset;
  `start_timestamp := TIMESTAMPTZ '...'` starts at the first chunk at or after that time.
- The scan ends after `max_messages` (default 1000) or when no message arrives for
  `idle_timeout_ms` (default 1000): the tail of the stream.
- The **next offset is the externalized cursor**. Each batch (`batch_size`, default 500)
  opens its own connection at that offset, so a long read resumes across batches and
  transports.
- Subscriptions start at a chunk boundary; messages before the requested offset are
  skipped. Deliveries are acknowledged only to return flow-control credit (streams require
  a prefetch); a stream never removes anything on ack.
- **It refuses anything that is not a stream**, three ways: it asks the Management API for
  the queue type first (when `MANAGEMENT_URL` is set); the broker itself rejects
  `x-stream-offset` on a classic or quorum queue; and a delivery without an
  `x-stream-offset` header is never acknowledged (closing the channel re-queues it).

### Classic and quorum queues: `peek_messages` (re-queues, opt-in)

> **Warning.** RabbitMQ has no read-only peek for classic and quorum queues. The only HTTP
> way is `POST /api/queues/{vhost}/{queue}/get` with `ackmode=reject_requeue_true`, which
> **delivers the messages and puts them back**. Each fetched message is marked
> `redelivered`; a **quorum queue counts a delivery attempt** against its `delivery-limit`
> (a message can then be dead-lettered or dropped); with active consumers the **order can
> change**. `peek_messages` therefore refuses to run unless you pass `peek_requeue := true`.
> Prefer streams for anything you need to read repeatedly.

```sql
SELECT message_id, key, payload_text, headers['x-death'] AS deaths
FROM rabbitmq.main.peek_messages('shop', 'orders.dlq', count := 20, peek_requeue := true);
```

Bodies are truncated to `truncate` bytes (default 50,000; `payload_bytes` is the full size).

### Messaging vocabulary

Shared by `stream_messages` and `peek_messages`, and the column contract for the other
Query.Farm messaging workers (vgi-kafka, vgi-kinesis, vgi-nats, vgi-pubsub) where the
concept exists:

| Column | Type | Meaning |
|---|---|---|
| *source columns* | `VARCHAR` | Where the message came from: here `vhost`, `queue`; Kafka `topic`, `partition`; NATS `stream`, `subject`; Pub/Sub `subscription`. |
| `sequence` | `BIGINT` | The message's position in its source: RabbitMQ stream offset, NATS stream sequence, Kafka offset. `NULL` where the source has no stable position (a classic queue). Named `sequence` because `OFFSET` is a reserved SQL word. |
| `timestamp` | `TIMESTAMPTZ` | Publish time: the AMQP `timestamp` property here (NULL when the publisher did not set it); the broker/log append time elsewhere. |
| `key` | `VARCHAR` | The routing/partition key: AMQP routing key, Kafka key (as text), NATS subject, Pub/Sub ordering key. |
| `payload` | `BLOB` | The raw body bytes. |
| `payload_text` | `VARCHAR` | The body as text when it is valid UTF-8, else `NULL`; cast with `::JSON`. |
| `payload_bytes` | `BIGINT` | Full body size (before any truncation). |
| `headers` | `MAP(VARCHAR, VARCHAR)` | Message headers/attributes, values as text (nested values as JSON). |
| `redelivered` | `BOOLEAN` | Delivered before (where the system tracks it). |

System-specific properties follow these columns (here `exchange`, `content_type`,
`content_encoding`, `message_id`, `correlation_id`, `reply_to`, `message_type`, `app_id`,
`user_id`, `priority`, `delivery_mode`, `expiration`).

**Consumer lag.** RabbitMQ queues have no per-consumer offsets, so a queue's lag is its
backlog: `queues().messages_ready` (not yet delivered) plus `messages_unacknowledged` (in
flight). Kafka-style `lag` columns elsewhere mean "messages the consumer has not processed",
which is `messages_ready` here.

## Examples

```sql
-- Where would a message with routing key orders.created.eu go? (topic semantics, offline)
SELECT destination_type, destination
FROM rabbitmq.main.bindings(vhost := 'shop')
WHERE source = 'orders' AND rabbitmq.main.topic_matches(routing_key, 'orders.created.eu');

-- Queues no exchange routes to (reachable only through the default exchange).
SELECT q.vhost, q.name FROM rabbitmq.main.queues() q
ANTI JOIN (SELECT vhost, destination FROM rabbitmq.main.bindings()
           WHERE source <> '' AND destination_type = 'queue') b
  ON b.vhost = q.vhost AND b.destination = q.name;

-- Is anything blocking publishers?
SELECT name, mem_alarm, disk_free_alarm, partitions FROM rabbitmq.main.nodes()
WHERE mem_alarm OR disk_free_alarm OR len(partitions) > 0;
SELECT check_name, reason FROM rabbitmq.main.health_checks() WHERE NOT ok;

-- Upgrade readiness: flags not enabled and deprecated features in use.
SELECT name FROM rabbitmq.main.feature_flags() WHERE state <> 'enabled' AND stability = 'stable';
SELECT name, deprecation_phase FROM rabbitmq.main.deprecated_features() WHERE in_use;

-- Who can write where?
SELECT u.name, p.vhost, p.write FROM rabbitmq.main.users() u
JOIN rabbitmq.main.permissions() p ON p.user = u.name ORDER BY u.name, p.vhost;

-- Snapshot queue depth to Parquet every few minutes for trend analysis.
COPY (SELECT now() AS at, vhost, name, messages_ready, messages_unacknowledged, consumers
      FROM rabbitmq.main.queues()) TO 'queue_depth.parquet';
```

## How it is tested

`make check` runs gofmt, go vet, golangci-lint, `go test` (masking, topic matching, value
decoding, retries, pagination, health-check 503 handling, message rows), the SQL suite over
the stdio, launch and HTTP transports, and vgi-lint at `fail_on = "info"`.

**CI needs no Docker and no credentials.** `test/sql/rabbitmq.test` talks to
`test/fixture_server/server.py`, which replays **recorded Management API responses** from
`test/fixture_server/recorded.json` (keyed by method, raw path, sorted query and, for POST,
the JSON body) plus `handwritten.json`, served under a `/broken` path prefix, for shapes a
healthy broker will not produce on demand (failing health checks, memory/disk alarms,
partitions, a 500, 3.x-style users with password hashes, nested credentials). The runner
starts two copies: one answers every request with **429 then 503** (both `Retry-After: 0`)
before the real answer, so retries are always exercised; the other demands basic auth or a
bearer token.

**The recordings come from a real broker.** `test/fixture/run-rabbitmq.sh start` runs
`rabbitmq:4-management` (RabbitMQ 4.4, about 160 MB RAM, capped at 512 MB) with
`definitions.json` imported at boot: two vhosts, three users, permissions and topic
permissions, classic queues, a **quorum** queue with a delivery limit, a **stream**,
topic/fanout/headers exchanges, an exchange-to-exchange binding, a policy, a **shovel and a
federation upstream with fake credentials in their URIs**. `seed.py` publishes six
deterministic messages (fixed ids, headers and AMQP timestamps, one binary body).
`hold_consumer.py` keeps a consumer with prefetch 2 open so connections, channels,
consumers and unacknowledged counts are non-empty.

`test/sql/stream.test` exercises `stream_messages` over AMQP and needs the live broker. It
is skipped without it, and that skip (`require-env RABBITMQ_LIVE_URL`) is the only one the
runner allows. To run it and to re-record:

```bash
test/fixture/run-rabbitmq.sh start      # prints the management URL and AMQP URL
uv run --no-project --with pika==1.3.2 python test/fixture/hold_consumer.py \
    amqp://admin:admin-pass@127.0.0.1:35673/shop &
rm test/fixture_server/recorded.json    # only to re-record
RABBITMQ_RECORD_UPSTREAM=http://127.0.0.1:35672 \
RABBITMQ_LIVE_URL=http://127.0.0.1:35672 \
RABBITMQ_AMQP_URL=amqp://admin:admin-pass@127.0.0.1:35673/ \
RABBITMQ_AMQP_NOAUTH_URL=amqp://127.0.0.1:35673/ make test-sql
kill %1; test/fixture/run-rabbitmq.sh stop
```

Point the worker at either fixture through the secret's `MANAGEMENT_URL` / `AMQP_URL`;
there are no test-only code paths.

## Known limitations

- `WHERE` clauses are not translated into API filters; use `vhost :=` and `name :=`.
- `stream_messages` uses AMQP 0-9-1, not the binary stream protocol (port 5552): no
  super streams, no server-side offset tracking, no filter values. Each batch opens a new
  connection.
- There is no non-destructive peek for classic and quorum queues in RabbitMQ;
  `peek_messages` is the documented opt-in compromise.
- Federation link status (`/api/federation-links`), stream protocol consumers and their
  offset lag (`/api/stream/consumers`, rabbitmq_stream_management) and per-object
  `columns=` projection are not exposed yet.
- `idle_since` and other times come from the broker's clock; rates are `NULL` when the
  broker runs with `management.rates_mode = none` or `disable_stats`.

## Development

See [AGENTS.md](AGENTS.md) for the layout, the recipe for adding a function and the sharp
edges. `make help` lists the targets; `make template-diff` shows upstream starter changes
(this worker started from vgi-starter-go v1.3.0).

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). RabbitMQ is a trademark of
Broadcom Inc.; this worker is not affiliated with or endorsed by Broadcom or the RabbitMQ
project. It uses [amqp091-go](https://github.com/rabbitmq/amqp091-go) (BSD-2-Clause).

# PGSync: Flows and Traces (Datadog APM)

This document describes the main execution flows in PGSync and how they appear as **traces** and **spans** in Datadog. Use it to interpret what you see in the APM UI and to debug performance.

Tracing is implemented with [Datadog ddtrace](https://docs.datadoghq.com/tracing/). The tracer is initialized in `bin/pgsync` (not in `bin/bootstrap` or `bin/parallel_sync`). Enable with `DD_TRACE_ENABLED=true` and a running Datadog agent.

---

## High-level architecture

- **PostgreSQL** is the source of truth. Changes are captured via:
  - **Logical replication** (WAL decoding) for initial/catch-up sync and replay.
  - **LISTEN/NOTIFY** for real-time events when running as a daemon.
- **Redis/Valkey** is used as a queue: the **producer** pushes change notifications; the **consumer** pops them and syncs to the search engine.
- **Elasticsearch/OpenSearch** receives the denormalized documents.

So in daemon mode you have two main "input" types: **producer** (PG → Redis) and **consumer** (Redis → OpenSearch). One-off runs use a **pull** flow (PG → OpenSearch, with optional WAL replay).

---

## Main flows and their traces

Each **trace** corresponds to one "unit of work" (one iteration / one batch). The **root span** is the first span created for that unit of work.

### 1. One-off pull (default run without `-d`)

**When:** You run `pgsync -c schema.json` (no `--daemon`). Used for initial sync or ad-hoc sync.

**What happens:** For each schema doc, PGSync syncs from PostgreSQL up to the current transaction ID: it runs a forward-pass query to build documents, bulk-indexes them to OpenSearch, then replays logical replication (WAL) up to the current LSN to catch any changes that happened during the sync.

**Root span:** `pgsync.pull`

**Typical span tree:**

```
pgsync.pull                         ← root (one trace per schema doc)
├── pgsync.sync                      ← forward-pass: build queries, fetch rows, yield docs
│   ├── pgsync.query_builder.build
│   ├── pgsync.fetchmany             ← per-node: full stream from PG, transform, yield
│   │   ├── pgsync.db.connect        ← connection pool checkout
│   │   ├── pgsync.db.execute        ← declare server-side cursor
│   │   ├── pgsync.db.cursor_fetch   ← FETCH rows from cursor (PG round-trip)
│   │   ├── pgsync.db.partition_iterate  ← iterate rows + yield
│   │   ├── pgsync.row_transform     ← Transform.transform + doc building per row
│   │   ├── pgsync.plugin_transform  ← per-doc plugin (if plugins configured)
│   │   ├── pgsync.yield_wait        ← generator suspension while bulk processes doc
│   │   ├── pgsync.db.cursor_close   ← close server-side cursor
│   │   └── pgsync.db.disconnect     ← return connection to pool
│   └── (auto-instrumented postgres.query spans)
├── pgsync.search.bulk               ← index the forward-pass docs
│   └── pgsync.search.streaming_bulk ← streaming_bulk: serialize + chunk + HTTP
│       └── (auto-instrumented elasticsearch.query / POST _bulk spans)
├── pgsync.logical_slot_changes      ← replay WAL and index change events
│   ├── pgsync.logical_slot (count_changes)
│   ├── pgsync.logical_slot (peek_changes)
│   ├── pgsync.search.bulk           ← per (tg_op, table) batch
│   └── pgsync.logical_slot (get_changes)
```

**Useful tags:** `index`, `database`, `txmin`, `txmax` on `pgsync.pull`; `row_count` on `pgsync.fetchmany`.

---

### 2. Daemon – producer (PG → Redis)

**When:** Daemon mode (`-d`). The producer listens on PostgreSQL (`LISTEN channel`) and, when it receives NOTIFY messages, pushes payloads to Redis.

**What happens:** Each time there are notifications to send (or a batch reaches `REDIS_WRITE_CHUNK_SIZE`), the app pushes that batch to Redis. One trace = one such push.

**Root span:** `pgsync.poll_db` (tag: `iteration_type=producer`)

**Typical span tree:**

```
pgsync.poll_db                  ← root (iteration_type=producer)
└── pgsync.redis.push           ← push batch to Redis
```

**Useful tags:** `index`, `payload_count`, `iteration_type=producer`.

---

### 3. Daemon – consumer (Redis → OpenSearch)

**When:** Daemon mode. The consumer polls Redis; when it gets payloads, it resolves them (filters, views) and syncs to OpenSearch/Elasticsearch via `on_publish`.

**What happens:** One trace = one "batch of payloads popped from Redis and processed". The batch is translated into sync operations (often involving `pgsync.sync` and `pgsync.search.bulk`).

**Root span:** `pgsync.poll_redis` (tag: `iteration_type=consumer`)

**Typical span tree:**

```
pgsync.poll_redis                      ← root (iteration_type=consumer)
├── pgsync.redis.pop                    ← simple pop (LRANGE+LTRIM)
│   OR pgsync.redis.pop_visible         ← read-only consumer pop (when PG_HOST_RO set)
│       ├── pgsync.redis.pg_visible_check  ← PG snapshot visibility query
│       └── pgsync.redis.lrem_loop         ← O(N) lrem per visible item
├── pgsync.payload_deserialize          ← Payload(**dict) construction for all payloads
├── pgsync.refresh_views                ← check + refresh materialized views
│   └── pgsync.refresh_view             ← per-view refresh (if mat views exist)
├── pgsync.on_publish                   ← handle batch: group payloads, resolve, sync, bulk
│   ├── pgsync.view_substitution        ← substitute view tables for base tables
│   │
│   ├── [per (tg_op, table) group]
│   │   ├── pgsync.search.bulk          ← wraps bulk() call
│   │   │   └── pgsync.search.streaming_bulk ← streaming_bulk: serialize + chunk + HTTP
│   │   │       └── (auto-instrumented elasticsearch.query / POST _bulk spans)
│   │   │
│   │   │   [generator _payloads() consumed by streaming_bulk]
│   │   │   ├── pgsync.payloads_validate    ← validation + get_node + PK check
│   │   │   ├── pgsync.resolve_filters      ← build filter dict
│   │   │   │   ├── pgsync.insert_op        ← INSERT: resolve through-table + FK filters
│   │   │   │   │   ├── pgsync.root_pk_resolver     ← batched search for root doc IDs by PK
│   │   │   │   │   │   └── pgsync.search.scan
│   │   │   │   │   ├── pgsync.root_fk_resolver     ← batched search by foreign keys
│   │   │   │   │   │   └── pgsync.search.scan
│   │   │   │   │   └── pgsync.through_node_resolver ← through-table direct FK resolution
│   │   │   │   ├── pgsync.update_op        ← UPDATE: resolve PK + FK filters
│   │   │   │   │   ├── pgsync.root_pk_resolver
│   │   │   │   │   └── pgsync.root_fk_resolver
│   │   │   │   ├── pgsync.delete_op        ← DELETE: resolve PK filters or delete root docs
│   │   │   │   │   └── pgsync.root_pk_resolver
│   │   │   │   └── pgsync.truncate_op      ← TRUNCATE: search and delete all matching docs
│   │   │   │
│   │   │   └── pgsync.sync                ← per filter chunk: build query + fetch + yield
│   │   │       ├── pgsync.query_builder.build
│   │   │       └── pgsync.fetchmany       ← full row stream lifecycle. Tag: row_count
│   │   │           ├── pgsync.db.connect          ← PG connection pool checkout
│   │   │           ├── pgsync.db.execute          ← declare server-side cursor
│   │   │           ├── pgsync.db.cursor_fetch     ← FETCH partition from cursor
│   │   │           ├── pgsync.db.partition_iterate ← iterate rows in partition + yield
│   │   │           ├── pgsync.row_transform       ← Transform.transform + doc building
│   │   │           ├── pgsync.plugin_transform    ← per-doc plugin execution
│   │   │           ├── pgsync.yield_wait          ← generator suspended for bulk consumer
│   │   │           ├── pgsync.db.cursor_close     ← close server-side cursor
│   │   │           └── pgsync.db.disconnect       ← return connection to pool
│   │
│   ├── pgsync.txid_current             ← SELECT TXID_CURRENT() or Redis get
│   └── pgsync.checkpoint.write
```

**Useful tags:** On `pgsync.poll_redis`: `index`, `payload_count`, `iteration_type=consumer`. On `pgsync.on_publish`: `tg_ops`, `tables`, `payload_count`. On `pgsync.fetchmany`: `table`, `row_count`. On `pgsync.redis.pop_visible`: `peeked_count`, `visible_count`, `lrem_count`.

---

### 4. Polling mode (`--polling`)

**When:** You run with `--polling` (e.g. read-only PG where replication slots are not available). The process wakes periodically and runs a full pull for each schema doc, then sleeps.

**What happens:** One trace = one "wake-up and pull all docs". The root span wraps the whole iteration (all docs + sleep is outside).

**Root span:** `pgsync.polling.iteration` (tag: `iteration_type=polling`)

**Typical span tree:**

```
pgsync.polling.iteration        ← root (iteration_type=polling)
├── pgsync.pull                 ← per schema doc (same subtree as in §1)
│   ├── pgsync.sync
│   ├── pgsync.search.bulk
│   └── pgsync.logical_slot_changes
│       └── ...
└── (next doc's pgsync.pull, etc.)
```

**Useful tags:** `iteration_type=polling`.

---

### 5. Analyze (`--analyze`)

**When:** You run `pgsync -c schema.json --analyze`. Checks that recommended indexes exist for the schema.

**What happens:** For each schema doc, the app walks the tree and checks indexes; no sync to OpenSearch. One trace per schema doc.

**Root span:** `pgsync.analyze` (tag: `iteration_type=analyze`)

**Typical span tree:**

```
pgsync.analyze                  ← root (iteration_type=analyze)
└── (postgres spans for index checks)
```

**Useful tags:** `index`, `iteration_type=analyze`.

---

## Span reference (quick lookup)

### Flow control spans

| Span | File | Meaning |
|------|------|---------|
| `pgsync.pull` | sync.py | Full sync: forward pass + WAL replay for one schema. |
| `pgsync.poll_db` | sync.py | Producer: one batch of PG notifications pushed to Redis. |
| `pgsync.poll_redis` | sync.py | Consumer: one batch of payloads popped and processed. |
| `pgsync.polling.iteration` | sync.py | Polling mode: one wake-up cycle. |
| `pgsync.analyze` | sync.py | Index analysis for one schema. |
| `pgsync.on_publish` | sync.py | Handle one batch: group, resolve filters, sync, bulk. |
| `pgsync.payload_deserialize` | sync.py | Construct Payload objects from Redis dicts. |
| `pgsync.refresh_views` | sync.py | Check and refresh materialized views. |
| `pgsync.view_substitution` | sync.py | Substitute view tables for base tables in payloads. |
| `pgsync.payloads_validate` | sync.py | Validation + get_node + PK check per payload group. |
| `pgsync.txid_current` | sync.py | SELECT TXID_CURRENT() or Redis get for checkpoint. |

### Filter resolution spans

| Span | File | Meaning |
|------|------|---------|
| `pgsync.resolve_filters` | sync.py | Wraps all filter resolution for one (tg_op, table) group. |
| `pgsync.insert_op` | sync.py | INSERT filter resolution. |
| `pgsync.update_op` | sync.py | UPDATE filter resolution. |
| `pgsync.delete_op` | sync.py | DELETE filter resolution. |
| `pgsync.truncate_op` | sync.py | TRUNCATE: search and delete all matching docs. |
| `pgsync.root_pk_resolver` | sync.py | Batched search to find root doc IDs by child primary keys. |
| `pgsync.root_fk_resolver` | sync.py | Batched search to find root doc IDs by child foreign keys. |
| `pgsync.through_node_resolver` | sync.py | Through-table direct FK resolution. |

### Sync + fetch spans

| Span | File | Meaning |
|------|------|---------|
| `pgsync.sync` | sync.py | Per filter chunk: build query, fetch rows, yield docs. |
| `pgsync.query_builder.build` | sync.py | Build SQL for one node in the tree. |
| `pgsync.fetchmany` | sync.py | Full row stream lifecycle: connect → fetch → iterate → transform → yield → close. Tags: `table`, `row_count`. |
| `pgsync.row_transform` | sync.py | Transform.transform + doc dict building per row. |
| `pgsync.plugin_transform` | sync.py | Per-doc plugin execution (e.g. JobCustomFields, Clients). |
| `pgsync.yield_wait` | sync.py | Time generator is suspended while bulk consumer processes doc. |

### Database spans (base.py)

| Span | File | Meaning |
|------|------|---------|
| `pgsync.db.connect` | base.py | Connection pool checkout. |
| `pgsync.db.execute` | base.py | Declare server-side cursor (`stream_results`). |
| `pgsync.db.cursor_fetch` | base.py | FETCH partition from server-side cursor (PG round-trip). |
| `pgsync.db.partition_iterate` | base.py | Iterate rows in partition + yield to consumer. |
| `pgsync.db.cursor_close` | base.py | Close server-side cursor. |
| `pgsync.db.disconnect` | base.py | Return connection to pool + clear compiled cache. |
| `pgsync.logical_slot_changes` | sync.py | Replay WAL: get changes from logical slot, group, bulk index. |
| `pgsync.logical_slot` | base.py | Resource: `get_changes` \| `peek_changes` \| `count_changes`. |

### Search engine spans (search_client.py)

| Span | File | Meaning |
|------|------|---------|
| `pgsync.search.bulk` | search_client.py | Bulk-index docs (wraps streaming_bulk or parallel_bulk). |
| `pgsync.search.streaming_bulk` | search_client.py | streaming_bulk consumption: serialization + chunking + HTTP. Tags: `chunk_size`, `max_retries`, `doc_count`, `error_count`. |
| `pgsync.search.parallel_bulk` | search_client.py | parallel_bulk consumption. Tags: `chunk_size`, `thread_count`, `queue_size`, `doc_count`, `error_count`. |

| `pgsync.search.scan` | search_client.py | Scroll search for primary-key resolution. |


### Redis spans (redisqueue.py)

| Span | File | Meaning |
|------|------|---------|
| `pgsync.redis.pop` | redisqueue.py | Simple pop (LRANGE+LTRIM). |
| `pgsync.redis.pop_visible` | redisqueue.py | Read-only consumer pop (when PG_HOST_RO set). Tags: `peeked_count`, `visible_count`, `lrem_count`. |
| `pgsync.redis.pg_visible_check` | redisqueue.py | PG snapshot visibility query for xmins. |
| `pgsync.redis.lrem_loop` | redisqueue.py | O(N) lrem loop — known bottleneck when queue is large. |
| `pgsync.redis.push` | redisqueue.py | Push items to Redis queue. |

### Other spans

| Span | File | Meaning |
|------|------|---------|
| `pgsync.refresh_view` | sync.py | REFRESH MATERIALIZED VIEW. |
| `pgsync.checkpoint.read` | sync.py | Read checkpoint from file or Redis. |
| `pgsync.checkpoint.write` | sync.py | Write checkpoint to file or Redis. |
| `pgsync.truncate_slots` | sync.py | Consume replication slot to advance. |

---

## Tags useful in Datadog

- **`iteration_type`** – `producer` \| `consumer` \| `polling` \| `analyze`. Use to filter by "kind" of work (e.g. only consumer traces).
- **`index`** – Search index / schema name.
- **`payload_count`** – Number of change events in the batch (producer/consumer).
- **`row_count`** – Number of rows processed in a fetchmany call.
- **`tg_ops`** – Trigger operations in the batch (e.g. `INSERT,UPDATE`).
- **`tables`** – Tables touched in the batch.
- **`table`** – Single table name on per-node spans.
- **`tg_op`** – Single trigger operation on resolve_filters.
- **`is_root`** / **`is_through`** – Node type flags.
- **`database`** – PostgreSQL database name.
- **`txmin`** / **`txmax`** – Transaction ID range for a pull.
- **`slot_name`** – Replication slot name.
- **`queue_key`** – Redis queue key.
- **`filter_size`** – Number of root table filters in a sync call.
- **`chunk_size`** / **`partition_index`** – DB fetch pagination.
- **`peeked_count`** / **`visible_count`** / **`lrem_count`** – Read-only consumer stats.

- **`exhausted`** – True when cursor_fetch finds no more partitions.
- **`stream_results`** – Whether server-side cursor is used.

---

## Tips for performance debugging in Datadog

1. **Filter by operation/resource**
   Use `resource_name:pgsync.poll_redis` to see consumer iterations; `resource_name:pgsync.poll_db` for producer.

2. **Filter by iteration type**
   Use `iteration_type:consumer` or `iteration_type:producer` to separate daemon producer vs consumer traffic.

3. **Where time goes in a consumer trace**
   Open a `pgsync.poll_redis` trace and compare:
   - Time in `pgsync.on_publish` vs children (`pgsync.sync`, `pgsync.search.bulk`).
   - Many or slow `pgsync.search.bulk` → index or network bottleneck.
   - Long `pgsync.sync` or `pgsync.query_builder.build` → query building or DB fetch cost.
   - Long `pgsync.fetchmany` → drill into `pgsync.db.*` spans to see if time is in `db.cursor_fetch` (PG round-trip), `db.connect` (pool exhaustion), or `pgsync.yield_wait` (bulk consumer backpressure).
   - Long `pgsync.resolve_filters` → check which `*_op` and `*_resolver` spans dominate. Large `pgsync.search.scan` times indicate slow scroll searches.
   - Slow `pgsync.redis.pop_visible` → check `pgsync.redis.lrem_loop` — the O(N) `lrem` calls scale linearly with queue size.

4. **Where time goes in a pull trace**
   In a `pgsync.pull` trace:
   - Long `pgsync.sync` → forward-pass query or fetch (check `pgsync.db.*` and postgres spans).
   - Long `pgsync.logical_slot_changes` or `pgsync.logical_slot` → WAL read or slot I/O.
   - Long `pgsync.search.bulk` → index write cost.

5. **Auto-instrumentation**
   `bin/pgsync` calls `patch_all()`, so you also get Datadog spans for **postgres**, **redis**, and **elasticsearch**/HTTP where applicable. Use them to see actual DB/Redis/OpenSearch time inside the PGSync spans above.

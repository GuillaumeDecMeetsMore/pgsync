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

So in daemon mode you have two main “input” types: **producer** (PG → Redis) and **consumer** (Redis → OpenSearch). One-off runs use a **pull** flow (PG → OpenSearch, with optional WAL replay).

---

## Main flows and their traces

Each **trace** corresponds to one “unit of work” (one iteration / one batch). The **root span** is the first span created for that unit of work.

### 1. One-off pull (default run without `-d`)

**When:** You run `pgsync -c schema.json` (no `--daemon`). Used for initial sync or ad-hoc sync.

**What happens:** For each schema doc, PGSync syncs from PostgreSQL up to the current transaction ID: it runs a forward-pass query to build documents, bulk-indexes them to OpenSearch, then replays logical replication (WAL) up to the current LSN to catch any changes that happened during the sync.

**Root span:** `pgsync.pull`

**Typical span tree:**

```
pgsync.pull                    ← root (one trace per schema doc)
├── pgsync.sync                 ← forward-pass: build queries, fetch rows, yield docs
│   ├── pgsync.query_builder.build
│   ├── pgsync.fetchmany        ← per-node: full stream from PG, transform, yield
│   │   └── pgsync.fetchmany.partition  ← one per DB chunk (QUERY_CHUNK_SIZE rows)
│   └── (postgres spans from fetchmany)
├── opensearch.bulk             ← index the forward-pass docs
├── pgsync.logical_slot_changes ← replay WAL and index change events
│   ├── pgsync.logical_slot (count_changes)
│   ├── pgsync.logical_slot (peek_changes)
│   ├── opensearch.bulk         ← per (tg_op, table) batch
│   └── pgsync.logical_slot (get_changes)
```

**Useful tags:** `index`, `database`, `txmin`, `txmax` on `pgsync.pull`; `batch_size` on `pgsync.logical_slot_changes`.

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

**When:** Daemon mode. The consumer polls Redis; when it gets payloads, it resolves them (filters, views) and syncs to OpenSearch via `on_publish`.

**What happens:** One trace = one “batch of payloads popped from Redis and processed”. The batch is translated into sync operations (often involving `pgsync.sync` and `opensearch.bulk`).

**Root span:** `pgsync.poll_redis` (tag: `iteration_type=consumer`)

**Typical span tree:**

```
pgsync.poll_redis               ← root (iteration_type=consumer)
├── (pgsync.redis.pop may appear as child if instrumentation order allows)
├── pgsync.on_publish            ← handle batch: build filters, call sync, bulk
│   ├── pgsync.sync              ← per filter chunk (build query + fetch + yield)
│   │   ├── pgsync.query_builder.build
│   │   ├── pgsync.fetchmany
│   │   └── (postgres spans)
│   ├── opensearch.bulk         ← one or more per batch
│   └── (opensearch.search if primary-key lookups are needed)
```

**Useful tags:** On `pgsync.poll_redis`: `index`, `payload_count`, `iteration_type=consumer`. On `pgsync.on_publish`: `tg_ops`, `tables`, `payload_count`.

---

### 4. Polling mode (`--polling`)

**When:** You run with `--polling` (e.g. read-only PG where replication slots are not available). The process wakes periodically and runs a full pull for each schema doc, then sleeps.

**What happens:** One trace = one “wake-up and pull all docs”. The root span wraps the whole iteration (all docs + sleep is outside).

**Root span:** `pgsync.polling.iteration` (tag: `iteration_type=polling`)

**Typical span tree:**

```
pgsync.polling.iteration        ← root (iteration_type=polling)
├── pgsync.pull                 ← per schema doc (same subtree as in §1)
│   ├── pgsync.sync
│   ├── opensearch.bulk
│   └── pgsync.logical_slot_changes
│       └── ...
└── (next doc’s pgsync.pull, etc.)
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

| Span name (resource)              | Where it runs              | Meaning |
|----------------------------------|----------------------------|--------|
| `pgsync.pull`                    | One-off sync, polling loop | Full sync from DB to OpenSearch for one schema (forward pass + WAL replay). |
| `pgsync.poll_db`                 | Daemon producer            | One batch of PG notifications pushed to Redis. Tag: `iteration_type=producer`. |
| `pgsync.poll_redis`              | Daemon consumer            | One batch of payloads popped from Redis and processed. Tag: `iteration_type=consumer`. |
| `pgsync.polling.iteration`       | Polling mode               | One wake-up: pull all schema docs. Tag: `iteration_type=polling`. |
| `pgsync.analyze`                 | Analyze mode               | Index analysis for one schema. Tag: `iteration_type=analyze`. |
| `pgsync.sync`                    | Inside pull or on_publish  | Build queries, fetch rows from PG, transform to docs (generator). Tags: `table`, `filter_size`, `root_filter_sample` (first 5 root IDs), `txmin`, `txmax`, `ctid` when set. |
| `pgsync.query_builder.build`     | Inside pgsync.sync          | Build SQL for one node (table) in the tree. |
| `pgsync.fetchmany`               | Inside pgsync.sync          | Wraps the full consumption of the fetchmany generator (one per node; streaming fetch + transform). Tag: `table`. |
| `pgsync.fetchmany.partition`     | base.py, inside fetchmany   | One per DB chunk: time to fetch one partition (up to `chunk_size` rows) from Postgres. Tags: `chunk_size`, `partition_index`. |
| `pgsync.on_publish`              | Daemon consumer             | Handle one batch of Redis payloads: apply filters, call sync, bulk to OpenSearch. |
| `pgsync.logical_slot_changes`    | Inside pull                 | Replay WAL: get changes from logical slot, group by (tg_op, table), bulk index. |
| `pgsync.logical_slot`            | base.py                    | Resource: `get_changes` \| `peek_changes` \| `count_changes` – low-level WAL slot I/O. |
| `pgsync.redis.pop`               | redisqueue.py              | Pop items from Redis queue. |
| `pgsync.redis.push`             | redisqueue.py              | Push items to Redis queue. |
| `opensearch.bulk`               | search_client.py           | Bulk-index a chunk of documents. |
| `opensearch.search`             | search_client.py           | Search (e.g. for primary-key resolution). |
| `pgsync.refresh_view`           | sync.py                    | Postgres REFRESH MATERIALIZED VIEW (I/O). Tags: `table`, `schema`. |
| `pgsync.checkpoint.read`        | sync.py                    | Read checkpoint from file or Redis (I/O). |
| `pgsync.checkpoint.write`       | sync.py                    | Write checkpoint to file or Redis (I/O). |
| `pgsync.truncate_slots`         | sync.py                    | Consume replication slot to advance (I/O; wraps logical_slot.get_changes). Tag: `slot_name`. |

---

## Tags useful in Datadog

- **`iteration_type`** – `producer` \| `consumer` \| `polling` \| `analyze`. Use to filter by “kind” of work (e.g. only consumer traces).
- **`index`** – Search index / schema name.
- **`payload_count`** – Number of change events in the batch (producer/consumer).
- **`tg_ops`** – Trigger operations in the batch (e.g. `INSERT,UPDATE`).
- **`tables`** – Tables touched in the batch.
- **`database`** – PostgreSQL database name (e.g. on `pgsync.pull`).
- **`txmin` / `txmax`** – Transaction ID range for a pull.
- **`slot_name`** – Replication slot (logical_slot spans).
- **`queue_key`** – Redis queue key (redis pop/push).

---

## Tips for performance debugging in Datadog

1. **Filter by operation/resource**  
   Use `resource_name:pgsync.poll_redis` to see consumer iterations; `resource_name:pgsync.poll_db` for producer.

2. **Filter by iteration type**  
   Use `iteration_type:consumer` or `iteration_type:producer` to separate daemon producer vs consumer traffic.

3. **Where time goes in a consumer trace**  
   Open a `pgsync.poll_redis` trace and compare:
   - Time in `pgsync.on_publish` vs children (`pgsync.sync`, `opensearch.bulk`).
   - Many or slow `opensearch.bulk` → index or network bottleneck.
   - Long `pgsync.sync` or `pgsync.query_builder.build` → query building or DB fetch cost.

4. **Where time goes in a pull trace**  
   In a `pgsync.pull` trace:
   - Long `pgsync.sync` → forward-pass query or fetch (check postgres spans).
   - Long `pgsync.logical_slot_changes` or `pgsync.logical_slot` → WAL read or slot I/O.
   - Long `opensearch.bulk` → index write cost.

5. **Auto-instrumentation**  
   `bin/pgsync` calls `patch_all()`, so you also get Datadog spans for **postgres**, **redis**, and **elasticsearch**/HTTP where applicable. Use them to see actual DB/Redis/OpenSearch time inside the PGSync spans above.

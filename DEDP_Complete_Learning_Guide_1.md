# Data Engineering Design Patterns — Complete Learning Guide
### Based on *Data Engineering Design Patterns* by Bartosz Konieczny

> **How to use this guide:** Every chapter follows the same layout — Chapter Context → Pattern Breakdowns → Real-World Analogies. All 40+ patterns are covered with their problems, solutions, trade-offs, and code blueprints.

---

## Foundational Context (Chapter 1)

**The Book's Case Study:** A blog analytics platform with three architectural layers:
- **Bronze Layer** — Raw, unfiltered data as ingested (quality issues present)
- **Silver Layer** — Cleansed, enriched data ready for downstream use
- **Gold Layer** — Aggregated, business-facing datasets (dashboards, ML inputs)

**What is a Data Engineering Design Pattern?**
- A *reusable, named template* for solving a recurring technical problem in data systems
- Like a cooking recipe: predefined ingredients (solution), customizable to context, with consequences (e.g., eating flan daily has health effects → using a pattern has trade-offs)
- Not the same as Gang-of-Four software patterns — those keep code clean; DE patterns keep *data* reliable, consistent, and valuable

---

# CHAPTER 2 — Data Ingestion Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** Every data system starts here. Before you can analyze, ML, or report — you need data. But data providers are unreliable: they use different schemas, have downtime, produce files at unpredictable times, or generate millions of small files that silently destroy performance.

**Medallion Impact:** Ingestion patterns feed the **Bronze layer** (raw data as-is). Some patterns (like Compactor) are also critical for Silver/Gold maintenance.

---

## 2. Master Pattern Breakdown

### Pattern: Full Loader

**The Concrete Problem:**
You have a reference dataset (e.g., device parameters from an external API). It changes a few times per week, has no `last_updated` column, and contains ~1 million rows. You can't detect what changed — so you can't do incremental loading.

**The Core Solution:**
Replace the entire dataset on every run. Two implementation modes:
- **EL (Extract-Load):** Copy files/tables as-is. No transformation. Best for same-type storage.
- **ETL (Extract-Transform-Load):** Add a thin transformation layer when source/destination formats differ.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Data Volume Variability** | Static compute fails if dataset doubles in size. Mitigation: auto-scaling. |
| **Concurrency / Consistency** | Drop-and-insert leaves a visibility gap. Consumers may read partial or empty tables. Use **transactions** or a **single-exposition abstraction** (view switching). |
| **No Rollback** | If overwrite fails mid-run, prior data is gone. Mitigation: Delta Lake/Iceberg time travel, or versioned tables behind a view. |

**Simplified Tech Blueprint:**
```python
# Apache Spark + Delta Lake (handles transactions automatically)
input_data = spark.read.schema(schema).json("s3://devices/list")
input_data.write.format("delta").mode("overwrite").save("s3://master/devices")

# PostgreSQL: versioned table + view swap (Airflow orchestrated)
COPY devices_v20240101 FROM '/data/dataset.csv' CSV DELIMITER ';' HEADER;
CREATE OR REPLACE VIEW devices AS SELECT * FROM devices_v20240101;

# AWS CLI: bucket-level sync
aws s3 sync s3://input-bucket s3://output-bucket --delete
```

---

### Pattern: Incremental Loader

**The Concrete Problem:**
A continuously growing visits table in a legacy transactional database. Full reloads waste resources. The dataset grows every hour. You only want new rows since the last run.

**The Core Solution:**
Two implementations:
1. **Delta Column:** Filter rows by `last_ingestion_time`. Remembers the high-water mark.
2. **Partition-Based:** Job processes one time-partition per run (e.g., `date=2024-01-01`). No state needed — execution date implies the partition.

```
Delta Column Implementation:
  [Query DB WHERE event_time BETWEEN last_run AND now] → [Write to Bronze]

Partition-Based Implementation:
  [Readiness Marker: wait for partition] → [Load entire partition] → [Bronze]
```

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Hard Deletes Invisible** | If source deletes a row, delta column won't detect it (deleted rows have no timestamp). Workaround: soft deletes (mark row as `is_deleted=true`). |
| **Late Data Misses** | Event-time delta columns miss records delivered late. Use processing-time column instead. |
| **Backfill Explosion** | Replaying 2 months = full load, not incremental. Mitigation: bound the ingestion window with `BETWEEN start AND end` — this also enables parallel backfilling. |

**Simplified Tech Blueprint:**
```python
# Apache Spark — delta column approach with time bounds
in_data = spark.read.text(input_path)
input_to_write = in_data.filter(
    f'ingestion_time BETWEEN "{date_from}" AND "{date_to}"'
)
input_to_write.write.mode('append').text(output_path)

# Airflow partition sensor (partition-based approach)
FileSensor(filepath='/data/input/date={{ ds }}', mode='reschedule')
```

---

### Pattern: Change Data Capture (CDC)

**The Concrete Problem:**
Incremental Loader latency is ~minutes (job scheduling overhead). Business requires changes to be available within 30 seconds. Additionally, hard deletes must be captured.

**The Core Solution:**
Read directly from the **database commit log** (PostgreSQL WAL, MySQL binlog) — a low-level, append-only record of all database operations. Debezium (Kafka Connect) streams these changes to Apache Kafka topics.

```
PostgreSQL WAL → [Debezium Kafka Connector] → Kafka Topic (dedp.schema.table)
                                                   ↓
                                    Each event: {op: INSERT/UPDATE/DELETE, before, after}
```

Delta Lake native alternative: enable `delta.enableChangeDataFeed = true` and use `readChangeFeed` option.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Ops Complexity** | Requires enabling WAL/commit log on the database server (needs DBA/ops help). |
| **Data Scope Limitation** | CDC captures changes *after* connector starts. Historical data requires separate ingestion. |
| **Streaming Semantics** | CDC data becomes "data in motion" with different JOIN semantics. A JOIN miss doesn't mean no data — it may mean the other stream is late. |
| **Extra Metadata Payload** | Records include `_change_type`, `_commit_version`, `_commit_timestamp` — consumers must handle these extra fields. |

**Simplified Tech Blueprint:**
```json
// Debezium Kafka Connect config for PostgreSQL
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres", "database.port": "5432",
  "schema.include.list": "dedp_schema",
  "topic.prefix": "dedp"
}
```
```python
# Delta Lake CDF
spark.readStream.format('delta') \
  .option('readChangeFeed', 'true') \
  .option('startingVersion', 0) \
  .table('events')
```

---

### Pattern: Passthrough Replicator

**The Concrete Problem:**
You need the exact same device reference dataset in dev/staging as production. The source API is non-idempotent (different results per call). You can't re-run the loader — you need a copy of production's data.

**The Core Solution:**
Exact data copy from one environment to another, **without transformation**. Two modes:
- **Compute-based (EL job):** Apache Spark `.read.text()` + `.write.text()` (raw text, no JSON parsing that could alter data).
- **Infrastructure-based:** Cloud replication policies (AWS S3 bucket replication via Terraform).

**Push vs Pull:**
- **Push:** Source environment copies data to targets (recommended — source controls throughput and protects itself from being destabilized by consumers).
- **Pull:** Consumer reads from source (risky — can destabilize source).

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Metadata Loss** | Replicating Parquet files without Delta Lake metadata = broken table. Replicating Kafka messages without preserving headers/order = corrupted stream. |
| **PII in Replicated Data** | If dataset has personal data, switch to Transformation Replicator instead. |
| **Infrastructure Latency** | Cloud-native replication has SLA delays — verify before committing to this approach. |

---

### Pattern: Transformation Replicator

**The Concrete Problem:**
Same as Passthrough Replicator, but the production dataset contains PII that cannot leave production. You need realistic test data without PII for staging.

**The Core Solution:**
Add a transformation layer between read and write. Transformation options:
- **Column removal:** `SELECT * EXCEPT (ip, latitude, longitude)`
- **Column masking/truncation:** `SUBSTRING(full_name, 2, LENGTH(full_name))`
- **Row-level mapping functions** for complex logic

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Silent Type Conversion** | Reading JSON timestamps with wrong format silently drops columns. Use `string` type for complex types to avoid interference. |
| **PII Schema Drift** | New PII fields won't be removed unless transformation rules are updated. Mitigation: use a data catalog with tagged columns to auto-generate transformation logic. |

---

### Pattern: Compactor

**The Concrete Problem:**
A real-time ingestion pipeline writes small files (10-minute microbtaches → many tiny Parquet files). After 3 months, batch jobs spend **70% of execution time listing files** and only 30% actually processing data.

**The Core Solution:**
Merge many small files into fewer, larger files. Different per technology:
- **Delta Lake:** `OPTIMIZE` command (transactional, safe for readers/writers)
- **Apache Iceberg:** `rewrite_data_files` action
- **Apache Hudi (MoR):** Merges row-format delta files with columnar base files (different semantic)
- **Apache Kafka:** Log compaction (keeps only latest value per key — deletes old entries)

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Compaction Timing** | Run too rarely → job still slow. Run too often → compaction job itself consumes resources. No one-size-fits-all. |
| **Consistency** | Safe in ACID formats (Delta, Iceberg). Risky in raw formats (JSON/CSV) where readers may see half-compacted state. |
| **Orphaned Files** | Compaction preserves source files! Run `VACUUM` after to reclaim disk space. But VACUUM has a retention threshold — don't vacuum too aggressively or you lose time-travel capability. |

**Simplified Tech Blueprint:**
```python
# Delta Lake compaction + cleanup
devices_table = DeltaTable.forPath(spark, table_dir)
devices_table.optimize().executeCompaction()
devices_table.vacuum()  # Removes old pre-compaction files (after retention window)

# Kafka topic compaction policy (config only)
log.cleanup.policy=compact
log.cleaner.min.compaction.lag.ms=3600000
```

---

### Pattern: Readiness Marker

**The Concrete Problem:**
Downstream consumers start processing your hourly Silver-layer dataset while it's still being generated. Result: incomplete analytics, angry stakeholders.

**The Core Solution:**
Signal when data is complete (pull-based). Three implementations:
1. **Flag file:** Apache Spark writes `_SUCCESS` after completing all writes. Consumers watch for this file.
2. **Conventional partition:** If job for partition `N` is running, consumers can safely process partition `N-1`.
3. **Commit log entry:** Delta Lake's new commit file acts as the readiness marker automatically.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **No Enforcement** | Nothing stops consumers from reading before the marker. Requires communication and agreed conventions. |
| **Late Data Breaks Convention** | If partition 9am is "closed" but late data arrives for 9am, convention breaks. Either declare partitions immutable (ignore late data) or notify consumers of re-opened partitions. |

**Simplified Tech Blueprint:**
```python
# Apache Airflow FileSensor waiting for _SUCCESS
FileSensor(filepath=f'{input_path}/_SUCCESS', mode='reschedule')

# Create COMPLETED marker at end of pipeline
@task
def create_readiness_file():
    with open(f'{dataset_dir}/COMPLETED', 'w') as f:
        f.write('')
```

---

### Pattern: External Trigger

**The Concrete Problem:**
A feature release happens at most once per week, unpredictably. A daily job refreshes data even when nothing changed — wasting compute resources.

**The Core Solution:**
Subscribe to event notifications and trigger pipelines only when data changes. Three steps:
1. **Subscribe** to a notification channel (message bus, S3 event, pub/sub)
2. **React** to events (filter, decode, decide)
3. **Trigger** the pipeline (call Airflow DAG REST API, start Lambda, etc.)

**Push vs Pull:**
- **Push (recommended):** Data source pushes notifications to a handler, which starts a pipeline instance. Minimal waste.
- **Pull:** Long-running job continuously polls. Wasteful but sometimes unavoidable.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Lost Events = Lost Runs** | If notification is dropped, pipeline never triggers. Protect with error management (dead-letter queues). |
| **Context Gap** | Simple ping-style triggers give no processing context. Always enrich trigger metadata: function version, event time, notification envelope. |

---

## 3. Real-World Analogies

```
Full Loader = Erasing your whiteboard and rewriting everything from scratch each meeting.
              Fast when content is small, slow and disruptive when it grows.

Incremental Loader = Only adding new lines to your shopping list since last visit.
                     Efficient, but you need to remember where you last stopped.

CDC = A secretary who watches every change you make to a document and instantly
      transcribes it to another document in another room.

Compactor = Decluttering your drawer: combine 50 sticky notes into 5 clean index cards.
            After decluttering, shred the old stickies (VACUUM).

Readiness Marker = A restaurant's "Order Ready" bell. Don't go to the counter until
                   the chef rings it — regardless of how hungry you are.

External Trigger = A doorbell. The house doesn't check the door every 5 seconds —
                   it only responds when someone actually rings.
```

```
DATA INGESTION FLOW:
┌──────────────────────────────────────────────────────┐
│  DATA PRODUCER (API / DB / Streaming Broker)         │
└──────────────┬────────────────────────────┬──────────┘
               │ Full / Incremental / CDC   │ Event Push
               ▼                            ▼
    ┌──────────────────┐        ┌────────────────────┐
    │ Compactor        │        │ External Trigger   │
    │ (reduces files)  │        │ (start pipeline)   │
    └──────────────────┘        └────────────────────┘
               │
               ▼
    ┌──────────────────┐
    │ Readiness Marker │  ← signals "data complete"
    └──────────────────┘
               │
               ▼
        BRONZE LAYER (Raw, as-is)
```

---

# CHAPTER 3 — Error Management Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** You cannot control your data providers. Networks fail, producers retry, late connections buffer events. Error management isn't optional — it's the armor your pipeline wears. Without it, one bad record can take down a streaming job that's been running for days.

**Medallion Impact:** Errors arise at all layers. Dead-lettered records belong to Bronze remediation. Deduplicated/late-data-integrated data flows into Silver.

---

## 2. Master Pattern Breakdown

### Pattern: Dead-Letter

**The Concrete Problem:**
A streaming job writes visits from Kafka to object store. Some producers send malformed records. Each time one arrives, the job crashes. You've been manually restarting it and patching checkpoint files for 3 days.

**The Core Solution:**
Wrap risky transformation code in a try-catch (streaming) or validity flag (SQL). Redirect failed records to a separate **dead-letter store** (Kafka topic or object store path). Main pipeline continues. Dead-letter store enables later analysis and optional replay.

```
Input → [Try Transform] → Success → Output Store
                        → Failure → Dead-Letter Store → Monitor → (Optional Replay Pipeline)
```

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Snowball Backfill Effect** | Replaying dead-lettered records into already-processed partitions forces downstream consumers to backfill too, cascading down the dependency chain. |
| **Ordering Broken** | Replay inserts records out of order. If consumers expect ordered delivery, this is a problem. |
| **Hidden Fatal Failures** | Dead-lettering hides errors. Add alerting: if >X% of records are dead-lettered, stop the pipeline. |
| **Error-Safe Functions** | SQL functions like CONCAT return NULL on failure (no exception). You must check `(input IS NOT NULL AND output IS NULL)` to detect failures — verbose and tricky. |

**Simplified Tech Blueprint:**
```python
# Apache Flink: side outputs
invalid_data_output = OutputTag('invalid_visits', Types.STRING())

def map_rows(self, json_payload):
    try:
        evt = json.loads(json_payload)
        yield json.dumps({'visit_id': evt['visit_id'], ...})
    except Exception as e:
        yield self.invalid_data_output, wrap_with_error(json_payload, e)

# Route side output to dead-letter Kafka topic
visits.get_side_output(invalid_data_output).sink_to(kafka_dead_letter_sink)
```

---

### Pattern: Windowed Deduplicator

**The Concrete Problem:**
Producers use automatic retries, causing duplicate events in Kafka. Your batch job must guarantee exactly-once processing per distinct visit record.

**The Core Solution:**
- **Batch:** `dropDuplicates(['key_columns'])` or `ROW_NUMBER() OVER (PARTITION BY ...) = 1`
- **Streaming:** Define a **time-based deduplication window** (watermark). Records with the same key seen within the window are dropped. State store retains seen keys for the window duration.

```
Stream: [A,B,A,C,B] → [watermark window 10min] → dedup state → [A,B,C]
After window expires, state is garbage collected (prevents infinite memory growth)
```

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Space vs Time Trade-off** | Short window = misses duplicates from slow producers. Long window = larger state store, more memory. |
| **Not Exactly-Once Delivery** | Even with dedup, retries during failures can still write duplicates before dedup runs. Combine with idempotency patterns from Chapter 4. |

**State Store Types:**
| Type | Speed | Fault Tolerance |
|---|---|---|
| Local (in-memory only) | Fastest | Data lost on restart |
| Local + fault-tolerance (persisted async) | Fast | Data mostly safe |
| Remote (Redis/RocksDB) | Slower | Always durable |

**Simplified Tech Blueprint:**
```python
# Apache Spark Streaming deduplication with watermark
visits_events \
  .withWatermark("visit_time", "10 minutes") \
  .dropDuplicates(["visit_id", "visit_time"])
```

---

### Pattern: Late Data Detector

**The Concrete Problem:**
Most events arrive within 15 seconds. But sometimes users buffer visits offline and flush them hours later. You need to detect which records are "late" to apply a dedicated strategy (ignore, integrate, flag).

**The Core Solution:**
Track **watermark** = `MAX(event_time per partition) − allowed_lateness`. Any record with `event_time < watermark` is considered late.

**Multi-source aggregation strategies:**
- **MIN across partitions** → follows the slowest source (more inclusive, larger buffer)
- **MAX across partitions** → follows the fastest source (aggressive, may drop late records)

⚠️ Never use MIN at the **per-partition level** — this causes "stuck in the past" state growth and open-close-open loops in stateful jobs.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Late Data Capture Varies** | Spark Structured Streaming detects late data but doesn't expose it easily. Flink gives explicit access to late records via side outputs. |
| **MAX Strategy + Skewed Sources** | If 4/5 sources are 40 minutes late, MAX watermark (from the fast source) will discard all records from the slow sources. |

---

### Pattern: Static Late Data Integrator

**The Concrete Problem:**
A daily batch job ignores any records older than today. But some late data up to 15 days old is still valuable. You want the daily pipeline to automatically include these past N days.

**The Core Solution:**
Define a **fixed lookback window** (e.g., 14 days). Every pipeline run reprocesses the current day PLUS the past N days. Three placement strategies:
1. **Sequential:** Late data first, then current (required for stateful pipelines with history dependencies)
2. **Parallel:** Current and late data at the same time
3. **Sequential reverse:** Current first, then late data

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Snowball Backfill** | Late data integration forces downstream consumers to also backfill. Notify them. |
| **Overlapping Backfills** | Replaying 3 days with a 4-day window means each day re-covers overlapping date ranges. Calculate the minimum backfill span carefully. |
| **Wasteful if No Late Data** | Fixed window always runs even when no late data exists. Combine with Dynamic Late Data Integrator if waste is a concern. |

---

### Pattern: Dynamic Late Data Integrator

**The Concrete Problem:**
The 15-day static window is too limiting. Business wants ALL late data integrated, even if it's 30 or 60 days old.

**The Core Solution:**
Maintain a **state table** that tracks:
- `partition` (e.g., `2024-12-17`)
- `last_processed_time`
- `last_update_time` (when did the source partition last change?)
- `is_processed` (lock for concurrent runs)

Query: `SELECT partition WHERE last_update_time > last_processed_time AND partition < current_partition AND is_processed = false`

Only truly modified partitions get reprocessed.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Concurrency Race Condition** | Parallel runs may independently detect the same partition to backfill and double-process it. Mitigation: `is_processed` lock + `depends_on_past=True` in Airflow. |
| **Stateful Pipelines = Deep Backfill** | A stateful job detecting late data 2 months old must rerun every day since then. This can be catastrophic. Consider a max lookback even for dynamic windows. |
| **Getting Last Update Time** | Not all stores expose partition modification time natively. Delta Lake requires reading `DeltaLog.getChanges()` — custom code. BigQuery has `INFORMATION_SCHEMA.PARTITIONS`. |

---

### Pattern: Filter Interceptor

**The Concrete Problem:**
After a job deployment, filtered data spikes from 15% to 90%. The physical query plan collapses all filter conditions into one — you can't see which specific condition is responsible.

**The Core Solution:**
Wrap each filter condition with an **accumulator counter** that increments when the condition matches. Collect and log these counters at job end.

**SQL equivalent:** Use a CASE WHEN subquery to flag which condition filtered each row, then GROUP BY the flag.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Runtime Overhead** | Small (accumulators are local) but non-zero. SQL version may require temp table. |
| **Streaming Complexity** | Counting filters in a streaming job requires managing state — transforms a stateless job into a stateful one. Add time-window boundaries. |

---

### Pattern: Checkpointer

**The Concrete Problem:**
A streaming job counts unique visits in 10-minute windows. If it crashes and restarts, it reprocesses data from the beginning.

**The Core Solution:**
Periodically persist the job's progress (offsets, state) to a durable external store. On restart, resume from the last checkpoint.

Two checkpoint storage strategies:
- **Framework-managed:** Spark Structured Streaming writes checkpoint files to object store automatically via `checkpointLocation`. Flink writes state snapshots on time intervals.
- **Data-store-managed:** Kafka SDK stores offsets in `__consumer_offsets` topic. Amazon KCL uses DynamoDB.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **At-Least-Once, Not Exactly-Once** | Checkpoint after write = at-least-once (duplicates on retry). Checkpoint before write = at-most-once (data loss). For exactly-once, combine with idempotency patterns. |
| **Latency vs Durability** | Frequent checkpoints = slower job. Infrequent checkpoints = more reprocessing on restart. |
| **State Size** | For stateful jobs (sessions, aggregations), checkpointing the entire state is expensive. |

```python
# Apache Spark Structured Streaming checkpointing
write_query = (input_stream.writeStream
  .option('checkpointLocation', f'{base_dir}/checkpoint')
  .foreachBatch(process_batch)
  .start())

# Apache Flink time-based checkpointing
env.enable_checkpointing(30000, mode=EXACTLY_ONCE)
env.get_checkpoint_config().enable_externalized_checkpoints(RETAIN_ON_CANCELLATION)
```

---

## 3. Real-World Analogies

```
Dead-Letter = A post office's "undeliverable mail" bin. Instead of throwing letters
              away or stopping operations, they pile up separately for human review.

Windowed Deduplicator = A concert ticket scanner that keeps a list of scanned barcodes
                        for the last 30 minutes. If you scan the same ticket twice within
                        that window, you're turned away.

Late Data Detector = A train station clock calculating delay. "Train was due at 10:00.
                     Current time: 10:15. Allowed lateness: 5 min. Train at 10:20 is
                     considered late."

Checkpointer = A video game auto-save. If your game crashes, you resume from the last
               save point, not from the very beginning.
```

---

# CHAPTER 4 — Idempotency Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** Error management keeps your pipeline running, but retries and backfills cause data to be processed multiple times. Without idempotency, you get duplicates, wrong aggregates, and double-charged customers. Idempotency guarantees: *run the job 10 times → same output as running it once.*

**Medallion Impact:** Idempotency spans all layers. Bronze needs it for safe re-ingestion; Silver and Gold need it to avoid duplicate aggregations.

---

## 2. Master Pattern Breakdown

### Pattern: Fast Metadata Cleaner

**The Concrete Problem:**
A daily batch job runs DELETE + INSERT on a growing visits table. After weeks, the DELETE operation scans terabytes and takes hours. Idempotency is now the bottleneck.

**The Core Solution:**
Instead of physical row deletion, use **metadata operations** (TRUNCATE / DROP TABLE) which are O(1) regardless of table size. The trick: split the logical table into many physical tables (e.g., one per week). Use a **view** to expose them as one unified dataset.

Workflow:
1. Detect if this run starts a new "granularity unit" (e.g., Monday → new weekly table)
2. TRUNCATE or DROP the current unit's table (fast!)
3. Recreate/update the view to include the new table
4. Insert data into the cleared table

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Backfill Granularity Lock** | Can't replay just one day if partitioned weekly — must replay the full week. |
| **Metadata Limits** | BigQuery: 4,000 partitions max. Redshift: 200,000 tables max. Use freezing (merge weekly→monthly tables) to stay within limits. |
| **Schema Evolution** | Adding a new required column triggers reprocessing for all tables in the partition. Optional columns can be added via `ALTER TABLE`. |

```python
# Airflow: Monday → new weekly table, Tuesday–Sunday → insert only
check_if_monday = BranchPythonOperator(
    task_id='check_day',
    python_callable=lambda: 'create_weekly_table' if is_monday() else 'dummy_task'
)
# SQL: create/truncate + update view
CREATE OR REPLACE VIEW visits AS SELECT * FROM visits_wk_2024_01 UNION ALL ...
```

---

### Pattern: Data Overwrite

**The Concrete Problem:**
Same daily batch idempotency need, but you work on an object store (S3, GCS) — no TRUNCATE/DROP support.

**The Core Solution:**
Use native overwrite mechanisms:
- **Spark:** `write.mode('overwrite')` — deletes existing files, writes new ones
- **Delta Lake:** `replaceWhere` for selective partition overwrite
- **SQL:** `INSERT OVERWRITE` or `DELETE FROM + INSERT INTO`
- **BigQuery:** `bq load --replace=true`

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Data Overhead** | Larger dataset = slower overwrite. Mitigate with partitioning (only overwrite the affected partition). |
| **VACUUM Needed** | DELETE in Delta/Iceberg doesn't physically remove files — they're marked as "not current." Run VACUUM to reclaim space, but respect the retention period or time-travel breaks. |

---

### Pattern: Merger (UPSERT)

**The Concrete Problem:**
You're syncing CDC changes from Kafka into a Delta Lake table. Each run only has a subset of changed rows (not the full table). You can't simply overwrite everything.

**The Core Solution:**
Use **MERGE (aka UPSERT)**: match records on a unique key, then:
- `WHEN MATCHED → UPDATE` (existing record changed)
- `WHEN NOT MATCHED → INSERT` (new record)
- `WHEN MATCHED AND is_deleted = true → DELETE` (soft-deleted record)

⚠️ Always include the `is_deleted` check in the INSERT clause — otherwise, deleted records may be re-inserted on the first run.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Backfill Inconsistency** | Replaying run at 08:00 with incremental data while current table has records from 09:00 and 10:00 = table temporarily has too many records (rows from future). Final state recovers only after all steps are replayed. |
| **Requires Unique Keys** | No immutable identity = no safe merge. You'll create unclearable duplicates. |
| **I/O Intensive** | MERGE reads data blocks, not just metadata. Mitigated by Delta Lake's statistics to skip non-matching files. |

```sql
MERGE INTO target AS t USING source AS s
ON t.type = s.type AND t.version = s.version
WHEN MATCHED AND s.is_deleted = true THEN DELETE
WHEN MATCHED AND s.is_deleted = false THEN UPDATE SET full_name = s.full_name
WHEN NOT MATCHED AND s.is_deleted = false THEN INSERT (...)
```

---

### Pattern: Stateful Merger

**The Concrete Problem:**
A data quality issue is found in the merged table. Business asks you to roll back to the last valid state before re-running backfill. The Merger pattern has no rollback capability.

**The Core Solution:**
Add a **state table** mapping `execution_time → delta_table_version`. Before each run:
1. Look up the version created by the *previous* execution
2. Compare it to the *current* table version
3. If they differ → we're in backfill mode → restore the table to the previous version
4. Run MERGE
5. Record the new version in the state table

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Requires Versioned Storage** | Only works with Delta Lake, Iceberg, or other versioned stores. For non-versioned stores, use a `devices_history` table with `execution_time` column as version proxy. |
| **VACUUM Erases Old Versions** | After the retention window, old versions are gone. Backfill only works within the retention period. |
| **Compaction Confuses Version Logic** | Compaction creates new versions without data changes. Use `version_for_current_execution_time - 1` (not `previous_execution_version`) to handle interleaved metadata operations. |

---

### Pattern: Keyed Idempotency

**The Concrete Problem:**
A streaming job generates user sessions and writes them to a key-value store (ScyllaDB). On restart, the same sessions get re-inserted. Key-value stores silently overwrite on the same key — but only if the key is the same across runs.

**The Core Solution:**
Design keys from **immutable attributes**. Use `append_time` (broker-assigned, never changes) instead of `event_time` (can shift with late data) for session key generation.

```python
# Session ID = hash of the earliest append_time for the user's first event in the window
# This is stable even if late events arrive after restart
session_id = hash(str(min_append_time))
```

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Kafka Compaction Risk** | If the first event (used for key) is compacted away, restarts use a different first event → different session ID → breaks idempotency. |
| **NoSQL Only (naturally)** | Works natively for key-value stores. For relational DBs, MERGE is needed (adds complexity). For Kafka, compaction is asynchronous → brief duplicates visible. |

---

### Pattern: Transactional Writer

**The Concrete Problem:**
A batch job using spot/preemptible instances gets interrupted mid-write. Tasks fail and retry. Consumers see partial, inconsistent data during the retry window.

**The Core Solution:**
Wrap writes in a **database transaction**. Data is only visible to consumers after the `COMMIT`. If anything fails → automatic `ROLLBACK` → data disappears completely (no partial visibility).

Two modes in distributed frameworks:
- **Task-level transactions** (easier): each task commits independently. Idempotency limited to current run.
- **Job-level transactions** (stronger): commit only when all tasks complete. Delta Lake implements this via commit log entries.

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Latency** | Consumers wait for the slowest task before seeing any data. |
| **Framework Support** | Apache Kafka transactional producer works with Flink, but NOT with Spark (notable gap). |
| **Idempotency Scope** | Transactions only guarantee uniqueness within one run. Backfill reruns still write duplicates unless combined with overwrite patterns. |

```python
# Apache Flink → Kafka exactly-once transactional producer
KafkaSink.builder() \
  .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE) \
  .set_property('transaction.timeout.ms', str(60 * 1000)) \
  .build()
```

---

### Pattern: Proxy

**The Concrete Problem:**
Legal requires all historical versions of a dataset to be preserved (immutable storage). But downstream consumers should only see the latest version from a single endpoint.

**The Core Solution:**
Write each dataset to a **uniquely named, write-once location** (versioned tables like `devices_20240101_143022`). Remove write permissions after creation. Expose all versions from a **single view** or manifest file that points to the latest.

Storage options:
- **Database views** pointing to the latest internal table
- **Object store WORM locks** (S3 Object Lock, Azure Immutability Policies, GCS Object Holds)
- **Delta Lake/Iceberg time travel** (natively versioned — each write creates a new version; old data persists within retention window)

**The Trade-Offs & Consequences:**
| Risk | Description |
|---|---|
| **Database View Support** | Not all databases support views (especially NoSQL). Fallback: manifest file listing current files. |
| **Partition Table Growth** | Creating one table per run eventually hits metadata limits (see Fast Metadata Cleaner). Add a freezing step. |

---

## 3. Real-World Analogies

```
Fast Metadata Cleaner = Erasing a whiteboard section header and rewriting under it,
                        rather than erasing every single word individually. The header
                        operation is instant.

Data Overwrite = Replacing a page in a binder with a new printed page. The old page
                 goes in the bin (but may still be recoverable from the recycling bin/VACUUM).

Merger = Track changes in Google Docs: new edits merge with existing content — they
         don't replace the whole document. Only changed lines update.

Proxy = A library's "New Arrivals" shelf that's always updated to the latest book edition.
        All previous editions are still in the stacks — but borrowers go to the shelf first.

Keyed Idempotency = A hotel room key. Swipe the same card 10 times — the door opens
                    the same way each time. The idempotent action is "open door."
```

---

# CHAPTER 5 — Data Value Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** Raw Bronze data is often incomplete and contextless. A visit event tells you the URL and timestamp — but not whether the visitor is a VIP user, what device they're on, or how many sessions they've had. Data value patterns transform raw data into *useful* data.

**Medallion Impact:** These patterns primarily produce **Silver** (enriched, cleansed) and **Gold** (aggregated, business-facing) layer data.

---

## 2. Master Pattern Breakdown

### Pattern: Static Joiner

**Problem:** Raw visit events lack user context (registration date, tier). The reference users dataset is static and doesn't change often.

**Solution:** JOIN visit events with static reference data using a key (`user_id`). For slowly evolving reference data, use **SCD Type 2 or 4**:
- **SCD Type 2:** Single table with `start_date`/`end_date` columns. Active record has `end_date = '9999-12-31'`.
- **SCD Type 4:** Two tables — current values in one, history in another.

**Trade-Offs:** Backfilling requires time-aware joins (use execution_date, not NOW()). API-backed reference data can drift unless materialized with timestamps.

---

### Pattern: Dynamic Joiner

**Problem:** Both datasets are in motion (streaming visits + streaming user profile changes). Static Joiner can't handle two moving streams.

**Solution:** Time-bounded stream-to-stream JOIN with a **GC watermark** buffer. The faster stream buffers unmatched records up to the allowed latency difference. GC watermark cleans up old unmatched records to prevent infinite state growth.

**Trade-Offs:**
- Buffer size = space vs. match rate trade-off
- Late data can still cause missed joins
- More complex state management than Static Joiner

```python
# Apache Spark: time-bounded stream join
visits.withWatermark('event_time', '10 minutes') \
  .join(ads.withWatermark('display_time', '10 minutes'),
        F.expr('display_time BETWEEN event_time AND event_time + INTERVAL 2 minutes'),
        'left_outer')
```

---

### Pattern: Wrapper

**Problem:** Visits come from multiple providers with different schemas. You need computed fields (e.g., `is_connected`, `page_referral_key`) alongside raw originals for debugging.

**Solution:** Add an extra *envelope layer* that contains both raw and computed attributes separately. Implementations:
- Nested struct (raw + computed sections)
- Flat table with naming convention (`raw_*` vs. computed columns)
- Separate table joined by unique key

**Trade-Offs:** Size doubles (raw + computed stored together). Mitigate with columnar projection.

---

### Pattern: Metadata Decorator

**Problem:** You want to attach technical metadata (job version, processing time) to records for debugging — but you *don't* want this information exposed to business users.

**Solution:** Use the data store's native metadata layer:
- **Kafka:** Headers (key-value pairs attached to each record, not visible in the message value)
- **Object stores:** File-level tags
- **Databases without native metadata:** Hidden column + view that excludes it, OR separate `processing_context` table joined by execution_time

**Trade-Offs:** Not all streaming brokers support headers (Amazon Kinesis does not). Hidden metadata columns add schema complexity.

---

### Pattern: Distributed Aggregator

**Problem:** Need OLAP cube statistics (count, avg duration) across geography, devices, and time — on terabytes of data split across many machines.

**Solution:** Use a distributed framework (Apache Spark) that automatically handles **shuffle** — moving related records to the same node for aggregation. The MapReduce paradigm: Map (local pre-aggregate) → Shuffle (network exchange) → Reduce (final aggregate).

**Trade-Offs:**
- Network shuffle is the primary latency cost
- **Data skew** (one key has 10x more records): use **salting** (add random prefix to key, aggregate twice — first on salted key, then on original key)
- Scaling may keep idle nodes in memory during shuffle for fault tolerance

---

### Pattern: Local Aggregator

**Problem:** Shuffle is expensive. Your data is already partitioned by the grouping key in the streaming broker, so shuffle is unnecessary.

**Solution:** Guarantee all records for a given grouping key land on the same partition. Consumers can then aggregate locally without any network exchange. Kafka Streams' `groupByKey()` does this automatically. Apache Spark needs `mapPartitions()` or `foreachPartition()` for the same effect.

**Trade-Offs:**
- Static partitioning schema required — scaling changes partition count → must reorganize all historical data (stop-the-world event for streaming)
- All consumers must use the same grouping key — no flexibility for different aggregation axes

---

### Pattern: Incremental Sessionizer

**Problem:** User sessions span multiple hourly partitions (max session = 3 hours). Analysts need to reconstruct full sessions from fragmented data.

**Solution:** Three storage spaces:
1. **Input dataset** (hourly partitioned visits)
2. **Completed sessions** (public, final sessions)
3. **Pending sessions** (private, in-flight sessions spanning multiple partitions)

Each run: load pending sessions from previous run + new input → apply sessionization logic → write completed sessions to output, write updated pending sessions to private store.

**Trade-Offs:**
- Late data for an event-time partition reopens "closed" sessions — downstream consumers see state changes for completed sessions (risk for fraud detection, etc.)
- Backfilling one partition → must backfill all subsequent partitions (forward-dependent state)

---

### Pattern: Stateful Sessionizer

**Problem:** Batch Incremental Sessionizer gives 1-hour latency. Stakeholders need near-real-time sessions.

**Solution:** Streaming stateful processing with a **state store** (in-memory + persisted to durable storage). State store plays the role of "pending sessions" from Incremental Sessionizer. Use either:
- **Session windows** (framework-managed, gap-based expiration)
- **Arbitrary stateful processing** (`applyInPandasWithState` in Spark, `ProcessFunction` in Flink) for complex custom logic

**Trade-Offs:**
- At-least-once processing (checkpoint interval means potential re-processing on restart)
- Scaling requires state rebalancing (expensive)
- Processing-time expiration is dangerous — use event-time expiration for predictability

---

### Pattern: Bin Pack Orderer

**Problem:** You must deliver visit events to a partner streaming broker (Kinesis, which has partial commit semantics) in event-time order. Bulk API writes partially fail, breaking ordering.

**Solution:**
1. Sort records by `(entity_key, event_time)`
2. Pack them into **bins** — each bin contains at most one record per entity
3. Deliver bins sequentially (bin 1 fully committed before bin 2 starts)

If a retry happens within a bin, it only affects that entity's position in the current bin — not other entities.

**Trade-Offs:** Custom bin-packing algorithm complexity. Retry of the full pipeline still breaks ordering.

---

### Pattern: FIFO Orderer

**Problem:** Need ordered delivery but not the complexity of Bin Pack. Volume is low and latency tolerance is higher.

**Solution:** Deliver one record at a time, get acknowledgment before sending the next. Or use bulk API with `max.in.flight.requests.per.connection=1`. Kafka idempotent producer allows up to 5 concurrent requests while still guaranteeing order.

**Trade-Offs:** Higher network overhead (1 request per record). Not suitable for high-volume scenarios. Does NOT guarantee exactly-once delivery on its own — combine with idempotency patterns.

---

## 3. Real-World Analogies

```
Static Joiner = Stamping a library card number on borrowed books to link book records
                to the borrower's profile. The library card (reference) rarely changes.

Dynamic Joiner = Matchmaking two streams of RSVPs and arrivals at a party. People RSVP
                 days in advance and arrive at the door. You need a buffer window to
                 match RSVPs with actual arrivals.

Distributed Aggregator = A city-wide vote count. Each district counts locally, then
                         sends results to the central election commission (shuffle = courier
                         delivering district envelopes). Final count = sum of all envelopes.

Bin Pack Orderer = A UPS truck with sorted packages per street address. It delivers
                   all packages at address 1 before driving to address 2. If one package
                   at address 1 fails (nobody home), retry that address before moving on.
```

---

# CHAPTER 6 — Data Flow Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** Individual patterns solve local problems. Data flow patterns coordinate *how pipelines connect and run together* — enabling cross-team data sharing, parallel execution, and controlled concurrency. These operate at the **data orchestration layer** (Airflow, Step Functions) and the **data processing layer** (Spark job logic).

---

## 2. Master Pattern Breakdown

### Pattern: Local Sequencer

**Problem:** A monolithic job with hundreds of lines fails often, restarts from scratch each time, and has unclear separation of concerns.

**Solution:** Decompose into sequential, dependent tasks. Each task has a single responsibility. Task B only runs if task A succeeded.

**When to use orchestration-level sequencing vs. job-level:**
- **Orchestration:** Tasks that should restart independently, paid API calls (avoid re-calling on retry), backfilling individual steps
- **Job-level:** Steps that must be atomic (processing + state table update)

```python
# Airflow: sequencing with >>
input_sensor >> load_data >> expose_table

# PySpark: implicit sequencing via variable dependency
raw = spark.read...
enriched = raw.join(reference, ...)
enriched.write...
```

---

### Pattern: Isolated Sequencer

**Problem:** Your pipeline's output feeds another team's pipeline. They're maintained independently — you can't merge them.

**Solution:** Connect physically isolated pipelines via:
- **Data-based:** Producer writes Readiness Marker → Consumer watches for it (loosely coupled)
- **Task-based:** Producer directly triggers consumer pipeline via API/ExternalTaskMarker (tightly coupled, harder to evolve independently)

**Trade-Offs:**
- Data-based: Consumer can switch data sources freely; communication about changes is optional
- Task-based: Name changes in consumer break producer; strong coupling requires coordinated evolution

---

### Pattern: Aligned Fan-In

**Problem:** 24 hourly loaders feed one daily aggregation. The daily job should only run once ALL 24 loaders complete.

**Solution:** Define 24 parallel branches that converge on a single downstream task. Downstream task only triggers after **all** parents succeed.

**Trade-Offs:**
- Infrastructure spikes (24 jobs run simultaneously)
- Scheduling skew (daily task waits for slowest hourly job)
- Consider incremental approach: run each hourly, aggregate in the last run

---

### Pattern: Unaligned Fan-In

**Problem:** One of the 24 hourly loaders fails. The current Aligned Fan-In blocks the daily aggregate completely — even when 23/24 succeeded.

**Solution:** Relax the trigger condition (`trigger_rule=ALL_DONE` in Airflow). Downstream task runs regardless of parent outcomes. Mark the output as "approximate" if not all parents succeeded.

**Trade-Offs:** Consumers must understand and handle partial datasets. Readability decreases (add documentation/flags to the output).

---

### Pattern: Parallel Split

**Problem:** During a migration, the same processed dataset must be written to both the old (CSV) and new (Delta Lake) formats simultaneously.

**Solution:** One common parent task; two parallel downstream tasks. At the data processing level, call `.persist()` to avoid reading the input dataset twice.

**Trade-Offs:**
- If one branch is much slower, it blocks pipeline completion
- Hardware requirements of branches may differ (CPU vs. memory optimized) — split into separate jobs

---

### Pattern: Exclusive Choice

**Problem:** After migration completes, only the new format should run. Old format should still run for backfills of pre-migration dates.

**Solution:** Add a condition evaluator task before the branching point. Based on execution date (or any condition), route to only one downstream task.

**Trade-Offs:** Multiple conditions degrade readability. Treat complexity as a signal to break the pipeline or use per-date pipelines.

---

### Pattern: Single Runner

**Problem:** Incremental sessionization pipeline — state of run N depends on run N-1. Running multiple instances simultaneously corrupts the state.

**Solution:** Set `max_active_runs=1` and `depends_on_past=True`. Guarantees sequential execution.

**Trade-Offs:** Backfilling is extremely slow (sequential). Latency accumulates if processing time exceeds schedule interval.

---

### Pattern: Concurrent Runner

**Problem:** Data ingestion pipelines are independent of each other. The Single Runner is unnecessarily serializing them during backfills.

**Solution:** Set `max_active_runs=5` (or more). Multiple runs execute simultaneously.

**Trade-Offs:**
- Shared resource contention (use workload management/quotas)
- Shared state in storage = race conditions (must use locking/sequentiality like `depends_on_past` for specific tasks)

---

## 3. Real-World Analogies

```
Local Sequencer = Assembly line: weld the frame, then paint it, then add wheels.
                  Each step depends on the previous one completing.

Isolated Sequencer = Two factories: Factory A completes a batch and rings a bell.
                     Factory B listens for the bell and starts its process.
                     They never directly talk — the bell is their only interface.

Aligned Fan-In = A jury verdict: all 12 jurors must agree before delivering judgment.
                 One undecided juror blocks the whole verdict.

Unaligned Fan-In = A school election where votes are tallied even if some ballot boxes
                   arrive late. You announce results with a footnote about missing boxes.

Parallel Split = A chef who prepares both a main course and a dessert simultaneously
                 from the same pantry inventory.
```

---

# CHAPTER 7 — Data Security Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** GDPR, CCPA, and similar regulations impose legal obligations to protect user data. Beyond compliance, a single misconfigured permission can allow one team to overwrite another's production data.

---

## 2. Master Pattern Breakdown

### Pattern: Vertical Partitioner (Security Context)

**Problem:** Personal data (birthday, IP, location) is repeated in millions of records. Deleting one user requires a full table scan and overwrite.

**Solution:** Split each row into **mutable** (changes per visit: page, time) and **immutable personal** (constant per user: email, DOB) components, stored in separate tables/locations. Deletion now only touches the single PII record, not millions of visit rows.

**Trade-Offs:** JOIN required at query time. Multiple data stores (polyglot persistence) requires multiple deletion pipelines.

---

### Pattern: In-Place Overwriter

**Problem:** Legacy system — terabytes in horizontal partitions. No time to refactor to Vertical Partitioner. Privacy regulation demands deletion capability now.

**Solution:** 
- **ACID stores (Delta Lake, Iceberg):** `DELETE WHERE user_id = 'X'` + `VACUUM` (to remove physical files with retained data)
- **Raw file formats (JSON/CSV):** Filter-out job → write clean data to staging area → promote staging to final location (atomic rename)
- **Kafka compacted topics:** Publish tombstone message (key=user_id, value=null) → compaction removes all prior records for that key

**Trade-Offs:** Massive I/O overhead. Columnar formats (Parquet) mitigate this via row-group statistics (skip files that don't contain the target user). Group deletion requests in batches.

---

### Pattern: Fine-Grained Accessor for Tables

**Problem:** Users authorized to read a table shouldn't see all columns (PII) or all rows (other users' data).

**Solution:**
- **Column-level:** `GRANT SELECT(col_a, col_b) ON table TO user` (Redshift, PostgreSQL)
- **Column masking:** Show column only to privileged users: `CREATE FUNCTION ip_mask... RETURN CASE WHEN is_member('engineers') THEN ip ELSE '.'`
- **Row-level:** Row access policies / ROW FILTER functions that dynamically add WHERE conditions based on the current user's session attributes

**Trade-Offs:** Row-level security functions limited to session attributes (user, group, IP). Complex nested types may require unnesting before column-level grants.

---

### Pattern: Fine-Grained Accessor for Resources

**Problem:** Cloud security audit finds one data job can read/write ALL buckets in the account — massively over-privileged.

**Solution:** Apply at-least-privilege principle at the cloud IAM level:
- **Resource-based:** Attach policy directly to the resource (GCS bucket IAM, S3 bucket policy)
- **Identity-based:** Attach permissions to the identity (AWS IAM role) assumed by the job

**Trade-Offs:** Many fine-grained policies = maintenance overhead. Wildcard-based (`visits*`) simplifies but may grant unintended future access. Cloud IAM has quota limits on custom policies.

---

### Pattern: Encryptor

**Problem:** Sensitive data at rest could be read if someone gains physical access to storage. Data in transit could be intercepted.

**Solution:**
- **Encryption at rest:** Server-side (cloud KMS handles key management → transparent to application) or client-side (application manages keys)
- **Encryption in transit:** TLS protocol. Enforce minimum version (TLS 1.2+) at service configuration level

**Trade-Offs:** CPU overhead for encryption/decryption. Key loss = data loss (cloud providers offer soft-delete grace periods for keys). Old TLS versions must be actively deprecated.

---

### Pattern: Anonymizer

**Problem:** Sharing dataset with external analytics firm. Dataset contains PII that users haven't consented to share with third parties.

**Solution:** Remove or alter PII with one of:
1. **Data removal** — drop the column entirely
2. **Data perturbation** — add noise (e.g., alter digits in IP)
3. **Synthetic data replacement** — replace with realistic-looking fake values (Faker library)

**Trade-Offs:** Removed data cannot be used in analytics. Synthetic values may distort ML training data.

---

### Pattern: Pseudo-Anonymizer

**Problem:** Full anonymization makes the dataset too sparse for analysis. Business still needs to segment users by geography without knowing exact locations.

**Solution:** Replace precise values with less-precise but still useful representations:
1. **Data masking** — partial hiding: `999-55-1040` → `XXX-XX-1040`
2. **Data tokenization** — replace with random tokens, store mapping in a secure vault
3. **Hashing** — irreversible one-way hash (email → SHA-256 digest)
4. **Encryption** — reversible by key-holder

**Trade-Offs:** Pseudo-anonymized data can be re-identified by combining tables (combination attacks). Hashing collapses multiple values to same hash — information loss. Data masking changes data types.

---

### Pattern: Secrets Pointer

**Problem:** API credentials accidentally committed to Git. Leaked credentials generated unexpected billing charges.

**Solution:** Store credentials in a secrets manager (AWS Secrets Manager, GCP Secret Manager). Code references the secret's *name* (a pointer), not its value. Value is fetched at runtime.

**Trade-Offs:** Cache invalidation (stale credentials cause connection failures). Credentials may appear in logs if carelessly logged. Requires access to the secrets manager service itself (another permission layer).

---

### Pattern: Secretless Connector

**Problem:** API keys are hard to rotate, easy to leak, and require management overhead.

**Solution:** Use cloud IAM identity-based access — no credentials at all. The service assumes an IAM role; the cloud validates permissions transparently. Alternative: certificate-based authentication (no password).

**Trade-Offs:** Cloud-provider specific. Certificate rotation requires supporting both old and new certificates simultaneously during the transition.

---

## 3. Real-World Analogies

```
Vertical Partitioner = A hospital keeping patient personal info in a locked cabinet
                       separate from medical records. Destroying a patient's file
                       only requires shredding one document, not all their visit notes.

Encryptor = A lockbox where even if someone steals the box, they can't open it
            without the combination. Server-side encryption = the bank holds the combo.

Anonymizer = Redacting a classified document with a black marker. The redacted text
             is completely unreadable and irretrievable.

Pseudo-Anonymizer = A witness protection program: the witness gets a new name and
                    location, but still has a human identity that can (in theory) be
                    reverse-mapped by those with access to the protected manifest.

Secrets Pointer = A keycard system: the door uses your employee ID (pointer) to check
                  the access database — the door never stores the password itself.
```

---

# CHAPTER 8 — Data Storage Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** Raw data stored without thought becomes unqueryable as it scales. Storage patterns are preemptive optimizations — they shape how data is written so that reads are dramatically faster and cheaper. Unlike adding compute (reactive), storage patterns are structural decisions made at design time.

---

## 2. Master Pattern Breakdown

### Pattern: Horizontal Partitioner

**Problem:** Rolling 4-day aggregation job filters data by date. As the table grows, the filter scans the whole table. Execution time grows O(n) with data volume.

**Solution:** Physically divide the dataset into separate directories/table partitions based on a **distribution key** (typically time: `year/month/day/hour`). The query engine skips partitions that don't match the filter — called **partition pruning**.

```
visits/
└── year=2024/month=01/day=01/  ← only this partition is scanned for 2024-01-01 queries
└── year=2024/month=01/day=02/
```

**Trade-Offs:**
- **Low cardinality required:** User IDs as partition key = millions of tiny partitions (metadata explosion). Use time or region-level attributes.
- **Skew:** Hot partitions (e.g., Monday visits >> Sunday) block streaming microbatch processing. Mitigate with backpressure buffers.
- **Immutability challenge:** Changing partition key = full dataset reorganization. Apache Iceberg allows metadata-only partition evolution (but doesn't move existing files).

---

### Pattern: Vertical Partitioner (Storage Context)

**Problem:** Visit table repeats immutable attributes (browser version, OS) in every row, causing storage waste.

**Solution:** Split rows into mutable (visit-specific) and immutable (device context) components stored in separate tables. Join them on demand using `visit_id`.

**Trade-Offs:** Read queries require JOIN. Multiple write operations per input record. Exposes the domain split to consumers — require documentation.

---

### Pattern: Bucket

**Problem:** User ID is too high-cardinality for horizontal partitioning (millions of distinct values). But 80% of queries filter/join on user ID.

**Solution:** Hash-based grouping: `bucket = hash(user_id) % N_buckets`. Records with the same user ID always land in the same bucket. Enables:
- **Bucket pruning:** Skip entire buckets when querying specific user IDs
- **Shuffle-free JOINs:** If two tables use the same bucketing config, joins happen locally (no network shuffle)

**Trade-Offs:** Bucket count is static — changing it requires full rewrite. Choosing N is hard (too few = large buckets; too many = small buckets wasted). Popular in Hive, Spark, AWS Athena.

---

### Pattern: Sorter

**Problem:** Batch jobs filter/sort by `event_time`. Even within a partition, the query must scan all rows.

**Solution:** Sort data within files by query-relevant columns. Columnar formats (Parquet) store min/max statistics per row group in the footer. Query engines use these statistics to **skip irrelevant row groups** entirely.

**Z-order (multi-column curved sort):** For queries filtering on 2+ columns (e.g., `visit_id AND page`), Z-order colocates related records more efficiently than lexicographical sort, reducing the number of data blocks read.

**Trade-Offs:** New records are initially unsorted (need periodic sort/compaction). Composite sort keys only benefit queries that reference leading columns. Z-order available in Delta Lake (`OPTIMIZE...ZORDER BY`) and Apache Iceberg.

---

### Pattern: Metadata Enhancer

**Problem:** Data analysts query a large partitioned dataset. They filter on a specific column, but the query still reads all files in the partition.

**Solution:** Leverage **columnar format footer statistics** (Apache Parquet). Each file's footer stores `min`, `max`, `null_count` for each column. Query engines read the tiny footer first — if the queried value is outside the column's min/max range, the file is skipped entirely.

Table file formats (Delta Lake, Iceberg) extend this with **commit log statistics**: number of rows, min/max values per column across all files written in that commit.

**Trade-Offs:** Statistics are for the specific file — cross-file statistics require additional metadata layers. Out-of-date database statistics (for relational DBs) cause suboptimal query plans — refresh with `ANALYZE TABLE`.

---

### Pattern: Dataset Materializer

**Problem:** A view combining 3 weekly tables is slow (runs the underlying query every time). Data analysts are frustrated with latency.

**Solution:** Pre-compute the view and store results as a **materialized view** or **table**. Consumers read from the materialized result — no query execution on each read.

Options:
- **Materialized view** (BigQuery, Redshift, Databricks): Supports automatic refresh (but not guaranteed immediate). Best for read-heavy workloads.
- **Table** (more flexible): Full refresh or incremental (MERGE new rows into existing materialized table). Works with all storage optimizations (partitioning, bucketing, Z-order).

**Trade-Offs:** Storage cost doubles (original + materialized). Refresh cost is the new bottleneck — incremental refresh helps for append-only workloads. Complex access policies across combined tables.

---

### Pattern: Manifest

**Problem:** BigQuery external table on S3 performs a full listing of all files in the bucket on every query — slow and expensive for millions of files.

**Solution:** Create a **manifest file** that explicitly lists all data files for a given dataset version. Readers use the manifest instead of listing storage. Delta Lake, Iceberg, Hudi all maintain commit logs that serve as automatic manifests.

For manual use cases: generate manifest on write (`DeltaTable.generate('symlink_format_manifest')`) and reference it in the external table definition.

**Trade-Offs:** Manifest grows with data — can reach GB sizes for streaming jobs. Apache Spark Structured Streaming had a historical bug where growing manifests caused restart failures (SPARK-27188, now fixed).

---

### Pattern: Normalizer

**Problem:** The visits table stores device name, OS, and browser for every visit row — millions of repeated identical values. Updates require touching every row.

**Solution:** Apply normal forms (NF):
- **1NF:** Eliminate repeating groups → each device becomes one row in a `devices` table
- **2NF:** All non-key attributes depend on the full primary key
- **3NF:** No transitive dependencies → `studio_country` depends on `studio`, not `game` → create a `studios` table

**Dimensional model variant:** Fact table (observations like visits) + dimension tables (context like browsers, dates, pages) = **Snowflake schema** (dimensions can have sub-dimensions).

**Trade-Offs:** JOINs are expensive at scale. Mitigate with broadcasting small dimension tables or using Data Denormalizer for query-time optimization.

---

### Pattern: Denormalizer

**Problem:** 80% of queries JOIN 8 tables — massive network shuffle cost on every query.

**Solution:** Pre-flatten related tables into a single wide table (**One Big Table**). Queries run without JOINs.

**Star schema variant:** Flatten sub-dimensions into top-level dimensions (fewer JOINs than snowflake but still some). Combine Normalizer (for write efficiency) with Denormalizer (for read efficiency) in a two-layer architecture.

**Trade-Offs:** Updates require changing millions of rows (instead of one). Dictionary encoding (mapping long strings to integer codes) reduces storage footprint of repeated values. "One Big Table" becomes an antipattern if it combines unrelated domains.

---

## 3. Real-World Analogies

```
Horizontal Partitioner = Filing cabinets for each year. Looking for 2024 documents?
                         Open the 2024 cabinet only. Don't touch 2020.

Bucket = A library organized by Dewey Decimal System buckets. All science books (500s)
         share a section. Finding science = go to that section, not the whole library.

Sorter = An alphabetically sorted phone book. Finding "Konieczny" = flip directly to K,
         not search every page. Parquet statistics are like the alphabetical tabs.

Normalizer = A relational database textbook's example: normalize data to reduce
             redundancy, even if it means more JOINs.

Denormalizer = A preprinted form letter where all relevant info is on one page —
               faster to read, but updating "Company Name" requires reprinting everything.
```

---

# CHAPTER 9 — Data Quality Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** Even perfectly stored, idempotent, enriched data can be *wrong*. Unique visitor counts drop 50%. Orders double-count. Wrong schema silently corrupts ML models. Data quality patterns catch these issues before consumers do.

---

## 2. Master Pattern Breakdown

### Pattern: Audit-Write-Audit-Publish (AWAP)

**Problem:** A deployed job computes unique visitors incorrectly. The bug runs for a week before anyone notices. A marketing campaign launches based on incorrect data.

**Solution:** Add TWO audit steps around the transformation:
1. **Pre-transform audit:** Validate input (file size, schema check, format correctness) — fast, metadata-based
2. **Post-transform audit:** Validate output (NULL counts, value distributions, completeness) — data-based
3. Write only on both audits passing (or apply dispatch/annotate strategies for soft failures)

**Audit failure outcomes:**
- **Fail pipeline** (strictest)
- **Dispatch** (route bad rows to dead-letter, promote good rows)
- **Annotate** (promote all, mark dataset with quality issues metadata)

**Trade-Offs:** Pre-audit may read data twice if not careful. Audit rules from today may miss data patterns of tomorrow. In streaming: window-based or staging-based audit adds latency.

---

### Pattern: Constraints Enforcer

**Problem:** Random NULL values appear in required fields of a batch pipeline. The job is already complex — adding validation code would make it worse.

**Solution:** Define constraints directly in the database/storage format:
- **Type constraints** → schema definition
- **Nullability constraints** → `NOT NULL` declaration
- **Value constraints** → `CHECK (event_time < NOW())` (Delta Lake, PostgreSQL)
- **Integrity constraints** → Foreign keys (relational DBs only)
- **Protobuf + protovalidate** → field-level validation in serialization formats

**Trade-Offs:** All-or-nothing (one bad row = nothing committed). Databases report only the first violation — verbose discovery of multiple issues. Consumers may need *stricter* constraints than the database enforces (still need AWAP on top).

---

### Pattern: Schema Compatibility Enforcer

**Problem:** An upstream team removes a field your sessionization job depends on — believing it was obsolete. Job crashes in production.

**Solution:** Register schemas in a **Schema Registry** (Kafka's Confluent Schema Registry) with a compatibility mode:
- **Backward:** New consumers can read old data (safe: add optional fields, remove optional fields)
- **Forward:** Old consumers can read new data (safe: add fields, delete optional fields)
- **Full:** Both directions
- **Transitive variants:** Guarantee across ALL historical versions (not just consecutive)

Implicit enforcement via Delta Lake schema evolution controls (`delta.enableChangeDataFeed`).

**Trade-Offs:** Schema evolution becomes harder (renaming = add new field + deprecate old one during transition). Interaction overhead with registry on every message.

---

### Pattern: Schema Migrator

**Problem:** Over 3 years, 60 columns accumulated in a flat structure. Users want logically grouped attributes (e.g., all user fields under `user` struct). But breaking this change would crash consumers.

**Solution:** Phased migration:
1. Add the **new structure** alongside the old fields
2. Agree on a **transition deadline** with consumers
3. Both old and new attributes are produced simultaneously during transition
4. After deadline, drop old attributes in a new schema version

**For field removal:** Agree on deadline, confirm no consumers use the field, then remove.

**Trade-Offs:** Transitional period doubles data size (old + new fields). Protobuf compilation limits for large field counts. Schema Migrator requires non-transitive compatibility mode.

---

### Pattern: Offline Observer

**Problem:** New pipeline has been running cleanly for a month. No major issues yet — but you want to monitor data distributions proactively without slowing down the main pipeline.

**Solution:** Run a **separate, decoupled** observability job on a different schedule. It reads processed data, computes statistics (NULL ratios, value distributions, row counts, lag), and writes to a monitoring layer (Prometheus, a metrics table).

**Trade-Offs:** Time lag between data generation and observation (could miss issues before consumers do). Infrequent scheduling = larger data volumes per run. Sampling helps with compute, but misses rare observations.

---

### Pattern: Online Observer

**Problem:** Offline Observer runs weekly. Consumers discovered the zip code format regression before the weekly observability job ran.

**Solution:** Embed observability directly into the data generation pipeline. After the transformation step, run quality checks and emit metrics immediately. Timing options:
- **Local Sequencer:** Observation → load (sequential, sees final state but adds latency)
- **Parallel Split:** Observation and load simultaneously (faster, but observation sees pre-load state)

**Trade-Offs:** Observation errors can fail the main pipeline. Sampling is recommended for streaming. Parallel split observes pre-load state — may miss database type coercion issues.

---

## 3. Real-World Analogies

```
AWAP = Quality control on an assembly line: incoming raw materials are checked
       (pre-audit), then finished products are tested before shipping (post-audit).
       Defective items are quarantined, not shipped.

Constraints Enforcer = A form with required fields marked with *. The submit button
                       doesn't work until all required fields are filled. The database
                       IS the form.

Schema Compatibility Enforcer = A USB standard: backward compatible means your new
                                USB-C cable still works with older USB-A ports via
                                adapters (optional fields = adapters).

Offline Observer = A doctor's annual physical checkup. You're not sick today, but
                   periodic measurement (blood pressure, cholesterol) catches issues
                   before symptoms appear.

Online Observer = A smartwatch monitoring your vitals in real time. Alerts immediately
                  when something is off — doesn't wait for the annual checkup.
```

---

# CHAPTER 10 — Data Observability Design Patterns

## 1. Chapter Overview & Context

**Why it matters:** Data quality patterns validate data. But what if the validation job itself never runs because the upstream flow is interrupted? Observability patterns ensure your entire data stack is *visible and monitorable* — not just the data, but the pipelines, timing, and dependencies.

---

## 2. Master Pattern Breakdown

### Pattern: Flow Interruption Detector

**Problem:** A streaming sync job stopped writing data to object store — but it didn't crash, so no alert fired. A consumer found out via complaint, not monitoring.

**Solution:**
- **Streaming (continuous delivery):** Alert when no records arrive for > 1 minute
- **Streaming (irregular delivery):** Alert when no-data window > acceptable threshold (avoids false positives from expected quiet periods)
- **Batch/at-rest:**
  - Metadata layer: `last_modified_time` of table hasn't changed within expected schedule
  - Data layer: Row count unchanged across two consecutive evaluation windows
  - Storage layer: No new files written within threshold

**Trade-Offs:** Threshold tuning is difficult (marketing campaigns cause legitimate spikes). Compaction creates new files but doesn't indicate new data — avoid compaction as a proxy for "data freshness."

---

### Pattern: Skew Detector

**Problem:** Your batch job processes a half-empty dataset. The data provider had generation issues, but you had no mechanism to detect this before processing.

**Solution:**
1. Define a **comparison window** (current run vs. previous day)
2. Set a **tolerance threshold** (e.g., ±50%)
3. Calculate skew:
   - **Window-to-window:** `current_volume / previous_volume` — simple ratio check
   - **Standard deviation ratio:** `STDDEV(partition_sizes) / AVG(partition_sizes) * 100` — detects uneven distribution across partitions (Kafka, PostgreSQL partitioned tables)

**Trade-Offs:** Seasonality causes legitimate variability (holiday traffic spikes). Communication with marketing/business teams required to prevent false alerts. Fatality loop: if skew causes a pipeline failure, the next comparison uses the failed day as baseline — misidentifying the recovery as another skew.

---

### Pattern: Lag Detector

**Problem:** Streaming consumer started falling behind the producer after a 30% data volume increase. Downstream consumers complained about delayed data.

**Solution:** Measure lag = `last_available_unit - last_processed_unit`. Units vary by store:
- Kafka: record offset per partition
- Delta Lake: table version number
- Time-partitioned tables: partition timestamp

**Aggregation strategies:**
- `MAX(lag)` → worst-case scenario (detects the single slowest partition)
- `P90/P95(lag)` → typical performance (better for SLA monitoring than average)

**Trade-Offs:** Data skew (one partition getting 10x more data) causes lag that isn't the consumer's fault. Fix the producer's partitioning strategy, not the consumer's speed.

---

### Pattern: SLA Misses Detector

**Problem:** 40-minute SLA for a 6am batch job. Downstream consumers at 8am. You need alerts when the job exceeds 40 minutes, before consumers are impacted.

**Solution:**
- **Batch:** `end_time - start_time > SLA_threshold` → alert
- **Streaming (microbatch):** Same calculation per microbatch iteration
- **Streaming (record-level):** `write_time - read_time` per record → aggregate with MAX/P95

**Event time SLA vs. Processing time SLA:**
- Processing time SLA: How fast does the consumer process what it receives?
- Event time SLA: End-to-end time from data generation to processing completion (includes network/producer delays)

Both are complementary. A consumer can respect processing time SLA but miss event time SLA due to producer delays.

**Trade-Offs:** Apache Airflow's SLA calculation is measured from *pipeline start time*, not *task start time* — unusual semantics to be aware of.

---

### Pattern: Dataset Tracker

**Problem:** You consume a dataset with inconsistent field types. Your provider says it's fine — the issue is from their upstream provider. You need to trace the full dependency chain.

**Solution:** Build a dependency tree of datasets (tables, folders, topics) across all teams. Two implementation modes:
- **Fully managed:** Databricks Unity Catalog, GCP Dataplex — auto-detects lineage from job execution plans (limited to specific connectors)
- **Manual:** Instrument pipelines to emit input/output events to OpenLineage API. Use Marquez UI to visualize.

**Trade-Offs:** Fully managed = vendor lock-in. Manual = custom work for non-standard operators.

---

### Pattern: Fine-Grained Tracker

**Problem:** 30-column denormalized table. New team members ask which source columns compose each output column.

**Solution:** **Column-level lineage** — analyze query execution plan to map output columns to input columns. OpenLineage + Spark integration captures this automatically.

**Row-level lineage** — add a `job_version`, `job_name`, `batch_version` header/column to each written record. Nested `parent_lineage` attributes trace the full processing chain.

**Trade-Offs:** Custom mapping functions (Python UDFs) are opaque — column lineage tools can't trace inside them. Row-level lineage requires manual annotation and separate visualization (not compatible with standard lineage UIs).

---

## 3. Real-World Analogies

```
Flow Interruption Detector = A heartbeat monitor. No pulse for 10 seconds → alarm.
                             The alarm doesn't fire just because pulse is slow —
                             only when there's complete absence.

Skew Detector = A supermarket checking inventory levels. If tomatoes drop from
                500kg to 50kg overnight (90% less than usual), that's either a
                data error or an extreme event — both worth investigating.

Lag Detector = A manufacturing plant's conveyor belt speed meter. If the belt
               falls behind by >10 items, production slows and an alert fires.

Dataset Tracker = A family tree for datasets: shows who gave birth to whom.
                  When something is wrong with your data, trace back to the
                  grandparent dataset causing the issue.

Fine-Grained Tracker = Ingredient labels on food: not just "contains flour" but
                       "flour from Mill A, batch 20240101." Know the exact origin
                       of every component.
```

---

# COMPLETE PATTERN QUICK REFERENCE

| Chapter | Pattern | Core Use Case | Key Trade-Off |
|---|---|---|---|
| **2. Ingestion** | Full Loader | Load complete dataset each run (no change detection possible) | Data consistency during overwrite |
| **2. Ingestion** | Incremental Loader | Load only new rows (delta column or partition-based) | Hard deletes invisible; backfill = full load |
| **2. Ingestion** | Change Data Capture | <30s latency; capture hard deletes from commit log | Ops complexity; streaming semantics |
| **2. Ingestion** | Passthrough Replicator | Exact copy between environments (no transformation) | Metadata/ordering preservation |
| **2. Ingestion** | Transformation Replicator | Copy while removing/masking PII | PII schema drift risk |
| **2. Ingestion** | Compactor | Merge small files into large ones | VACUUM needed; timing trade-off |
| **2. Ingestion** | Readiness Marker | Signal when dataset is complete | No enforcement; late data breaks convention |
| **2. Ingestion** | External Trigger | Event-driven pipeline triggering | Lost events = silent pipeline gaps |
| **3. Errors** | Dead-Letter | Route bad records aside, keep pipeline running | Snowball backfill; hidden failures |
| **3. Errors** | Windowed Deduplicator | Remove duplicates in batch or streaming | Space vs. dedup window size |
| **3. Errors** | Late Data Detector | Detect and classify late records via watermark | MAX strategy + skew = dropped records |
| **3. Errors** | Static Late Data Integrator | Reprocess past N days automatically | Overlapping backfills |
| **3. Errors** | Dynamic Late Data Integrator | Reprocess only partitions with new late data | Concurrency race conditions; deep stateful backfill |
| **3. Errors** | Filter Interceptor | Count records filtered by each condition | SQL verbose; streaming requires state |
| **3. Errors** | Checkpointer | Streaming fault tolerance (resume from last offset) | At-least-once semantics |
| **4. Idempotency** | Fast Metadata Cleaner | Idempotent writes via TRUNCATE/DROP (fast) | Granularity = backfill boundary |
| **4. Idempotency** | Data Overwrite | Idempotent writes via data layer replacement | VACUUM needed; I/O overhead |
| **4. Idempotency** | Merger | UPSERT incremental changes into existing table | Backfill inconsistency; unique key required |
| **4. Idempotency** | Stateful Merger | Merger with rollback capability via state table | Requires versioned storage; compaction confuses version logic |
| **4. Idempotency** | Keyed Idempotency | Use immutable attributes as keys (e.g., append_time) | NoSQL-oriented; Kafka compaction risk |
| **4. Idempotency** | Transactional Writer | All-or-nothing writes (COMMIT/ROLLBACK) | Framework support gaps; latency |
| **4. Idempotency** | Proxy | Immutable writes with single access point (view) | Database view support varies |
| **5. Value** | Static Joiner | Enrich events with static reference data (JOIN) | Backfill needs time-aware join |
| **5. Value** | Dynamic Joiner | Join two streaming datasets with time boundaries | GC watermark = missed joins trade-off |
| **5. Value** | Wrapper | Separate raw and computed attributes in same record | Size doubles; domain split confusion |
| **5. Value** | Metadata Decorator | Attach technical metadata without exposing to users | Not all stores support headers |
| **5. Value** | Distributed Aggregator | Aggregate across distributed cluster (shuffle) | Network shuffle = latency; data skew |
| **5. Value** | Local Aggregator | Aggregate without shuffle (pre-partitioned input) | Static partitions; one grouping key per consumer |
| **5. Value** | Incremental Sessionizer | Build sessions from batch hourly partitions | Late data reopens "closed" sessions |
| **5. Value** | Stateful Sessionizer | Build sessions from streaming data (real-time) | State rebalancing on scale; checkpoint latency |
| **5. Value** | Bin Pack Orderer | Ordered delivery with partial commit semantics | Custom bin-packing complexity |
| **5. Value** | FIFO Orderer | Simple ordered delivery (1-at-a-time or ordered bulk) | High I/O overhead |
| **6. Flow** | Local Sequencer | Chain tasks sequentially within pipeline | Boundary definition complexity |
| **6. Flow** | Isolated Sequencer | Connect independent team pipelines | Loose (data) vs. tight (task) coupling |
| **6. Flow** | Aligned Fan-In | Merge parallel branches — all must succeed | Infrastructure spikes; scheduling skew |
| **6. Flow** | Unaligned Fan-In | Merge parallel branches — partial success OK | Readability; partial data communication |
| **6. Flow** | Parallel Split | One parent → two or more parallel children | Blocked execution; hardware differences |
| **6. Flow** | Exclusive Choice | One parent → only one child (condition-based) | Complexity factory; hidden logic |
| **6. Flow** | Single Runner | Max 1 concurrent pipeline instance | Slow backfilling; latency accumulation |
| **6. Flow** | Concurrent Runner | Multiple parallel pipeline instances | Resource starvation; shared state race conditions |
| **7. Security** | Vertical Partitioner | Split rows to minimize PII deletion footprint | JOIN overhead; polyglot persistence |
| **7. Security** | In-Place Overwriter | Delete PII from existing storage (legacy systems) | Massive I/O; VACUUM required |
| **7. Security** | Fine-Grained Accessor (Tables) | Column/row-level access control | Limited to session attributes for row-level |
| **7. Security** | Fine-Grained Accessor (Resources) | Cloud IAM least-privilege for cloud resources | Maintenance overhead; quota limits |
| **7. Security** | Encryptor | Protect data at rest and in transit | CPU overhead; key loss = data loss |
| **7. Security** | Anonymizer | Remove/replace PII before external sharing | Information loss reduces analytics value |
| **7. Security** | Pseudo-Anonymizer | Partially protect PII while preserving utility | Re-identification via dataset combination |
| **7. Security** | Secrets Pointer | Reference credentials by name, not value | Cache invalidation; log leakage risk |
| **7. Security** | Secretless Connector | No credentials — IAM or certificate-based auth | Cloud-specific; certificate rotation overhead |
| **8. Storage** | Horizontal Partitioner | Divide dataset by low-cardinality key (time, region) | High-cardinality = metadata explosion |
| **8. Storage** | Vertical Partitioner | Split rows into mutable/immutable tables | JOIN at query time; multiple write operations |
| **8. Storage** | Bucket | Group high-cardinality rows into hash buckets | Immutable count; choosing N is hard |
| **8. Storage** | Sorter | Sort data within files for skip-based reads | Unsorted new files; composite key order |
| **8. Storage** | Metadata Enhancer | Use Parquet/Delta statistics to skip files | Stats can go stale |
| **8. Storage** | Dataset Materializer | Pre-compute slow queries as materialized tables | Refresh cost; storage doubles |
| **8. Storage** | Manifest | Replace repeated listing with explicit file list | Manifest size growth |
| **8. Storage** | Normalizer | Eliminate data redundancy via NF or snowflake schema | JOIN-heavy queries |
| **8. Storage** | Denormalizer | Flatten tables into One Big Table or star schema | Costly updates; "trash bag" antipattern risk |
| **9. Quality** | AWAP | Pre- and post-transform data validation | Rules coverage drift; streaming latency |
| **9. Quality** | Constraints Enforcer | Database-level validation (type, nullability, value) | All-or-nothing; consumer-specific rules still needed |
| **9. Quality** | Schema Compatibility Enforcer | Prevent breaking schema changes | Evolution harder; registry overhead |
| **9. Quality** | Schema Migrator | Safe phased schema evolution | Transition period doubles data size |
| **9. Quality** | Offline Observer | Decoupled background data monitoring | Delayed detection; compute batching |
| **9. Quality** | Online Observer | Embedded real-time data monitoring | Pipeline failure risk; streaming sampling |
| **10. Observability** | Flow Interruption Detector | Alert on missing/stopped data flow | Threshold tuning; compaction false positives |
| **10. Observability** | Skew Detector | Alert on abnormal data volume changes | Seasonality; fatality loop |
| **10. Observability** | Lag Detector | Measure consumer lag behind producer | Data skew (producer issue, not consumer) |
| **10. Observability** | SLA Misses Detector | Alert when processing exceeds time budget | Event time vs. processing time SLA distinction |
| **10. Observability** | Dataset Tracker | Build dependency graph across datasets/teams | Vendor lock or custom code for non-standard jobs |
| **10. Observability** | Fine-Grained Tracker | Column-level and row-level lineage | Custom UDFs are opaque; row-level needs separate viz |

---

*Guide generated from: "Data Engineering Design Patterns" by Bartosz Konieczny (O'Reilly)*
*All 40+ patterns covered across 10 chapters | Tools referenced: Apache Spark, Flink, Airflow, Delta Lake, Kafka, Iceberg, BigQuery, PostgreSQL, AWS, GCP, Azure*

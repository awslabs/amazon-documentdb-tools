# Amazon DocumentDB MVU CDC Migrator Tool

The purpose of MVU CDC migrator tool is to migrate the cluster wide changes from source Amazon DocumentDB cluster to target Amazon DocumentDB cluster.

It enables a near-zero downtime [major version upgrade (MVU)](https://docs.aws.amazon.com/documentdb/latest/devguide/docdb-mvu.html) from Amazon DocumentDB 3.6 to Amazon DocumentDB 5.0.

This tool is only recommended for performing MVU from Amazon DocumentDB 3.6. If you are performing MVU from Amazon DocumentDB 4.0 to 5.0, we recommend using the AWS Database Migration Service CDC approach.

---

## Prerequisites

- Python 3.7+
- pymongo 3.7+

```terminal
  pip3 install "pymongo>=3.7"
```

---

## How to use

1. Clone the repository and go to the tool folder:

```terminal
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/mvu-tool/
```

2. Capture a change stream resume token from the source cluster. This marks the point in the change stream where replication will begin. The tool will wait until a write (insert, update, or delete) occurs on the source before it can capture a token.

```terminal
python3 mvu-cdc-migrator.py \
  --source-uri <source-cluster-uri> \
  --start-position 0 \
  --get-resume-token
```

3. Create a snapshot of the source cluster and restore it to the target cluster. The resume token captured in step 2 ensures CDC will replay all changes that occurred after that point.

4. Start the CDC replication process using the resume token from step 2:

```terminal
python3 mvu-cdc-migrator.py \
  --source-uri <source-cluster-uri> \
  --target-uri <target-cluster-uri> \
  --start-position <resume-token-from-step-2> \
  --verbose
```

5. Monitor the "secs behind" metric in the output. Once it reaches 0 and stays there, the target is caught up and you can perform your cutover.

---

## Configuration

The tool accepts the following arguments:

| Argument | Required | Default | Description |
| ---------- | ---------- | --------- | ------------- |
| `--source-uri` | Yes | — | Source cluster connection URI |
| `--start-position` | Yes | — | Change stream resume token, or `0` to get the current token |
| `--target-uri` | No | — | Target cluster connection URI (required unless using `--get-resume-token`) |
| `--source-database` | No | — | Source database name. If omitted, replicates all databases |
| `--threads` | No | `1` | Total number of worker threads (hash-partitioned + dedicated). Must be greater than the number of `--single-thread-collections` |
| `--single-thread-collections` | No | — | Comma-separated `db.collection` namespaces requiring single-threaded processing (see [Threading Model](#threading-model)) |
| `--max-operations-per-batch` | No | `100` | Maximum operations per batch |
| `--max-seconds-between-batches` | No | `5` | Maximum seconds to wait before processing an incomplete batch |
| `--duration-seconds` | No | `0` | Seconds to run before exiting (`0` = run forever) |
| `--feedback-seconds` | No | `15` | Seconds between progress output |
| `--dry-run` | No | `false` | Read source changes only, do not apply to target |
| `--verbose` | No | `false` | Enable verbose logging |
| `--get-resume-token` | No | `false` | Wait for a write on the source, capture the change stream resume token, and exit |
| `--skip-python-version-check` | No | `false` | Permit execution on Python 3.6 and prior |

---

## Threading Model

Each worker thread opens its own change stream cursor against the source cluster and receives all events. Events are filtered locally and each thread only processes events assigned to it. This means the source cluster serves the same stream N times (once per thread), increasing read load on the source proportionally to the thread count. The parallelism benefits the write side (batching and committing to the target), not the read side.

When selecting `--threads`, consider both the available vCPUs on the machine running the tool and the read capacity of the source cluster.

The tool uses two types of worker threads:

**Hash-partitioned pool**

- Events are distributed across the hash pool based on `sha512(_id) % pool_size`. This guarantees that all events for the same document land on the same thread in order. Safe for collections where `_id` is the only unique index. The pool size equals `--threads` minus the number of `--single-thread-collections`.

**Dedicated threads** (`--single-thread-collections`)

- Each listed collection gets its own dedicated thread, separate from the hash pool. All events for that collection are processed sequentially in change stream order.

### When to use `--single-thread-collections`

Collections that have a unique index on fields OTHER than `_id` must be listed in `--single-thread-collections` when running with multiple threads.

The hash-partitioned pool assigns each change stream event to a thread based on `sha512(_id) % N`. This means all events for the same `_id` always land on the same thread.

**Safe case — updates to the same document (`_id` stays the same)**

An application updates `{_id: A, token: "old", user: "u1"}` to `{_id: A, token: "new", user: "u1"}`. Both the before and after have `_id: A`, so both events hash to the same thread and are applied in order.

**Unsafe case — compound key rotates across different `_id` values**

Some workloads rotate values by deleting a document and inserting a new one with a different `_id`, but contain the same compound unique key values. For example, assume a collection with a unique index on `(token, user)` contains document `{_id: A, token: "x", user: "u1"}`. The following operations are performed, in order:

- Source event 1: Delete `{_id: A, token: "x", user: "u1"}`
- Source event 2: Insert `{_id: B, token: "x", user: "u1"}`

Since these are different `_id` values, they may hash to different threads. Each thread processes its own queue independently and there is no coordination between them.

The replay needs to apply the delete of `_id: A` followed by the insert of `_id: B`. But because they're on separate threads, the order is not guaranteed:

| Step | Thread assigned to `_id: A` | Thread assigned to `_id: B` | Collection state |
| --- | --- | --- | --- |
| — | — | — | `_id: A` exists holding `(x, u1)` |
| 1 | Queued | Attempts Insert `_id: B` - `E11000` because unique key `(x, u1)` is still held by `_id: A` | `_id: A` exists |
| 2 | Queued | Tool skips the failed insert (see [Duplicate key handling](#duplicate-key-handling)) | `_id: A` exists |
| 3 | Commits Delete `_id: A` - success | — | No documents (data loss) |

The tool skipped the insert at step 2 because it handles all `E11000` errors as non-fatal (see [Duplicate key handling](#duplicate-key-handling) below). It cannot distinguish this cross-thread race from a legitimately safe-to-skip duplicate. The delete then removes the only remaining document.

Routing these types of collections to a dedicated thread via `--single-thread-collections` guarantees that both events regardless of `_id` are processed sequentially on the same thread in change stream order. The delete executes first, freeing the compound key, followed by the insert which succeeds.

### Thread layout example

With `--threads 8 --single-thread-collections "mydb.device_tokens,mydb.sessions"`:

| Threads | Role | Events handled |
|---------|------|----------------|
| 0-5 | Hash-partitioned pool | All collections NOT listed in single-thread-collections |
| 6 | Dedicated | `mydb.device_tokens` only |
| 7 | Dedicated | `mydb.sessions` only |

Total worker processes: 8 (6 hash-partitioned + 2 dedicated).

### Duplicate key handling

There may be a delay between when you capture the change stream token and when you start applying changes, and during that time your application may continue to write data (insert, update, delete). During the replay of change stream events, the tool may encounter duplicate key errors (`E11000`) for documents that already exist in the restored target. The tool handles this by:

1. Attempting the batch with `InsertOne`
2. On failure, retrying with `ReplaceOne` based on `_id` (handles any `_id` conflicts)
3. Skipping any `E11000` error operation and continuing (handles compound key conflicts from the overlap window)

This skip is safe because within a single-threaded stream, the conflicting document is transient. The delete that frees the compound key always follows later in the same ordered stream.

---

## Example usage

Capture the cluster wide change stream token:

```terminal
python3 mvu-cdc-migrator.py \
  --source-uri <source-cluster-uri> \
  --start-position 0 \
  --verbose \
  --get-resume-token
```

Capture a database-level change stream token (required for database-level replication):

```terminal
python3 mvu-cdc-migrator.py \
  --source-uri <source-cluster-uri> \
  --source-database <database-name> \
  --start-position 0 \
  --verbose \
  --get-resume-token
```

Migrate CDC changes (single-threaded):

```terminal
python3 mvu-cdc-migrator.py \
  --source-uri <source-cluster-uri> \
  --target-uri <target-cluster-uri> \
  --start-position <change-stream-token> \
  --verbose
```

Migrate CDC changes (multi-threaded with dedicated threads for compound-key collections):

```terminal
python3 mvu-cdc-migrator.py \
  --source-uri <source-cluster-uri> \
  --target-uri <target-cluster-uri> \
  --start-position <change-stream-token> \
  --threads 8 \
  --single-thread-collections "mydb.device_tokens,mydb.user_sessions" \
  --verbose
```

---

## Important notes

- When using `--get-resume-token`, you must specify `--source-database` if you intend to run database-level replication. A cluster-level token cannot be used for database-level replication.
- The `--start-position` token must match the scope of your replication (cluster-level token for cluster replication, database-level token for database replication).
- Collections with unique indexes on non-`_id` fields **must** use `--single-thread-collections` when running multi-threaded. Failure to do so may result in data loss due to cross-thread ordering violations.

### Stopping and restarting

The tool does not perform a graceful shutdown on Ctrl+C (SIGINT). When interrupted, any in-progress batch that has not yet been committed to the target is discarded.

To restart, use the last `resume token` value printed from the output. This token only updates after a batch is successfully committed to the target, so it is always safe to resume from. Events from the discarded in-progress batch will be re-read and re-applied on restart, and duplicate key handling ensures this is idempotent with no data loss.

# Progress Reporting

SyncDB prints a progress bar as data moves between source and target.

## Modes

| Mode | Behavior | Best for |
|------|----------|----------|
| `multi_line` | New line per batch (default) | CI logs, log files |
| `one_line` | Overwrites same line per table, then commits it | Interactive terminals |
| `none` | Silent — no output | Scheduled jobs, custom logging |

```python
from syncdb import ProgressMode, SyncDB

# Interactive terminal
sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.one_line)

# CI pipeline
sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.multi_line)

# No progress output
sync = SyncDB(source=src, target=dst, progress_mode=ProgressMode.none)

# String values are also accepted
sync = SyncDB(source=src, target=dst, progress_mode="one_line")
```

## Progress bar format

When a total row count is available, the bar shows fill percentage, row count, and elapsed time:

```text
public.orders     [=============>.......................]   40%    4,000 / 10,000  1.2s
public.customers  [====================]  100%             8,200 /  8,200  0.9s
```

When the count query fails (e.g., insufficient permissions), it falls back to a running total:

```text
public.orders     [   4,000 rows  1.2s ]
```

## Summary table

After all tables are synced, SyncDB prints a summary table. Control this with the `verbose` parameter:

```python
# Print a summary (default)
sync = SyncDB(source=src, target=dst, verbose="standard")

# Print a detailed summary with all TableSyncResult fields
sync = SyncDB(source=src, target=dst, verbose="detailed")

# Suppress the summary entirely
sync = SyncDB(source=src, target=dst, verbose=None)
```

**Standard summary:**

```text
SyncDB summary (standard)
+------------------+--------+--------------+---------+---------+------+
| table            | mode   | rows written | batches | created | time |
+------------------+--------+--------------+---------+---------+------+
| public.orders    | append | 52,341       | 11      | no      | 3.2s |
| public.customers | append |  8,200       |  2      | yes     | 0.9s |
+------------------+--------+--------------+---------+---------+------+
total: 60,541 rows in 13 batches across 2 tables in 4.1s
```

## Redirecting output

Point output to a file or any `TextIO` stream:

```python
import sys

sync = SyncDB(
    source=src,
    target=dst,
    verbose_stream=sys.stderr,   # write summary to stderr
)
```

## Custom logging integration

Suppress all built-in output and use the returned results with your own logger:

```python
import logging
from syncdb import ProgressMode, SyncDB

log = logging.getLogger("etl")

sync = SyncDB(
    source=src,
    target=dst,
    verbose=None,
    progress_mode=ProgressMode.none,
)

results = sync.sync_tables({...})

for r in results:
    log.info(
        "sync complete table=%s rows=%d batches=%d duration=%.1fs",
        r.destination, r.rows_written, r.batches, r.duration_seconds,
    )
```

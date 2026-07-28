# DECISIONS.md

## 1. Which exact line(s) prevent two workers from claiming the same job, and why is that operation atomic across separate OS processes?

`queuectl/db.py`, `claim_next_job()`, lines 152-171. The core is a single
`BEGIN IMMEDIATE` transaction wrapping one `UPDATE`:

```sql
BEGIN IMMEDIATE;
UPDATE jobs
SET state = 'processing', updated_at = ?, worker_pid = ?
WHERE id = (
    SELECT id FROM jobs
    WHERE state = 'pending' OR (state = 'failed' AND next_run_at <= ?)
    ORDER BY created_at ASC
    LIMIT 1
)
AND state IN ('pending', 'failed')
RETURNING *;
COMMIT;
```
The job is claimed using a single UPDATE statement with a nested subquery that selects the next available row. Since the selection and update happen together in one statement, there is no gap where another worker can claim the same job.

The transaction starts with BEGIN IMMEDIATE, which acquires SQLite's write lock before the update begins. This prevents any other connection from starting a conflicting write transaction until the current one commits or rolls back.

This guarantee works across multiple processes, not just threads, because SQLite uses operating system file locks (fcntl/flock) instead of in-process locks. If two queuectl worker processes try to claim a job at the same time, the OS grants the lock to one of them first. After that worker commits, the second worker re-evaluates the subquery against the updated database, so it either claims a different pending job or finds none. The same job can never be claimed twice.

I also configured busy_timeout=30000 in get_conn(), so a worker waits for the database lock instead of immediately failing with a "database is locked" error.

I avoided using a separate SELECT followed by an UPDATE, as that creates a classic check-then-act race where two workers could select the same pending job before either updates it. Using a single atomic UPDATE with the WHERE state IN (...) check removes that race condition.


## 2. A worker is `SIGKILL`ed halfway through a job. Walk through, step by step, what state the job is in and how it eventually runs again. What is the worst-case delay before recovery?

A worker calls claim_next_job(), which marks the job as processing, records the current timestamp in updated_at, and stores its worker_pid.
The worker then executes the job using subprocess.run(command, shell=True) and waits for it to finish.
If the worker is terminated with SIGKILL, the process stops immediately. Since SIGKILL cannot be caught, no cleanup code runs and the database is not updated. The job remains stuck in the processing state with the old worker_pid and updated_at values.
Every worker calls reap_stale_jobs() at the start of each loop before claiming new work. If a job has been in processing longer than DEFAULT_LEASE_SECONDS (15s), it is reset to pending, its worker_pid is cleared, and it becomes available again. I intentionally do not increment attempts or apply backoff here because a worker crash is an infrastructure failure, not a job failure.
Once reset to pending, the job is claimed normally by claim_next_job() and starts again from scratch.

Worst-case recovery time: The lease is 15 seconds, and every running worker checks for stale jobs before attempting to claim new ones. After a worker is restarted, recovery happens on its first loop iteration, so the maximum recovery time is roughly the 15-second lease plus a small startup delay—well within the required 60 seconds.

Trade-off: A fixed 15-second lease can incorrectly recover a job that is still running if it legitimately takes longer than 15 seconds, causing it to execute twice. This implementation assumes jobs are either fast or idempotent. A production system would periodically refresh updated_at (heartbeats) while a worker is alive so that only truly abandoned jobs are recovered. I left that out to keep the implementation simple while documenting it as a known limitation.


## 3. Does `dlq retry` reset `attempts`? Why is that the right call?

Yes. In db.py, dlq_retry() resets attempts to 0 and clears next_run_at.

I chose this because dlq retry is a manual operator action that gives a failed job a fresh start, not just one extra attempt. If attempts weren't reset, a job that had already exhausted its max_retries would return to the DLQ after its very next failure, making the retry command far less useful.

Resetting the counter lets the job go through the normal retry and backoff cycle again, which is the expected behavior after the underlying issue has been fixed.

The trade-off is that a job can be retried from the DLQ indefinitely if the root cause is never resolved. I considered this acceptable because dlq retry is an explicit human action rather than an automatic one, so an operator is expected to notice and stop repeatedly retrying a job that continues to fail.


## 4. What designs did you consider and reject for `worker stop` (cross-process signaling), and why?

Chosen approach: PID files. Each worker creates a .queuectl/workers/<pid>.json file when it starts and removes it on a clean shutdown. When worker stop is run from any terminal, it scans this directory, checks whether each PID is still alive using os.kill(pid, 0), removes any stale PID files left behind after a crash, and sends SIGTERM to every active worker with os.kill.

Rejected: Relying only on process-group signal propagation. Pressing Ctrl+C in the same terminal as worker start automatically sends signals to the worker processes, but that only works from that terminal. Since the assignment requires stopping workers from a different terminal, I needed a way to discover their PIDs, so PID files became the primary solution.

Rejected: A Unix domain control socket. While it provides a cleaner communication channel, it requires a dedicated listener process, socket lifecycle management, and handling stale socket files after crashes. That adds complexity without much benefit for a take-home project.

I also considered storing worker information in a SQLite workers table with heartbeats. That would integrate well with the existing database and could later support more reliable worker liveness checks and lease recovery. I chose PID files instead because they satisfy the assignment requirements with much less code.

Rejected: Process-group IDs. Tracking process groups instead of individual PIDs wouldn't simplify the implementation, since commands like queuectl status still need to report individual workers. Using PID files for both discovery and control keeps the design straightforward.


## 5. If priorities were added tomorrow (high-priority jobs jump the queue), which parts of your design survive unchanged and which break?

**Survives unchanged:**
- The atomicity mechanism (Q1) — claiming is still "one UPDATE picks a row
  via a subquery, in one transaction." Priority just changes the subquery's
  `ORDER BY`.
- Crash recovery (`reap_stale_jobs`) — completely orthogonal to ordering;
  it only cares about `state` and `updated_at`.
- The DLQ, retry/backoff math, and `dlq retry` semantics — none of that
  depends on queue ordering.
- The worker-discovery/`worker stop` mechanism (PID files) — unrelated to
  job ordering.
- The CLI/interface contract (`--json`, signal semantics) — unaffected.

**Breaks or needs extension:**
- The claim query's `ORDER BY created_at ASC` (`db.py` line 162) is the one
  line that directly encodes "FIFO." Adding priority means adding a
  `priority INTEGER` column to the schema and changing that single
  `ORDER BY` to `ORDER BY priority DESC, created_at ASC`. This is a small,
  contained change precisely because ordering and atomicity are separate
  concerns in this design — the subquery decides *which* row, the
  surrounding UPDATE decides *how safely* it's claimed.
- `enqueue_job()` and the CLI's `enqueue` command would need a new,
  optional `priority` field in the job JSON (defaulting to, say, 0), plus
  schema migration handling for existing DB files created before the
  column existed (currently `SCHEMA` uses `CREATE TABLE IF NOT EXISTS`,
  which won't add columns to an already-created table — a real migration
  step, e.g. `ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0`
  guarded by a check, would be needed).
- Backoff-driven re-queueing (`next_run_at`) interacts with priority in a
  way that needs a design call: does a low-priority job whose backoff timer
  just expired jump ahead of a high-priority job that's still waiting on
  its own backoff? The current `ORDER BY` would need a documented tie-break
  rule here, which doesn't exist today because there's only one ordering
  dimension.
- `queuectl status`/`queuectl list` output would likely want to show
  priority, and `config` might grow a `default-priority` knob — purely
  additive, not breaking.

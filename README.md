# Queueincli


A minimal, crash-safe, multi-process background job queue, driven entirely
by a CLI. Built for the QueueCTL backend take-home assignment.

- **Storage:** a single SQLite file (`.queuectl/queue.db`), giving crash-safe
  persistence and real cross-process write locking for free.
- **Workers:** real OS processes (not threads), can be started from separate
  terminals, and safely race each other to claim jobs.
- **Retries:** exponential backoff (`base ** attempts`), configurable.
- **Crash recovery:** jobs stuck in `processing` because their worker died
  are automatically reclaimed within ~15s (see `DECISIONS.md` Q2).

See `DECISIONS.md` for the specific design questions the assignment asks to
be answered (atomicity, crash recovery, DLQ semantics, cross-process
signaling, and how priorities would change the design).

## Setup

Requires Python 3.8+ (uses `sqlite3`'s `RETURNING` clause, needs SQLite
3.35+; standard on any Python 3.10+ install, and on most 3.8/3.9 builds
too — check with `python3 -c "import sqlite3; print(sqlite3.sqlite_version)"`).

```bash
git clone <this-repo>
cd queuectl
pip install -e .
```

This installs a `queuectl` console script. Alternatively, run it without
installing via `python3 -m queuectl <command>` from the repo root.

`queuectl` operates on a `.queuectl/` directory created in your **current
working directory** — like `git`, each directory gets its own independent
queue. Run all commands for a given queue from the same directory (or set
`QUEUECTL_HOME` to an explicit path to override this).

## Usage

```bash
# Add jobs
queuectl enqueue --id job3 --command "exit 1" --max-retries 2
queuectl enqueue --id job3 --command "sleep 2 && echo done" --max-retries 2
queuectl enqueue --id job3 --command "exit 1" --max-retries 2

# Start 3 worker processes in the foreground (blocks; Ctrl+C to stop gracefully)
queuectl worker start --count 3

# ...from another terminal, while workers are running:
queuectl worker stop        # graceful: lets in-flight jobs finish
queuectl status             # counts per state + active worker PIDs
queuectl list --state pending
queuectl list --state failed --json     # machine-readable, stdout is JSON only

queuectl dlq list
queuectl dlq retry job3     # re-enqueues, resets attempts to 0

queuectl config show
queuectl config set max-retries 5
queuectl config set backoff-base 3
```

### Job JSON fields

| field         | required | default            | notes                                  |
|---------------|----------|---------------------|-----------------------------------------|
| `id`          | yes      | —                   | must be unique                          |
| `command`     | yes      | —                   | run via the shell (`subprocess`, `shell=True`) |
| `max_retries` | no       | current config value | frozen onto the job at creation time  |

## Architecture

```
queuectl/
  db.py      -- SQLite schema, config, atomic job-claim, retry/DLQ transitions,
                crash-recovery sweep
  worker.py  -- per-process worker loop: claim -> run -> mark done/failed,
                signal handling for graceful shutdown, PID-file lifecycle
  cli.py     -- argparse command surface, worker process spawning/supervision
```

**Job lifecycle:** `pending` → `processing` → `completed` | `failed` (retry
scheduled) → back to `pending` (after backoff) → ... → `dead` (DLQ).

**Concurrency:** each `--count N` worker is a real forked OS process
(`multiprocessing.Process`), each independently polling and claiming jobs
via one atomic SQLite `UPDATE ... WHERE id = (SELECT ... LIMIT 1) RETURNING
*` inside a `BEGIN IMMEDIATE` transaction — see `DECISIONS.md` Q1 for why
this is safe across processes, not just threads.

**Crash recovery:** every worker sweeps for jobs stuck in `processing` past
a lease window (15s) before each attempt to claim new work, resetting them
to `pending` without penalizing `attempts`. See `DECISIONS.md` Q2 for the
worst-case timing.

**Worker discovery / `worker stop`:** each worker OS process writes a PID
file to `.queuectl/workers/<pid>.json` on start and removes it on clean
exit. `worker stop`, run as a separate command (from any terminal),
discovers live workers by scanning that directory and verifying liveness
with `os.kill(pid, 0)`, then sends each a real `SIGTERM`. See `DECISIONS.md`
Q4 for alternatives considered.

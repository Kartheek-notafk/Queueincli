
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

DATA_DIR = Path(os.environ.get("QUEUECTL_HOME", os.path.join(os.getcwd(), ".queuectl")))
DB_PATH = DATA_DIR / "queue.db"
WORKERS_DIR = DATA_DIR / "workers"

DEFAULT_LEASE_SECONDS = 15
SWEEP_INTERVAL_SECONDS = 3


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    WORKERS_DIR.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_conn():

    ensure_dirs()
    conn = sqlite3.connect(str(DB_PATH), timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    command      TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL DEFAULT 3,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    next_run_at  TEXT,               -- earliest time a 'failed' job may be retried
    worker_pid   INTEGER,            -- pid currently holding the lease, if any
    last_error   TEXT                -- last non-zero exit code / crash note
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

DEFAULT_CONFIG = {
    "max_retries": "3",
    "backoff_base": "2",
}


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        for k, v in DEFAULT_CONFIG.items():
            conn.execute(
                "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (k, v)
            )


def get_config(conn, key: str) -> str:
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    if row is None:
        return DEFAULT_CONFIG[key]
    return row["value"]


def set_config(key: str, value: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def enqueue_job(job: dict):
    with get_conn() as conn:
        max_retries = job.get("max_retries")
        if max_retries is None:
            max_retries = int(get_config(conn, "max_retries"))
        ts = now_iso()
        conn.execute(
            """
            INSERT INTO jobs (id, command, state, attempts, max_retries,
                               created_at, updated_at, next_run_at, worker_pid, last_error)
            VALUES (?, ?, 'pending', 0, ?, ?, ?, NULL, NULL, NULL)
            """,
            (job["id"], job["command"], max_retries, ts, ts),
        )


# ---------------------------------------------------------------------------
# THE ATOMICITY-CRITICAL PATH
# ---------------------------------------------------------------------------
def claim_next_job(worker_pid: int):

    ts = now_iso()
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """
                UPDATE jobs
                SET state = 'processing', updated_at = ?, worker_pid = ?
                WHERE id = (
                    SELECT id FROM jobs
                    WHERE state = 'pending'
                       OR (state = 'failed' AND next_run_at <= ?)
                    ORDER BY created_at ASC
                    LIMIT 1
                )
                AND state IN ('pending', 'failed')
                RETURNING *
                """,
                (ts, worker_pid, ts),
            )
            row = cur.fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return dict(row) if row else None


def mark_completed(job_id: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE jobs SET state='completed', updated_at=?, worker_pid=NULL, last_error=NULL WHERE id=?",
            (now_iso(), job_id),
        )


def mark_failed(job_id: str, attempts: int, max_retries: int, backoff_base: int, exit_code: int):

    ts = now_iso()
    with get_conn() as conn:
        if attempts >= max_retries:
            conn.execute(
                "UPDATE jobs SET state='dead', attempts=?, updated_at=?, worker_pid=NULL, last_error=? WHERE id=?",
                (attempts, ts, f"exit code {exit_code}", job_id),
            )
        else:
            delay = backoff_base ** attempts
            next_run = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + delay)
            )
            conn.execute(
                "UPDATE jobs SET state='failed', attempts=?, updated_at=?, next_run_at=?, worker_pid=NULL, last_error=? WHERE id=?",
                (attempts, ts, next_run, f"exit code {exit_code}", job_id),
            )


def reap_stale_jobs(lease_seconds: int = DEFAULT_LEASE_SECONDS) -> list:

    cutoff = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - lease_seconds)
    )
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                """
                UPDATE jobs
                SET state='pending', worker_pid=NULL, updated_at=?, last_error='recovered after worker crash'
                WHERE state='processing' AND updated_at < ?
                RETURNING id
                """,
                (now_iso(), cutoff),
            )
            recovered = [r["id"] for r in cur.fetchall()]
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return recovered


def list_jobs(state: str = None) -> list:
    with get_conn() as conn:
        if state:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE state=? ORDER BY created_at", (state,)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]


def status_counts() -> dict:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT state, COUNT(*) as c FROM jobs GROUP BY state"
        ).fetchall()
        return {r["state"]: r["c"] for r in rows}


def dlq_retry(job_id: str) -> bool:

    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE jobs SET state='pending', attempts=0, next_run_at=NULL, "
            "updated_at=?, worker_pid=NULL, last_error=NULL WHERE id=? AND state='dead'",
            (now_iso(), job_id),
        )
        return cur.rowcount > 0

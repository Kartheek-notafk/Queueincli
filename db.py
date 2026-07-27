"""
SQLite persistence layer for queuectl.
"""
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

DATA_DIR = Path(
    os.environ.get(
        "QUEUECTL_HOME",
        os.path.join(os.getcwd(), ".queuectl"),
    )
)

DB_PATH = DATA_DIR / "queue.db"


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_conn():
    ensure_dirs()

    conn = sqlite3.connect(
        str(DB_PATH),
        timeout=30,
        isolation_level=None,
    )

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
    next_run_at  TEXT,
    worker_pid   INTEGER,
    last_error   TEXT
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

        for key, value in DEFAULT_CONFIG.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO config(key, value)
                VALUES (?, ?)
                """,
                (key, value),
            )


def get_config(conn, key: str) -> str:
    row = conn.execute(
        "SELECT value FROM config WHERE key=?",
        (key,),
    ).fetchone()

    if row is None:
        return DEFAULT_CONFIG[key]

    return row["value"]


def set_config(key: str, value: str):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO config(key, value)
            VALUES (?, ?)
            ON CONFLICT(key)
            DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
def enqueue_job(job: dict):
    """
    Insert a new job into the queue.
    """
    with get_conn() as conn:
        max_retries = job.get("max_retries")

        if max_retries is None:
            max_retries = int(get_config(conn, "max_retries"))

        ts = now_iso()

        conn.execute(
            """
            INSERT INTO jobs (
                id,
                command,
                state,
                attempts,
                max_retries,
                created_at,
                updated_at,
                next_run_at,
                worker_pid,
                last_error
            )
            VALUES (
                ?, ?, 'pending',
                0,
                ?,
                ?,
                ?,
                NULL,
                NULL,
                NULL
            )
            """,
            (
                job["id"],
                job["command"],
                max_retries,
                ts,
                ts,
            ),
        )


def list_jobs(state: str = None) -> list:
    """
    Return all jobs or jobs of a particular state.
    """
    with get_conn() as conn:

        if state:
            rows = conn.execute(
                """
                SELECT *
                FROM jobs
                WHERE state = ?
                ORDER BY created_at
                """,
                (state,),
            ).fetchall()

        else:
            rows = conn.execute(
                """
                SELECT *
                FROM jobs
                ORDER BY created_at
                """
            ).fetchall()

        return [dict(row) for row in rows]


def status_counts() -> dict:
    """
    Return number of jobs in each state.
    """
    with get_conn() as conn:

        rows = conn.execute(
            """
            SELECT
                state,
                COUNT(*) AS c
            FROM jobs
            GROUP BY state
            """
        ).fetchall()

        return {
            row["state"]: row["c"]
            for row in rows
        }

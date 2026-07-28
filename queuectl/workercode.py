import json
import os
import signal
import subprocess
import sys
import time

from queuectl import db as db

POLL_INTERVAL = 1.0  # seconds between "queue is empty" checks
IDLE_SLEEP_STEP = 0.2  # sleep in short steps so shutdown signals are responsive


class _ShutdownFlag:
    def __init__(self):
        self.requested = False


def _pid_file(pid: int):
    return db.WORKERS_DIR / f"{pid}.json"


def write_pid_file(pid: int):
    db.ensure_dirs()
    data = {"pid": pid, "started_at": db.now_iso()}
    _pid_file(pid).write_text(json.dumps(data))


def remove_pid_file(pid: int):
    try:
        _pid_file(pid).unlink()
    except FileNotFoundError:
        pass


def list_worker_pids() -> list:
    """
    Discover currently-live workers by scanning PID files and checking
    each PID is actually alive (os.kill(pid, 0)). Stale files (worker was
    SIGKILLed and never got to clean up its own file) are pruned here so
    `status`/`worker stop` don't report ghosts.
    """
    db.ensure_dirs()
    live = []
    for f in db.WORKERS_DIR.glob("*.json"):
        try:
            pid = int(f.stem)
        except ValueError:
            continue
        if _is_alive(pid):
            live.append(pid)
        else:
            try:
                f.unlink()
            except FileNotFoundError:
                pass
    return live


def _is_alive(pid: int) -> bool:
    if os.name == "nt":
        try:
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if handle == 0:
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        except Exception:
            return False

    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def run_job(job: dict) -> int:
    """Execute a job's command via the shell; return its exit code."""
    creationflags = 0

    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    proc = subprocess.run(
        job["command"],
        shell=True,
        creationflags=creationflags,
    )
    return proc.returncode


def _stop_file_path() -> os.PathLike:
    db.ensure_dirs()
    return db.WORKERS_DIR.parent / "stop"


def stop_requested() -> bool:
    return _stop_file_path().exists()


def worker_loop(worker_label: str):
    """
    Entry point run inside each worker OS process (see cli.py, which
    spawns one multiprocessing.Process per --count, each getting its own
    real PID). Runs until asked to stop.
    """
    pid = os.getpid()
    write_pid_file(pid)

    print(f"[worker {worker_label} pid={pid}] started", file=sys.stderr, flush=True)

    try:
        while True:
            if stop_requested():
                print(f"[worker {worker_label} pid={pid}] stop requested", file=sys.stderr, flush=True)
                break

            recovered = db.reap_stale_jobs()
            for jid in recovered:
                print(
                    f"[worker {worker_label} pid={pid}] recovered stale job {jid}",
                    file=sys.stderr, flush=True,
                )

            job = db.claim_next_job(pid)
            if job is None:
                slept = 0.0
                while slept < POLL_INTERVAL and not stop_requested():
                    time.sleep(IDLE_SLEEP_STEP)
                    slept += IDLE_SLEEP_STEP
                continue

            print(
                f"[worker {worker_label} pid={pid}] running job {job['id']}: {job['command']}",
                file=sys.stderr, flush=True,
            )
            exit_code = run_job(job)

            if exit_code == 0:
                db.mark_completed(job["id"])
                print(f"[worker {worker_label} pid={pid}] job {job['id']} completed",
                      file=sys.stderr, flush=True)
            else:
                new_attempts = job["attempts"] + 1
                with db.get_conn() as conn:
                    backoff_base = int(db.get_config(conn, "backoff_base"))
                db.mark_failed(
                    job["id"], new_attempts, job["max_retries"], backoff_base, exit_code
                )
                print(
                    f"[worker {worker_label} pid={pid}] job {job['id']} failed "
                    f"(exit {exit_code}, attempt {new_attempts}/{job['max_retries']})",
                    file=sys.stderr, flush=True,
                )

            if stop_requested():
                print(f"[worker {worker_label} pid={pid}] stop requested", file=sys.stderr, flush=True)
                break
    finally:
        remove_pid_file(pid)
        print(f"[worker {worker_label} pid={pid}] stopped", file=sys.stderr, flush=True)

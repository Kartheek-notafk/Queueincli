import argparse
import json
import multiprocessing
import os
import signal
import sys
import time

from . import db
from . import worker as workermod

STOP_TIMEOUT = 20  # seconds to wait for workers to finish their in-flight job


def cmd_enqueue(args):
    db.init_db()
    try:
        payload = json.loads(args.job_json)
    except json.JSONDecodeError as e:
        print(f"error: invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if "id" not in payload or "command" not in payload:
        print("error: job JSON must include at least 'id' and 'command'", file=sys.stderr)
        sys.exit(1)

    existing = db.list_jobs()
    if any(j["id"] == payload["id"] for j in existing):
        print(f"error: job id '{payload['id']}' already exists", file=sys.stderr)
        sys.exit(1)

    db.enqueue_job(payload)
    print(f"enqueued job {payload['id']}")


def _worker_entry(label):
    workermod.worker_loop(label)


def cmd_worker_start(args):
    db.init_db()
    procs = []
    for i in range(args.count):
        p = multiprocessing.Process(target=_worker_entry, args=(f"{i}",), daemon=False)
        p.start()
        procs.append(p)

    print(f"started {len(procs)} worker process(es): "
          f"{[p.pid for p in procs]} (Ctrl+C for graceful shutdown)")

    stopping = {"flag": False}

    def _forward_shutdown(signum, frame):
        if stopping["flag"]:
            return
        stopping["flag"] = True
        print("\nshutdown requested, signaling workers to finish current job...",
              file=sys.stderr)
        for p in procs:
            if p.pid:
                try:
                    os.kill(p.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

    signal.signal(signal.SIGTERM, _forward_shutdown)
    signal.signal(signal.SIGINT, _forward_shutdown)

    for p in procs:
        p.join()

    print("all workers stopped")


def cmd_worker_stop(args):
    db.init_db()
    pids = workermod.list_worker_pids()
    if not pids:
        print("no running workers found")
        return

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    deadline = time.time() + STOP_TIMEOUT
    remaining = set(pids)
    while remaining and time.time() < deadline:
        remaining = {p for p in remaining if workermod._is_alive(p)}
        if remaining:
            time.sleep(0.3)

    if remaining:
        print(f"warning: workers still running after {STOP_TIMEOUT}s: {sorted(remaining)}",
              file=sys.stderr)
    else:
        print(f"stopped {len(pids)} worker(s): {pids}")


def cmd_status(args):
    db.init_db()
    counts = db.status_counts()
    live = workermod.list_worker_pids()
    for state in ("pending", "processing", "completed", "failed", "dead"):
        print(f"{state:>10}: {counts.get(state, 0)}")
    print(f"{'workers':>10}: {len(live)} active ({live})")


def cmd_list(args):
    db.init_db()
    jobs = db.list_jobs(state=args.state)
    if args.json:
        # Contract: ONLY the JSON array goes to stdout.
        print(json.dumps(jobs))
    else:
        if not jobs:
            print("(no jobs)")
        for j in jobs:
            print(f"{j['id']:20} {j['state']:12} attempts={j['attempts']}/{j['max_retries']:<3} "
                  f"cmd={j['command']}")


def cmd_dlq_list(args):
    db.init_db()
    jobs = db.list_jobs(state="dead")
    if args.json:
        print(json.dumps(jobs))
    else:
        if not jobs:
            print("(DLQ empty)")
        for j in jobs:
            print(f"{j['id']:20} attempts={j['attempts']}/{j['max_retries']} "
                  f"last_error={j['last_error']}")


def cmd_dlq_retry(args):
    db.init_db()
    ok = db.dlq_retry(args.job_id)
    if ok:
        print(f"re-enqueued {args.job_id} (attempts reset to 0)")
    else:
        print(f"error: job '{args.job_id}' not found in DLQ", file=sys.stderr)
        sys.exit(1)


def cmd_config_set(args):
    db.init_db()
    key = args.key.replace("-", "_")
    if key not in ("max_retries", "backoff_base"):
        print(f"error: unknown config key '{args.key}'", file=sys.stderr)
        sys.exit(1)
    db.set_config(key, str(args.value))
    print(f"set {args.key} = {args.value} "
          f"(applies to jobs enqueued from now on; existing jobs keep their own max_retries)")


def cmd_config_show(args):
    db.init_db()
    with db.get_conn() as conn:
        for key in ("max_retries", "backoff_base"):
            print(f"{key}: {db.get_config(conn, key)}")


def build_parser():
    p = argparse.ArgumentParser(prog="queuectl", description="A tiny persistent job queue.")
    sub = p.add_subparsers(dest="command", required=True)

    p_enq = sub.add_parser("enqueue", help="Add a new job")
    p_enq.add_argument("job_json", help='JSON job spec, e.g. \'{"id":"job1","command":"sleep 2"}\'')
    p_enq.set_defaults(func=cmd_enqueue)

    p_worker = sub.add_parser("worker", help="Manage workers")
    worker_sub = p_worker.add_subparsers(dest="worker_command", required=True)

    p_wstart = worker_sub.add_parser("start", help="Start workers in the foreground")
    p_wstart.add_argument("--count", type=int, default=1)
    p_wstart.set_defaults(func=cmd_worker_start)

    p_wstop = worker_sub.add_parser("stop", help="Gracefully stop all running workers")
    p_wstop.set_defaults(func=cmd_worker_stop)

    p_status = sub.add_parser("status", help="Summary of job states & active workers")
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list", help="List jobs by state")
    p_list.add_argument("--state", choices=["pending", "processing", "completed", "failed", "dead"])
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

    p_dlq = sub.add_parser("dlq", help="View or retry DLQ jobs")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_command", required=True)

    p_dlq_list = dlq_sub.add_parser("list", help="List dead-lettered jobs")
    p_dlq_list.add_argument("--json", action="store_true")
    p_dlq_list.set_defaults(func=cmd_dlq_list)

    p_dlq_retry = dlq_sub.add_parser("retry", help="Re-enqueue a dead job")
    p_dlq_retry.add_argument("job_id")
    p_dlq_retry.set_defaults(func=cmd_dlq_retry)

    p_config = sub.add_parser("config", help="Manage configuration")
    config_sub = p_config.add_subparsers(dest="config_command", required=True)

    p_cset = config_sub.add_parser("set", help="Set a config value")
    p_cset.add_argument("key", help="max-retries | backoff-base")
    p_cset.add_argument("value")
    p_cset.set_defaults(func=cmd_config_set)

    p_cshow = config_sub.add_parser("show", help="Show current config")
    p_cshow.set_defaults(func=cmd_config_show)

    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

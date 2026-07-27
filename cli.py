import argparse
import json
import sys

from . import db


def cmd_enqueue(args):
    db.init_db()

    try:
        payload = json.loads(args.job_json)
    except json.JSONDecodeError as e:
        print(f"error: invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if "id" not in payload or "command" not in payload:
        print(
            "error: job JSON must include at least 'id' and 'command'",
            file=sys.stderr,
        )
        sys.exit(1)

    existing = db.list_jobs()

    if any(job["id"] == payload["id"] for job in existing):
        print(
            f"error: job id '{payload['id']}' already exists",
            file=sys.stderr,
        )
        sys.exit(1)

    db.enqueue_job(payload)
    print(f"enqueued job {payload['id']}")


def cmd_list(args):
    db.init_db()

    jobs = db.list_jobs(state=args.state)

    if args.json:
        print(json.dumps(jobs))
        return

    if not jobs:
        print("(no jobs)")
        return

    for job in jobs:
        print(
            f"{job['id']:20} "
            f"{job['state']:12} "
            f"attempts={job['attempts']}/{job['max_retries']:<3} "
            f"cmd={job['command']}"
        )


def cmd_status(args):
    db.init_db()

    counts = db.status_counts()

    for state in (
        "pending",
        "processing",
        "completed",
        "failed",
        "dead",
    ):
        print(f"{state:>10}: {counts.get(state, 0)}")
def cmd_config_set(args):
    db.init_db()

    key = args.key.replace("-", "_")

    if key not in (
        "max_retries",
        "backoff_base",
    ):
        print(
            f"error: unknown config key '{args.key}'",
            file=sys.stderr,
        )
        sys.exit(1)

    db.set_config(key, str(args.value))

    print(f"set {args.key} = {args.value}")


def cmd_config_show(args):
    db.init_db()

    with db.get_conn() as conn:
        for key in (
            "max_retries",
            "backoff_base",
        ):
            print(f"{key}: {db.get_config(conn, key)}")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="A tiny persistent job queue.",
    )

    sub = parser.add_subparsers(
        dest="command",
        required=True,
    )

    p_enqueue = sub.add_parser(
        "enqueue",
        help="Add a new job",
    )

    p_enqueue.add_argument(
        "job_json",
        help='JSON job spec, e.g. \'{"id":"job1","command":"echo hello"}\'',
    )

    p_enqueue.set_defaults(func=cmd_enqueue)

    p_list = sub.add_parser(
        "list",
        help="List jobs",
    )

    p_list.add_argument(
        "--state",
        choices=[
            "pending",
            "processing",
            "completed",
            "failed",
            "dead",
        ],
    )

    p_list.add_argument(
        "--json",
        action="store_true",
    )

    p_list.set_defaults(func=cmd_list)

    p_status = sub.add_parser(
        "status",
        help="Show queue status",
    )

    p_status.set_defaults(func=cmd_status)

    p_config = sub.add_parser(
        "config",
        help="Manage configuration",
    )

    config_sub = p_config.add_subparsers(
        dest="config_command",
        required=True,
    )

    p_set = config_sub.add_parser(
        "set",
        help="Set configuration value",
    )

    p_set.add_argument(
        "key",
        help="max-retries | backoff-base",
    )

    p_set.add_argument("value")

    p_set.set_defaults(func=cmd_config_set)

    p_show = config_sub.add_parser(
        "show",
        help="Show configuration",
    )

    p_show.set_defaults(func=cmd_config_show)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

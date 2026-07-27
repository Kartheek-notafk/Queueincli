import argparse

from . import db


def cmd_config_set(args):
    db.init_db()

    key = args.key.replace("-", "_")

    if key not in ("max_retries", "backoff_base"):
        print(f"Unknown configuration key: {args.key}")
        return

    db.set_config(key, str(args.value))
    print(f"Set {key} = {args.value}")


def cmd_config_show(args):
    db.init_db()

    with db.get_conn() as conn:
        for key in ("max_retries", "backoff_base"):
            print(f"{key}: {db.get_config(conn, key)}")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="A tiny persistent job queue."
    )

    sub = parser.add_subparsers(
        dest="command",
        required=True
    )

    config = sub.add_parser(
        "config",
        help="Manage configuration"
    )

    config_sub = config.add_subparsers(
        dest="config_command",
        required=True
    )

    set_cmd = config_sub.add_parser(
        "set",
        help="Set configuration value"
    )

    set_cmd.add_argument("key")
    set_cmd.add_argument("value")
    set_cmd.set_defaults(func=cmd_config_set)

    show_cmd = config_sub.add_parser(
        "show",
        help="Show configuration"
    )

    show_cmd.set_defaults(func=cmd_config_show)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

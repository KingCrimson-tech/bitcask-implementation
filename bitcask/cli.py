import argparse
import sys
import json

from bitcask.db import Bitcask


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="bitcask",
        description="Bitcask-style log-structured key-value store.",
        epilog="example: python -m bitcask.cli --dir ./data put user:1 '{\"name\":\"Chinmay\"}'",
    )
    parser.add_argument(
        "--dir",
        default="./data",
        help="data directory (default: ./data)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    put = sub.add_parser("put", help="store a key/value pair")
    put.add_argument("key")
    put.add_argument("value")

    get = sub.add_parser("get", help="print the value for a key")
    get.add_argument("key")

    p_delete = sub.add_parser("delete", help="Delete a key (appends tombstone).")
    p_delete.add_argument("key")
    
    p_stats = sub.add_parser("stats", help="Show store statistics as JSON.")

    p_compact = sub.add_parser(
        "compact",
        help="Merge old segments, keeping only live records.",
    )

    args = parser.parse_args(argv)

    with Bitcask(args.dir) as db:
        if args.command == "put":
            db.put(args.key, args.value)
            return 0

        if args.command == "get":
            try:
                print(db.get(args.key))
            except KeyError:
                print("KeyError: not found", file=sys.stderr)
                return 1
            return 0

        elif args.command == "delete":
            db.delete(args.key)

        elif args.command == "stats":
            print(json.dumps(db.stats()))

        elif args.command == "compact":
            result = db.compact()
            print(json.dumps(result))

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
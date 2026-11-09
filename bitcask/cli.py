import argparse
import sys

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

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
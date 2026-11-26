import argparse
import os

from .db import Ledger
from .worker import run


def main():
    parser = argparse.ArgumentParser(prog="python -m relayledger")
    parser.add_argument("command", choices=("migrate", "worker"))
    if parser.parse_args().command == "migrate":
        Ledger(os.environ["DATABASE_URL"]).migrate()
    else:
        run()


if __name__ == "__main__":
    main()


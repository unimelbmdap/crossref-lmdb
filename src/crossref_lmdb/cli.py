"""
Handles the command-line interface (CLI).
"""

from __future__ import annotations

import argparse
import pathlib
import logging
import sys

import crossref_lmdb.create
import crossref_lmdb.update


LOGGER = logging.getLogger("crossref-lmdb")


def main() -> None:

    parser = setup_parser()

    parsed_args = parser.parse_args()

    try:
        run(args=parsed_args)
    except Exception as err:
        print(err)
        if parsed_args.debug:
            raise err
        sys.exit(1)


def setup_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        description="Interact with Crossref data via a Lightning database",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Print error tracebacks",
    )

    subparsers = parser.add_subparsers(dest="command")

    create_parser = subparsers.add_parser(
        "create",
        help="Create a Lightning database from Crossref public data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    update_parser = subparsers.add_parser(
        "update",
        help="Update a Lighting database with new data from the web API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    create_parser.add_argument(
        "--public-data-dir",
        type=pathlib.Path,
        required=True,
        help="Path to the Crossref public data directory",
    )

    create_parser.add_argument(
        "--db-dir",
        type=pathlib.Path,
        required=True,
        help="Path to the directory to write the database files",
    )

    for subparser in (create_parser, update_parser):
        subparser.add_argument(
            "--compression-level",
            type=int,
            required=False,
            choices=list(range(-1, 10)),
            default=-1,
            help=(
                "Level of compression to use for metadata; 0 is no compression, -1 is "
                + "the default level of compression (6), and between 1 and 9 is the "
                + "level where 1 is the least and 9 is the most"
            ),
        )

        subparser.add_argument(
            "--filter-path",
            type=pathlib.Path,
            required=False,
            help=(
                "Path to a Python module file containing a function for filtering DOIs "
                + "(see documentation for details)"
            ),
        )

        subparser.add_argument(
            "--show-progress",
            help="Enable or disable a progress bar",
            default=True,
            action=argparse.BooleanOptionalAction,
        )

    create_parser.add_argument(
        "--commit-frequency",
        type=int,
        required=False,
        default=1_000,
        help=(
            "How frequently to commit additions to the database, in units of DOIs. "
            + "Higher values should be faster, but require more memory."
        ),
    )

    create_parser.add_argument(
        "--max-db-size-gb",
        type=float,
        default="2000",
        help=(
            "Maximum size that the database can grow to, in GB units. "
            + "See the documentation for details."
        )
    )

    update_parser.add_argument(
        "--db-dir",
        type=pathlib.Path,
        required=True,
        help="Path to the directory containing the database files",
    )

    update_parser.add_argument(
        "--email-address",
        type=str,
        required=True,
        help=(
            "Email address to provide to the Crossref web API so as to be "
            + "able to use the polite pool"
        ),
    )

    update_parser.add_argument(
        "--from-date",
        required=False,
        help=(
            "A date from which to search for updated records, specified in "
            "`YYYY[-MM[-DD]]` format (i.e., month and day are optional)"
        ),
    )

    update_parser.add_argument(
        "--filter-arg",
        required=False,
        help=(
            "A Crossref web API filter string for restricting DOIs."
        ),
    )

    return parser


def run(args: argparse.Namespace) -> None:

    if args.command is None:
        parser = setup_parser()
        parser.print_help()
        return

    LOGGER.debug(f"Running command with arguments: {args}")

    if args.command == "create":

        create_args = crossref_lmdb.create.CreateParams(
            public_data_dir=args.public_data_dir,
            db_dir=args.db_dir,
            max_db_size_gb=args.max_db_size_gb,
            compression_level=args.compression_level,
            commit_frequency=args.commit_frequency,
            filter_path=args.filter_path,
            show_progress=args.show_progress,
        )

        crossref_lmdb.create.run(args=create_args)

    elif args.command == "update":

        update_args = crossref_lmdb.update.UpdateParams(
            db_dir=args.db_dir,
            email_address=args.email_address,
            from_date=args.from_date,
            compression_level=args.compression_level,
            filter_path=args.filter_path,
            filter_arg=args.filter_arg,
            show_progress=args.show_progress,
        )

        crossref_lmdb.update.run(args=update_args)

    else:
        raise ValueError(f"Unexpected command: {args.command}")


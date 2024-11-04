"""
Handles the command-line interface (CLI).
"""

from __future__ import annotations

import argparse
import pathlib
import logging
import sys
import os

import crossref_lmdb.params
import crossref_lmdb.main


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
        description="Interact with Crossref data via a Lightning database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Print error tracebacks.",
    )

    subparsers = parser.add_subparsers(dest="command")

    create_parser = subparsers.add_parser(
        "create",
        help="Create a Lightning database from Crossref public data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    update_parser = subparsers.add_parser(
        "update",
        help="Update a Lighting database with new data from the web API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    create_parser.add_argument(
        "--public-data-dir",
        type=pathlib.Path,
        required=True,
        help="Path to the Crossref public data directory.",
    )

    create_parser.add_argument(
        "--db-dir",
        type=pathlib.Path,
        required=True,
        help="Path to the directory to write the database files.",
    )

    create_parser.add_argument(
        "--start-from-file-num",
        type=int,
        required=False,
        default=0,
        help="Begin processing from this file number in the public data archive.",
    )

    for subparser in (create_parser, update_parser):

        subparser.add_argument(
            "--commit-frequency",
            type=int,
            required=False,
            default=20_000,
            help=(
                "How often to commit changes to the database, in units of number "
                + "of items."
            ),
        )

        subparser.add_argument(
            "--compression-level",
            type=int,
            required=False,
            choices=list(range(-1, 10)),
            default=-1,
            help=(
                "Level of compression to use for metadata; 0 is no compression, -1 is "
                + "the default level of compression (6), and between 1 and 9 is the "
                + "level where 1 is the least and 9 is the most."
            ),
        )

        subparser.add_argument(
            "--filter-path",
            type=pathlib.Path,
            required=False,
            help=(
                "Path to a Python module file containing a function for filtering "
                + "DOIs. "
                + "This function must be called `filter_func` and accept one "
                + "parameter, "
                + "which contains a dict-like interface to item metadata. The function "
                + "returns False if the item is to be filtered out and True otherwise."
            ),
        )

        subparser.add_argument(
            "--show-progress",
            help="Enable or disable a progress bar.",
            default=True,
            action=argparse.BooleanOptionalAction,
        )

        is_windows = os.name == "nt"

        default_max_db_size_gb = 2 if is_windows else 2000

        subparser.add_argument(
            "--max-db-size-gb",
            type=float,
            default=default_max_db_size_gb,
            help=(
                "Maximum size that the database can grow to, in GB units. "
                + "Note that this is set to a smaller default on Windows (2 GB), "
                + "due to it pre-allocating space. "
            ),
        )

    update_parser.add_argument(
        "--db-dir",
        type=pathlib.Path,
        required=True,
        help="Path to the directory containing the LMDB database files.",
    )

    update_parser.add_argument(
        "--email-address",
        type=str,
        required=True,
        help=(
            "Email address to provide to the Crossref web API so as to be "
            + "able to use the polite pool."
        ),
    )

    update_parser.add_argument(
        "--from-date",
        required=False,
        help=(
            "A date from which to search for updated records, specified in "
            "`YYYY[-MM[-DD]]` format (i.e., month and day are optional)."
        ),
    )

    update_parser.add_argument(
        "--filter-arg",
        required=False,
        help="A Crossref web API filter string for restricting DOIs.",
    )

    return parser


def run(args: argparse.Namespace) -> None:

    if args.command is None:
        parser = setup_parser()
        parser.print_help()
        return

    LOGGER.debug(f"Running command with arguments: {args}")

    action_args: crossref_lmdb.params.CreateParams | crossref_lmdb.params.UpdateParams

    if args.command == "create":

        action_args = crossref_lmdb.params.CreateParams(
            public_data_dir=args.public_data_dir,
            db_dir=args.db_dir,
            max_db_size_gb=args.max_db_size_gb,
            commit_frequency=args.commit_frequency,
            compression_level=args.compression_level,
            filter_path=args.filter_path,
            show_progress=args.show_progress,
            start_from_file_num=args.start_from_file_num,
        )

    elif args.command == "update":

        action_args = crossref_lmdb.params.UpdateParams(
            db_dir=args.db_dir,
            email_address=args.email_address,
            from_date=args.from_date,
            commit_frequency=args.commit_frequency,
            compression_level=args.compression_level,
            max_db_size_gb=args.max_db_size_gb,
            filter_path=args.filter_path,
            filter_arg=args.filter_arg,
            show_progress=args.show_progress,
        )

    else:
        raise ValueError(f"Unexpected command: {args.command}")

    crossref_lmdb.main.run(params=action_args)

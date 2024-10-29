
from __future__ import annotations

import pathlib
import os
import dataclasses
import typing
import logging
import gzip
import zlib
import datetime

import lmdb

import simdjson

import alive_progress

import crossref_lmdb.utils
import crossref_lmdb.filt
import crossref_lmdb.items


LOGGER = logging.getLogger("crossref_lmdb")



@dataclasses.dataclass
class CreateParams:
    public_data_dir: pathlib.Path
    db_dir: pathlib.Path
    max_db_size_gb: float = 2000.0
    compression_level: int = -1
    commit_frequency: int = 1_000
    filter_path: pathlib.Path | None = None
    show_progress: bool = True
    filter_func: crossref_lmdb.filt.FilterFunc | None = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.validate()
        self.set_filter_func()

    @property
    def max_db_size_bytes(self) -> int:

        multiplier = (
            1000  # MB
            * 1000  # KB
            * 1000  # B
        )

        n_bytes = self.max_db_size_gb * multiplier

        if not n_bytes.is_integer():
            msg = f"Unexpected number of bytes: {n_bytes}"
            raise ValueError(msg)

        return int(n_bytes)

    def set_filter_func(self) -> None:

        if self.filter_path is None:
            self.filter_func = None
            return None

        self.filter_func = crossref_lmdb.filt.get_filter_func(
            filter_path=self.filter_path
        )

    def validate(self) -> None:

        errors: list[str] = []

        if not self.public_data_dir.exists() or not self.public_data_dir.is_dir():
            errors.append(
                f"Public data directory ({self.public_data_dir}) does not exist "
                + "or is not a directory"
            )

        if not (self.public_data_dir / "0.json.gz").exists():
            errors.append(
                f"Public data directory ({self.public_data_dir}) does contain the "
                + "expected CrossRef data"
            )

        if not self.db_dir.exists():
            errors.append(
                f"Output database directory ({self.db_dir}) does not exist"
            )

        if self.db_dir.exists():

            with os.scandir(self.db_dir) as it:
                for _ in it:
                    errors.append(
                        f"Output database directory ({self.db_dir}) is not empty"
                    )
                    break

        if self.max_db_size_gb <= 0:
            errors.append(
                f"Invalid maximum database size ({self.max_db_size_gb})"
            )

        if self.compression_level not in list(range(-1, 9 + 1)):
            errors.append(
                f"Invalid compression level ({self.compression_level})"
            )

        if self.commit_frequency <= 0:
            errors.append(
                f"Invalid commit frequency ({self.commit_frequency})"
            )

        if self.filter_path is not None:

            if not self.filter_path.exists():
                errors.append(
                    f"Filter path ({self.filter_path}) does not exist"
                )

            if self.filter_path.exists() and self.filter_path.suffix != ".py":
                errors.append(
                    f"Filter path ({self.filter_path}) does not seem to be a Python "
                    + "source file (no .py extension)"
                )

        if any(errors):
            error_msg = "\n".join(
                ["Encountered the following errors with the provided arguments:"]
                + [
                    f"\t{error}"
                    for error in errors
                ]
            )
            raise ValueError(error_msg)


def run(args: CreateParams) -> None:

    most_recent_indexed = datetime.datetime(year=1900, month=1, day=1)

    with lmdb.Environment(
        path=str(args.db_dir),
        map_size=args.max_db_size_bytes,
        subdir=True,
    ) as env:

        LOGGER.info(f"Created LMDB database at {args.db_dir}")

        item_iterator = iter_public_data_items(
            public_data_dir=args.public_data_dir,
            filter_func=args.filter_func,
            show_progress=args.show_progress,
        )

        has_more_items = True

        while has_more_items:

            counter = 0

            with env.begin(write=True) as txn:

                while counter < args.commit_frequency:

                    try:
                        item = next(item_iterator)
                    except StopIteration:
                        has_more_items = False
                        break

                    (doi, _) = crossref_lmdb.items.insert_item(
                        item=item,
                        txn=txn,
                        compression_level=args.compression_level,
                        overwrite=False,
                    )

                    try:
                        indexed_datetime = crossref_lmdb.items.get_indexed_datetime(
                            item=item
                        )
                    except ValueError as err:
                        msg = f"Unexpected JSON format for DOI {doi}"
                        raise ValueError(msg) from err

                    if indexed_datetime is None:
                        LOGGER.warning(f"No indexed date for DOI {doi}")
                    elif indexed_datetime > most_recent_indexed:
                        most_recent_indexed = indexed_datetime

                    counter += 1

                LOGGER.debug("Committing items")

        most_recent_indexed_str = most_recent_indexed.isoformat()

        LOGGER.info(f"Most recent item index time was: {most_recent_indexed_str}")

        with env.begin(write=True) as txn:
            txn.put(
                key=b"__most_recent_indexed",
                value=zlib.compress(
                    most_recent_indexed_str.encode(),
                    level=args.compression_level,
                )
            )


def iter_public_data_items(
    public_data_dir: pathlib.Path,
    filter_func: crossref_lmdb.filt.FilterFunc | None = None,
    show_progress: bool = True,
) -> typing.Iterator[simdjson.Object]:

    # just a bare sorted doesn't sort in numerical order for the
    # 0, 1, ..., 10, 11, ..., 100, 101, ... convention used in
    # the raw data filenames; need to convert them to integers instead
    gz_paths = sorted(
        public_data_dir.glob("*.json.gz"),
        key: lambda x: int(x.name[:-len(".json.gz")]),
    )

    n_paths = len(gz_paths)

    with alive_progress.alive_bar(
        total=n_paths,
        disable=not show_progress,
    ) as progress_bar:

        for gz_path in gz_paths:

            with gzip.open(gz_path, "rb") as handle:

                data = handle.read()

                json_items = crossref_lmdb.items.prepare_json_items(
                    data=data
                )

                yield from crossref_lmdb.items.iter_items(
                    items=json_items,
                    filter_func=filter_func,
                )

                progress_bar()

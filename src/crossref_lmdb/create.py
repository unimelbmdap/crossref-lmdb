
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


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


FilterFunc: typing.TypeAlias = typing.Callable[[simdjson.Object], bool]


@dataclasses.dataclass
class CreateParams:
    public_data_dir: pathlib.Path
    db_dir: pathlib.Path
    max_db_size_gb: float = 2000.0
    compression_level: int = -1
    commit_frequency: int = 1_000
    filter_path: pathlib.Path | None = None
    show_progress: bool = True
    filter_func: FilterFunc | None = dataclasses.field(init=False)

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
            return

        filter_code = self.filter_path.read_text()

        filter_locals = crossref_lmdb.utils.run_code_from_text(code=filter_code)

        if "filter_func" not in filter_locals:
            raise ValueError(
                f"No function named `filter_func` present in {self.filter_path}"
            )

        self.filter_func = typing.cast(
            FilterFunc,
            filter_locals["filter_func"],
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
                for item in it:
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

                    item_bytes = typing.cast(bytes, item.mini)

                    try:
                        doi = str(item["DOI"])
                    except KeyError:
                        mini = item.mini
                        LOGGER.warning(f"No DOI found in item {item_bytes.decode()}")
                        continue

                    doi_bytes = doi.encode("utf8")

                    item_compressed = zlib.compress(
                        item_bytes,
                        level=args.compression_level,
                    )

                    success = txn.put(
                        key=doi_bytes,
                        value=item_compressed,
                        overwrite=False,
                    )

                    if not success:
                        LOGGER.warning(f"DOI {doi} already present in database")

                    try:
                        item_indexed = item["indexed"]
                    except KeyError:
                        LOGGER.warning(f"No indexed date for DOI {doi}")

                    if not isinstance(item_indexed, simdjson.Object):
                        msg = f"Unexpected JSON format for DOI {doi}"
                        raise ValueError(msg)

                    try:
                        item_datetime_str = item_indexed["date-time"]
                    except KeyError:
                        LOGGER.warning(f"No indexed date for DOI {doi}")

                    if not isinstance(item_datetime_str, str):
                        msg = f"Unexpected JSON format for DOI {doi}"
                        raise ValueError(msg)

                    indexed_datetime = parse_indexed_datetime(
                        indexed_datetime=item_datetime_str
                    )

                    if indexed_datetime > most_recent_indexed:
                        most_recent_indexed = indexed_datetime

                    counter += 1

        with env.begin(write=True) as txn:
            txn.put(
                key="__most_recent_indexed".encode("utf8"),
                value=most_recent_indexed.isoformat().encode("utf8"),
            )


def parse_indexed_datetime(indexed_datetime: str) -> datetime.datetime:

    if not indexed_datetime.endswith("Z"):
        msg = f"Unexpected date format in `{indexed_datetime}`"
        raise ValueError(msg)

    return datetime.datetime.fromisoformat(indexed_datetime[:-1])



def iter_public_data_items(
    public_data_dir: pathlib.Path,
    filter_func: FilterFunc | None = None,
    show_progress: bool = True,
) -> typing.Iterator[simdjson.Object]:

    gz_paths = sorted(public_data_dir.glob("*.gz"))

    n_paths = len(gz_paths)

    json_error_msg = "Invalid JSON"

    with alive_progress.alive_bar(
        total=n_paths,
        disable=not show_progress,
    ) as progress_bar:

        for path_num, gz_path in enumerate(gz_paths, 1):

            with gzip.open(gz_path, "rb") as handle:

                data = handle.read()

                parser = simdjson.Parser()

                json_data = parser.parse(data)

                if not isinstance(json_data, simdjson.Object):
                    raise ValueError(json_error_msg)

                json_items = json_data["items"]
                if not isinstance(json_items, simdjson.Array):
                    raise ValueError(json_error_msg)

                for json_item in json_items:

                    if not isinstance(json_item, simdjson.Object):
                        raise ValueError(json_error_msg)

                    if json_item.get("DOI", None) is None:
                        continue

                    if filter_func is not None and not filter_func(json_item):
                        continue

                    yield json_item

                    progress_bar()

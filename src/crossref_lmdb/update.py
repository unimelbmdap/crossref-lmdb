
from __future__ import annotations

import pathlib
import os
import dataclasses
import typing
import logging
import gzip
import zlib
import datetime
import math

import lmdb

import simdjson

import email_validator

import alive_progress

import crossref_lmdb.utils
import crossref_lmdb.filt
import crossref_lmdb.web


LOGGER = logging.getLogger("crossref_lmdb")


@dataclasses.dataclass
class UpdateParams:
    db_dir: pathlib.Path
    email_address: str
    from_date: str | None = None
    max_db_size_gb: float = 2000.0
    filter_path: pathlib.Path | None = None
    filter_arg: str | None = None
    compression_level: int = -1
    show_progress: bool = True
    filter_func: crossref_lmdb.filt.FilterFunc | None = dataclasses.field(init=False)

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

    def __post_init__(self) -> None:
        self.validate()
        self.set_filter_func()

    def set_filter_func(self) -> None:

        if self.filter_path is None:
            self.filter_func = None
            return None

        self.filter_func = crossref_lmdb.filt.get_filter_func(
            filter_path=self.filter_path
        )

    def validate(self) -> None:

        errors: list[str] = []

        if not self.db_dir.exists():
            errors.append(
                f"Database directory ({self.db_dir}) does not exist"
            )

        try:
            email_validator.validate_email(
                self.email_address,
                check_deliverability=False,
            )
        except email_validator.EmailNotValidError as err:
            errors.append(str(err))

        if self.from_date is not None:
            for potential_date_format in [
                "%Y",
                "%Y-%m",
                "%Y-%m-%d",
            ]:

                try:
                    datetime.datetime.strptime(self.from_date, potential_date_format)
                except ValueError:
                    pass
                else:
                    break

            else:
                errors.append(
                    f"From date `{self.from_date}` not in a valid format"
                )

        if self.max_db_size_gb <= 0:
            errors.append(
                f"Invalid maximum database size ({self.max_db_size_gb})"
            )

        if self.compression_level not in list(range(-1, 9 + 1)):
            errors.append(
                f"Invalid compression level ({self.compression_level})"
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


def run(args: UpdateParams) -> None:

    client = crossref_lmdb.web.CrossRefWebAPI(email_address=args.email_address)

    client.set_rate_limit()

    from_date = args.from_date

    # if we haven't been provided a from date, then grab it from the database
    if from_date is None:
        with crossref_lmdb.db.DBReader(db_dir=args.db_dir) as db:
            from_date = db.most_recent_indexed

    if not isinstance(from_date, str):
        raise ValueError()

    cursor = "*"

    # do an initial brief query to get a total
    total_query = form_query(
        from_date=from_date,
        filter_arg=args.filter_arg,
        cursor=cursor,
        only_doi=True,
        n_rows=1,
    )

    total_response = client.call(query=total_query)

    total_message = total_response.json()["message"]

    total_results = total_message["total-results"]
    total_msg = f"A total of {total_results:,} items have been updated since {args.from_date}"

    n_rows = 500

    n_pages = math.ceil(total_results / n_rows)

    if args.filter_arg is not None:
        total_msg += f" (given a filter parameter of `{args.filter_arg}`)"

    LOGGER.info(total_msg)

    most_recent_indexed = datetime.datetime(year=1900, month=1, day=1)

    with lmdb.Environment(
        path=str(args.db_dir),
        readonly=False,
        map_size=args.max_db_size_bytes,
    ) as env:

        with alive_progress.alive_bar(
            total=n_pages,
            disable=not args.show_progress,
            unit="pages",
        ) as progress_bar:

            cursor = "*"

            more_pages = True

            while more_pages:

                query = form_query(
                    from_date=from_date,
                    filter_arg=args.filter_arg,
                    cursor=cursor,
                    n_rows=n_rows,
                )

                response = client.call(query=query)

                parser = simdjson.Parser()

                data = parser.parse(response.content)

                if not isinstance(data, simdjson.Object):
                    raise ValueError()

                message = data["message"]

                if not isinstance(message, simdjson.Object):
                    raise ValueError()

                items = message["items"]

                if not isinstance(items, simdjson.Array):
                    raise ValueError

                n_items = len(items)

                more_pages = n_items > 0

                cursor = str(message["next-cursor"])

                with env.begin(write=True) as txn:

                    for item in crossref_lmdb.items.iter_items(
                        items=items,
                        filter_func=args.filter_func,
                    ):

                        (doi, _) = crossref_lmdb.items.insert_item(
                            item=item,
                            txn=txn,
                            compression_level=args.compression_level,
                            overwrite=True,
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

                progress_bar()

        most_recent_indexed_str = most_recent_indexed.isoformat()

        LOGGER.info(f"Most recent item index time was: {most_recent_indexed_str}")

        with env.begin(write=True) as txn:
            txn.put(
                key=b"__most_recent_indexed",
                value=zlib.compress(
                    most_recent_indexed_str.encode(),
                    level=args.compression_level,
                ),
                overwrite=True,
            )


def form_query(
    from_date: str,
    filter_arg: str | None = None,
    n_rows: int = 500,
    cursor: str = "*",
    only_doi: bool = False,
    sort_results: bool = True,
) -> str:

    query = "works?"

    filt = f"filter=from-index-date:{from_date}"

    if filter_arg is not None:
        filt += "," + filter_arg

    rows = f"rows={n_rows}"

    cursor = f"cursor={cursor}"

    select = (
        "select=DOI"
        if only_doi
        else None
    )

    sort: str | None = (
        "sort=indexed"
        if sort_results
        else None
    )

    order: str | None = (
        "order=asc"
        if sort_results
        else None
    )

    params = [
        param
        for param in (filt, rows, cursor, select, sort, order)
        if param is not None
    ]

    return query + "&".join(params)

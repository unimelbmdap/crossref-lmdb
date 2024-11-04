"""
Interface for creating and updating the database.
"""

from __future__ import annotations

import typing
import datetime
import logging
import zlib
import types

import lmdb

import simdjson

import tenacity

import crossref_lmdb.params
import crossref_lmdb.web
import crossref_lmdb.file
import crossref_lmdb.items
import crossref_lmdb.db
import crossref_lmdb.date


LOGGER = logging.getLogger("crossref_lmdb")


def run(
    params: crossref_lmdb.params.CreateParams | crossref_lmdb.params.UpdateParams,
) -> None:

    start_time = datetime.datetime.now()

    LOGGER.info(f"Beginning processing at {start_time}")

    # only if updating do we need a 'from' date
    if isinstance(params, crossref_lmdb.params.UpdateParams):
        from_date = get_from_date(params=params)

    most_recent_indexed = datetime.datetime(year=1900, month=1, day=1)

    try:
        with crossref_lmdb.db.DBReader(db_dir=params.db_dir) as db:
            most_recent_indexed = datetime.datetime.fromisoformat(
                db.most_recent_indexed
            )
    except Exception:
        LOGGER.info("No most-recently indexed item found in the database")
        pass

    with lmdb.Environment(
        path=str(params.db_dir),
        readonly=False,
        map_size=params.max_db_size_bytes,
        subdir=True,
    ) as env:

        # we acquire items either from a file (create) or via the web API (update)
        item_source = (
            crossref_lmdb.file.FileSource(
                public_data_dir=params.public_data_dir,
                show_progress=params.show_progress,
                filter_func=params.filter_func,
                start_from_file_num=params.start_from_file_num,
            )
            if isinstance(params, crossref_lmdb.params.CreateParams)
            else crossref_lmdb.web.WebSource(
                email_address=params.email_address,
                from_date=from_date,
                show_progress=params.show_progress,
                filter_func=params.filter_func,
                filter_arg=params.filter_arg,
            )
        )

        item_iterator = item_source.iter_item()

        with Inserter(
            env=env,
            commit_frequency=params.commit_frequency,
            compression_level=params.compression_level,
            most_recent_indexed=most_recent_indexed,
        ) as item_inserter:

            for item in item_iterator:
                item_inserter.insert_item(item=item)

    end_time = datetime.datetime.now()

    LOGGER.info(f"Finished processing at {end_time}")

    time_taken_s = (end_time - start_time).total_seconds()

    time_taken_hours = time_taken_s / 60 / 60

    LOGGER.info(f"Processing took {time_taken_hours:.4f} hours")


class Inserter:

    def __init__(
        self,
        env: lmdb.Environment,
        commit_frequency: int,
        compression_level: int,
        most_recent_indexed: datetime.datetime,
    ) -> None:

        self.env = env
        self.commit_frequency = commit_frequency
        self.compression_level = compression_level
        self.most_recent_indexed = most_recent_indexed

        self.txn: lmdb.Transaction | None = None
        self.item_count = 0

    def __enter__(self) -> Inserter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:

        if self.txn is not None:
            self.commit()

    def insert_item(self, item: simdjson.Object) -> None:

        item_bytes = typing.cast(bytes, item.mini)

        try:
            doi = str(item["DOI"])
        except KeyError:
            LOGGER.warning(f"No DOI found in item {item_bytes.decode()}")
            return

        LOGGER.debug(f"Inserting item with doi {doi}")

        doi_bytes = doi.encode()

        item_compressed = zlib.compress(
            item_bytes,
            level=self.compression_level,
        )

        self.insert(
            key=doi_bytes,
            value=item_compressed,
        )

        indexed_datetime = crossref_lmdb.date.get_indexed_datetime(item=item)

        if indexed_datetime is None:
            LOGGER.warning(f"No indexed date for DOI {doi}")

        elif indexed_datetime > self.most_recent_indexed:

            most_recent_indexed_str = indexed_datetime.isoformat()

            self.insert(
                key=b"__most_recent_indexed",
                value=zlib.compress(
                    most_recent_indexed_str.encode(),
                    level=self.compression_level,
                ),
            )

            self.most_recent_indexed = indexed_datetime

    def insert(
        self,
        key: bytes,
        value: bytes,
    ) -> None:

        if self.txn is None:
            self.init_txn()

        if self.txn is None:
            raise ValueError()

        self.txn.put(
            key=key,
            value=value,
        )

        self.item_count += 1

        if self.item_count % self.commit_frequency == 0:
            self.commit()
            self.txn = None

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        stop=tenacity.stop_after_attempt(20),
        retry=tenacity.retry_if_exception_type(lmdb.Error),
        after=tenacity.after_log(logger=LOGGER, log_level=logging.WARNING),
    )
    def commit(self) -> None:
        if self.txn is None:
            raise ValueError()
        self.txn.commit()

    def init_txn(self) -> None:
        self.txn = self.env.begin(write=True)


def get_from_date(params: crossref_lmdb.params.UpdateParams) -> str:

    from_date = params.from_date

    # if we haven't been provided a from date, then grab it from the database
    if from_date is None:
        with crossref_lmdb.db.DBReader(db_dir=params.db_dir) as db:
            from_date_dt = datetime.datetime.fromisoformat(db.most_recent_indexed)

        from_date = from_date_dt.strftime("%Y-%m-%d")

    if not isinstance(from_date, str):
        raise ValueError()

    return from_date

from __future__ import annotations

import logging
import typing
import zlib
import datetime
import abc
import collections
import pathlib
import gzip

import lmdb

import simdjson

import alive_progress

import crossref_lmdb.filt
import crossref_lmdb.utils

LOGGER = logging.getLogger("crossref_lmdb")


Item: typing.TypeAlias = simdjson.Object


class ItemSource(abc.ABC, collections.abc.Iterator[Item]):

    @property
    @abc.abstractmethod
    def total_items(self) -> int:
        ...

    @property
    @abc.abstractmethod
    def total_units(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def filter_func(self) -> crossref_lmdb.filt.FilterFunc | None:
        ...

    @property
    @abc.abstractmethod
    def show_progress(self) -> bool:
        ...

    def __iter__(self) -> typing.Iterator[Item]:
        self._item_iter = self.iter_item()
        return self

    def __next__(self) -> Item:
        return next(self._item_iter)

    @abc.abstractmethod
    def iter_unfiltered_items_data(self) -> typing.Iterable[bytes]:
        ...

    def iter_item(self) -> typing.Iterator[Item]:

        with alive_progress.alive_bar(
            total=self.total_items,
            disable=not self.show_progress,
        ) as progress_bar:

            for data in self.iter_unfiltered_items_data():

                json_items = prepare_json_items(data=data)

                for item in json_items:

                    if not isinstance(item, simdjson.Object):
                        LOGGER.error("Invalid JSON")
                        raise ValueError("Invalid JSON")

                    if item.get("DOI", None) is None:
                        item_bytes = typing.cast(bytes, item.mini)
                        LOGGER.debug(
                            f"Item {item_bytes.decode()} does not have a DOI; skipping"
                        )

                    elif self.filter_func is not None and not self.filter_func(item):
                        item_bytes = typing.cast(bytes, item.mini)
                        LOGGER.debug(f"Filtered out item {item_bytes.decode()}")

                    else:
                        yield item

                progress_bar()




def insert_item(
    item: simdjson.Object,
    txn: lmdb.Transaction,
    compression_level: int,
    overwrite: bool,
) -> tuple[str, bool]:

    item_bytes = typing.cast(bytes, item.mini)

    try:
        doi = str(item["DOI"])
    except KeyError:
        LOGGER.warning(f"No DOI found in item {item_bytes.decode()}")
        return ("", False)

    doi_bytes = doi.encode()

    item_compressed = zlib.compress(
        item_bytes,
        level=compression_level,
    )

    success: bool = txn.put(
        key=doi_bytes,
        value=item_compressed,
        overwrite=overwrite,
    )

    if not success:

        if not overwrite:
            raise ValueError("Unexpected response")

        LOGGER.warning(f"DOI {doi} already present in database and not overwriting")

    return (doi, success)


def prepare_json_items(data: bytes) -> simdjson.Array:

    json_error_msg = "Invalid JSON"

    parser = simdjson.Parser()

    json_data = parser.parse(data)

    if not isinstance(json_data, simdjson.Object):
        raise ValueError(json_error_msg)

    json_items = json_data["items"]
    if not isinstance(json_items, simdjson.Array):
        raise ValueError(json_error_msg)

    return json_items


def get_indexed_datetime(
    item: simdjson.Object
) -> datetime.datetime | None:

    try:
        item_indexed = item["indexed"]
    except KeyError:
        return None

    if not isinstance(item_indexed, simdjson.Object):
        raise ValueError("A")

    try:
        item_datetime_str = item_indexed["date-time"]
    except KeyError:
        return None

    if not isinstance(item_datetime_str, str):
        raise ValueError("B")

    indexed_datetime = crossref_lmdb.utils.parse_indexed_datetime(
        indexed_datetime=item_datetime_str
    )

    return indexed_datetime

from __future__ import annotations

import datetime
import logging
import typing
import zlib

import lmdb

import simdjson

import crossref_lmdb.filt

LOGGER = logging.getLogger("crossref_lmdb")


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


def iter_items(
    items: simdjson.Array,
    filter_func: crossref_lmdb.filt.FilterFunc | None = None,
) -> typing.Iterator[simdjson.Object]:

    for item in items:

        if not isinstance(item, simdjson.Object):
            raise ValueError("Invalid JSON")

        if item.get("DOI", None) is None:
            item_bytes = typing.cast(bytes, item.mini)
            LOGGER.debug(
                f"Item {item_bytes.decode()} does not have a DOI; skipping"
            )
            continue

        if filter_func is not None and not filter_func(item):
            item_bytes = typing.cast(bytes, item.mini)
            LOGGER.debug(f"Filtered out item {item_bytes.decode()}")
            continue

        yield item


def parse_indexed_datetime(indexed_datetime: str) -> datetime.datetime:

    if not indexed_datetime.endswith("Z"):
        msg = f"Unexpected date format in `{indexed_datetime}`"
        raise ValueError(msg)

    return datetime.datetime.fromisoformat(indexed_datetime[:-1])

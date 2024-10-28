from __future__ import annotations

import datetime
import logging
import typing

import simdjson

import crossref_lmdb.filt

LOGGER = logging.getLogger("crossref_lmdb")


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

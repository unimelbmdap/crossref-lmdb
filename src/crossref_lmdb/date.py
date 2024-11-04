"""
Miscellaneous date-handling utility functions.
"""

from __future__ import annotations

import datetime
import logging
import typing

import simdjson

LOGGER = logging.getLogger("crossref_lmdb")


def get_indexed_datetime(item: simdjson.Object) -> datetime.datetime | None:

    try:
        item_indexed = item["indexed"]
    except KeyError:
        return None

    if not isinstance(item_indexed, simdjson.Object):
        raise ValueError()

    try:
        item_datetime_str = item_indexed["date-time"]
    except KeyError:
        return None

    if not isinstance(item_datetime_str, str):
        raise ValueError()

    indexed_datetime = parse_indexed_datetime(indexed_datetime=item_datetime_str)

    return indexed_datetime


def parse_indexed_datetime(indexed_datetime: str) -> datetime.datetime:

    if not indexed_datetime.endswith("Z"):
        msg = f"Unexpected date format in `{indexed_datetime}`"
        raise ValueError(msg)

    return datetime.datetime.fromisoformat(indexed_datetime[:-1])


def get_published_date(item: simdjson.Object) -> datetime.date | None:

    json_msg = "Unexpected JSON format"

    if not isinstance(item, simdjson.Object):
        raise ValueError(json_msg)

    try:
        published = item["published"]
    except KeyError:
        return None

    if not isinstance(published, simdjson.Object):
        raise ValueError(json_msg)

    try:
        date_parts = published["date-parts"]
    except KeyError:
        return None

    if not isinstance(date_parts, simdjson.Array):
        raise ValueError(json_msg)

    dates: list[datetime.date] = []

    for raw_date_parts in date_parts:

        if not isinstance(raw_date_parts, simdjson.Array):
            raise ValueError(json_msg)

        if len(raw_date_parts) == 1:
            (year,) = typing.cast(tuple[int], raw_date_parts)
            month = day = 1
        elif len(raw_date_parts) == 2:
            (year, month) = typing.cast(tuple[int, int], raw_date_parts)
            day = 1
        elif len(raw_date_parts) == 3:
            (year, month, day) = typing.cast(tuple[int, int, int], raw_date_parts)
        else:
            msg = f"Unknown date format: {raw_date_parts}"
            raise ValueError(msg)

        dates.append(datetime.date(year=year, month=month, day=day))

    return max(dates)

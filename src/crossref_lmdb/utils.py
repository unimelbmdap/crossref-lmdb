from __future__ import annotations

import datetime
import logging
import typing

import simdjson

LOGGER = logging.getLogger("crossref_lmdb")



def parse_indexed_datetime(indexed_datetime: str) -> datetime.datetime:

    if not indexed_datetime.endswith("Z"):
        msg = f"Unexpected date format in `{indexed_datetime}`"
        raise ValueError(msg)

    return datetime.datetime.fromisoformat(indexed_datetime[:-1])


def get_published_date(item: simdjson.Object) -> datetime.date | None:

    assert isinstance(item, simdjson.Object)

    try:
        published = item["published"]
    except KeyError:
        return None

    try:
        assert isinstance(published, simdjson.Object)
        date_parts = published["date-parts"]
    except KeyError:
        return None

    assert isinstance(date_parts, simdjson.Array)

    dates: list[datetime.date] = []

    for raw_date_parts in date_parts:

        assert isinstance(raw_date_parts, simdjson.Array)

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

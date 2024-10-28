from __future__ import annotations

import datetime



def parse_indexed_datetime(indexed_datetime: str) -> datetime.datetime:

    if not indexed_datetime.endswith("Z"):
        msg = f"Unexpected date format in `{indexed_datetime}`"
        raise ValueError(msg)

    return datetime.datetime.fromisoformat(indexed_datetime[:-1])

from __future__ import annotations

import contextlib
import typing
import types
import pathlib
import collections
import zlib

import lmdb

import simdjson


class DBReader(collections.abc.Mapping[str, simdjson.Object]):

    def __init__(
        self,
        db_dir: pathlib.Path,
    ) -> None:

        self._db_dir = db_dir

        self._special_keys = (
            "__most_recent_indexed",
        )

        self._env = lmdb.Environment(
            path=str(self._db_dir),
            readonly=True,
        ).__enter__()

        self._txn = self._env.begin(write=False).__enter__()

        self._cursor = self._txn.cursor().__enter__()

    def __enter__(self) -> DBReader:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> bool:

        self._cursor.__exit__(exc_type, exc_val, exc_tb)
        self._txn.__exit__(exc_type, exc_val, exc_tb)
        self._env.__exit__(exc_type, exc_val, exc_tb)  # type: ignore[union-attr]

        return True

    def __len__(self) -> int:

        n_total_entries: int = self._env.stat()["entries"]  # type: ignore[union-attr]

        return n_total_entries - len(self._special_keys)

    def __getitem__(self, key: object) -> simdjson.Object:

        key_str = str(key)

        raw_item = self._txn.get(key_str.encode())

        raw_data = self._extract_value(raw_value=raw_item)

        parser = simdjson.Parser()

        data = parser.parse(raw_data)

        if not isinstance(data, simdjson.Object):
            raise ValueError("Unexpected item type")

        return data

    def __iter__(self) -> typing.Iterator[str]:

        self._cursor.first()

        self._has_more = True

        return self

    def __next__(self) -> str:

        key: str | None = None

        while key is None or key in self._special_keys:

            if not self._has_more:
                raise StopIteration()

            key = self._cursor.key().decode()

            self._has_more = self._cursor.next()

        return key

    @staticmethod
    def _extract_value(raw_value: bytes) -> bytes:

        try:
            value = zlib.decompress(raw_value)
        except zlib.error as err:
            value = raw_value

        return value

    @property
    def most_recent_indexed(self) -> str:

        key = b"__most_recent_indexed"

        value = self._extract_value(
            raw_value=self._txn.get(key)
        ).decode()

        return value




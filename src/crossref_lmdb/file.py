"""
Interface for reading items from a file.
"""

from __future__ import annotations

import pathlib
import gzip
import typing
import logging

import crossref_lmdb.items
import crossref_lmdb.filt


LOGGER = logging.getLogger("crossref_lmdb")


class FileSource(crossref_lmdb.items.ItemSource):

    def __init__(
        self,
        public_data_dir: pathlib.Path,
        show_progress: bool,
        filter_func: crossref_lmdb.filt.FilterFunc | None,
        start_from_file_num: int,
    ) -> None:

        self.public_data_dir = public_data_dir
        self.start_from_file_num = start_from_file_num
        self._show_progress = show_progress
        self._filter_func = filter_func

        self.gz_paths = sorted(
            self.public_data_dir.glob("*.json.gz"),
            key=self.file_num_from_path,
        )

    @property
    def total_items(self) -> int:
        return len(self.gz_paths) - self.start_from_file_num

    @property
    def total_units(self) -> str:
        return "gz_files"

    @property
    def show_progress(self) -> bool:
        return self._show_progress

    @property
    def filter_func(self) -> crossref_lmdb.filt.FilterFunc | None:
        return self._filter_func

    @staticmethod
    def file_num_from_path(path: pathlib.Path) -> int:
        return int(path.name[: -len(".json.gz")])

    def iter_unfiltered_items_data(self) -> typing.Iterable[bytes]:

        LOGGER.info(f"Starting from file with number: {self.start_from_file_num}")

        for gz_path in self.gz_paths:

            file_num = self.file_num_from_path(gz_path)

            if file_num < self.start_from_file_num:
                continue

            with gzip.open(gz_path, "rb") as handle:

                data = handle.read()

                yield data

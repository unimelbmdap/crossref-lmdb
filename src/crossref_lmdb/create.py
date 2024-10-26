
from __future__ import annotations

import pathlib
import os
import dataclasses
import typing

import lmdb

import simdjson

import crossref_lmdb.utils


FilterFunc: typing.TypeAlias = typing.Callable[[simdjson.Object], bool]


@dataclasses.dataclass
class CreateParams:
    public_data_dir: pathlib.Path
    db_dir: pathlib.Path
    max_db_size_gb: float = 2000.0
    compression_level: int = -1
    commit_frequency: int = 1_000
    filter_path: pathlib.Path | None = None
    progress_bar: bool = True
    filter_func: FilterFunc | None = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.validate()
        self.set_filter_func()

    def set_filter_func(self) -> None:

        if self.filter_path is None:
            self.filter_func = None
            return

        filter_code = self.filter_path.read_text()

        filter_locals = crossref_lmdb.utils.run_code_from_text(code=filter_code)

        if "filter_func" not in filter_locals:
            raise ValueError(
                f"No function named `filter_func` present in {self.filter_path}"
            )

        self.filter_func = typing.cast(
            FilterFunc,
            filter_locals["filter_func"],
        )

    def validate(self) -> None:

        errors: list[str] = []

        if not self.public_data_dir.exists() or not self.public_data_dir.is_dir():
            errors.append(
                f"Public data directory ({self.public_data_dir}) does not exist "
                + "or is not a directory"
            )

        if not (self.public_data_dir / "0.json.gz").exists():
            errors.append(
                f"Public data directory ({self.public_data_dir}) does contain the "
                + "expected CrossRef data"
            )

        if not self.db_dir.exists():
            errors.append(
                f"Output database directory ({self.db_dir}) does not exist"
            )

        if self.db_dir.exists():

            with os.scandir(self.db_dir) as it:
                for item in it:
                    errors.append(
                        f"Output database directory ({self.db_dir}) is not empty"
                    )
                    break

        if self.max_db_size_gb <= 0:
            errors.append(
                f"Invalid maximum database size ({self.max_db_size_gb})"
            )

        if self.compression_level not in list(range(-1, 9 + 1)):
            errors.append(
                f"Invalid compression level ({self.compression_level})"
            )

        if self.commit_frequency <= 0:
            errors.append(
                f"Invalid commit frequency ({self.commit_frequency})"
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


def run(args: CreateParams) -> None:
    pass

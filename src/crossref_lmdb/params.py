"""
Handles the parameters for database creation and updating.
"""

from __future__ import annotations

import abc
import dataclasses
import pathlib
import datetime

import email_validator

import crossref_lmdb.filt


@dataclasses.dataclass
class Params(abc.ABC):
    db_dir: pathlib.Path
    max_db_size_gb: float
    filter_path: pathlib.Path | None
    compression_level: int
    show_progress: bool
    commit_frequency: int
    filter_func: crossref_lmdb.filt.FilterFunc | None = dataclasses.field(init=False)

    @property
    def max_db_size_bytes(self) -> int:

        multiplier = 1000 * 1000 * 1000  # MB  # KB  # B

        n_bytes = self.max_db_size_gb * multiplier

        if not isinstance(n_bytes, int) and not n_bytes.is_integer():
            msg = f"Unexpected number of bytes: {n_bytes}"
            raise ValueError(msg)

        return int(n_bytes)

    def __post_init__(self) -> None:
        self.validate()
        self.set_filter_func()

    def set_filter_func(self) -> None:

        if self.filter_path is None:
            self.filter_func = None
            return None

        self.filter_func = crossref_lmdb.filt.get_filter_func(
            filter_path=self.filter_path
        )

    def validate(self) -> None:

        errors = self._do_validation()

        if any(errors):
            error_msg = "\n".join(
                ["Encountered the following errors with the provided arguments:"]
                + [f"\t{error}" for error in errors]
            )
            raise ValueError(error_msg)

    def _do_validation(self) -> list[str]:

        errors: list[str] = []

        if not self.db_dir.exists():
            errors.append(f"Database directory ({self.db_dir}) does not exist")

        if self.max_db_size_gb <= 0:
            errors.append(f"Invalid maximum database size ({self.max_db_size_gb})")

        if self.compression_level not in list(range(-1, 9 + 1)):
            errors.append(f"Invalid compression level ({self.compression_level})")

        if self.filter_path is not None:

            if not self.filter_path.exists():
                errors.append(f"Filter path ({self.filter_path}) does not exist")

            if self.filter_path.exists() and self.filter_path.suffix != ".py":
                errors.append(
                    f"Filter path ({self.filter_path}) does not seem to be a Python "
                    + "source file (no .py extension)"
                )

        return errors


@dataclasses.dataclass
class UpdateParams(Params):
    email_address: str
    from_date: str | None
    filter_arg: str | None

    def _do_validation(self) -> list[str]:

        errors = super()._do_validation()

        try:
            email_validator.validate_email(
                self.email_address,
                check_deliverability=False,
            )
        except email_validator.EmailNotValidError as err:
            errors.append(str(err))

        if self.from_date is not None:
            for potential_date_format in [
                "%Y",
                "%Y-%m",
                "%Y-%m-%d",
            ]:

                try:
                    datetime.datetime.strptime(self.from_date, potential_date_format)
                except ValueError:
                    pass
                else:
                    break

            else:
                errors.append(f"From date `{self.from_date}` not in a valid format")

        return errors


@dataclasses.dataclass
class CreateParams(Params):
    public_data_dir: pathlib.Path
    start_from_file_num: int

    def _do_validation(self) -> list[str]:

        errors = super()._do_validation()

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

        return errors

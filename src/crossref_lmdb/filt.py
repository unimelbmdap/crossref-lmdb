"""
Handles the loading of a filtering function from a file.
"""

from __future__ import annotations

import typing
import pathlib

import simdjson


FilterFunc: typing.TypeAlias = typing.Callable[[simdjson.Object], bool]


def get_filter_func(filter_path: pathlib.Path) -> FilterFunc:

    filter_code = filter_path.read_text()

    filter_locals = run_code_from_text(code=filter_code)

    if "filter_func" not in filter_locals:
        raise ValueError(f"No function named `filter_func` present in {filter_path}")

    filter_func = typing.cast(
        FilterFunc,
        filter_locals["filter_func"],
    )

    return filter_func


def run_code_from_text(
    code: str,
) -> dict[str, object]:

    code_locals: dict[str, object] = {}

    exec(code, {}, code_locals)

    return code_locals

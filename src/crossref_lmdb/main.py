from __future__ import annotations

import pathlib
import typing
import enum
import datetime

import lmdb

import simdjson

import crossref_lmdb.params
import crossref_lmdb.web
import crossref_lmdb.file
import crossref_lmdb.items


class Action(enum.Enum):
    CREATE = "create"
    UPDATE = "update"

@typing.overload
def run(
    action: typing.Literal["create"],
    params: crossref_lmdb.params.CreateParams,
) -> None:
    ...
@typing.overload
def run(
    action: typing.Literal["update"],
    params: crossref_lmdb.params.UpdateParams,
) -> None:
    ...
def run(
    action: typing.Literal["create", "update"],
    params: crossref_lmdb.params.CreateParams | crossref_lmdb.params.UpdateParams,
) -> None:

    action_params = (
        typing.cast(
            crossref_lmdb.params.CreateParams,
            params,
        )
        if action == "create"
        else typing.cast(
            crossref_lmdb.params.UpdateParams,
            params,
        )
    )

    with lmdb.Environment(
        path=str(params.db_dir),
        readonly=False,
        map_size=params.max_db_size_bytes,
        subdir=True,
    ) as env:

        item_source: crossref_lmdb.items.ItemSource

        if action == "create":
            reveal_type(action_params)
            item_source = crossref_lmdb.file.FileSource(
                public_data_dir=params.public_data_dir,
                show_progress=params.show_progress,
                filter_func=params.filter_func,
                start_from_file_num=params.start_from_file_num,
            )
        else:
            item_source = crossref_lmdb.web.WebSource(
                email_address=params.email_address,
                from_date=params.from_date,
                show_progress=params.show_progress,
                filter_func=params.filter_func,
                filter_arg=params.filter_arg,
            )

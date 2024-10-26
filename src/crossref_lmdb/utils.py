from __future__ import annotations


def run_code_from_text(
    code: str,
) -> dict[str, object]:

    code_locals: dict[str, object] = {}

    exec(code, {}, code_locals)

    return code_locals

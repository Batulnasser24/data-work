from __future__ import annotations
import pandas as pd

def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    *,
    validate: str, #way of connection (many to one or one to one)
    suffixes: tuple[str, str] = ("", "_r"),
    ) -> pd.DataFrame:
    return left.merge(right, how="left", on=on, validate=validate, suffixes=suffixes)
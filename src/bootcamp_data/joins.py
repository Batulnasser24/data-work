from __future__ import annotations
import pandas as pd

def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    *,
    validate: str,
    suffixes: tuple[str, str] = ("", "_r"),
) -> pd.DataFrame:
    """
    Performs a left join between two DataFrames with mandatory validation.
    
    Args:
        left: The primary DataFrame.
        right: The DataFrame to join.
        on: Column name(s) to join on.
        validate: Type of join check ('one_to_one', 'one_to_many', 'many_to_one').
                 This ensures the relationship matches your business logic.
        suffixes: Suffixes to apply to overlapping column names.
    
    Returns:
        pd.DataFrame: The merged dataset.
    """
    return left.merge(
        right, 
        how="left", 
        on=on, 
        validate=validate, 
        suffixes=suffixes
    )
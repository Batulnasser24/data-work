import re
import pandas as pd
from typing import List, Dict, Optional

# Pre-compiled regex for stripping extra whitespace efficiently
_WS_PATTERN = re.compile(r"\s+")

def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes missing values in a DataFrame.
    Returns a DataFrame with counts and percentages.
    """
    return (
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Creates shadow columns (flags) for specified columns.
    Example: 'age' -> 'age__isna' (True/False)
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[f"{c}__isna"] = out[c].isna()
    return out

def normalize_text(s: pd.Series) -> pd.Series:
    """
    Performs text cleaning: strip, lowercase (casefold), 
    and collapses multiple spaces into one.
    """
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_WS_PATTERN, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: Dict[str, str]) -> pd.Series:
    """
    Maps categorical values based on a dictionary.
    Keeps the original value if not found in the map.
    """
    return s.map(lambda x: mapping.get(x, x))
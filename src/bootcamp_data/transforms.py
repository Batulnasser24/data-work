import re
import pandas as pd
from typing import List, Dict, Optional, Tuple

# --- Configuration & Constants ---
# Pre-compiled regex for efficiency in text cleaning
_WS_PATTERN = re.compile(r"\s+")

# --- 1. Text Transformation & Normalization ---

def normalize_text(s: pd.Series) -> pd.Series:
    """
    Cleans text by stripping, using casefold for robust matching,
    and collapsing multiple whitespaces into one.
    """
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_WS_PATTERN, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: Dict[str, str]) -> pd.Series:
    """
    Maps values in a series using a dictionary. 
    Returns original value if not found in the map.
    """
    return s.map(lambda x: mapping.get(x, x))

# --- 2. Temporal (Datetime) Engineering ---

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    """
    Converts a column to datetime objects safely.
    Uses 'coerce' to handle invalid date formats as NaT.
    """
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add common time grouping keys (month, day-of-week, hour, etc.)."""
   
    ts = pd.to_datetime(df[ts_col]) 
    
    return df.assign(
        date=ts.dt.date,          
        year=ts.dt.year,          
        month=ts.dt.to_period("M").astype("string"), 
        dow=ts.dt.day_name(),    
        hour=ts.dt.hour,          
    )

# --- 3. Outlier Detection & Handling ---

def iqr_bounds(s: pd.Series, k: float = 1.5) -> Tuple[float, float]:
    """
    Calculates the Lower and Upper bounds using the Interquartile Range (IQR).
    Commonly used to detect outliers.
    """
    x = s.dropna()
    if x.empty:
        return 0.0, 0.0
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """
    Adds a boolean flag column indicating if a value is an outlier 
    based on the IQR method. Format: 'column__is_outlier'
    """
    lo, hi = iqr_bounds(df[col], k=k)
    is_outlier = (df[col] < lo) | (df[col] > hi)
    return df.assign(**{f"{col}__is_outlier": is_outlier})

def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """
    Caps extreme values (outliers) at specific percentiles 
    to reduce their impact without removing them.
    """
    x = s.dropna()
    if x.empty:
        return s
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)

# --- 4. Missingness Handling & Reporting ---

def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a summary report of missing values (count and percentage).
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
    Adds boolean columns indicating if the original value was missing.
    Format: 'column_name__isna'
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[f"{c}__isna"] = out[c].isna()
    return out
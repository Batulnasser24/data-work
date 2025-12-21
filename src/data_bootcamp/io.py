import pandas as pd
from pathlib import Path

def read_orders_csv(path: Path) -> pd.DataFrame:
    """Reads a CSV file containing orders data and returns a DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Orders file not found at: {path}")
    
    print(f"📖 Reading orders from: {path.name}")
    return pd.read_csv(path)

def read_users_csv(path: Path) -> pd.DataFrame:
    """Reads a CSV file containing users data and returns a DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Users file not found at: {path}")
    
    print(f"📖 Reading users from: {path.name}")
    return pd.read_csv(path)

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Writes a DataFrame to a Parquet file, ensuring the directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"✅ Data successfully written to: {path}")

def read_parquet(path: Path) -> pd.DataFrame:
    """Reads a Parquet file and returns a DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found at: {path}")
        
    return pd.read_parquet(path)
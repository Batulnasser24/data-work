import pandas as pd
from pathlib import Path
 
try:
    from data_bootcamp.config import PROJ_PATHS
except ImportError:
    # في حال تشغيل الملف بشكل منفصل
    PROJ_PATHS = None

def read_orders_csv(path: Path) -> pd.DataFrame:
    """
    Reads a CSV file containing orders data and returns a DataFrame.
    """
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"❌ Error reading orders CSV at {path}: {e}")
        raise

def read_users_csv(path: Path) -> pd.DataFrame:
    """
    Reads a CSV file containing users data and returns a DataFrame.
    """
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"❌ Error reading users CSV at {path}: {e}")
        raise

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """
    Writes a DataFrame to a Parquet file.
    """
    try:
        # التأكد من وجود المجلد (processed أو غيره) قبل الحفظ
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        print(f"✅ Data successfully written to: {path.name}")
    except Exception as e:
        print(f"❌ Error writing Parquet file: {e}")
        raise

def read_parquet(path: Path) -> pd.DataFrame:
    """
    Reads a Parquet file and returns a DataFrame.
    """
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"❌ Error reading Parquet file: {e}")
        raise
import logging
import sys
import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# Setup Project Paths
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, write_parquet
from bootcamp_data.transforms import (
    add_missing_flags,
    normalize_text,
    apply_mapping,
    missingness_report
)
from bootcamp_data.quality import assert_in_range, assert_non_empty

# Logger Configuration
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("DataCleaning")

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures that numeric columns are correctly typed as numbers.
    This prevents comparison errors between strings and integers.
    """
    df = df.copy()
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    return df

def main():
    paths = make_paths(ROOT)
    log.info("🚀 Starting Data Cleaning Pipeline")

    try:
        # 1. Load Data
        df_raw = read_orders_csv(paths.raw / "orders.csv")
        assert_non_empty(df_raw, "orders_raw")
        log.info(f"Loaded {len(df_raw)} rows from raw/orders.csv")

        # 2. Enforce Schema (Fixes the Comparison Error)
        log.info("Enforcing numeric schema for amount and quantity...")
        df_numeric = enforce_schema(df_raw)

        # 3. Generate Missingness Artifact
        # We run this on df_numeric to see if schema enforcement created new NaNs
        report = missingness_report(df_numeric)
        report_path = ROOT / "reports" / "missingness_summary.csv"
        report_path.parent.mkdir(exist_ok=True)
        report.to_csv(report_path)
        log.info(f"Missingness report saved to {report_path}")

        # 4. Clean and Transform
        log.info("Applying text normalization and missingness flags...")
        status_map = {"paid": "paid", "refunded": "refund", "refund": "refund"}
        
        df_clean = (
            df_numeric.assign(
                status_clean=lambda d: apply_mapping(normalize_text(d["status"]), status_map)
            )
            .pipe(add_missing_flags, cols=["amount", "quantity"])
        )

        # 5. Data Quality Checks (Now safe from string vs int errors)
        log.info("Running range validation checks...")
        assert_in_range(df_clean["amount"], lo=0, name="order_amount")
        assert_in_range(df_clean["quantity"], lo=0, name="order_quantity")

        # 6. Export and Metadata
        paths.processed.mkdir(parents=True, exist_ok=True)
        output_file = paths.processed / "orders_clean.parquet"
        write_parquet(df_clean, output_file)
        
        # Save Run Metadata for tracking
        meta = {
            "execution_time": datetime.now(timezone.utc).isoformat(),
            "status": "success",
            "rows_processed": len(df_clean),
            "columns": df_clean.columns.tolist()
        }
        (paths.processed / "_run_meta.json").write_text(json.dumps(meta, indent=2))
        
        log.info(f"✅ Pipeline completed successfully. Output: {output_file}")

    except Exception as e:
        log.error(f"❌ Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
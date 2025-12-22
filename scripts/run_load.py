import json
import logging
import sys
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# 1. Project Path Configuration
# Ensures the script can find the 'src' directory relative to its location
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

# Internal project imports
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet

# 2. Logging Setup
# Standard formatting for professional debugging and monitoring
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(name)s: %(message)s"
)
log = logging.getLogger("DataIngestion")

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes data types to ensure consistency across the pipeline.
    Prevents errors during parquet writing and downstream analysis.
    """
    df = df.copy()
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    if 'quantity' in df.columns:
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    return df

def main() -> None:
    # Initialize directory paths
    p = make_paths(ROOT)
    
    log.info("🚀 Starting Day 1 Ingestion Pipeline...")

    try:
        # 3. Data Loading & Schema Enforcement
        log.info("📥 Loading raw datasets...")
        orders_raw = read_orders_csv(p.raw / "orders.csv")
        users_raw = read_users_csv(p.raw / "users.csv")

        log.info("🛠️ Enforcing data schema...")
        orders = enforce_schema(orders_raw)
        users = users_raw.copy() # Assuming users schema is simple for now

        log.info("📊 Stats: Orders=%d, Users=%d", len(orders), len(users))

        # 4. Persistence (Saving to Parquet)
        p.processed.mkdir(parents=True, exist_ok=True)
        out_orders = p.processed / "orders.parquet"
        out_users = p.processed / "users.parquet"

        log.info("💾 Saving datasets to Parquet format...")
        write_parquet(orders, out_orders)
        write_parquet(users, out_users)

        # 5. Metadata Collection (Reproducibility)
        # Tracks exactly when and what was processed
        run_metadata = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "pipeline_stage": "day1_load",
            "counts": {
                "orders_rows": int(len(orders)),
                "users_rows": int(len(users))
            },
            "output_files": {
                "orders": str(out_orders.relative_to(ROOT)),
                "users": str(out_users.relative_to(ROOT))
            }
        }
        
        meta_path = p.processed / "_run_meta.json"
        meta_path.write_text(json.dumps(run_metadata, indent=2), encoding="utf-8")

        log.info("✅ Processed output directory: %s", p.processed)
        log.info("📄 Metadata successfully logged to: %s", meta_path)
        log.info("✨ Pipeline finished successfully!")

    except Exception as e:
        log.error("❌ Pipeline failed: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
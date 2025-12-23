import logging
import sys
import pandas as pd
from pathlib import Path

# 1. Project Path Configuration
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

# Internal Project Imports
from bootcamp_data.config import make_paths
from bootcamp_data.quality import (
    require_columns, 
    assert_non_empty, 
    assert_unique_key
)
from bootcamp_data.transforms import (
    parse_datetime, 
    add_time_parts, 
    winsorize, 
    add_outlier_flag
)
from bootcamp_data.joins import safe_left_join

# 2. Logger Configuration
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("AnalyticsPipeline")

def main() -> None:
    # Initialize directory paths
    p = make_paths(ROOT)
    
    log.info("🚀 Starting Day 3: Analytics Table Pipeline...")

    # 3. Load Cleaned Datasets
    try:
        orders = pd.read_parquet(p.processed / "orders_clean.parquet")
        users  = pd.read_parquet(p.processed / "users.parquet")
    except FileNotFoundError as e:
        log.error(f"❌ Input files not found. Make sure Day 1 & 2 scripts ran successfully: {e}")
        return

    # 4. Data Quality Checks (Pre-Join)
    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")
    
    # Ensure users table is unique by user_id to prevent join explosion
    assert_unique_key(users, "user_id")

    # 5. Temporal Engineering (Time Intelligence)
    log.info("⏳ Processing timestamps and extracting time parts...")
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    n_missing_ts = int(orders_t["created_at"].isna().sum())
    log.info(f"Missing created_at after parse: {n_missing_ts} / {len(orders_t)}")

    # 6. Secure Left Join (Orders + Users)
    log.info("🔗 Performing safe join (many_to_one)...")
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # Validate Join Integrity
    assert len(joined) == len(orders_t), "⚠️ Row count changed! Potential join explosion detected."

    # Calculate Match Rate for Monitoring
    match_rate = 1.0 - float(joined["country"].isna().mean())
    log.info(f"Final Row Count: {len(joined)}")
    log.info(f"Country Match Rate: {round(match_rate, 3)}")

    # 7. Outlier Detection and Handling
    log.info("🛡️ Flagging outliers and winsorizing amount...")
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    # 8. Business Summary (Revenue by Country)
    log.info("📊 Generating business summary: Revenue by Country...")
    summary = (
        joined.groupby("country", dropna=False)
              .agg(
                  order_count=("order_id", "size"), 
                  total_revenue=("amount", "sum")
              )
              .reset_index()
              .sort_values("total_revenue", ascending=False)
    )

    # Print a clean report to the terminal
    print("\n" + "="*50)
    print("           REVENUE SUMMARY BY COUNTRY")
    print("="*50)
    print(summary.to_string(index=False))
    print("="*50 + "\n")

    # 9. Persistence (Exporting Outputs)
    # Save the main analytics table
    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)
    
    # Save the summary report to reports folder
    report_path = ROOT / "reports" / "revenue_by_country.csv"
    report_path.parent.mkdir(exist_ok=True)
    summary.to_csv(report_path, index=False)

    log.info(f"💾 Analytics table saved to: {out_path}")
    log.info(f"📊 Business report saved to: {report_path}")
    log.info("✨ Analytics pipeline finished successfully!")

if __name__ == "__main__":
    main()
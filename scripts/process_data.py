"""Process raw data into analytics table."""
import pandas as pd
import numpy as np
from pathlib import Path

def process_data():
    raw_dir = Path("data/raw").resolve()
    processed_dir = Path("data/processed").resolve()
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Load raw data
    orders = pd.read_csv(raw_dir / "orders.csv")
    users = pd.read_csv(raw_dir / "users.csv")
    
    # Clean and transform
    orders["created_at"] = pd.to_datetime(orders["created_at"], errors="coerce")
    orders["amount"] = pd.to_numeric(orders["amount"], errors="coerce")
    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce")
    
    # Standardize status
    orders["status_clean"] = orders["status"].str.lower().str.strip()
    orders["status_clean"] = orders["status_clean"].replace({
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
        "pending": "pending"
    })
    
    # Remove rows with critical missing values
    orders_clean = orders.dropna(subset=["order_id", "user_id", "created_at"])
    
    # Merge with users
    analytics = orders_clean.merge(users[["user_id", "country"]], on="user_id", how="left")
    
    # Add derived columns
    analytics["month"] = analytics["created_at"].dt.to_period("M").astype(str)
    analytics["date"] = analytics["created_at"].dt.date
    
    # Winsorize amounts (clip at 1st and 99th percentiles)
    q1 = analytics["amount"].quantile(0.01)
    q99 = analytics["amount"].quantile(0.99)
    analytics["amount_winsor"] = analytics["amount"].clip(q1, q99)
    
    # Save processed data
    analytics.to_parquet(processed_dir / "analytics_table.parquet", index=False)
    
    print(f"✅ Processed {len(analytics)} records")
    print(f"📊 Columns: {analytics.columns.tolist()}")
    print(f"💾 Saved to: {processed_dir / 'analytics_table.parquet'}")
    
    return analytics

if __name__ == "__main__":
    process_data()

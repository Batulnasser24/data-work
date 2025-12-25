"""Generate messy raw data for EDA."""
import pandas as pd
import numpy as np
from pathlib import Path

# Setup Path
data_dir = Path("data/raw").resolve()
data_dir.mkdir(parents=True, exist_ok=True)

def generate_messy_data(n_users=1000, n_orders=5000):
    # --- USERS DATA ---
    user_ids = [f"{i:04d}" for i in range(1, n_users + 1)]
    users = pd.DataFrame({
        "user_id": user_ids,
        "country": np.random.choice(["SA", "AE", "KW", "QA"], n_users),
        "signup_date": pd.date_range("2025-01-01", "2025-12-22", periods=n_users).strftime("%Y-%m-%d")
    })
    
    # --- ORDERS DATA ---
    status_options = ["Paid", "paid", "PAID", "Refunded", "refund", "refunded", "Pending"]
    full_date_range = pd.date_range(start="2025-01-01 00:00", end="2025-12-22 23:59", freq="min")

    orders = pd.DataFrame({
        "order_id": [f"A{i:04d}" for i in range(1, n_orders + 1)],
        "user_id": np.random.choice(user_ids, n_orders),
        "amount": np.random.uniform(5.0, 500.0, n_orders).round(2).astype(str),
        "quantity": np.random.randint(1, 10, n_orders).astype(str),
        "created_at": np.random.choice(full_date_range, n_orders),
        "status": np.random.choice(status_options, n_orders)
    })

    # 1. Inject 5% Conflicting Duplicates
    num_dupes = int(n_orders * 0.05)
    dupes = orders.iloc[:num_dupes].copy()
    dupes["created_at"] = dupes["created_at"] + pd.Timedelta(hours=5)
    dupes["status"] = "refunded"
    orders = pd.concat([orders, dupes], ignore_index=True).sample(frac=1).reset_index(drop=True)

    # 2. Inject Random Empty Values (NaNs) in 10% of rows
    cols_to_mess_up = ["amount", "quantity", "created_at", "status"]
    for col in cols_to_mess_up:
        mask = np.random.random(len(orders)) < 0.10
        orders.loc[mask, col] = np.nan

    # 3. Format dates to string
    orders["created_at"] = pd.to_datetime(orders["created_at"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Save
    users.to_csv(data_dir / "users.csv", index=False)
    orders.to_csv(data_dir / "orders.csv", index=False)
    
    print(f"✅ Created {len(users)} users and {len(orders)} orders.")
    print(f"⚠️ Injected null values into ~10% of order data (excluding IDs).")
    
    return users, orders

if __name__ == "__main__":
    generate_messy_data()

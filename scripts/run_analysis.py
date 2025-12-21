import sys
from pathlib import Path

# Setup Path
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR / "src"))

from data_bootcamp.config import PROJ_PATHS
from data_bootcamp.io import read_users_csv, write_parquet

def main():
    # 1. Define paths using PROJ_PATHS
    input_file = PROJ_PATHS.raw / "users.csv"
    output_file = PROJ_PATHS.processed / "users_processed.parquet"

    try:
        # 2. Ingest
        users_df = read_users_csv(input_file)
        
        # 3. Simple Process (Example: Drop duplicates)
        users_df = users_df.drop_duplicates()
        
        # 4. Save
        write_parquet(users_df, output_file)
        
        print("\n🚀 Pipeline finished successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
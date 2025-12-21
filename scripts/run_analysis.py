import sys
from pathlib import Path

# 1. تحديد جذر المشروع (Project Root)
#parents[0] هو مجلد scripts، و parents[1] هو المجلد الرئيسي DATA-WORK
ROOT = Path(__file__).resolve().parents[1]

# 2. إضافة مجلد src إلى sys.path لتمكين الاستيراد من data_bootcamp
SRC_PATH = str(ROOT / "src")
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# 3. الآن نقوم باستيراد الأدوات التي برمجناها
try:
    from data_bootcamp.config import PROJ_PATHS
    from data_bootcamp.io import read_orders_csv, write_parquet
    print("✅ Successfully linked to data_bootcamp package.")
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)

def main():
    print(f"🚀 Running analysis from: {ROOT}")
    
    # مثال: قراءة ملف من مجلد raw
    # افترضنا وجود ملف اسمه orders.csv
    input_path = PROJ_PATHS.raw / "orders.csv"
    
    if input_path.exists():
        print(f"📦 Loading data from: {input_path}")
        df = read_orders_csv(input_path)
        
        # عرض أول 5 أسطر للتأكد
        print("📊 Data Preview:")
        print(df.head())
        
        # هنا يمكنك إضافة كود التحليل الخاص بك...
    else:
        print(f"⚠️ Warning: No file found at {input_path}")
        print("💡 Please place your orders.csv file in the data/raw folder.")

if __name__ == "__main__":
    main()
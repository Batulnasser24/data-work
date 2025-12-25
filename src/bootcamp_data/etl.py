import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
import pandas as pd

# --- 1. تعريف الإعدادات (Configuration) ---
@dataclass(frozen=True)
class ETLConfig:
    """صندوق يحتوي على جميع مسارات المجلدات والملفات المستخدمة في المشروع"""
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

    @classmethod
    def from_root(cls, root: Path) -> "ETLConfig":
        """دالة مساعدة لإنشاء المسارات تلقائياً بناءً على المجلد الرئيسي"""
        data = root / "data"
        return cls(
            root=root,
            raw_orders=data / "raw" / "orders.csv",
            raw_users=data / "raw" / "users.csv",
            out_orders_clean=data / "processed" / "orders_clean.parquet",
            out_users=data / "processed" / "users.parquet",
            out_analytics=data / "processed" / "analytics_table.parquet",
            run_meta=data / "processed" / "_run_meta.json"
        )

# --- 2. الاستيراد من ملفات المشروع الأخرى ---
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
# لاحظي هنا: استوردنا transform فقط، ولم نستورد ETLConfig لأنها معرفة بالأعلى
from bootcamp_data.transforms import transform 

log = logging.getLogger(__name__)

# --- 3. تحميل البيانات ---
def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info("Loading raw CSV files...")
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users

# --- 4. حفظ المخرجات ---
def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    """حفظ النتائج النهائية بشكل منظم."""
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    # فصل أعمدة المستخدمين للحصول على جدول طلبات نظيف
    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns] + [
        c for c in analytics.columns if c.endswith("_user")
    ]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)

# --- 5. تسجيل الميتا داتا ---
def write_run_meta(
    cfg: ETLConfig, *, orders_raw: pd.DataFrame, users: pd.DataFrame, analytics: pd.DataFrame
) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean())
        if "country" in analytics.columns
        else None
    )

    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_out_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")

# --- 6. المايسترو (The Orchestrator) ---
def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    log.info("🚀 Starting ETL Pipeline")
    
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics=analytics, users=users, cfg=cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, orders_raw=orders_raw, users=users, analytics=analytics)
    
    log.info("✅ ETL Pipeline completed successfully!")
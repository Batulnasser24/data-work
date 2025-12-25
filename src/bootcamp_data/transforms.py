import re
import pandas as pd
from typing import List, Dict, Tuple

# استيراد أدوات الجودة والربط من الملفات المخصصة لها
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.joins import safe_left_join

# --- الإعدادات والثوابت ---
_WS_PATTERN = re.compile(r"\s+")

# --- 1. معالجة النصوص وتنظيفها ---

def normalize_text(s: pd.Series) -> pd.Series:
    """تنظيف النصوص: حذف المسافات الزائدة، توحيد حالة الأحرف (صغير)، ودمج المسافات المتعددة."""
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_WS_PATTERN, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: Dict[str, str]) -> pd.Series:
    """استبدال القيم بناءً على قاموس محدد؛ مع الاحتفاظ بالقيمة الأصلية إذا لم توجد في القاموس."""
    return s.map(lambda x: mapping.get(x, x))

# --- 2. هندسة البيانات الزمنية (Datetime) ---

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    """تحويل العمود إلى صيغة تاريخ ووقت بأمان (وضع NaT في حال وجود خطأ)."""
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """استخراج أجزاء الوقت (السنة، الشهر، اليوم، الساعة) لعمل التحليلات لاحقاً."""
    ts = pd.to_datetime(df[ts_col]) 
    return df.assign(
        date=ts.dt.date,          
        year=ts.dt.year,          
        month=ts.dt.to_period("M").astype("string"), 
        dow=ts.dt.day_name(),    
        hour=ts.dt.hour,          
    )

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """فرض السكيما: التأكد من أنواع الأعمدة (نصوص للمعرفات، أرقام للمبالغ والكميات)."""
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    )

# --- 3. اكتشاف ومعالجة القيم الشاذة (Outliers) ---

def iqr_bounds(s: pd.Series, k: float = 1.5) -> Tuple[float, float]:
    """حساب الحدود الدنيا والعليا باستخدام المدى الربيعي (IQR)."""
    x = s.dropna()
    if x.empty:
        return 0.0, 0.0
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """إضافة عمود يحدد ما إذا كانت القيمة شاذة أم لا (True/False)."""
    lo, hi = iqr_bounds(df[col], k=k)
    is_outlier = (df[col] < lo) | (df[col] > hi)
    return df.assign(**{f"{col}__is_outlier": is_outlier})

def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """تقليص القيم المتطرفة جداً وحصرها بين نسب مئوية محددة (Capping)."""
    x = s.dropna()
    if x.empty:
        return s
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)

# --- 4. معالجة البيانات المفقودة ---
def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    توليد تقرير ملخص للقيم المفقودة (العدد والنسبة المئوية).
    """
    return (
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """إضافة أعمدة توضح ما إذا كانت البيانات الأصلية مفقودة قبل المعالجة."""
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[f"{c}__isna"] = out[c].isna()
    return out

# --- 5. دالة التحويل الرئيسية (The Orchestrator) ---

def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    """
    المنسق الرئيسي لعمليات التحويل:
    1. التحقق من جودة البيانات (Quality Checks).
    2. تنظيف وفرض السكيما.
    3. الربط مع بيانات المستخدمين.
    4. معالجة القيم الشاذة.
    """
    # 1. فحوصات الفشل السريع (Fail-fast)
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    # 2. خريطة تنظيف الحالات (Status Mapping)
    status_map = {"paid": "paid", "refund": "refund", "refunded": "refund"}

    # 3. معالجة جدول الطلبات (Pipeline Chaining)
    orders = (
        orders_raw
        .pipe(enforce_schema)
        .assign(
            status_clean=lambda d: apply_mapping(normalize_text(d["status"]), status_map)
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    # 4. الربط مع المستخدمين (Join)
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # 5. التحقق بعد الربط ومعالجة القيم الشاذة للمبالغ
    assert len(joined) == len(orders), "Row count changed (join explosion?)"
    
    joined = (
        joined
        .assign(amount_winsor=winsorize(joined["amount"]))
        .pipe(add_outlier_flag, col="amount", k=1.5)
    )

    return joined
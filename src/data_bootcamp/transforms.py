import pandas as pd

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    يتأكد من أن أعمدة البيانات لها الأنواع الصحيحة (Strings للـ IDs و Numbers للمبالغ).
    """
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        # تحويل المبالغ إلى أرقام عشرية مع معالجة الأخطاء
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        # تحويل الكمية إلى أرقام صحيحة مع معالجة الأخطاء
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    )
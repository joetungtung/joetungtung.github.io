# 嘗試的時間欄位清單（依序檢查）
TIME_CANDIDATES = [
    "event_time",
    "endTime", "startTime",
    "deviceReceiptTime", "managerReceiptTime",
    "Manager Receipt Time",
    "Event Start Time", "Event End Time"
]

def pick_time_column(df):
    """從候選清單裡找出第一個存在的時間欄位"""
    for col in TIME_CANDIDATES:
        if col in df.columns:
            return col
    return None

def parse_time_series(s):
    """
    嘗試把各種格式轉成 UTC datetime：
    - ISO8601
    - UNIX epoch 秒 / 毫秒
    """
    def _to_ts(x):
        try:
            if pd.isna(x):
                return pd.NaT
            xs = str(x).strip()
            if xs.isdigit():
                iv = int(xs)
                if iv > 10_000_000_000:  # 毫秒 epoch
                    return pd.to_datetime(iv, unit="ms", utc=True)
                else:                     # 秒 epoch
                    return pd.to_datetime(iv, unit="s", utc=True)
            return pd.to_datetime(xs, utc=True, errors="coerce", infer_datetime_format=True)
        except Exception:
            return pd.NaT

    return s.apply(_to_ts)

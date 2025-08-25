from datetime import timezone
import pandas as pd
import numpy as np

LOCAL_TZ = "Asia/Taipei"

def parse_time_series(xs: pd.Series) -> pd.Series:
    s = xs.copy()

    # 數值型：epoch（秒/毫秒/微秒）
    if np.issubdtype(s.dtype, np.number):
        s = pd.to_numeric(s, errors="coerce")
        # 判斷位數：秒(<=1e10), 毫秒(<=1e13), 微秒(>1e13)
        sec   = s[(s > 1e9)  & (s <= 1e11)]
        msec  = s[(s > 1e11) & (s <= 1e14)]
        usec  = s[(s > 1e14)]
        out = pd.Series(index=s.index, dtype="datetime64[ns, UTC]")

        if not sec.empty:
            out.loc[sec.index]  = pd.to_datetime(sec, unit="s", utc=True, errors="coerce")
        if not msec.empty:
            out.loc[msec.index] = pd.to_datetime(msec, unit="ms", utc=True, errors="coerce")
        if not usec.empty:
            out.loc[usec.index] = pd.to_datetime(usec, unit="us", utc=True, errors="coerce")
        return out

    # 字串型：先嘗試 ArcSight 常見格式（AM/PM）
    s = s.astype(str).str.strip()
    # 例：8/27/25 5:30:11 PM 或 08/27/2025 17:30:11
    try_formats = [
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ]
    dt = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in try_formats:
        mask = dt.isna()
        if not mask.any():
            break
        dt.loc[mask] = pd.to_datetime(s[mask], format=fmt, errors="coerce")

    # 剩下交給自動解析（最後手段）
    dt = dt.fillna(pd.to_datetime(s, errors="coerce", utc=False))

    # 指定為本地時區再轉 UTC（避免被當成 UTC 或其他時區）
    dt = dt.dt.tz_localize(LOCAL_TZ, nonexistent="shift_forward", ambiguous="NaT", errors="coerce")
    return dt.dt.tz_convert(timezone.utc)
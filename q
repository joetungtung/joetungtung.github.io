def parse_time_series(xs: pd.Series) -> pd.Series:
    s = xs.copy()

    # --- 1) 數值型 epoch（自動判斷秒/毫秒/微秒）---
    if np.issubdtype(s.dtype, np.number):
        s = pd.to_numeric(s, errors="coerce")
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns, UTC]")
        sec  = s[(s > 1e9)  & (s <= 1e11)]
        msec = s[(s > 1e11) & (s <= 1e14)]
        usec = s[(s > 1e14)]
        if not sec.empty:
            out.loc[sec.index]  = pd.to_datetime(sec,  unit="s",  utc=True, errors="coerce")
        if not msec.empty:
            out.loc[msec.index] = pd.to_datetime(msec, unit="ms", utc=True, errors="coerce")
        if not usec.empty:
            out.loc[usec.index] = pd.to_datetime(usec, unit="us", utc=True, errors="coerce")
        return out

    # --- 2) 字串型（先試常見格式，再交給自動解析）---
    s = s.astype(str).str.strip()
    try_formats = [
        "%m/%d/%y %I:%M:%S %p",   # 08/27/25 5:30:11 PM
        "%m/%d/%Y %I:%M:%S %p",   # 08/27/2025 5:30:11 PM
        "%Y-%m-%d %H:%M:%S",      # 2025-08-27 17:30:11
        "%Y/%m/%d %H:%M:%S",      # 2025/08/27 17:30:11
    ]
    dt = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in try_formats:
        mask = dt.isna()
        if not mask.any():
            break
        dt.loc[mask] = pd.to_datetime(s[mask], format=fmt, errors="coerce")

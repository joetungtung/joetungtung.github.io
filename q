def parse_time_series(xs: pd.Series) -> pd.Series:
    """
    把各種時間表示（epoch / 多種日期字串）轉成 UTC 的 pandas Series。
    支援: 
      - epoch 秒/毫秒/微秒
      - 12h: 08/23/25 12:36:27 PM、08-23-25 12-36-27 PM
      - 24h: 2025-08-23 17:36:27、2025/08/23 17:36:27
      - 上述含毫秒 .fff
    解析失敗的元素保留 NaT（上游會 dropna）。
    """
    s0 = xs  # 原始
    # -------- 1) 數值型 epoch（優先）---------
    if np.issubdtype(s0.dtype, np.number) or pd.to_numeric(s0, errors="coerce").notna().all():
        v = pd.to_numeric(s0, errors="coerce")
        out = pd.Series(pd.NaT, index=v.index, dtype="datetime64[ns, UTC]")
        sec  = v[(v > 1e9)  & (v <= 1e11)]
        msec = v[(v > 1e11) & (v <= 1e14)]
        usec = v[(v > 1e14)]
        if not sec.empty:  out.loc[sec.index]  = pd.to_datetime(sec,  unit="s",  utc=True, errors="coerce")
        if not msec.empty: out.loc[msec.index] = pd.to_datetime(msec, unit="ms", utc=True, errors="coerce")
        if not usec.empty: out.loc[usec.index] = pd.to_datetime(usec, unit="us", utc=True, errors="coerce")
        return out

    # -------- 2) 字串型（向量化 + 多格式 + fallback）---------
    s = s0.astype(str).str.strip()
    dt = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")  # 先做成「無時區」的

    # (a) AM/PM：同時支援 / 與 -，時間分隔 : 或 -
    # 先把 - 形式正規化成 / 與 :
    ampm_mask = s.str.contains(r"\b(?:AM|PM)\b", case=False, na=False, regex=True)
    if ampm_mask.any():
        sa = s.where(ampm_mask)
        sa = sa.str.replace(r"^(\d{1,2})-(\d{1,2})-(\d{2,4})", r"\1/\2/\3", regex=True)
        sa = sa.str.replace(r"(\d{1,2})-(\d{2})-(\d{2})\s*(AM|PM)", r"\1:\2:\3 \4", regex=True, case=False)

        # 嘗試有/無 4 位年份、有/無毫秒
        fmts_ampm = [
            "%m/%d/%Y %I:%M:%S %p",
            "%m/%d/%y %I:%M:%S %p",
            "%m/%d/%Y %I:%M:%S.%f %p",
            "%m/%d/%y %I:%M:%S.%f %p",
        ]
        tmp = pd.Series(pd.NaT, index=sa.index, dtype="datetime64[ns]")
        for fmt in fmts_ampm:
            mask = tmp.isna()
            if not mask.any(): break
            parsed = pd.to_datetime(sa.where(mask), format=fmt, errors="coerce")
            tmp.loc[parsed.notna()] = parsed[parsed.notna()]
        dt.loc[tmp.notna()] = tmp[tmp.notna()]

    # (b) 24h：- 或 /，有/無毫秒
    mask24 = dt.isna()  # 尚未成功者
    if mask24.any():
        s24 = s.where(mask24)
        # 如果時間用 - 分隔，把 HH-MM-SS 轉回 HH:MM:SS（避免誤動到日期）
        s24 = s24.str.replace(r"(\s\d{1,2})-(\d{2})-(\d{2})\b", r"\1:\2:\3", regex=True)
        fmts_24 = [
            "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S.%f",
            "%m/%d/%Y %H:%M:%S.%f", "%m-%d-%Y %H:%M:%S.%f",
        ]
        for fmt in fmts_24:
            mask = dt.isna()
            if not mask.any(): break
            parsed = pd.to_datetime(s24.where(mask), format=fmt, errors="coerce")
            dt.loc[parsed.notna()] = parsed[parsed.notna()]

    # (c) 落網之魚：通用 fallback（速度仍可接受，rows~萬級OK）
    still = dt.isna()
    if still.any():
        parsed = pd.to_datetime(s.where(still), errors="coerce", cache=True)
        dt.loc[parsed.notna()] = parsed[parsed.notna()]

    # 全部視為本地時間（台北），再轉 UTC
    out = pd.Series(pd.NaT, index=dt.index, dtype="datetime64[ns, UTC]")
    ok = dt.notna()
    if ok.any():
        out.loc[ok] = dt.loc[ok].dt.tz_localize("Asia/Taipei").dt.tz_convert("UTC")
    return out
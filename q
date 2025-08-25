# --- 2) 字串型（向量化、明確格式；避免 dateutil fallback） ---
s = s.astype(str).str.strip()
out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns, UTC]")

# (a) 12小時制：08/27/2025 5:30:11 PM 或 08/27/25 5:30:11 PM
mask_ampm = s.str.contains(r"(AM|PM)", case=False, na=False)
if mask_ampm.any():
    mY4 = pd.to_datetime(
        s.where(mask_ampm),
        format="%m/%d/%Y %I:%M:%S %p",
        errors="coerce",
    )
    mY2 = pd.to_datetime(
        s.where(mask_ampm),
        format="%m/%d/%y %I:%M:%S %p",
        errors="coerce",
    )
    m = mY4.fillna(mY2)
    if m.notna().any():
        out.loc[m.notna()] = (
            m[m.notna()]
            .dt.tz_localize("Asia/Taipei")
            .dt.tz_convert("UTC")
        )

# (b) 24小時制：2025-08-27 17:30:11 或 2025/08/27 17:30:11
mask_24h = ~mask_ampm
if mask_24h.any():
    m1 = pd.to_datetime(s.where(mask_24h), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    m2 = pd.to_datetime(s.where(mask_24h), format="%Y/%m/%d %H:%M:%S", errors="coerce")
    m = m1.fillna(m2)
    if m.notna().any():
        out.loc[m.notna()] = (
            m[m.notna()]
            .dt.tz_localize("Asia/Taipei")
            .dt.tz_convert("UTC")
        )

return out





# 先把數值型欄位統一成 float（避免 422 型別衝突）
if "bytes" in df.columns:
    df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce").astype("float64")

# 要當 tag 的欄位（存在才加入）
tag_cols = [c for c in ["src_ip", "dst_ip", "severity", "device", "protocol", "name"] if c in df.columns]

# 直接用 DataFrame 一次寫入（最快）
write_api.write(
    bucket=BUCKET,
    org=ORG,  # 建議用 orgID；跟你 curl 成功那個一致
    record=df,
    data_frame_measurement_name="arcsight_event",
    data_frame_tag_columns=tag_cols,
    data_frame_timestamp_column="event_ts"
)

print(f"[OK] wrote {len(df)} points to {BUCKET}/arcsight_event (time col: {time_col})")

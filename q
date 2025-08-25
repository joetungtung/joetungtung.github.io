# --- 2) 字串型（先正規化，再向量化解析；避免 dateutil fallback） ---
import re

s = s.astype(str).str.strip()
out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns, UTC]")

# 先把明顯的「AM/PM 但用 - 作分隔」正規化成我們要的格式
# 例如：8-23-25 12-36-27 PM  ->  8/23/25 12:36:27 PM
mask_ampm = s.str.contains(r"\b(?:AM|PM)\b", case=False, na=False, regex=True)
if mask_ampm.any():
    s_ampm = s.where(mask_ampm)
    # 日期  m-d-yy 或 m-d-yyyy  ->  m/d/yy 或 m/d/yyyy
    s_ampm = s_ampm.str.replace(r"^(\d{1,2})-(\d{1,2})-(\d{2,4})", r"\1/\2/\3", regex=True)
    # 時間  HH-MM-SS AM/PM     ->  HH:MM:SS AM/PM
    s_ampm = s_ampm.str.replace(r"(\d{1,2})-(\d{2})-(\d{2})\s*(AM|PM)", r"\1:\2:\3 \4", regex=True, case=False)

    mY4 = pd.to_datetime(s_ampm, format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
    mY2 = pd.to_datetime(s_ampm, format="%m/%d/%y %I:%M:%S %p",  errors="coerce")
    m = mY4.fillna(mY2)
    if m.notna().any():
        out.loc[m.notna()] = (
            m[m.notna()]
            .dt.tz_localize("Asia/Taipei")
            .dt.tz_convert("UTC")
        )

# 其餘（沒有 AM/PM）的 24h 制（同樣可能用 / 或 -）
mask_24h = ~mask_ampm
if mask_24h.any():
    s_24 = s.where(mask_24h)
    # 常見兩種：2025-08-23 17:30:11、2025/08/23 17:30:11
    # 如果時間用 - 分隔，先轉成 :
    s_24 = s_24.str.replace(r"(\d{1,2})-(\d{2})-(\d{2})(?!\d)", r"\1:\2:\3", regex=True)
    m1 = pd.to_datetime(s_24, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    m2 = pd.to_datetime(s_24, format="%Y/%m/%d %H:%M:%S", errors="coerce")
    m = m1.fillna(m2)
    if m.notna().any():
        out.loc[m.notna()] = (
            m[m.notna()]
            .dt.tz_localize("Asia/Taipei")
            .dt.tz_convert("UTC")
        )

return out
# 2) 欄位名正規化：去 BOM、去空白、全部小寫、空白→底線
df.columns = [str(c).replace("\ufeff", "").strip().lower().replace(" ", "_") for c in df.columns]
print("[DEBUG] columns(normalized):", list(df.columns))

# 3) 在「正規化後」的欄位中選時間欄位
TIME_CANDIDATES_NORM = [
    "event_time", "manager_receipt_time", "device_receipt_time",
    "end_time", "start_time", "event_start_time", "event_end_time",
]
time_col = next((c for c in TIME_CANDIDATES_NORM if c in df.columns), None)
if not time_col:
    raise ValueError(f"missing time column; tried: {TIME_CANDIDATES_NORM}")
print(f"[DEBUG] using time column: {time_col}")

# 4) 解析時間 → 固定建立 'event_ts'
raw_ts = df[time_col].copy()
df["event_ts"] = parse_time_series(df[time_col])

# 驗證 event_ts 是否成功建立
if "event_ts" not in df.columns:
    raise RuntimeError("event_ts was not created")
df = df.dropna(subset=["event_ts"])
if df.empty:
    raise ValueError(
        f"all timestamps in '{time_col}' are invalid after parsing; examples={raw_ts.head(3).tolist()}"
    )

print("[DEBUG] ts_min =", df["event_ts"].min(), "ts_max =", df["event_ts"].max(), "rows =", len(df))

# 5) 數值欄位型別（避免 422）
for num_col in ["bytes", "src_port", "dst_port"]:
    if num_col in df.columns:
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce").astype("float64")

# 6) 決定 tag 欄位（存在才用）
TAG_CANDIDATES = [
    "device_vendor", "agent_name", "agent_type", "agent_id",
    "transport_protocol", "device_action",
    "attacker_geo_country_name", "target_geo_country_name",
    "attacker_address", "attacker_port", "target_address", "target_port",
]
tag_cols = [c for c in TAG_CANDIDATES if c in df.columns]
for c in tag_cols:
    df[c] = df[c].astype(str)   # tag 欄位強制轉字串

print("[DEBUG] tag_cols used:", tag_cols)
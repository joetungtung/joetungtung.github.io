# 建議當 tag 的欄位（存在才用）。你可以依你的 CSV 欄名調整/擴充
TAG_CANDIDATES = [
    "src_ip", "dst_ip", "src_port", "dst_port",
    "Attacker Address", "Target Address",
    "Agent Name", "Agent ID", "Agent Severity",
    "Device Vendor", "Device Product", "Device Action", "Protocol"
]

# 若你想把欄名統一成好讀的 key（可選）：
# 例：把空白改底線，大小寫統一
rename_map = {c: c.strip().replace(" ", "_") for c in TAG_CANDIDATES if c in df.columns}
df = df.rename(columns=rename_map)

# 重新以改名後的集合決定 tag 欄位清單
tag_cols = [rename_map.get(c, c) for c in TAG_CANDIDATES if rename_map.get(c, c) in df.columns]

# 數值欄位型別（避免 422）
for num_col in ["bytes", "src_port", "dst_port"]:
    if num_col in df.columns:
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce").astype("float64")




write_api.write(
    bucket=BUCKET,
    org=ORG,  # 建議用 orgID
    record=df,
    data_frame_measurement_name="arcsight_event",
    data_frame_tag_columns=tag_cols,        # ← 就是這裡
    data_frame_timestamp_column="event_ts"
)
print(f"[OK] wrote {len(df)} points to {BUCKET}/arcsight_event with tags={tag_cols}")

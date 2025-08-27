# 2) 欄位名正規化：去 BOM、去空白、全部小寫、空白→底線
df.columns = (
    df.columns
      .map(lambda c: str(c).replace("\ufeff", ""))
      .map(lambda c: c.strip().lower().replace(" ", "_"))
)
print("[DEBUG] columns(normalized):", list(df.columns))

# 建議當 tag 的欄位（存在才用）
TAG_CANDIDATES = [
    "device_vendor", "agent_name", "agent_type", "agent_id",
    "transport_protocol", "device_action",
    "attacker_geo_country_name", "target_geo_country_name",
    "attacker_address", "attacker_port", "target_address", "target_port",
]

# 只挑出真的存在於 DataFrame 的欄位當 tag
tag_cols = [c for c in TAG_CANDIDATES if c in df.columns]

# tag 欄位強制轉字串（避免 dtype 造成落到 field）
for c in tag_cols:
    df[c] = df[c].astype(str)

print("[DEBUG] tag_cols used:", tag_cols)
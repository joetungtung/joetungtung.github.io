# --- Country → (lat, lon) ---------------
# 先嘗試載入同目錄的 country_centroids.csv（可自備、易擴充）
# 檔案格式：country,lat,lon   例如：Taiwan,23.6978,120.9605
_COUNTRY_MAP = {}

def _load_country_csv():
    import csv, os
    path = os.path.join(os.path.dirname(__file__), "country_centroids.csv")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                k = str(row["country"]).strip().lower()
                try:
                    _COUNTRY_MAP[k] = (float(row["lat"]), float(row["lon"]))
                except Exception:
                    pass

# 內建一份常見國家座標（不夠可以用 CSV 覆蓋/擴充）
_BUILTIN_COUNTRIES = {
    "taiwan": (23.6978, 120.9605),
    "japan": (36.2048, 138.2529),
    "united states": (37.0902, -95.7129),
    "united kingdom": (55.3781, -3.4360),
    "germany": (51.1657, 10.4515),
    "canada": (56.1304, -106.3468),
    "china": (35.8617, 104.1954),
    "south korea": (35.9078, 127.7669),
    "russia": (61.5240, 105.3188),
    "france": (46.2276, 2.2137),
    "india": (20.5937, 78.9629),
    "australia": (-25.2744, 133.7751),
    "poland": (51.9194, 19.1451),
    "afghanistan": (33.9391, 67.7100),
}

# 常見別名（大小寫、縮寫統一）
_ALIASES = {
    "us": "united states",
    "usa": "united states",
    "u.s.": "united states",
    "uk": "united kingdom",
    "great britain": "united kingdom",
    "south korea": "south korea",
    "korea, republic of": "south korea",
    "russian federation": "russia",
    "u.a.e.": "united arab emirates",
}

def _norm_country(name: str) -> str:
    if name is None:
        return ""
    s = str(name).strip().lower()
    if not s:
        return s
    s = _ALIASES.get(s, s)
    return s

def country_to_coords(name: str):
    """
    回傳 (lat, lon)。找不到回傳 (nan, nan)。
    CSV > 內建表 > 失敗→NaN
    """
    import numpy as np
    k = _norm_country(name)
    if not _COUNTRY_MAP:
        # 第一次呼叫時嘗試載入 CSV
        _load_country_csv()
    if k in _COUNTRY_MAP:
        return _COUNTRY_MAP[k]
    if k in _BUILTIN_COUNTRIES:
        return _BUILTIN_COUNTRIES[k]
    return (np.nan, np.nan)





# --- 產生 src/dst 國名（供 Route 與統計使用） ---
# 你的 CSV 欄位（已經標準化成底線小寫），請依實際名稱對應：
SRC_COUNTRY_COL = "attacker_geo_country_name"
DST_COUNTRY_COL = "target_geo_country_name"

# 若不存在就補空字串，避免後續 map 爆掉
for col in [SRC_COUNTRY_COL, DST_COUNTRY_COL]:
    if col not in df.columns:
        df[col] = ""

# 另外保留簡短欄位名（在 Query 比較好打）
df["src"] = df[SRC_COUNTRY_COL].astype(str)
df["dst"] = df[DST_COUNTRY_COL].astype(str)

# --- 轉換成座標欄位（給 Geomap Route: Coords 用） ---
def _col_to_coords(series):
    # series → 兩個 list：lat_list, lon_list
    lats, lons = [], []
    for v in series.astype(str).fillna(""):
        lat, lon = country_to_coords(v)
        lats.append(lat)
        lons.append(lon)
    return lats, lons

src_lats, src_lons = _col_to_coords(df["src"])
dst_lats, dst_lons = _col_to_coords(df["dst"])

df["src_lat"] = pd.to_numeric(src_lats, errors="coerce").astype("float64")
df["src_lon"] = pd.to_numeric(src_lons, errors="coerce").astype("float64")
df["dst_lat"] = pd.to_numeric(dst_lats, errors="coerce").astype("float64")
df["dst_lon"] = pd.to_numeric(dst_lons, errors="coerce").astype("float64")
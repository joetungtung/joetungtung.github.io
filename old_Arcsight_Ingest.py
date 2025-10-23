import os
import time
import pandas as pd
import numpy as np
from datetime import timezone
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS


LOCAL_TZ = "Asia/Taipei"

# ======== 基本設定（請依你的環境修改） ========
INFLUX_URL   = "http://127.0.0.1:8086"
INFLUX_TOKEN = "TUNNT9zHsfHU2nFhjxL63i8HMpqLpSGv5J5hzrq9x-79DAxmTaOv5EbAr31OZaaz1zVrN1PjG4paGZJuvII57Q=="          # 建議改用環境變數，先跑通再說
ORG          = "LINE BANK SOC"                 # 也可用 orgID（字串）
BUCKET       = "SOC"

WATCH_DIR = r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming"
DONE_DIR  = r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\processed"
FAIL_DIR  = r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\failed"

# 你 ArcSight CSV 欄位名稱（可調整）；缺的欄位會自動以預設值代入
REQUIRED_TIME_COL = "event_time"               # 事件時間欄位
OPTIONAL_COLS = {
    "src_ip": "", "dst_ip": "", "src_port": 0, "dst_port": 0,
    "protocol": "", "severity": "", "bytes": 0, "device": "", "message": ""
}
MEASUREMENT = "arcsight_event"





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
        "Afghanistan": (33.0, 65.0),
    "Albania": (41.0, 20.0),
    "Algeria": (28.0, 1.66),
    "Andorra": (42.5, 1.5),
    "Angola": (-12.5, 18.5),
    "Argentina": (-34.0, -64.0),
    "Armenia": (40.0, 45.0),
    "Australia": (-25.0, 133.0),
    "Austria": (47.3, 13.3),
    "Azerbaijan": (40.5, 47.5),
    "Bahamas": (25.0, -77.4),
    "Bahrain": (26.0, 50.5),
    "Bangladesh": (24.0, 90.0),
    "Belarus": (53.0, 28.0),
    "Belgium": (50.8, 4.0),
    "Belize": (17.0, -88.7),
    "Benin": (9.5, 2.25),
    "Bhutan": (27.5, 90.5),
    "Bolivia": (-16.7, -64.7),
    "Bosnia and Herzegovina": (44.0, 18.0),
    "Botswana": (-22.0, 24.0),
    "Brazil": (-10.0, -55.0),
    "Brunei": (4.5, 114.7),
    "Bulgaria": (42.7, 25.5),
    "Burkina Faso": (12.3, -1.5),
    "Burundi": (-3.5, 30.0),
    "Cambodia": (12.6, 104.9),
    "Cameroon": (6.0, 12.0),
    "Canada": (56.0, -106.0),
    "Chile": (-30.0, -71.0),
    "China": (35.0, 103.0),
    "Colombia": (4.0, -72.0),
    "Costa Rica": (10.0, -84.0),
    "Croatia": (45.0, 15.5),
    "Cuba": (21.5, -80.0),
    "Cyprus": (35.0, 33.0),
    "Czech Republic": (49.8, 15.5),
    "Denmark": (56.0, 10.0),
    "Dominican Republic": (19.0, -70.7),
    "Ecuador": (-1.5, -78.5),
    "Egypt": (27.0, 30.0),
    "El Salvador": (13.7, -89.0),
    "Estonia": (59.0, 26.0),
    "Ethiopia": (9.1, 40.5),
    "Finland": (64.0, 26.0),
    "France": (46.0, 2.0),
    "Georgia": (42.0, 43.5),
    "Germany": (51.0, 9.0),
    "Greece": (39.0, 22.0),
    "Guatemala": (15.5, -90.2),
    "Honduras": (14.5, -86.5),
    "Hong Kong": (22.3, 114.2),
    "Hungary": (47.0, 20.0),
    "Iceland": (65.0, -18.0),
    "India": (20.0, 77.0),
    "Indonesia": (-5.0, 120.0),
    "Iran": (32.0, 53.0),
    "Iraq": (33.0, 44.0),
    "Ireland": (53.0, -8.0),
    "Israel": (31.5, 34.8),
    "Italy": (42.8, 12.8),
    "Jamaica": (18.0, -77.0),
    "Japan": (36.0, 138.0),
    "Jordan": (31.0, 36.0),
    "Kazakhstan": (48.0, 68.0),
    "Kenya": (1.0, 38.0),
    "Kuwait": (29.5, 47.5),
    "Kyrgyzstan": (41.0, 75.0),
    "Laos": (18.0, 105.0),
    "Latvia": (57.0, 25.0),
    "Lebanon": (33.8, 35.8),
    "Libya": (27.0, 17.0),
    "Lithuania": (55.0, 24.0),
    "Luxembourg": (49.8, 6.1),
    "Macau": (22.1, 113.5),
    "Mauritius": (-20.3, 57.5),
    "Malaysia": (2.5, 112.5),
    "Maldives": (3.0, 73.0),
    "Malta": (35.9, 14.5),
    "Mexico": (23.6, -102.5),
    "Moldova": (47.0, 29.0),
    "Monaco": (43.7, 7.4),
    "Mongolia": (46.9, 103.8),
    "Montenegro": (42.7, 19.3),
    "Morocco": (31.8, -7.1),
    "Myanmar": (21.9, 95.9),
    "Nepal": (28.4, 84.1),
    "Netherlands": (52.3, 5.3),
    "New Zealand": (-41.0, 174.0),
    "Nicaragua": (12.8, -85.2),
    "Nigeria": (10.0, 8.0),
    "North Korea": (40.0, 127.0),
    "Norway": (61.0, 8.0),
    "Oman": (21.0, 57.0),
    "Pakistan": (30.0, 70.0),
    "Panama": (9.0, -80.0),
    "Paraguay": (-23.0, -58.0),
    "Peru": (-9.2, -75.0),
    "Philippines": (13.0, 122.0),
    "Poland": (52.0, 20.0),
    "Portugal": (39.5, -8.0),
    "Qatar": (25.3, 51.2),
    "Romania": (45.9, 25.0),
    "Russia": (61.5, 105.3),
    "Saudi Arabia": (24.0, 45.0),
    "Seychelles": (-4.6, 55.4),
    "Serbia": (44.0, 21.0),
    "Singapore": (1.3, 103.8),
    "Slovakia": (48.7, 19.7),
    "Slovenia": (46.1, 14.8),
    "South Africa": (-29.0, 24.0),
    "South Korea": (36.5, 127.9),
    "Spain": (40.0, -4.0),
    "Sri Lanka": (7.8, 80.7),
    "Sweden": (62.0, 15.0),
    "Switzerland": (46.8, 8.2),
    "Syria": (35.0, 38.5),
    "Taiwan": (23.7, 121.0),
    "Tajikistan": (38.5, 71.0),
    "Thailand": (15.8, 100.9),
    "Tunisia": (34.0, 9.0),
    "Turkey": (39.0, 35.0),
    "Turkmenistan": (39.1, 59.4),
    "Ukraine": (49.0, 32.0),
    "United Arab Emirates": (24.0, 54.0),
    "United Kingdom": (54.0, -2.0),
    "United States": (37.1, -95.7),
    "Uruguay": (-32.5, -56.0),
    "Uzbekistan": (41.0, 64.0),
    "Venezuela": (8.0, -66.0),
    "Vietnam": (14.1, 108.3),
    "Yemen": (15.5, 48.5),
    "Zimbabwe": (-19.0, 29.2),
}

# 常見別名（大小寫、縮寫統一）
_ALIASES = {
    "USA": "United States",
    "U.S.A.": "United States",
    "United States of America": "United States",
    "Russian Federation": "Russia",
    "Viet Nam": "Vietnam",
    "Korea, Republic of": "South Korea",
    "Republic of Korea": "South Korea",
    "North Korea": "North Korea",
    "Korea, Democratic People's Republic of": "North Korea",
    "Iran, Islamic Republic of": "Iran",
    "Syria, Arab Republic": "Syria",
    "Taiwan, Province of China": "Taiwan",
    "Mainland China": "China",
    "Hongkong": "Hong Kong",
    "Great Britain": "United Kingdom",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "Ivory Coast": "Côte d'Ivoire",
}





# ============== GEO 修補：DataFrame 版（任何國家都會回填） ==============
# 可選：補一些常見別名；不會覆蓋你原本已有的 ALIASES
_ALIASES.update({
    "U.S.A.": "United States",
    "USA": "United States",
    "UK": "United Kingdom",
    "Hong Kong, SAR": "Hong Kong",
    "Viet Nam": "Vietnam",
    "Republic of Korea": "South Korea",
    "Korea, Republic of": "South Korea",
    "Russian Federation": "Russia",
    "Taiwan, Province of China": "Taiwan",
    "ROC": "Taiwan",
    "TWN": "Taiwan",
})

def apply_geo_fixes_df(df):
    import pandas as pd

    # --- helpers ---
    def norm(s):
        if pd.isna(s):
            return None
        return " ".join(str(s).strip().split())

    alias_lower = {str(k).lower(): v for k, v in _ALIASES.items()}

    def normalize_country(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        t = norm(val)
        if not t:
            return None
        return _ALIASES.get(t, alias_lower.get(t.lower(), t))

    def pick_col(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def ensure_col(name):
        if name not in df.columns:
            df[name] = pd.NA
        return name

    def fill_side(country_candidates, lat_candidates, lon_candidates):
        # 找到實際的國名欄位；沒有就跳過
        ccol = pick_col(country_candidates)
        if not ccol:
            return

        # 正規化國名
        df[ccol] = df[ccol].map(normalize_country)

        # 緯度/經度欄位：有就用、沒有就建立預設名
        lcol = pick_col(lat_candidates) or ensure_col(lat_candidates[0])
        rcol = pick_col(lon_candidates) or ensure_col(lon_candidates[0])

        # 型別轉成數值，非數字->NaN
        df[lcol] = pd.to_numeric(df[lcol], errors="coerce")
        df[rcol] = pd.to_numeric(df[rcol], errors="coerce")

        # 準備國名 -> (lat/lon) 對照
        lat_map = {k: float(v[0]) for k, v in _BUILTIN_COUNTRIES.items()
                   if isinstance(v, (tuple, list)) and len(v) == 2}
        lon_map = {k: float(v[1]) for k, v in _BUILTIN_COUNTRIES.items()
                   if isinstance(v, (tuple, list)) and len(v) == 2}

        country_series = df[ccol]

        # 需要回填的條件：NaN 或 0
        need_lat = df[lcol].isna() | (df[lcol] == 0.0)
        need_lon = df[rcol].isna() | (df[rcol] == 0.0)

        df.loc[need_lat, lcol] = country_series.map(lat_map)
        df.loc[need_lon, rcol] = country_series.map(lon_map)

    # src / dst 都處理；包含常見欄位名的候選清單
    fill_side(
        country_candidates=["src_country", "attacker_geo_country_name", "src_geo_country_name"],
        lat_candidates=["src_lat", "src_geo_lat", "src_geo_latitude"],
        lon_candidates=["src_lon", "src_geo_lon", "src_geo_longitude"],
    )
    fill_side(
        country_candidates=["dst_country", "target_geo_country_name", "dst_geo_country_name"],
        lat_candidates=["dst_lat", "dst_geo_lat", "dst_geo_latitude"],
        lon_candidates=["dst_lon", "dst_geo_lon", "dst_geo_longitude"],
    )

    return df
# ========================== GEO 修補：結束 ==========================






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






# ======== 連線與批次寫入 ========
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG)
writer = client.write_api(write_options=WriteOptions(batch_size=1000, flush_interval=1000))
write_api = client.write_api(write_options=SYNCHRONOUS)

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

'''
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
            return pd.to_datetime(xs, utc=True, errors="coerce")
        except Exception:
            return pd.NaT

    return s.apply(_to_ts)
'''


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


def to_float_safe(v, default=0.0):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return float(default)
        return float(v)
    except Exception:
        return float(default)

def ingest_csv(path: str) -> bool:
    print(f"[INGEST] {path}")
    try:
        # 1) 更聰明地讀：自動偵測分隔符、吃掉 BOM
        df = pd.read_csv(
            path,
            encoding="utf-8-sig",     # 自動去 BOM；必要時改 "cp1252"
            on_bad_lines="skip",
            engine="python",
            sep=None                  # 讓 pandas 自動偵測逗號/分號/Tab
        )

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
            df[c] = df[c].astype(str)  # tag 欄位強制轉字串

        print("[DEBUG] tag_cols used:", tag_cols)






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






        # --- 寫入前：對 DataFrame 做 GEO 修補 ---
        df = apply_geo_fixes_df(df)  # <<< 加這一行

        write_api.write(
            bucket=BUCKET,
            org=ORG,
            record=df,
            data_frame_measurement_name="arcsight_event",
            data_frame_tag_columns=tag_cols,
            data_frame_timestamp_column="event_ts",
        )






        # 直接用 DataFrame 一次寫入（最快）
        write_api.write(
            bucket=BUCKET,
            org=ORG,  # 建議用 orgID；跟你 curl 成功那個一致
            record=df,
            data_frame_measurement_name="arcsight_event",
            data_frame_tag_columns=tag_cols,
            data_frame_timestamp_column="event_ts"
        )
        # --- 強化自檢結束 ---

        # 5) 其他欄位補齊（沿用 OPTIONAL_COLS）
        for col, default in OPTIONAL_COLS.items():
            if col not in df.columns:
                df[col] = default

        print(f"[OK] wrote {len(df)} points to {BUCKET}/{MEASUREMENT} (time col: {time_col})")
        return True

    except Exception as e:
        print(f"[ERROR] ingest failed: {e}")
        return False

class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        src = event.src_path
        if not src.lower().endswith(".csv"):
            return

        # 等檔案寫完（避免 ArcSight 還在寫）
        time.sleep(1.0)

        ok = ingest_csv(src)
        dst_dir = DONE_DIR if ok else FAIL_DIR
        os.makedirs(dst_dir, exist_ok=True)
        dst = os.path.join(dst_dir, os.path.basename(src))
        try:
            os.replace(src, dst)
            print(f"[MOVE] {src} -> {dst}")
        except Exception as e:
            print(f"[WARN] move failed: {e}")

if __name__ == "__main__":
    # 建目錄
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(DONE_DIR, exist_ok=True)
    os.makedirs(FAIL_DIR, exist_ok=True)

    # 啟動監控
    observer = Observer()
    observer.schedule(NewFileHandler(), WATCH_DIR, recursive=False)
    observer.start()
    print(f"[WATCHING] {WATCH_DIR}")
    print(f"[TARGET] InfluxDB: {INFLUX_URL}, org={ORG}, bucket={BUCKET}, measurement={MEASUREMENT}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    client.close()
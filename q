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

        # 2) 欄位名正規化：去空白、去 BOM 殘留、統一大小寫
        df.columns = (
            df.columns
              .map(lambda c: str(c).replace("\ufeff", ""))
              .map(lambda c: c.strip())
        )
        print("[DEBUG] columns:", list(df.columns))

        # 3) 找可用的時間欄位（沿用我上一則訊息的 TIME_CANDIDATES / pick_time_column）
        time_col = pick_time_column(df)
        if not time_col:
            raise ValueError(f"missing time column; tried: {TIME_CANDIDATES}")

        print(f"[DEBUG] using time column: {time_col}")
        # 4) 解析時間（沿用 parse_time_series）
        raw_ts = df[time_col].copy()

        df["event_ts"] = parse_time_series(df[time_col])
        df = df.dropna(subset=["event_ts"])
        if df.empty:
            bad = raw_ts.head(3).tolist()  # xs為原始時間欄位
            raise ValueError(f"all timestamps in '{time_col}' are invalid after parsing")

        # --- 強化自檢開始 ---
        print("[DEBUG] using time column:", time_col)
        print("[DEBUG] ts_min =", df["event_ts"].min(), "ts_max =", df["event_ts"].max(), "rows =", len(df))

        preview = 0
        # 先把數值型欄位統一成 float（避免 422 型別衝突）
        if "bytes" in df.columns:
            df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce").astype("float64")

        # 建議當 tag 的欄位（存在才用）。你可以依你的 CSV 欄名調整/擴充
        TAG_CANDIDATES = [
            "device_vendor",
            "agent_name",
            "agent_type",
            "agent_id"
            "transport_protocol",
            "device_action",
            "attacker_geo_country_name",
            "target_geo_country_name",
            "attacker_address",
            "attacker_port",
            "target_address",
            "target_port",

        ]
        '''
        TAG_CANDIDATES = [
            "end_time", "start_time", "name", "device_vendor",
            "attacker_address", "attacker_port",
            "agent_type", "transport_protocol", "agent_severity",
            "device_action", "attacker_geo_location_info", "attacker_geo_country_name", "target_address",
            "target_port", "target_geo_location_info", "target_geo_country_name", "manager_receipt_time",
            "agent_id", "agent_name",
        ]
        '''
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

        # 6) 逐漸寫入（改用 row.event_ts）
        count = 0
        for row in df.itertuples(index=False):
            p = (
                Point(MEASUREMENT)
                .tag("src_ip", str(getattr(row, "src_ip", "")))
                .tag("dst_ip", str(getattr(row, "dst_ip", "")))
                .tag("protocol", str(getattr(row, "protocol", "")))
                .tag("severity", str(getattr(row, "severity", "")))
                .tag("device", str(getattr(row, "device", "")))
                .field("bytes", to_float_safe(getattr(row, "bytes", 0.0)))
                .field("message", str(getattr(row, "message", ""))[:1024])
                .time(getattr(row, "event_ts").to_pydatetime().replace(tzinfo=timezone.utc))
            )
            writer.write(BUCKET, ORG, p)
            count += 1

        print(f"[OK] wrote {count} points to {BUCKET}/{MEASUREMENT} (time col: {time_col})")
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

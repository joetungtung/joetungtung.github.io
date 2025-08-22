import os
import time
import pandas as pd
from datetime import timezone
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from influxdb_client import InfluxDBClient, Point, WriteOptions

# ======== 基本設定（請依你的環境修改） ========
INFLUX_URL   = "http://localhost:8086"
INFLUX_TOKEN = "<把你的 Token 貼在這>"          # 建議改用環境變數，先跑通再說
ORG          = "LINE BANK SOC"                 # 也可用 orgID（字串）
BUCKET       = "SOC"

WATCH_DIR = r"C:\arcsight-data\incoming"
DONE_DIR  = r"C:\arcsight-data\processed"
FAIL_DIR  = r"C:\arcsight-data\failed"

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
        # 讀檔
        df = pd.read_csv(path)

        # 檢查時間欄位
        if REQUIRED_TIME_COL not in df.columns:
            raise ValueError(f"missing '{REQUIRED_TIME_COL}' column")

        # 轉時間（盡量容錯）
        # 建議 ArcSight 匯出成 ISO8601（例: 2025-08-23T01:10:00Z）
        df[REQUIRED_TIME_COL] = pd.to_datetime(
            df[REQUIRED_TIME_COL], errors="coerce", utc=True, infer_datetime_format=True
        )
        df = df.dropna(subset=[REQUIRED_TIME_COL])
        if df.empty:
            raise ValueError("no valid timestamps after parsing")

        # 保證所有用到的欄位都存在
        for col, default in OPTIONAL_COLS.items():
            if col not in df.columns:
                df[col] = default

        # 逐列寫入
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
                .time(getattr(row, REQUIRED_TIME_COL).to_pydatetime().replace(tzinfo=timezone.utc))
            )
            writer.write(BUCKET, ORG, p)
            count += 1

        print(f"[OK] wrote {count} points to {BUCKET}/{MEASUREMENT}")
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



event_time,src_ip,dst_ip,protocol,severity,bytes,device,message
2025-08-23T01:10:00Z,10.0.0.1,8.8.8.8,udp,low,128,fw01,DNS query
2025-08-23T01:11:00Z,10.0.0.2,1.1.1.1,tcp,high,2048,proxy01,HTTPS connect

# -*- coding: utf-8 -*-
import os
from pathlib import Path
import pandas as pd
from dateutil import parser as dt
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions
from zoneinfo import ZoneInfo
import re
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("influxdb_client.client.write").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ========= Influx 連線 =========
INFLUX_URL = "http://127.0.0.1:8086"
ORG        = "LINE BANK SOC"
BUCKET     = "SOC"
TOKEN      = "TUNNT9zHsfHU2nFhjxL63i8HMpqLpSGv5J5hzrq9x-79DAxmTaOv5EbAr31OZaaz1zVrN1PjG4paGZJuvII57Q=="
MEAS       = "arcsight_event"   # 你原本就用這個

# ========= 檔案來源 =========
DOWNLOAD_ROOT = Path("downloads")
SOURCES = [
    {"key": "radware", "dir": DOWNLOAD_ROOT / "Radware"},
    {"key": "fw",      "dir": DOWNLOAD_ROOT / "FW"},
    {"key": "waf",     "dir": DOWNLOAD_ROOT / "WAF"},
    # {"key": "apexone","dir": DOWNLOAD_ROOT / "ApexOne"},  # 之後開
]

# ========= 欄位對應（候選名，依你們 CSV 實際欄名修改）=========
# 時間：用 End Time，沒有再退回 Start Time
COLMAP_TIME        = ["End Time", "Start Time", "Time", "Timestamp", "event_time"]

# IP
COLMAP_SRCIP       = ["Attacker Address", "SrcIP", "SourceIP", "client_ip"]
COLMAP_DSTIP       = ["Target Address",   "DstIP", "DestinationIP", "server_ip"]

# 動作、嚴重度
COLMAP_ACTION      = ["Device Action", "Action", "decision", "Result"]
COLMAP_SEVERITY    = ["Agent Severity", "Severity", "level", "Priority"]

# 國家（FW 沒有國家，Radware/WAF 有）
COLMAP_SRCCOUNTRY  = ["Attacker Geo Country Name", "SrcCountry", "client_country"]
COLMAP_DSTCOUNTRY  = ["Target Geo Country Name",   "DstCountry", "server_country"]



EXTRA_TAG_CANDIDATES = {
    # 共同常見
    "protocol":       ["Transport Protocol", "Protocol"],
    "app_protocol":   ["Application Protocol"],
    "rule_name":      ["Name", "Rule Name", "Policy Name"],
    "device_vendor":  ["Device Vendor", "Vendor"],
    "device_name":    ["Device Host Name", "Hostname", "Device Name"],
    "agent_type":     ["Agent Type"],
    "agent_name":     ["Agent Name"],

    # FW 專用
    "device_in_if":   ["Device Inbound Interface",  "Ingress Interface", "In Interface"],
    "device_out_if":  ["Device Outbound Interface", "Egress Interface",  "Out Interface"],
}



TAG_COLS = [
    "product","device_action","severity",
    "attacker_geo_country_name","target_geo_country_name",
    "protocol","app_protocol","rule_name","device_vendor","device_name",
    "agent_type","agent_name","device_in_if","device_out_if"
]



EXTRA_FIELD_CANDIDATES = {
    # 數值（可 sum/avg）
    "bytes_in":   ["Bytes In",  "bytes_in",  "tx_bytes"],
    "bytes_out":  ["Bytes Out", "bytes_out", "rx_bytes"],
    "src_port":   ["Attacker Port", "Src Port", "Source Port"],
    "dst_port":   ["Target Port",   "Dst Port", "Destination Port"],
    "flex_num1":  ["Flex Number1"],

    # 高基數（避免做 tag）
    "message":        ["Message"],
    "request_url":    ["Request Url", "URL"],
    "request_method": ["Request Method"],
    "file_path":      ["File Path"],
    "device_address": ["Device Address"],
}

# ========= 你現有的修補函式（地理補點）=========
# 照你們之前檔案的匯入方式
try:
    from geo_fixes import apply_geo_fixes_df
except Exception:
    # 若檔名不同，改成你實際的模組/函式
    def apply_geo_fixes_df(df: pd.DataFrame) -> pd.DataFrame:
        return df  # 先不做事，避免匯入失敗

# ========= 小工具 =========
def pick(raw: pd.DataFrame, names: list):
    """回傳第一個存在於 raw 的欄位 Series，若都沒有回傳 None"""
    for n in names:
        if n in raw.columns:
            return raw[n]
    return None

_EPOCH_RE = re.compile(r"^\d{9,}$")  # 9 位以上的純數字，就當作 epoch

def _epoch_to_utc_ts(v: float) -> pd.Timestamp | pd.NaT:
    try:
        v = float(v)
    except Exception:
        return pd.NaT
    # 自動判斷單位：ns / ms / s
    if v > 1e14:        # ns
        secs = v / 1_000_000_000.0
    elif v > 1e11:      # ms
        secs = v / 1_000.0
    else:               # s
        secs = v
    return pd.Timestamp(datetime.fromtimestamp(secs, tz=timezone.utc))

def to_ts(s: pd.Series) -> pd.Series:
    """把各種時間格式轉成『UTC 時區的 pandas.Timestamp』。
       規則：
       1) 純數字 → epoch（自動判斷 s/ms/ns）
       2) 有時區的字串（含 Z 或 ±HH:MM）→ 直接轉成 UTC
       3) 沒時區的字串 → 視為 Asia/Taipei 後轉成 UTC
    """
    if s is None:
        return pd.Series(pd.NaT, index=s.index)

    ss = s.astype(str).str.strip()

    # 1) 先處理純 epoch
    is_epoch = ss.str.match(_EPOCH_RE)
    out = pd.Series(pd.NaT, index=ss.index, dtype="datetime64[ns, UTC]")
    if is_epoch.any():
        out.loc[is_epoch] = ss[is_epoch].apply(_epoch_to_utc_ts)

    # 2) 其餘當作日期字串
    others = ~is_epoch
    if others.any():
        # 2a) 先嘗試解析（不強制加 UTC），保留原有時區
        parsed = pd.to_datetime(ss[others], errors="coerce", utc=False, infer_datetime_format=True)

        if parsed.notna().any():
            # 有時區資訊 → 轉 UTC
            has_tz = parsed.dt.tz.notna()
            if has_tz.any():
                out.loc[others[others].index[has_tz]] = parsed[has_tz].dt.tz_convert("UTC")

            # 沒時區資訊 → 當作台北本地，再轉 UTC
            no_tz = ~has_tz
            if no_tz.any():
                localized = parsed[no_tz].dt.tz_localize(ZoneInfo("Asia/Taipei"), nonexistent="shift_forward", ambiguous="NaT")
                out.loc[others[others].index[no_tz]] = localized.dt.tz_convert("UTC")

    return out


def ensure_common_cols(df: pd.DataFrame) -> pd.DataFrame:
    common = [
        "event_ts","src_ip","dst_ip","device_action","severity","count",
        "attacker_geo_country_name","target_geo_country_name",
        "src_lat","src_lon","dst_lat","dst_lon"
    ]
    for c in common:
        if c not in df.columns:
            df[c] = pd.NA
    if "count" in df.columns:
        try:
            df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(1).astype("Int64")
        except Exception:
            df["count"] = 1
    else:
        df["count"] = 1
    return df

def parse_csv_generic(path: Path, product_key: str) -> pd.DataFrame:
    raw = pd.read_csv(path)
    raw.columns = (
        raw.columns
            .astype(str)
            .str.replace("\ufeff", "", regex=False)  # 去掉 BOM
            .str.strip()  # 去頭尾空白
    )
    df = pd.DataFrame()

    # 時間
    tcol = pick(raw, COLMAP_TIME)
    if tcol is not None:
        df["event_ts"] = to_ts(tcol)
        ok = int(df["event_ts"].notna().sum())
        bad = int(df["event_ts"].isna().sum())
        print(f"[DEBUG][time] file='{path.name}' source='{tcol.name}' parsed_ok={ok} missing={bad}")
    else:
        # 沒有時間欄就用檔案 mtime 避免整批丟失
        df["event_ts"] = pd.to_datetime(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc))
        print(f"[DEBUG][time] file='{path.name}' source='mtime' parsed_ok={len(df)} missing=0")

    # 基本欄位
    df["src_ip"]   = pick(raw, COLMAP_SRCIP)
    df["dst_ip"]   = pick(raw, COLMAP_DSTIP)
    df["device_action"] = pick(raw, COLMAP_ACTION)
    df["severity"] = pick(raw, COLMAP_SEVERITY)

    df["attacker_geo_country_name"] = pick(raw, COLMAP_SRCCOUNTRY)
    df["target_geo_country_name"]   = pick(raw, COLMAP_DSTCOUNTRY)

    df = ensure_common_cols(df)

    # ---- 額外 TAGS ----
    for out_col, candidates in EXTRA_TAG_CANDIDATES.items():
        s = pick(raw, candidates)
        if s is not None:
            df[out_col] = s.astype("string")

    # ---- 額外 FIELDS ----
    for out_col, candidates in EXTRA_FIELD_CANDIDATES.items():
        s = pick(raw, candidates)
        if s is not None:
            # 嘗試轉成數值；轉不動就保留字串（例如 URL）
            num = pd.to_numeric(s, errors="coerce")
            if num.notna().any():  # 裡面有數字
                df[out_col] = num
            else:
                df[out_col] = s.astype("string")

    # 附加欄：產品
    df["product"] = product_key

    # 清掉完全沒時間的列
    df = df[~df["event_ts"].isna()].copy()

    return df

def _sanitize_for_influx(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) 把 pandas 的 pd.NA/NaN 全部換成 None（Influx 不吃 pd.NA）
    df = df.where(pd.notna(df), None)

    # 2) tag 欄位用 object，保留 None（不要變字串 'None'）
    for c in TAG_COLS:
        if c in df.columns:
            df[c] = df[c].astype(object)

    # 3) 可空布林 dtype -> object，避免 "boolean value of NA is ambiguous"
    for c in df.select_dtypes(include="boolean").columns:
        df[c] = df[c].astype(object)

    return df


def fix_influx_type_conflicts(df: pd.DataFrame):
    for col in df.columns:
        if df[col].dtype == object:  # 文字欄位
            # 針對 "passed", "blocked" 等關鍵字避免被當成 boolean
            df[col] = df[col].astype(str)
        elif df[col].dtype == bool:
            # Boolean 轉成字串 "true"/"false"
            df[col] = df[col].apply(lambda x: "true" if x else "false")
    return df

def write_to_influx(df: pd.DataFrame) -> bool:
    if df.empty:
        print("[INFO] nothing to write.")
        return True
    try:
        safe_numeric_fields = ["bytes_in","bytes_out","src_port","dst_port","flex_num1","count"]

        # —— 只留安全欄位（時間 + tags + 數值欄位）——
        keep_cols = (["event_ts"]
                     + [c for c in TAG_COLS if c in df.columns]
                     + [c for c in safe_numeric_fields if c in df.columns])
        df = df.loc[:, [c for c in keep_cols if c in df.columns]].copy()

        # 數值欄位轉數字
        for c in safe_numeric_fields:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # —— 保證至少有一個 field 可寫 ——（⚠️取代原本的 dropna）
        if not any(c in df.columns for c in safe_numeric_fields):
            df["count"] = 1.0
        else:
            # 這些列的所有數值欄位都是 NaN → 給它 count=1 當保底
            mask_all_nan = df[[c for c in safe_numeric_fields if c in df.columns]].isna().all(axis=1)
            if mask_all_nan.any():
                df.loc[mask_all_nan, "count"] = 1.0

        # 時間正規化（仍建議保留）
        df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce", utc=True)
        df = df[~df["event_ts"].isna()].copy()

        # 統一數值欄位型別（避免 field type 衝突）
        numeric_fields = ["bytes_in", "bytes_out", "src_port", "dst_port", "flex_num1", "count"]
        for c in numeric_fields:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

        # --- 統一可疑欄位為字串，避免 invalid boolean 問題 ---
        force_string_fields = [
            "device_action", "severity", "rule_name",
            "agent_type", "agent_name",
            "protocol", "app_protocol",
        ]
        for c in force_string_fields:
            if c in df.columns:
                df[c] = df[c].astype(str)

        # Influx 友善清洗（一定保留）
        df = _sanitize_for_influx(df)

        # —— 分批同步寫入（最穩）——
        from math import ceil
        BATCH_SIZE = 10_000
        with InfluxDBClient(url=INFLUX_URL, org=ORG, token=TOKEN, timeout=300_000) as cli:
            w = cli.write_api(write_options=SYNCHRONOUS)
            total = len(df); batches = ceil(total / BATCH_SIZE)
            for i in range(batches):
                chunk = df.iloc[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                w.write(
                    bucket=BUCKET,
                    org=ORG,
                    record=chunk,
                    data_frame_measurement_name=MEAS,
                    data_frame_tag_columns=[c for c in TAG_COLS if c in chunk.columns],
                    data_frame_timestamp_column="event_ts",
                )
                print(f"[OK] wrote chunk {i+1}/{batches} rows={len(chunk)}")
        print(f"[OK] wrote total {len(df)} rows to {BUCKET}/{MEAS}")
        return True
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"[ERROR] write influx failed: {e}")
        return False

def main():
    total_files = 0
    total_rows  = 0

    for s in SOURCES:
        key = s["key"]
        d   = s["dir"]
        if not d.exists():
            print(f"[INFO] source '{key}' dir not exists: {d}")
            continue

        # 只抓 .csv，依修改時間排序（舊到新）
        csvs = sorted([p for p in d.glob("*.csv")], key=lambda x: x.stat().st_mtime)
        if not csvs:
            print(f"[INFO] no csvs for source '{key}' in {d}")
            continue

        print(f"[INFO] === {key} === files={len(csvs)} dir={d}")

        for p in csvs:
            try:
                # 1) 解析單一檔案
                df = parse_csv_generic(p, key)
                print(f"[PARSE] {key}: {p.name} -> {len(df)} rows")
                total_files += 1
                if df.empty:
                    continue

                # 2) 地理補點
                try:
                    df = apply_geo_fixes_df(df)
                except Exception as ge:
                    print(f"[WARN] geo fixes skipped for {p.name}: {ge}")

                # 3) 寫入（write_to_influx 內已有分批/清洗）
                df = fix_influx_type_conflicts(df)
                ok = write_to_influx(df)
                if ok:
                    total_rows += len(df)

            except Exception as e:
                print(f"[WARN] parse/write failed {key}: {p.name} ({e})")

    print(f"[SUMMARY] files={total_files}, rows_written~={total_rows}")

def smoke_test_influx():
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS
    import pandas as pd
    from datetime import datetime, timezone

    test = pd.DataFrame([{
        "event_ts": pd.Timestamp(datetime.now(timezone.utc)),
        "product": "smoke",
        "count": 1
    }])
    try:
        with InfluxDBClient(url=INFLUX_URL, org=ORG, token=TOKEN, timeout=60000) as cli:
            w = cli.write_api(write_options=SYNCHRONOUS)
            w.write(
                bucket=BUCKET, org=ORG, record=test,
                data_frame_measurement_name=MEAS,
                data_frame_tag_columns=["product"],
                data_frame_timestamp_column="event_ts",
            )
        print("[SMOKE] OK: single row written")
    except Exception as e:
        import traceback; traceback.print_exc()
        print("[SMOKE] FAIL:", e)

# 先只跑煙囪測試（要測完整流程時把下一行註解掉）
# smoke_test_influx();  import sys; sys.exit(0)

if __name__ == "__main__":
    main()
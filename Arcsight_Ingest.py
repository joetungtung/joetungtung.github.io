# -*- coding: utf-8 -*-
import os
from pathlib import Path
import pandas as pd
from dateutil import parser as dt
from datetime import datetime

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

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

def to_ts(s):
    def _p(x):
        try:
            return dt.parse(str(x))
        except Exception:
            return pd.NaT
    return s.apply(_p)

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
            df["count"] = df["count"].fillna(1).infer_objects(copy=False).astype("Int64")
        except Exception:
            df["count"] = 1
    else:
        df["count"] = 1
    return df

def parse_csv_generic(path: Path, product_key: str) -> pd.DataFrame:
    raw = pd.read_csv(path)
    df = pd.DataFrame()

    # 時間
    tcol = pick(raw, COLMAP_TIME)
    if tcol is not None:
        df["event_ts"] = to_ts(tcol)
    else:
        # 沒有時間欄就用檔案 mtime 當時間，避免整批丟失
        df["event_ts"] = pd.to_datetime(datetime.fromtimestamp(path.stat().st_mtime))

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

def write_to_influx(df: pd.DataFrame) -> bool:
    if df.empty:
        print("[INFO] nothing to write.")
        return True
    try:
        with InfluxDBClient(url=INFLUX_URL, org=ORG, token=TOKEN) as cli:
            w = cli.write_api(write_options=SYNCHRONOUS)
            df = _sanitize_for_influx(df)
            w.write(
                bucket=BUCKET,
                org=ORG,
                record=df,
                data_frame_measurement_name=MEAS,
                data_frame_tag_columns=TAG_COLS,
                data_frame_timestamp_column="event_ts",
            )
        print(f"[OK] wrote {len(df)} rows to {BUCKET}/{MEAS}")
        return True
    except Exception as e:
        print(f"[ERROR] write influx failed: {e}")
        return False

def main():
    frames = []
    for s in SOURCES:
        key = s["key"]
        d   = s["dir"]
        if not d.exists():
            continue
        # 只抓 .csv 檔
        csvs = sorted([p for p in d.glob("*.csv")], key=lambda x: x.stat().st_mtime)
        if not csvs:
            continue

        for p in csvs:
            try:
                df = parse_csv_generic(p, key)
                frames.append(df)
                print(f"[PARSE] {key}: {p.name} -> {len(df)} rows")
            except Exception as e:
                print(f"[WARN] parse failed {key}: {p.name} ({e})")

    if not frames:
        print("[INFO] no new dataframes")
        return

    df_all = pd.concat(frames, ignore_index=True)

    # 地理補點（你的 _ALIASES / _BUILTIN_COUNTRIES 會在這裡生效）
    try:
        df_all = apply_geo_fixes_df(df_all)
    except Exception as e:
        print(f"[WARN] geo fixes skipped: {e}")

    write_to_influx(df_all)

if __name__ == "__main__":
    main()
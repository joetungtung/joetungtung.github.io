# Official_Delete_Influx.py  —— 強化版（可直接覆蓋）
# 功能：
# 1) delete_measurement()  ：刪某個 measurement 在指定時間窗內的資料
# 2) delete_range_all()    ：刪整個 bucket 在指定時間窗內的所有 measurement
# 共同特性：InfluxDBClient timeout 可調、逾時重試、刪除後快速驗證

from __future__ import annotations
import time
from typing import Optional

# 依你環境填入 ↓↓↓
INFLUX_URL = "http://127.0.0.1:8086"
TOKEN      = "<YOUR_TOKEN>"
ORG        = "LINE BANK SOC"
BUCKET     = "SOC"

from influxdb_client import InfluxDBClient
from urllib3.exceptions import ReadTimeoutError
# 某些版本若無 ReadTimeoutError，可改用：
# from requests.exceptions import ReadTimeout as ReadTimeoutError


# =============== 共用小工具 ===============
def _mk_client(timeout_ms: int) -> InfluxDBClient:
    """建立帶逾時的 client（毫秒）"""
    return InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG, timeout=timeout_ms)


def _retry_delete(delete_fn, max_retries: int) -> None:
    """對 delete_fn() 做指數退避重試（專處理 ReadTimeoutError）"""
    for attempt in range(max_retries):
        try:
            delete_fn()
            return
        except ReadTimeoutError:
            backoff = min(2 ** attempt, 30)  # 1,2,4,8,16,30...
            print(f"[WARN] delete timeout (attempt {attempt+1}/{max_retries}), retry in {backoff}s…")
            time.sleep(backoff)
    print("[ERROR] delete still timing out after retries；刪除可能仍在後端進行。")


def _verify_empty(client: InfluxDBClient, start: str, stop: str,
                  measurement: Optional[str] = None) -> bool:
    """刪除後快速驗證：查 1 筆看看是否還有資料"""
    if measurement:
        predicate = f'|> filter(fn: (r) => r._measurement == "{measurement}")'
        label = f'measurement="{measurement}"'
    else:
        predicate = ""
        label = "ALL measurements"

    q = f'''
    from(bucket:"{BUCKET}")
      |> range(start: {start}, stop: {stop})
      {predicate}
      |> limit(n: 1)
    '''
    tables = client.query_api().query(q)
    if not tables:
        print(f"[OK] {label} 已清空（{start} ~ {stop}）")
        return True
    else:
        print(f"[WARN] 刪除後仍查到殘留（可能是延遲/時間窗未涵蓋），稍後可再驗證。")
        return False


# =============== 1) 刪指定 measurement ===============
def delete_measurement(
    measurement: str,
    start: str = "1970-01-01T00:00:00Z",
    stop:  str = "2100-01-01T00:00:00Z",
    timeout_ms: int = 120_000,
    max_retries: int = 5
) -> None:
    """
    刪除 bucket 內某個 measurement 在 start~stop 的資料，並做驗證。
    start/stop 必須是 RFC3339 UTC 時間字串，例如 "2025-08-01T00:00:00Z"
    """
    predicate = f'_measurement="{measurement}"'
    with _mk_client(timeout_ms) as client:
        delete_api = client.delete_api()
        _retry_delete(
            lambda: delete_api.delete(start=start, stop=stop,
                                      predicate=predicate, bucket=BUCKET, org=ORG),
            max_retries=max_retries
        )
        _verify_empty(client, start, stop, measurement=measurement)

    print(f"[DONE] Deleted measurement={measurement!r} in bucket={BUCKET!r} from {start} ~ {stop}")


# =============== 2) 刪整個 bucket（所有 measurement） ===============
def delete_range_all(
    start: str,
    stop: str,
    timeout_ms: int = 120_000,
    max_retries: int = 5
) -> None:
    """
    刪 bucket 在 start~stop 的所有資料（不限 measurement），並做驗證。
    注意：這會很重，請確認時間窗！
    """
    with _mk_client(timeout_ms) as client:
        delete_api = client.delete_api()
        _retry_delete(
            lambda: delete_api.delete(start=start, stop=stop,
                                      predicate="", bucket=BUCKET, org=ORG),
            max_retries=max_retries
        )
        _verify_empty(client, start, stop, measurement=None)

    print(f"[DONE] Deleted ALL data in bucket={BUCKET!r} from {start} ~ {stop}")


# =============== 範例執行（可註解掉） ===============
if __name__ == "__main__":
    # 範例1：清空 arcsight_event 全時段
    # delete_measurement("arcsight_event",
    #                    start="1970-01-01T00:00:00Z",
    #                    stop="2100-01-01T00:00:00Z")

    # 範例2：清空整個 bucket 某時間窗
    # delete_range_all(start="2025-08-01T00:00:00Z", stop="2025-08-31T00:00:00Z")

    pass
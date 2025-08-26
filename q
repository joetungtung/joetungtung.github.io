# delete_influx.py
from influxdb_client import InfluxDBClient
from datetime import datetime, timezone

# === 必填：請換成你自己的 ===
INFLUX_URL   = "http://127.0.0.1:8086"
ORG          = "<你的orgID或org名稱>"   # 建議用 orgID
TOKEN        = "<你的Token>"
BUCKET       = "SOC"

def delete_measurement(measurement: str,
                       start: str = "1970-01-01T00:00:00Z",
                       stop: str  = "2100-01-01T00:00:00Z"):
    """
    刪除 bucket 內某個 measurement 在 start~stop 的所有資料。
    start/stop 必須是 RFC3339 UTC 時間字串。
    """
    predicate = f'_measurement="{measurement}"'
    with InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG) as client:
        delete_api = client.delete_api()
        delete_api.delete(start=start, stop=stop,
                          predicate=predicate, bucket=BUCKET, org=ORG)
    print(f"[DONE] Deleted measurement={measurement!r} in bucket={BUCKET!r} from {start} to {stop}")

def delete_range_all(start: str, stop: str):
    """
    刪除 bucket 在 start~stop 的所有資料（不限定 measurement）。
    """
    with InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG) as client:
        client.delete_api().delete(start=start, stop=stop, bucket=BUCKET, org=ORG)
    print(f"[DONE] Deleted ALL data in bucket={BUCKET!r} from {start} to {stop}")

if __name__ == "__main__":
    # 例1：刪掉 arcsight_event 全部資料
    delete_measurement("arcsight_event")

    # # 例2：只刪某個時間區間（UTC）
    # delete_measurement("arcsight_event",
    #                    start="2025-08-24T00:00:00Z",
    #                    stop ="2025-08-25T00:00:00Z")

    # # 例3：整桶清空某時段
    # delete_range_all(start="1970-01-01T00:00:00Z", stop="2100-01-01T00:00:00Z")
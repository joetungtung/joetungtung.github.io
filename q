import time
from urllib3.exceptions import ReadTimeoutError
# 若你的套件版本沒有 ReadTimeoutError，就改抓 requests.exceptions.ReadTimeout

def delete_measurement(
    measurement: str,
    start: str = "1970-01-01T00:00:00Z",
    stop:  str = "2100-01-01T00:00:00Z",
    timeout_ms: int = 120_000,     # (1) 調長 timeout：120 秒
    max_retries: int = 5           # (2) 刪除逾時自動重試
) -> None:
    """
    刪掉 bucket 內某個 measurement 在 start~stop 的資料。
    並在刪除後做一次查詢驗證。
    """
    predicate = f'_measurement="{measurement}"'

    # === (1) InfluxDBClient 加上 timeout 參數（毫秒） ===
    with InfluxDBClient(url=INFLUX_URL, token=TOKEN, org=ORG, timeout=timeout_ms) as client:
        delete_api = client.delete_api()

        # === (2) 刪除時加上重試（指數退避）===
        for attempt in range(max_retries):
            try:
                delete_api.delete(
                    start=start,
                    stop=stop,
                    predicate=predicate,
                    bucket=BUCKET,
                    org=ORG,
                )
                break  # 成功就離開重試迴圈
            except ReadTimeoutError as e:
                # 1s, 2s, 4s, 8s, 30s（封頂）
                backoff = min(2 ** attempt, 30)
                print(f"[WARN] delete timeout (attempt {attempt+1}/{max_retries}), retry in {backoff}s…")
                time.sleep(backoff)
        else:
            print("[ERROR] delete still timing out after retries；刪除可能仍在後端進行。")

        # === (3) 刪除後做快速驗證 ===
        q = f'''
        from(bucket:"{BUCKET}")
          |> range(start: {start}, stop: {stop})
          |> filter(fn: (r) => r._measurement == "{measurement}")
          |> limit(n: 1)
        '''
        tables = client.query_api().query(q)

        if not tables:  # 沒有任何表 → 已清空
            print(f"[OK] {measurement!r} 已清空（{start} ~ {stop}）")
        else:
            # 仍查到資料：可能是延遲 compaction 或 predicate/時間窗沒涵蓋到
            print(f"[WARN] 刪除後仍查到殘留（可能是延遲/時間窗未涵蓋），建議稍後再查一次。")

    print(f"[DONE] Deleted measurement={measurement!r} in bucket={BUCKET!r} from {start} ~ {stop}")
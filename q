import os
import pandas as pd

# 你的測試 CSV 路徑 (放在 incoming/ 裡)
WATCH_DIR = r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming"

# 這裡就是你自己設定好的 TAG_CANDIDATES
TAG_CANDIDATES = [
    "device_vendor",
    "agent_type",
    "agent_name",
    "agent_id",
    "transport_protocol",
    "device_action",
    "attacker_geo_country_name",
    "target_geo_country_name",
    # 如果你想要 IP/Port 也當 tag，就加這些：
    # "attacker_address", "target_address", "attacker_port", "target_port",
]

def debug_check_csv(path: str):
    print(f"\n[DEBUG] 檢查檔案: {path}")
    df = pd.read_csv(path)

    # 欄位名稱清理 (跟正式程式一致)
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(" ", "_")
    )

    print("[DEBUG] 欄位轉換測試結果：")
    for col in df.columns:
        if col in TAG_CANDIDATES:
            print(f"  ✔ {col} → TAG")
        else:
            print(f"  ✘ {col} → FIELD")

    print(f"[DEBUG] 總欄位數: {len(df.columns)}")


def main():
    files = [f for f in os.listdir(WATCH_DIR) if f.endswith(".csv")]
    if not files:
        print("[INFO] 沒有找到任何 CSV 檔案")
        return

    for file in files:
        debug_check_csv(os.path.join(WATCH_DIR, file))


if __name__ == "__main__":
    main()
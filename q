# purge_files.py
import os, time
from pathlib import Path

# === 要清的資料夾清單 ===
FOLDERS   = [
    r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming",
    r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\processed",
    r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\failed"
]
KEEP_DAYS = 30   # 保留天數

def purge_folder(folder: str, keep_days: int):
    now = time.time()
    cutoff = now - keep_days * 86400
    p = Path(folder)
    n = 0
    if not p.exists():
        print(f"[SKIP] {folder} 不存在")
        return
    for f in p.glob("*"):
        try:
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                n += 1
        except Exception as e:
            print("[WARN]", f, e)
    print(f"[DONE] {folder} 刪除了 {n} 個舊檔案 (> {keep_days}d)")

def main():
    for folder in FOLDERS:
        purge_folder(folder, KEEP_DAYS)

if __name__ == "__main__":
    main()
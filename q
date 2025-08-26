# —— 前面：cutoff 已經算好，account 也建立好 ——
processed_folder = account.inbox / PROCESSED   # 你的子資料夾

# 只要 id，節省流量
qs = processed_folder.all().filter(datetime_received__lt=cutoff).only('id')

print(f"[INFO] Will delete {qs.count()} old mails in '{PROCESSED}' before {cutoff} ...")

try:
    # 方案A：整批刪（bulk delete）。大多數版本這是「硬刪」。
    deleted = qs.delete()   # 有些版本會回傳刪除數量，沒有就回 None
    print(f"[DONE] Bulk deleted {deleted if deleted is not None else 'selected'} old mails.")
except Exception as e:
    # 若你的 exchangelib 版本不支援 bulk delete，就退回逐筆 soft_delete
    print("[WARN] Bulk delete not supported, fallback to per-item soft_delete:", e)
    n = 0
    for item in qs:     # 不要用 iterator()，直接迭代
        try:
            item.soft_delete()  # 或改 item.delete() 做硬刪
            n += 1
        except Exception as ee:
            print("[WARN] delete failed:", ee)
    print(f"[DONE] Soft-deleted {n} mails.")
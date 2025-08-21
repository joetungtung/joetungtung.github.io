from exchangelib import UTC  # 檔頭如未 import 記得補上

print("[step] 5. querying emails...", flush=True)

# 先列資料夾基本資訊
try:
    print("[debug] folder =", folder.absolute, "name=", folder.name)
    print("[debug] class =", folder.__class__.__name__)
    print("[debug] total_count =", folder.total_count, "unread =", folder.unread_count)
except Exception as e:
    print("[warn] unable to read folder counters:", e)

# A) 完全不加條件：抓最新 10 封來看看（避免因時間或 regex 把信都過濾掉）
try:
    print("[debug] probing latest items (no filter) ...", flush=True)
    probe = []
    for i, it in enumerate(folder.all().order_by('-datetime_received').iterator()):
        probe.append((it.datetime_received, field(it.subject)))
        if i >= 9:
            break
    print(f"[debug] probe size = {len(probe)}")
    for dt, sj in probe:
        print("   ", dt, "-", sj)
except Exception as e:
    print("[error] probing items failed:", e)

# B) 若你要用 lookback_hours，再用 UTC 算（避免時區誤差）
lookback_hours = int(cfg["dryrun"].get("lookback_hours", 24) or 0)
items = []
if lookback_hours > 0:
    since = datetime.utcnow().replace(tzinfo=UTC) - timedelta(hours=lookback_hours)
    print("[debug] using time filter since(UTC) =", since)
    try:
        qs = folder.filter(datetime_received__gt=since).order_by("-datetime_received")
        items = list(qs)[: cfg["exchange"].get("max_emails_per_run", 100)]
        print(f"[step] 5.1 got {len(items)} items (with time filter)")
    except Exception as e:
        print("[error] filtered query failed:", e)
else:
    try:
        qs = folder.all().order_by("-datetime_received")
        items = list(qs)[: cfg["exchange"].get("max_emails_per_run", 100)]
        print(f"[step] 5.1 got {len(items)} items (no time filter)")
    except Exception as e:
        print("[error] unfiltered query failed:", e)
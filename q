print("[step] 5. querying emails...", flush=True)

# 先列資料夾資訊
try:
    print("[debug] folder =", folder.absolute, "name=", folder.name)
    print("[debug] class =", folder.__class__.__name__)
    print("[debug] total_count =", folder.total_count, "unread =", folder.unread_count)
except Exception as e:
    print("[warn] unable to read folder counters:", e)

# A) 完全不加條件：抓最新 10 封（你的 exchangelib 版本沒有 iterator()，直接切片）
try:
    print("[debug] probing latest items (no filter)...", flush=True)
    qs_probe = folder.all().order_by('-datetime_received')
    probe_items = list(qs_probe[:10])  # 直接切片取前 10 封
    print(f"[debug] probe size = {len(probe_items)}")
    for it in probe_items:
        print("   ", it.datetime_received, "-", field(it.subject))
except Exception as e:
    print("[error] probing items failed:", e)

# B) 用 EWSDateTime 做時間過濾（這是重點修正）
lookback_hours = int(cfg["dryrun"].get("lookback_hours", 24) or 0)
items = []
if lookback_hours > 0:
    since_ews = EWSDateTime.now(tz=UTC) - timedelta(hours=lookback_hours)  # 必須用 EWSDateTime
    print("[debug] using time filter since(UTC|EWSDateTime) =", since_ews)
    try:
        qs = folder.filter(datetime_received__gte=since_ews).order_by("-datetime_received")
        items = list(qs[: cfg["exchange"].get("max_emails_per_run", 100)])
        print(f"[step] 5.1 got {len(items)} items (with time filter)")
    except Exception as e:
        print("[error] filtered query failed:", e)
else:
    try:
        qs = folder.all().order_by("-datetime_received")
        items = list(qs[: cfg["exchange"].get("max_emails_per_run", 100)])
        print(f"[step] 5.1 got {len(items)} items (no time filter)")
    except Exception as e:
        print("[error] unfiltered query failed:", e)
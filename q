print("[step] 5. querying emails...", flush=True)

# 基本資訊
print("[debug] folder =", folder.absolute, "name=", folder.name)
print("[debug] class =", folder.__class__.__name__)
print("[debug] total_count =", folder.total_count, "unread =", folder.unread_count)

# 先看最新 10 封（完全不加條件）
qs_probe = folder.all().order_by('-datetime_received')
probe_items = list(qs_probe[:10])
print(f"[debug] probe size = {len(probe_items)}")
for it in probe_items:
    print("   ", it.datetime_received, "-", field(it.subject))

# 用 TPE 做時間過濾（或把 lookback_hours 設 0 表示不加時間條件）
lookback_hours = int(cfg["dryrun"].get("lookback_hours", 24) or 0)
if lookback_hours > 0:
    since_ews = EWSDateTime.now(tz=TPE) - timedelta(hours=lookback_hours)  # ★改成 TPE
    print("[debug] using time filter since(TPE|EWSDateTime) =", since_ews)
    qs = folder.filter(datetime_received__gte=since_ews).order_by("-datetime_received")
else:
    qs = folder.all().order_by("-datetime_received")

items = list(qs[: cfg["exchange"].get("max_emails_per_run", 100)])
print(f"[step] 5.1 got {len(items)} items", flush=True)
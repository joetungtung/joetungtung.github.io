# ===== STEP 5  最終合併版：撈信 + 表格輸出 =====
from exchangelib import EWSDateTime, UTC  # 檔頭若未 import 要補

print("[step] 5. querying emails...", flush=True)
print("[debug] folder =", folder.absolute, "name=", folder.name)
print("[debug] class =", folder.__class__.__name__)
print("[debug] total_count =", folder.total_count, "unread =", folder.unread_count)

# 5.0 Probe：完全不加條件，列最新 10 封，確認真的能抓到
try:
    qs_probe = folder.all().order_by("-datetime_received")
    probe_items = list(qs_probe[:10])
    print(f"[debug] probe size = {len(probe_items)}")
    for it in probe_items:
        print("   ", it.datetime_received, "-", field(it.subject))
except Exception as e:
    print("[error] probing items failed:", e)

# 5.1 正式查詢：用 EWSDateTime + TPE 做時間過濾（或 lookback_hours=0 表示不加時間條件）
lookback_hours = int(cfg["dryrun"].get("lookback_hours", 24) or 0)
if lookback_hours > 0:
    since_ews = EWSDateTime.now(tz=TPE) - timedelta(hours=lookback_hours)  # ★注意：EWSDateTime，不是 EWSTimeZone
    print("[debug] using time filter since(TPE|EWSDateTime) =", since_ews)
    qs = folder.filter(datetime_received__gte=since_ews).order_by("-datetime_received")
else:
    qs = folder.all().order_by("-datetime_received")

# 這裡一次把 items 取出來，以後都用這份，不要再重新查詢或使用未定義的 since 變數
items = list(qs[: cfg["exchange"].get("max_emails_per_run", 100)])
print(f"[step] 5.1 got {len(items)} items", flush=True)

# 5.2 表格輸出（只對現有 items 做排序，不要再重抓）
items = sorted(items, key=lambda x: x.datetime_received, reverse=True)

print(pad("Date(TPE)", 20), pad("From", 28), pad("Subject", 64),
      pad("Action", 16), pad("Key", 12), pad("Priority", 10), "Reason")
print("-" * 160)

sk = cfg.get("skip", {})

for it in items:
    dt   = it.datetime_received.astimezone(TPE).strftime("%Y-%m-%d %H:%M:%S")
    frm  = field(getattr(it.sender, "email_address", "")) or field(getattr(it.sender, "name", ""))
    subj = field(it.subject)
    body = body_text(it)

    # 命中 skip 就標示 SKIP
    if (any_substr(subj, sk.get("subject_contains")) or any_regex(subj, sk.get("subject_regex")) or
        any_substr(body, sk.get("body_contains")) or any_regex(body, sk.get("body_regex")) or
        any_substr(frm,  sk.get("from_contains"))):
        print(pad(dt,20), pad(frm,28), pad(subj,64),
              pad("SKIP",16), pad("",12), pad("",10), "命中 skip 規則")
        continue

    # 未略過 → 進入判斷
    result = decide(cfg, subj, body)
    print(pad(dt,20), pad(frm,28), pad(subj,64),
          pad(result["action"],16), pad(result.get("issue_key",""),12),
          pad(result.get("priority",""),10), "非skip")
# ===== STEP 5 結束 =====
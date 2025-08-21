# ===== 表格輸出（這段開始到 for 迴圈結束，整段貼上覆蓋） =====
# 用我們前面拿到的 items，不要再重新查詢或使用 since 這個變數
items = sorted(items, key=lambda x: x.datetime_received, reverse=True)

# 表頭（用 TPE）
print(pad("Date(TPE)", 20), pad("From", 28), pad("Subject", 64),
      pad("Action", 16), pad("Key", 12), pad("Priority", 10), "Reason")
print("-" * 160)

sk = cfg.get("skip", {})

for it in items:
    dt   = it.datetime_received.astimezone(TPE).strftime("%Y-%m-%d %H:%M:%S")
    frm  = field(getattr(it.sender, "email_address", "")) or field(getattr(it.sender, "name", ""))
    subj = field(it.subject)
    body = body_text(it)

    # ---- 略過規則（命中其一就跳過）----
    if (any_substr(subj, sk.get("subject_contains")) or any_regex(subj, sk.get("subject_regex")) or
        any_substr(body, sk.get("body_contains")) or any_regex(body, sk.get("body_regex")) or
        any_substr(frm,  sk.get("from_contains"))):
        print(pad(dt,20), pad(frm,28), pad(subj,64),
              pad("SKIP",16), pad("",12), pad("",10), "命中 skip 規則")
        continue

    # ---- 沒被略過：進入判斷（之後會接 Jira）----
    result = decide(cfg, subj, body)
    print(pad(dt,20), pad(frm,28), pad(subj,64),
          pad(result["action"],16), pad(result.get("issue_key",""),12),
          pad(result.get("priority",""),10), "非skip")
# ===== 表格輸出到這裡結束 =====
# 原本：
# qs = inbox.filter(is_read=False)
# if FILTER_SENDER:
#     qs = qs.filter(sender__email_address=FILTER_SENDER)   # <-- 這行會導致 InvalidField
# if FILTER_SUBJ_KEYWORD:
#     qs = qs.filter(subject__contains=FILTER_SUBJ_KEYWORD)

# 改成（只用伺服器端 is_read，其他在本地判斷）：
qs = inbox.filter(is_read=False)

saved_count = 0
for item in qs.only("subject", "attachments", "datetime_received", "sender"):
    # 本地過濾寄件者
    if FILTER_SENDER:
        try:
            s = (item.sender.email_address or "").lower()
        except Exception:
            s = ""
        if s != FILTER_SENDER.lower():
            continue

    # 本地過濾主旨關鍵字
    if FILTER_SUBJ_KEYWORD:
        if FILTER_SUBJ_KEYWORD.lower() not in (item.subject or "").lower():
            continue

    has_saved = False
    for att in item.attachments:
        from exchangelib import FileAttachment
        if isinstance(att, FileAttachment):
            name = (att.name or "").strip()
            if name.lower().endswith(".csv"):
                out_path = os.path.join(INCOMING_DIR, name)
                base, ext = os.path.splitext(out_path)
                idx = 1
                while os.path.exists(out_path):
                    out_path = f"{base}({idx}){ext}"
                    idx += 1
                with open(out_path, "wb") as f:
                    f.write(att.content)
                print(f"[SAVE] {name} -> {out_path}")
                has_saved = True
                saved_count += 1

    if has_saved:
        item.is_read = True
        item.save()
        # 若要搬移到 Inbox/ProcessedArcsight：
        processed_folder = ensure_folder(account.inbox, ("ProcessedArcsight",))
        try:
            item.move(processed_folder)
        except Exception as e:
            print(f"[WARN] move failed: {e}")

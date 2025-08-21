def main():
    print("[step] 1. loading config...", flush=True)
    cfg = load_cfg()

    print("[step] 2. connecting EWS...", flush=True)
    acct = connect_ews(cfg["exchange"])
    # 強制打一個輕量請求驗證連線
    print("[step] 2.1 account.inbox.total_count =", acct.inbox.total_count, flush=True)

    print("[step] 3. dumping folder tree files...", flush=True)
    dump_tree_to_file(acct, start="ROOT", max_depth=3, out_path="folder_tree_root.txt")
    list_root_children(acct, out_path="root_children.txt")
    search_folder(acct, "Notice", out_path="search_notice.txt")

    print("[step] 4. resolving mailbox...", flush=True)
    box = cfg["exchange"].get("mailbox") or "INBOX"
    print("[debug] mailbox from config =", repr(box), flush=True)
    folder = get_folder_by_path(acct, box)
    print(f"[info] Using mailbox: {box} (resolved: {folder.absolute})", flush=True)

    print("[step] 5. querying emails...", flush=True)
    lookback_hours = int(cfg["dryrun"].get("lookback_hours", 24) or 0)
    if lookback_hours > 0:
        since = datetime.now(TPE) - timedelta(hours=lookback_hours)
        qs = folder.filter(datetime_received__gt=since).order_by("-datetime_received")
    else:
        qs = folder.all().order_by("-datetime_received")

    items = list(qs)[: cfg["exchange"].get("max_emails_per_run", 100)]
    print(f"[step] 5.1 got {len(items)} items", flush=True)

    # 表頭
    print(pad("Date(UTC)", 20), pad("From", 28), pad("Subject", 64),
          pad("Action", 16), pad("Key", 12), pad("Priority", 10), "Reason")
    print("-"*160)

    sk = cfg.get("skip", {})
    # 若 lookback_hours=0，這行會用到未定義的 since，先保險判斷再用
    if lookback_hours > 0:
        items = sorted(folder.filter(datetime_received__gte=since),
                       key=lambda x: x.datetime_received,
                       reverse=True)
    else:
        items = items  # 保持 server 端新→舊排序

    for it in items:
        dt   = it.datetime_received.astimezone(TPE).strftime("%Y-%m-%d %H:%M:%S")
        frm  = field(getattr(it.sender, "email_address", "")) or field(getattr(it.sender, "name", ""))
        subj = field(it.subject)
        body = body_text(it)

        if any_substr(subj, sk.get("subject_contains")) or any_regex(subj, sk.get("subject_regex")) \
           or any_substr(body, sk.get("body_contains")) or any_regex(body, sk.get("body_regex")) \
           or any_substr(frm,  sk.get("from_contains")):
            print(pad(dt,20), pad(frm,28), pad(subj,64), pad("SKIP",16),
                  pad("",12), pad("",10), "命中 skip 規則")
            continue

        result = decide(cfg, subj, body)
        print(pad(dt, 20), pad(frm, 28), pad(subj, 64),
              pad(result["action"], 16), pad(result.get("issue_key", ""), 12),
              pad(result.get("priority", ""), 10), "非skip")

    print("[done] all steps finished.", flush=True)
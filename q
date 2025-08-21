def get_folder_by_path(account, path_str: str):
    """
    支援 'INBOX/Notice/Dynatrace' 或 '收件匣/Notice/Dynatrace'。
    失敗時會用 absolute 路徑後綴做全樹比對（忽略大小寫/空白/全形斜線），提高成功率。
    """
    def norm(s: str) -> str:
        # 統一成半形斜線、去空白、無視大小寫
        return (s or "").replace("／", "/").replace("\\", "/").replace(" ", "").casefold()

    if not path_str or not str(path_str).strip():
        print("[debug] path_str 空白，預設用 inbox")
        return account.inbox

    raw = str(path_str)
    parts = [p.strip() for p in raw.replace("／", "/").split("/") if p.strip()]
    inbox_display = account.inbox.name
    print(f"[debug] 解析 mailbox: {raw!r} → parts={parts} (inbox display={inbox_display!r})")

    # 1) 逐層走
    first = parts[0].casefold()
    if first in ("inbox", inbox_display.casefold(), "收件匣"):
        folder = account.inbox
        parts = parts[1:]
        print(f"[debug] 從 inbox 開始；剩餘 parts={parts}")
    else:
        folder = account.msg_folder_root
        print(f"[debug] 從 msg_folder_root 開始；parts={parts}")

    try:
        for p in parts:
            want = norm(p)
            kids = list(folder.children.only('name').all())
            match = None
            for ch in kids:
                if norm(ch.name) == want:
                    match = ch; break
            if not match:
                # 寬鬆再試：移除空白比對
                for ch in kids:
                    if norm(ch.name) == want:
                        match = ch; break
            if not match:
                raise LookupError(p)
            folder = match
        print(f"[debug] 逐層解析成功 → {folder.absolute}")
        return folder
    except LookupError as miss:
        miss_part = str(miss)
        print(f"[warn] 逐層解析在「{miss_part}」這一層失敗，啟用全樹後綴比對…")

    # 2) 後備：用 absolute 路徑後綴全樹比對
    want_suffix = "/" + "/".join(parts if parts else [])
    want_suffix_n = norm(want_suffix)
    best = None
    for f in account.root.walk():
        abs_path = getattr(f, "absolute", "") or ""
        if norm(abs_path).endswith(want_suffix_n):
            best = f
            break
    if best:
        print(f"[debug] 後綴比對命中 → {best.absolute}")
        return best

    # 3) 列候選，幫助修正
    print(f"[錯誤] 找不到資料夾路徑：{raw!r}")
    print("=== 候選（同層/常見）===")
    try:
        for ch in account.inbox.children.only('name').all():
            print(" - INBOX/", ch.name, sep="")
    except Exception:
        pass
    try:
        for ch in account.msg_folder_root.children.only('name').all():
            print(" - ROOT/", ch.name, sep="")
    except Exception:
        pass
    raise RuntimeError(f"無法解析 mailbox 路徑：{raw!r}")
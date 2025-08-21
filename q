for p in parts:
    want = norm(p)

    # 先取子項，再過濾出「真‧資料夾」（有 name 且有 children，或類名包含 Folder）
    kids_raw = list(folder.children.all())
    kids = [ch for ch in kids_raw
            if hasattr(ch, "name") and (hasattr(ch, "children") or "Folder" in ch.__class__.__name__)]

    match = None
    # 精確比對（不分大小寫、去全半形斜線與空白）
    for ch in kids:
        if norm(ch.name) == want:
            match = ch
            break

    if not match:
        # 再寬鬆一次（其實 norm 已處理空白／斜線，但保留這段保險）
        for ch in kids:
            if norm(ch.name) == want:
                match = ch
                print(f"[debug] 使用寬鬆比對命中：{ch.name}")
                break

    if not match:
        raise LookupError(p)

    folder = match
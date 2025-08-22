import unicodedata, re  # 若已匯入就不用再加

def _norm_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"\s+", "", s, flags=re.UNICODE)  # 移除所有空白(含NBSP/零寬)
    s = s.replace("／", "/").replace("\\", "/")
    return s.casefold()

def build_folder_index(account):
    by_abs = {}
    by_parent = {}
    for f in account.root.walk():  # 只走資料夾
        abs_path = getattr(f, "absolute", "") or ""
        name = getattr(f, "name", "")
        n_abs = _norm_name(abs_path)
        n_name = _norm_name(name)
        by_abs[n_abs] = f
        parent_abs = "/".join(abs_path.split("/")[:-1]) if "/" in abs_path else ""
        n_parent = _norm_name(parent_abs)
        by_parent.setdefault(n_parent, {})[n_name] = f
    return by_abs, by_parent




def get_folder_by_path(account, path_str: str, idx=None):
    if not path_str or not str(path_str).strip():
        return account.inbox

    # 取得索引（外面可先建好傳進來）
    if idx is None:
        by_abs, by_parent = build_folder_index(account)
    else:
        by_abs, by_parent = idx

    parts_raw = [p.strip() for p in str(path_str).replace("／", "/").split("/") if p.strip()]
    inbox_display = _norm_name(account.inbox.name)
    parts_norm = [_norm_name(p) for p in parts_raw]

    # 起點：INBOX/收件匣 → inbox；否則 root
    if parts_norm and parts_norm[0] in ("inbox", inbox_display, _norm_name("收件匣")):
        current = account.inbox
        cur_abs_n = _norm_name(getattr(current, "absolute", ""))
        remain = parts_norm[1:]
    else:
        current = account.msg_folder_root
        cur_abs_n = _norm_name(getattr(current, "absolute", ""))
        remain = parts_norm

    # 逐層用 parent 索引比對
    for want in remain:
        children = by_parent.get(cur_abs_n, {})
        hit = children.get(want)
        if not hit:
            # 限縮在 inbox 子樹救援
            inbox_abs_n = _norm_name(getattr(account.inbox, "absolute", ""))
            candidates = []
            for abs_n, folder in by_abs.items():
                if abs_n.startswith(inbox_abs_n) and _norm_name(getattr(folder, "name", "")) == want:
                    parent_abs = "/".join((getattr(folder, "absolute", "") or "").split("/")[:-1])
                    if _norm_name(parent_abs) == cur_abs_n:
                        candidates.append(folder)
            if not candidates:
                print(f"[錯誤] 在「{getattr(current,'absolute','')}」找不到：{parts_raw[len(parts_norm)-len(remain)]}")
                print("  可用子資料夾：", ", ".join(sorted([getattr(v,'name','') for v in children.values()])))
                raise RuntimeError("folder path segment not found")
            hit = candidates[0]
        current = hit
        cur_abs_n = _norm_name(getattr(current, "absolute", ""))

    return current




print("[step] 2.9 building folder index…", flush=True)
IDX = build_folder_index(acct)

box = cfg["exchange"].get("mailbox") or "INBOX"
folder = get_folder_by_path(acct, box, idx=IDX)  # ★ 多傳 idx=IDX
print(f"[info] Using mailbox: {box} (Resolved: {getattr(folder,'absolute','')})", flush=True)


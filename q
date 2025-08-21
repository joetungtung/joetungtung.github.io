from itertools import islice

def list_root_children_fast(account, out_path="root_children.txt", limit=200):
    root = account.msg_folder_root
    rows = ["=== Root children (fast) ==="]
    try:
        # 只拉 name 欄位、限制最多 N 個，避免卡太久
        for ch in islice(root.children.only('name').all(), limit):
            rows.append(f"- {_safe_str(getattr(ch,'name','<unknown>'))}")
    except Exception as e:
        rows.append(f"<error: {e}>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(rows))
    print(f"[info] fast 根目錄第一層已輸出到 {out_path}")


def list_inbox_children_fast(account, out_path="inbox_children.txt", limit=200):
    inbox = account.inbox
    rows = ["=== Inbox children (fast) ==="]
    try:
        for ch in islice(inbox.children.only('name').all(), limit):
            rows.append(f"- {_safe_str(getattr(ch,'name','<unknown>'))}")
    except Exception as e:
        rows.append(f"<error: {e}>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(rows))
    print(f"[info] fast 收件匣第一層已輸出到 {out_path}")


def search_folder_fast(account, keyword, out_path="folder_search.txt", limit=5000):
    """用 walk() 但只收集命中的項目，避免寫爆；limit 是最多檢視的節點數"""
    kw = keyword.casefold()
    rows = [f"=== Search '{keyword}' (fast) ==="]
    seen = 0
    try:
        for f in account.root.walk():
            seen += 1
            if seen > limit:
                rows.append(f"<hit limit {limit}>")
                break
            nm = _safe_str(getattr(f, "name", ""))
            if kw in nm.casefold():
                rows.append(f"- {nm}    {_safe_str(getattr(f,'absolute',''))}")
    except Exception as e:
        rows.append(f"<error: {e}>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(rows))
    print(f"[info] fast 搜尋結果寫入 {out_path}")



print("[step] 3a. list root children (fast)...", flush=True)
list_root_children_fast(acct, out_path="root_children.txt")

print("[step] 3b. list inbox children (fast)...", flush=True)
list_inbox_children_fast(acct, out_path="inbox_children.txt")

print("[step] 3c. search keyword (fast)...", flush=True)
search_folder_fast(acct, "Notice", out_path="search_notice.txt")
# 需要找 Dynatrace 就改成：
# search_folder_fast(acct, "Dynatrace", out_path="search_dynatrace.txt")
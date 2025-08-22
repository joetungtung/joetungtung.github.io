# C:\arcsight-data\ews_fetch.py
import os
from exchangelib import (
    Credentials, Account, DELEGATE, Configuration, NTLM, FileAttachment, EWSTimeZone
)
from exchangelib.folders import Inbox
from exchangelib.queryset import Q

# ==== 依你環境修改 ====
EMAIL    = "arcsight-reports@company.com"   # 收報表的信箱
USERNAME = r"COMPANY\yourusername"          # 或者直接用 EMAIL 也行
PASSWORD = "YourAppPassword"                # 建議用應用程式密碼
EWS_URL  = "https://mail.company.com/EWS/Exchange.asmx"

INCOMING_DIR = r"C:\arcsight-data\incoming" # 這裡就是 arcsight_ingest.py 在看的資料夾
PROCESSED_PATH = ("Processed", "ArcSight")  # 下載後要移動的郵件資料夾路徑（會自動建立）

# 過濾條件（可留空）
FILTER_SENDER = "arcsight@company.com"      # 只處理這個寄件者；空字串=不過濾
FILTER_SUBJ_KEYWORD = "ArcSight"            # 主旨關鍵字；空字串=不過濾
# ======================

def ensure_folder(account, path_tuple):
    """確保子資料夾存在（例如 ('Processed','ArcSight') ）"""
    folder = account.root
    for name in path_tuple:
        if name not in [f.name for f in folder.children]:
            folder / name  # 觸發層級瀏覽
            folder = folder.add_subfolder(name=name)
        else:
            folder = folder / name
    return folder

def main():
    os.makedirs(INCOMING_DIR, exist_ok=True)

    cfg = Configuration(server=EWS_URL, credentials=Credentials(USERNAME, PASSWORD), auth_type=NTLM)
    account = Account(
        primary_smtp_address=EMAIL,
        config=cfg,
        autodiscover=False,
        access_type=DELEGATE,
    )

    inbox: Inbox = account.inbox
    qs = inbox.filter(is_read=False)  # 只抓未讀
    if FILTER_SENDER:
        qs = qs.filter(sender__email_address=FILTER_SENDER)
    if FILTER_SUBJ_KEYWORD:
        qs = qs.filter(subject__contains=FILTER_SUBJ_KEYWORD)

    processed_folder = ensure_folder(account, PROCESSED_PATH)
    saved_count = 0

    for item in qs.only("subject", "attachments", "datetime_received"):
        # 只處理 .csv 附件
        has_saved = False
        for att in item.attachments:
            if isinstance(att, FileAttachment):
                name = (att.name or "").strip()
                if name.lower().endswith(".csv"):
                    out_path = os.path.join(INCOMING_DIR, name)
                    # 如同名檔案已存在，改名避免覆蓋
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
            # 標已讀並移到 Processed/ArcSight
            item.is_read = True
            item.save()
            item.move(processed_folder)

    print(f"[DONE] saved {saved_count} csv file(s)")

if __name__ == "__main__":
    main()

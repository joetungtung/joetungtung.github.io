# ews_cleanup.py
from datetime import timedelta
from exchangelib import Credentials, Configuration, Account, DELEGATE, EWSDateTime, EWSTimeZone

EWS_URL   = "https://webmail.linebank.com.tw/EWS/Exchange.asmx"
USERNAME  = "<你的AD帳號>"
PASSWORD  = "<你的密碼>"
EMAIL     = "<你的信箱>"
PROCESSED = "ProcessedArcsight"   # 收件匣底下的資料夾名稱
KEEP_DAYS = 30

def main():
    creds  = Credentials(USERNAME, PASSWORD)
    config = Configuration(service_endpoint=EWS_URL, credentials=creds)
    account = Account(primary_smtp_address=EMAIL, credentials=creds,
                      autodiscover=False, config=config, access_type=DELEGATE)

    tz = EWSTimeZone.timezone("Asia/Taipei")
    cutoff = tz.localize(EWSDateTime.now() - timedelta(days=KEEP_DAYS))

    processed_folder = account.inbox / PROCESSED  # 不需匯入 Inbox 類別
    qs = processed_folder.all().filter(datetime_received__lt=cutoff)
    total = qs.count()
    print(f"[INFO] Deleting {total} old mails in '{PROCESSED}' before {cutoff} ...")

    # 想先丟垃圾桶用 soft_delete()，要直接刪用 delete()
    for item in qs.iterator(page_size=200):
        item.soft_delete()
    print("[DONE] EWS cleanup finished.")

if __name__ == "__main__":
    main()
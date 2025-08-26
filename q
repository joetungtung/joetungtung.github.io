from datetime import timedelta
from exchangelib import Credentials, Configuration, Account, DELEGATE, EWSDateTime, EWSTimeZone, Inbox, Folder

EWS_URL   = "https://webmail.linebank.com.tw/EWS/Exchange.asmx"
USERNAME  = "<你的AD帳號>"
PASSWORD  = "<你的密碼>"
EMAIL     = "<你的信箱>"
PROCESSED = "ProcessedArcsight"  # 你移動郵件的資料夾名稱
KEEP_DAYS = 30

def main():
    creds = Credentials(USERNAME, PASSWORD)
    config = Configuration(server=EWS_URL.replace("https://","").replace("/EWS/Exchange.asmx",""),
                           credentials=creds, auth_type=None, service_endpoint=EWS_URL)
    account = Account(primary_smtp_address=EMAIL, credentials=creds, autodiscover=False,
                      config=config, access_type=DELEGATE)

    tz = EWSTimeZone.timezone("Asia/Taipei")
    cutoff = tz.localize(EWSDateTime.now() - timedelta(days=KEEP_DAYS))

    processed = account.inbox / PROCESSED  # 你的子資料夾
    qs = processed.all().filter(datetime_received__lt=cutoff)
    total = qs.count()
    print(f"[INFO] Deleting {total} old mails in {PROCESSED} before {cutoff} ...")

    for item in qs.iterator(page_size=100):
        item.delete()  # 若想先丟垃圾桶，用 item.soft_delete()
    print("[DONE] EWS cleanup finished.")

if __name__ == "__main__":
    main()





import os, time
from pathlib import Path

FOLDER    = r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\processed"
KEEP_DAYS = 30

def main():
    now = time.time()
    cutoff = now - KEEP_DAYS * 86400
    p = Path(FOLDER)
    n = 0
    for f in p.glob("*"):
        try:
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                n += 1
        except Exception as e:
            print("[WARN]", f, e)
    print(f"[DONE] Deleted {n} old files (> {KEEP_DAYS}d) in {FOLDER}")

if __name__ == "__main__":
    main()






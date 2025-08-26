# ews_fetch.py  — 掃描 OWA 規則已搬移的「ProcessedArcsight」資料夾
# 功能：只撈未讀 + 檢查點(上次處理時間) → 下載 CSV 附件至 incoming → 設已讀 → 更新檢查點
from pathlib import Path
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from exchangelib import (
    Credentials, Configuration, Account, DELEGATE,
    EWSDateTime, FileAttachment
)

# ======== 你要改的參數 ========
EWS_URL   = "https://webmail.linebank.com.tw/EWS/Exchange.asmx"
USERNAME  = "<你的AD帳號>"
PASSWORD  = "<你的密碼>"
EMAIL     = "<你的信箱>"
TARGET_FOLDER = "ProcessedArcsight"  # 規則搬移到的資料夾（收件匣底下）

INCOMING_DIR  = Path(r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming")
STATE_FILE    = Path(r"D:\Joe\Develop\GrafanaInfluxdb\Autoimport\state\ews_state.json")
ONLY_CSV      = True
MAX_PER_RUN   = 1000     # 單次最多處理多少封，避免一次拉太多
PAGE_SIZE     = 200      # exchangelib 會自動分頁，這裡是「最多遍歷數」，可保持這個值

# ======== 小工具 ========
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"last_dt": None, "last_id": None}

def save_state(dt_ews: EWSDateTime, item_id: str):
    ensure_dir(STATE_FILE.parent)
    STATE_FILE.write_text(json.dumps({
        "last_dt": dt_ews.ewsformat(),
        "last_id": item_id
    }), encoding="utf-8")

def to_ews_datetime(dt_local: datetime) -> EWSDateTime:
    # 將台北時間轉為 UTC，再轉 EWSDateTime
    dt_utc = dt_local.astimezone(timezone.utc)
    return EWSDateTime.from_datetime(dt_utc)

# ======== 主程式 ========
def main():
    ensure_dir(INCOMING_DIR)

    # 連線
    creds  = Credentials(USERNAME, PASSWORD)
    config = Configuration(service_endpoint=EWS_URL, credentials=creds)
    account = Account(primary_smtp_address=EMAIL, credentials=creds,
                      autodiscover=False, config=config, access_type=DELEGATE)

    # 目標資料夾（收件匣底下）
    folder = account.inbox / TARGET_FOLDER

    # 載入檢查點
    state = load_state()
    print("[INFO] last checkpoint:", state)

    # 基本查詢：有附件、未讀
    qs = folder.all().filter(has_attachments=True, is_read=False)

    # 加上「上次處理時間」之後的信件（防漏）
    if state["last_dt"]:
        qs = qs.filter(datetime_received__gt=EWSDateTime.from_string(state["last_dt"]))

    # 只取必要欄位，並依時間先後處理
    qs = qs.only("id", "subject", "datetime_received", "attachments", "is_read").order_by("datetime_received")

    handled = 0
    preview = 0

    for item in qs:
        # 安全保險：避免無限處理
        if handled >= MAX_PER_RUN:
            break

        got_file = False
        for att in item.attachments:
            if isinstance(att, FileAttachment):
                if ONLY_CSV and att.name and not att.name.lower().endswith(".csv"):
                    continue
                # 存檔
                out = INCOMING_DIR / att.name
                out.write_bytes(att.content)
                print(f"[SAVE] {att.name} -> {out}")
                got_file = True

        if got_file:
            # 設已讀（可選）
            try:
                item.is_read = True
                item.save()
            except Exception as e:
                print("[WARN] set read failed:", e)

            handled += 1
            # 更新檢查點（用「最後處理成功的那封」）
            save_state(item.datetime_received, item.id)

    print(f"[DONE] processed={handled}")

if __name__ == "__main__":
    main()
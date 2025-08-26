from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from exchangelib import EWSDateTime

KEEP_DAYS = 1  # 你的設定

# 以台北時間計算，再轉 UTC，最後轉成 EWSDateTime
cutoff_local = datetime.now(ZoneInfo("Asia/Taipei")) - timedelta(days=KEEP_DAYS)
cutoff_utc   = cutoff_local.astimezone(timezone.utc)
cutoff       = EWSDateTime.from_datetime(cutoff_utc)
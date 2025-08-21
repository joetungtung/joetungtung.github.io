[info] Using mailbox: 收件匣/Notice/Dynatrace (resolved: /root/資訊存放區頂端/收件匣/Notice/Dynatrace)
[step] 5. querying emails...
[debug] folder = /root/資訊存放區頂端/收件匣/Notice/Dynatrace name= Dynatrace
[debug] class = Messages
[debug] total_count = 19836 unread = 71
[debug] probing latest items (no filter) ...
[error] probing items failed: 'QuerySet' object has no attribute 'iterator'
D:\Joe\Develop\email-to-jira\detect_emails.py:362: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  since = datetime.utcnow().replace(tzinfo=UTC) - timedelta(hours=lookback_hours)
[debug] using time filter since(UTC) = 2025-08-20 22:22:59.358350+00:00
[step] 5.1 got 0 items (with time filter)
Date(UTC)            From                         Subject                                                          Action           Key          Priority   Reason
----------------------------------------------------------------------------------------------------------------------------------------------------------------
[done] all steps finished.

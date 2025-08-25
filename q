    # 4) 解析時間
    df["event_ts"] = parse_time_series(df[time_col])
    df = df.dropna(subset=["event_ts"])
    if df.empty:
        raise ValueError(f"all timestamps in '{time_col}' are invalid after parsing")

    # --- 強化自檢開始 ---
    print("[DEBUG] using time column:", time_col)
    print("[DEBUG] ts_min =", df["event_ts"].min(), "ts_max =", df["event_ts"].max(), "rows =", len(df))

    preview = 0
    for _, row in df.iterrows():
        p = (
            Point("arcsight_event")
            .tag("src_ip", str(getattr(row, "src_ip", "")))
            .tag("dst_ip", str(getattr(row, "dst_ip", "")))
            .tag("severity", str(getattr(row, "severity", "")))
            .tag("device", str(getattr(row, "device", "")))
            .field("message", str(getattr(row, "message", ""))[:1024])
            .field("bytes", int(float(getattr(row, "bytes", 0) or 0)))
            .time(row.event_ts.to_pydatetime().replace(tzinfo=timezone.utc))
        )

        if preview < 2:  # 只印前兩筆避免洗版
            print("[LP]", p.to_line_protocol())
            preview += 1

        write_api.write(BUCKET, ORG, p)
    # --- 強化自檢結束 ---

    print(f"[OK] wrote {len(df)} points to {BUCKET}/arcsight_event (time col: {time_col})")
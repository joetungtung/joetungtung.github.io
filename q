def ingest_csv(path: str) -> bool:
    print(f"[INGEST] {path}")
    try:
        # 1) 更聰明地讀：自動偵測分隔符、吃掉 BOM
        df = pd.read_csv(
            path,
            encoding="utf-8-sig",     # 自動去 BOM；必要時改 "cp1252"
            on_bad_lines="skip",
            engine="python",
            sep=None                  # 讓 pandas 自動偵測逗號/分號/Tab
        )

        # 2) 欄位名正規化：去空白、去 BOM 殘留、統一大小寫
        df.columns = (
            df.columns
              .map(lambda c: str(c).replace("\ufeff", ""))
              .map(lambda c: c.strip())
        )
        print("[DEBUG] columns:", list(df.columns))

        # 3) 找可用的時間欄位（沿用我上一則訊息的 TIME_CANDIDATES / pick_time_column）
        time_col = pick_time_column(df)
        if not time_col:
            raise ValueError(f"missing time column; tried: {TIME_CANDIDATES}")

        # 4) 解析時間（沿用 parse_time_series）
        df["__event_time__"] = parse_time_series(df[time_col])
        df = df.dropna(subset=["__event_time__"])
        if df.empty:
            raise ValueError(f"all timestamps in '{time_col}' are invalid after parsing")

        # 5) 其他欄位補齊（沿用 OPTIONAL_COLS）
        for col, default in OPTIONAL_COLS.items():
            if col not in df.columns:
                df[col] = default

        # 6) 寫入（保持原樣）
        count = 0
        for row in df.itertuples(index=False):
            p = (
                Point(MEASUREMENT)
                .tag("src_ip", str(getattr(row, "src_ip", "")))
                .tag("dst_ip", str(getattr(row, "dst_ip", "")))
                .tag("protocol", str(getattr(row, "protocol", "")))
                .tag("severity", str(getattr(row, "severity", "")))
                .tag("device", str(getattr(row, "device", "")))
                .field("bytes", to_float_safe(getattr(row, "bytes", 0.0)))
                .field("message", str(getattr(row, "message", ""))[:1024])
                .time(getattr(row, "__event_time__").to_pydatetime().replace(tzinfo=timezone.utc))
            )
            writer.write(BUCKET, ORG, p)
            count += 1

        print(f"[OK] wrote {count} points to {BUCKET}/{MEASUREMENT} (time col: {time_col})")
        return True

    except Exception as e:
        print(f"[ERROR] ingest failed: {e}")
        return False

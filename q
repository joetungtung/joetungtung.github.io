# 4) 解析時間（沿用 parse_time_series）
        df["event_ts"] = parse_time_series(df[time_col])
        df = df.dropna(subset=["event_ts"])
        if df.empty:
            raise ValueError(f"all timestamps in '{time_col}' are invalid after parsing")

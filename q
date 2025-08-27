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

        print(f"[DEBUG] using time column: {time_col}")
        # 4) 解析時間（沿用 parse_time_series）
        raw_ts = df[time_col].copy()

        df["event_ts"] = parse_time_series(df[time_col])
        df = df.dropna(subset=["event_ts"])
        if df.empty:
            bad = raw_ts.head(3).tolist()  # xs為原始時間欄位
            raise ValueError(f"all timestamps in '{time_col}' are invalid after parsing")

        # --- 強化自檢開始 ---
        print("[DEBUG] using time column:", time_col)
        print("[DEBUG] ts_min =", df["event_ts"].min(), "ts_max =", df["event_ts"].max(), "rows =", len(df))

        preview = 0
        # 先把數值型欄位統一成 float（避免 422 型別衝突）
        if "bytes" in df.columns:
            df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce").astype("float64")

        # 建議當 tag 的欄位（存在才用）。你可以依你的 CSV 欄名調整/擴充
        TAG_CANDIDATES = [
            "device_vendor",
            "agent_name",
            "agent_type",
            "agent_id",
            "transport_protocol",
            "device_action",
            "attacker_geo_country_name",
            "target_geo_country_name",
            "attacker_address",
            "attacker_port",
            "target_address",
            "target_port",

        ]

        # 若你想把欄名統一成好讀的 key（可選）：
        # 例：把空白改底線，大小寫統一
        rename_map = {c: c.strip().replace(" ", "_") for c in TAG_CANDIDATES if c in df.columns}
        df = df.rename(columns=rename_map)

        # 重新以改名後的集合決定 tag 欄位清單
        tag_cols = [rename_map.get(c, c) for c in TAG_CANDIDATES if rename_map.get(c, c) in df.columns]

        # 數值欄位型別（避免 422）
        for num_col in ["bytes", "src_port", "dst_port"]:
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce").astype("float64")

        # 直接用 DataFrame 一次寫入（最快）
        write_api.write(
            bucket=BUCKET,
            org=ORG,  # 建議用 orgID；跟你 curl 成功那個一致
            record=df,
            data_frame_measurement_name="arcsight_event",
            data_frame_tag_columns=tag_cols,
            data_frame_timestamp_column="event_ts"
        )

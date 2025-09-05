from(bucket: "SOC")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  // 只保留座標欄位，並確保都有值
  |> keep(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
  |> filter(fn: (r) =>
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon and
      r.src_lat != "nan" and r.src_lon != "nan" and
      r.dst_lat != "nan" and r.dst_lon != "nan"
  )
  // 以「來源/目的座標」當 key 分組，算每條路徑的事件數
  |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
  |> count()
  |> rename(columns: {_value: "events"})
  // 解除分組，才能排序整體 Top
  |> group()
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 20)
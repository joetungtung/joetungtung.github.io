from(bucket: "SOC")
  |> range(start: -48h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  // 保留必要欄位 + _time，避免 Grafana 看不到
  |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
  // 嘗試轉換成 float，若是字串也會轉數字
  |> map(fn: (r) => ({
        r with
        src_lat: float(v: r.src_lat),
        src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat),
        dst_lon: float(v: r.dst_lon),
    })
  )
  // 濾掉經緯度為空的資料
  |> filter(fn: (r) =>
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon
  )
  // 以 (src_lat,src_lon,dst_lat,dst_lon) 做 group
  |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
  |> count(column: "src_lat")   // 隨便挑一個欄位來 count
  |> rename(columns: {_value: "events"})
  // 整體排序，取 Top 50 條路徑
  |> group()
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 50)
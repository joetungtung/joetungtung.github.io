// ① 先把用得到的欄位變成寬表（有 src/dst 及四個座標）
base =
  from(bucket: "SOC")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> filter(fn: (r) =>
      r._field == "src" or r._field == "dst" or
      r._field == "src_lat" or r._field == "src_lon" or
      r._field == "dst_lat" or r._field == "dst_lon"
    )
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    // 確保六個欄位都有值（避免地圖找不到座標）
    |> filter(fn: (r) =>
      exists r.src and r.src != "" and
      exists r.dst and r.dst != "" and
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon
    )

// ② 每一列標上 1.0，方便做總量彙整
counts =
  base
    |> map(fn: (r) => ({ r with one: 1.0 }))
    |> group(columns: ["src","dst"])      // 以 src→dst 成對分組
    |> sum(column: "one")                 // 對 one 加總，得到事件數
    |> rename(columns: {one: "events"})
    |> keep(columns: ["src","dst","events"])

// ③ 取每組 src→dst 的代表座標（拿第一筆即可）
coords =
  base
    |> group(columns: ["src","dst"])
    |> first(column: "src_lat")
    |> first(column: "src_lon")
    |> first(column: "dst_lat")
    |> first(column: "dst_lon")
    |> keep(columns: ["src","dst","src_lat","src_lon","dst_lat","dst_lon"])

// ④ 合併事件數與座標，並控制線條數量
join(tables: {a: counts, b: coords}, on: ["src","dst"])
  |> keep(columns: ["src","dst","events","src_lat","src_lon","dst_lat","dst_lon"])
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 200)
// ① 先把需要的欄位轉成寬表
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
    |> filter(fn: (r) =>
      exists r.src and exists r.dst and
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon
    )

// ② 每個 (src,dst) 的事件數（指定欄位來數）
counts =
  base
    |> group(columns: ["src","dst"])
    |> count(column: "src")
    |> rename(columns: {_value: "events"})
    |> keep(columns: ["src","dst","events"])

// ③ 每組 (src,dst) 的代表座標（取第一筆）
coords =
  base
    |> group(columns: ["src","dst"])
    |> first(column: "src_lat")
    |> first(column: "src_lon")
    |> first(column: "dst_lat")
    |> first(column: "dst_lon")
    |> keep(columns: ["src","dst","src_lat","src_lon","dst_lat","dst_lon"])

// ④ 合併
join(tables: {a: counts, b: coords}, on: ["src","dst"])
  |> keep(columns: ["src","dst","events","src_lat","src_lon","dst_lat","dst_lon"])
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 200)
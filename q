// ① 先把要用的 fields 收齊並 pivot 成欄位
base =
  from(bucket: "SOC")
    |> range(start: $__timeFrom(), stop: $__timeTo())
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    // 只取路徑需要的六個欄位
    |> filter(fn: (r) =>
      r._field == "src" or r._field == "dst" or
      r._field == "src_lat" or r._field == "src_lon" or
      r._field == "dst_lat" or r._field == "dst_lon"
    )
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    // 確保六個欄位都存在（避免 NaN 造成地圖報黃驚嘆號）
    |> filter(fn: (r) =>
      exists r.src and exists r.dst and
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon
    )

// ② 計算每個 src→dst 的事件數
counts =
  base
    |> group(columns: ["src","dst"])
    |> count()
    |> rename(columns: {_value: "events"})
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

// ④ 合併「事件數」與「座標」
join(tables: {a: counts, b: coords}, on: ["src","dst"])
  |> keep(columns: ["src","dst","events","src_lat","src_lon","dst_lat","dst_lon"])
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 200)   // 避免線太多造成面板卡頓
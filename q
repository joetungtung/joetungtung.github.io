from(bucket: "SOC")
  |> range(start: -12h)                            // 自行調時間窗
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  // 只拿有座標的資料
  |> filter(fn: (r) => exists r.dst_lat and exists r.dst_lon)
  // 把 _field / _value 轉成多欄位（等價 schema.fieldsAsCols()，但更穩）
  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  // 強制轉成 float，避免字串造成 No data
  |> map(fn: (r) => ({
      r with
      src_lat: if exists r.src_lat then float(v: r.src_lat) else float(v: 0.0),
      src_lon: if exists r.src_lon then float(v: r.src_lon) else float(v: 0.0),
      dst_lat: float(v: r.dst_lat),
      dst_lon: float(v: r.dst_lon),
    }))
  // 只留下地圖需要的欄位
  |> keep(columns: ["_time","src","dst","src_lat","src_lon","dst_lat","dst_lon"])
  |> limit(n: 500)
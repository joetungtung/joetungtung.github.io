from(bucket: "SOC")
  |> range(start: -48h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  // 只留需要的欄位，保留 _time 以免 Grafana 看不到資料
  |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
  // 經緯度轉成 float（若本來就是數字也沒關係）
  |> map(fn: (r) => ({
        r with
        src_lat: float(v: r.src_lat),
        src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat),
        dst_lon: float(v: r.dst_lon),
    })
  )
  // 濾掉 NaN / 空值
  |> filter(fn: (r) =>
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon and
      not isNaN(v: r.src_lat) and not isNaN(v: r.src_lon) and
      not isNaN(v: r.dst_lat) and not isNaN(v: r.dst_lon)
  )
  // 以座標做 group，計算每條路徑的事件數
  |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
  |> count(column: "src_lat")
  |> rename(columns: {_value: "events"})
  // 解除分組後排序取前 50
  |> group()
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 50)
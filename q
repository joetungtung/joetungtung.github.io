import "influxdata/influxdb/schema"

data =
  from(bucket: "SOC")
    |> range(start: -6h)                                   // 時間自行調
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> schema.fieldsAsCols()                               // 把各 field 攤成同一列
    |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
    // 轉成數值 + 建一個事件計數欄位
    |> map(fn: (r) => ({
        r with
        src_lat: float(v: r.src_lat),
        src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat),
        dst_lon: float(v: r.dst_lon),
        events: 1.0
    }))
    // 過濾掉缺值或 0,0 的座標
    |> filter(fn: (r) =>
        exists r.src_lat and exists r.src_lon and exists r.dst_lat and exists r.dst_lon and
        r.src_lat != 0.0 and r.src_lon != 0.0 and r.dst_lat != 0.0 and r.dst_lon != 0.0
    )
    // 降取樣，避免點數過多（可依需要調整 every）
    |> aggregateWindow(every: 1m, fn: sum, createEmpty: false)
    |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon","events"])
    |> limit(n: 5000)

data
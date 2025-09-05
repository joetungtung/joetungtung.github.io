import "influxdata/influxdb/schema"

// 1) 先把原始事件縮成「唯一路徑」+ 計數
base =
  from(bucket: "SOC")
    |> range(start: -12h)                                   // 時間自行調
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> schema.fieldsAsCols()
    |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
    |> map(fn: (r) => ({                                   // 確保是數值
        r with
        src_lat: float(v: r.src_lat),
        src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat),
        dst_lon: float(v: r.dst_lon),
    }))
    |> filter(fn: (r) =>                                    // 濾掉 0 或空
        exists r.src_lat and exists r.src_lon and exists r.dst_lat and exists r.dst_lon and
        r.src_lat != 0.0 and r.src_lon != 0.0 and r.dst_lat != 0.0 and r.dst_lon != 0.0
    )
    |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
    |> count(column: "_time")                                // 對每條路徑計數
    |> rename(columns: {_value: "events"})                   // 計數欄改名
    |> group()                                               // 還原為單一表（便於後續 union）

// 2) 為 Route layer 準備兩筆（src / dst），欄名統一成 latitude / longitude
src =
  base
    |> map(fn: (r) => ({
        latitude:  r.src_lat,
        longitude: r.src_lon,
        events:    r.events,
        gid:       string(v: r.src_lat) + "," + string(v: r.src_lon) + "→" + string(v: r.dst_lat) + "," + string(v: r.dst_lon),
        hop:       0.0
    }))

dst =
  base
    |> map(fn: (r) => ({
        latitude:  r.dst_lat,
        longitude: r.dst_lon,
        events:    r.events,
        gid:       string(v: r.src_lat) + "," + string(v: r.src_lon) + "→" + string(v: r.dst_lat) + "," + string(v: r.dst_lon),
        hop:       1.0
    }))

// 3) 合併為「兩筆一組」並按 hop 排序，Route 會把同組兩點連線
union(tables: [src, dst])
  |> group(columns: ["gid"])                                  // 每組 = 一條線
  |> sort(columns: ["hop"], desc: false)
  |> keep(columns: ["latitude","longitude","events","gid"])   // 給地圖用
  |> limit(n: 5000)
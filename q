import "influxdata/influxdb/schema"

// A) 取四個座標欄，轉 float，濾掉缺值與非法值
base =
  from(bucket: "SOC")
    |> range(start: -12h)                                   // 視需要調整
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> schema.fieldsAsCols()
    |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
    |> map(fn: (r) => ({
        r with
        src_lat: float(v: r.src_lat),
        src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat),
        dst_lon: float(v: r.dst_lon),
    }))
    |> filter(fn: (r) =>
        exists r.src_lat and exists r.src_lon and
        exists r.dst_lat and exists r.dst_lon and
        r.src_lat != 0.0 and r.src_lon != 0.0 and
        r.dst_lat != 0.0 and r.dst_lon != 0.0 and
        r.src_lat >= -90.0 and r.src_lat <= 90.0 and
        r.dst_lat >= -90.0 and r.dst_lat <= 90.0 and
        r.src_lon >= -180.0 and r.src_lon <= 180.0 and
        r.dst_lon >= -180.0 and r.dst_lon <= 180.0
    )

// B) 以同一路徑（四座標組）做計數聚合
routesAgg =
  base
    |> map(fn: (r) => ({ r with events: 1.0 }))             // 先做常數欄
    |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
    |> map(fn: (r) => ({ r with _value: r.events }))        // **關鍵**：把 events 複製到 _value
    |> sum()                                                // 對 _value 做加總
    |> rename(columns: {_value: "events"})                  // 把結果改名回 events
    |> group()
    |> keep(columns: ["src_lat","src_lon","dst_lat","dst_lon","events"])
    |> limit(n: 1000)

// C) 展成兩筆（src/dst），用 gid 把兩點連成一條線
src =
  routesAgg
    |> map(fn: (r) => ({
        latitude:  r.src_lat,
        longitude: r.src_lon,
        events:    r.events,
        gid:       string(v: r.src_lat) + "," + string(v: r.src_lon) + "→" +
                   string(v: r.dst_lat) + "," + string(v: r.dst_lon),
        hop:       0.0
    }))

dst =
  routesAgg
    |> map(fn: (r) => ({
        latitude:  r.dst_lat,
        longitude: r.dst_lon,
        events:    r.events,
        gid:       string(v: r.src_lat) + "," + string(v: r.src_lon) + "→" +
                   string(v: r.dst_lat) + "," + string(v: r.dst_lon),
        hop:       1.0
    }))

union(tables: [src, dst])
  |> group(columns: ["gid"])
  |> sort(columns: ["hop"], desc: false)
  |> keep(columns: ["latitude","longitude","events","gid"])
  |> limit(n: 5000)
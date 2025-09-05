import "influxdata/influxdb/schema"

// 1) 抽出四個座標欄位，轉數值、過濾空值
base =
  from(bucket: "SOC")
    |> range(start: -12h)                                   // 先用近12h；要更大再調
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
        r.dst_lat != 0.0 and r.dst_lon != 0.0
    )
    |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
    |> count()                                              // ← 重點：不帶 column，計每群筆數
    |> rename(columns: {_value: "events"})
    |> group()

// 2) 展成兩筆：src點 與 dst點，欄名統一為 latitude / longitude
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

// 3) 合併並依 gid 排序（同一 gid 的兩筆會被 Route 畫成一條線）
union(tables: [src, dst])
  |> group(columns: ["gid"])
  |> sort(columns: ["hop"], desc: false)
  |> keep(columns: ["latitude","longitude","events","gid"])
  |> limit(n: 5000)
import "influxdata/influxdb/schema"

base =
  from(bucket: "SOC")
    |> range(start: -12h)                                // 時間窗自己調
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
        exists r.src_lat and exists r.src_lon and exists r.dst_lat and exists r.dst_lon and
        r.src_lat != 0.0 and r.src_lon != 0.0 and r.dst_lat != 0.0 and r.dst_lon != 0.0
    )
    |> duplicate(column: "_time", as: "route_id")        // ① 先複製出一欄
    |> map(fn: (r) => ({ r with route_id: string(v: r.route_id) }))   // ② 轉成字串，便於分組

src =
  base
    |> keep(columns: ["_time","route_id","src_lat","src_lon"])
    |> rename(columns: {src_lat: "latitude", src_lon: "longitude"})

dst =
  base
    |> keep(columns: ["_time","route_id","dst_lat","dst_lon"])
    |> rename(columns: {dst_lat: "latitude", dst_lon: "longitude"})

union(tables: [src, dst])
  |> group(columns: ["route_id"])                        // 每個 route_id = 一條線
  |> sort(columns: ["_time"], desc: false)
  |> keep(columns: ["_time","route_id","latitude","longitude"])       // 明確保留
  |> limit(n: 2000)                                      // 太多就縮短時間窗或調小 n
import "influxdata/influxdb/schema"

base =
  from(bucket: "SOC")
    |> range(start: -6h)                      // 先用較小時段，畫線更快
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> schema.fieldsAsCols()
    |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
    |> map(fn: (r) => ({
        _time: r._time,
        src_lat: float(v: r.src_lat), src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat), dst_lon: float(v: r.dst_lon),
    }))
    |> filter(fn: (r) =>
        exists r.src_lat and exists r.src_lon and
        exists r.dst_lat and exists r.dst_lon and
        r.src_lat != 0.0 and r.src_lon != 0.0 and
        r.dst_lat != 0.0 and r.dst_lon != 0.0)

src =
  base
    |> keep(columns: ["_time","src_lat","src_lon"])
    |> rename(columns: {src_lat: "latitude", src_lon: "longitude"})
    |> set(key: "hop", value: "0")

dst =
  base
    |> keep(columns: ["_time","dst_lat","dst_lon"])
    |> rename(columns: {dst_lat: "latitude", dst_lon: "longitude"})
    |> set(key: "hop", value: "1")

routes =
  union(tables: [src, dst])
    |> map(fn: (r) => ({ r with
        hop: int(v: r.hop),              // Route 層的排序要數字
        gid: string(v: r._time),         // 同一條線的 track id
        events: 1.0                      // 之後可當線寬/顏色用
    }))
    |> group(columns: ["gid","hop"])
    |> keep(columns: ["gid","hop","latitude","longitude","events"])

routes
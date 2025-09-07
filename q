import "influxdata/influxdb/schema"

base =
  from(bucket: "SOC")
    |> range(start: -12h)
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> schema.fieldsAsCols()
    // 如果只看「到台灣」的連線，打開下一行；否則先註解
    // |> filter(fn: (r) => r.target_geo_country_name == "Taiwan")
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
    // 🔑 這裡就先做出「同一路徑」的唯一 ID
    |> map(fn: (r) => ({
      r with
      route_id: string(v: r.src_lat) + "," + string(v: r.src_lon) + "→" +
                string(v: r.dst_lat) + "," + string(v: r.dst_lon),
    }))
    |> limit(n: 2000)   // 先限流，避免點太多

src =
  base
    |> keep(columns: ["route_id","src_lat","src_lon"])
    |> rename(columns: {src_lat: "latitude", src_lon: "longitude"})
    |> map(fn: (r) => ({ r with hop: 0 }))

dst =
  base
    |> keep(columns: ["route_id","dst_lat","dst_lon"])
    |> rename(columns: {dst_lat: "latitude", dst_lon: "longitude"})
    |> map(fn: (r) => ({ r with hop: 1 }))

union(tables: [src, dst])
  |> keep(columns: ["route_id","hop","latitude","longitude"])
  |> limit(n: 4000)
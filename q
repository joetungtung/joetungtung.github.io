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








import "influxdata/influxdb/schema"

base =
  from(bucket: "SOC")
    |> range(start: -6h)                                    // 取最近6小時，可自行調整
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> filter(fn: (r) => r.target_geo_country_name == "Taiwan")  // ★ 限定目的地是台灣
    |> schema.fieldsAsCols()
    |> keep(columns: ["_time","attacker_address","src_lat","src_lon","dst_lat","dst_lon"])
    |> map(fn: (r) => ({
        r with
        src_lat: float(v: r.src_lat),
        src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat),
        dst_lon: float(v: r.dst_lon)
    }))
    |> filter(fn: (r) =>
        exists r.src_lat and exists r.src_lon and
        exists r.dst_lat and exists r.dst_lon and
        r.src_lat != 0.0 and r.src_lon != 0.0 and
        r.dst_lat != 0.0 and r.dst_lon != 0.0
    )

// === 加上 hop 與 route_id ===
routes =
  base
    |> map(fn: (r) => ({
        r with
        // hop = 0 → 表示來源； hop = 1 → 表示目的地
        hop: 0,
        latitude: r.src_lat,
        longitude: r.src_lon,
        route_id: string(v: r.src_lat) + "," + string(v: r.src_lon) + "=>" + string(v: r.dst_lat) + "," + string(v: r.dst_lon)
    }))
    |> union(
      tables: base
        |> map(fn: (r) => ({
            r with
            hop: 1,
            latitude: r.dst_lat,
            longitude: r.dst_lon,
            route_id: string(v: r.src_lat) + "," + string(v: r.src_lon) + "=>" + string(v: r.dst_lat) + "," + string(v: r.dst_lon)
        }))
    )
    |> group(columns: ["route_id"])
    |> sort(columns: ["hop"], desc: false)   // 確保來源在前，目的地在後
    |> keep(columns: ["_time","latitude","longitude","hop","route_id"])

routes



invalid: error @35:8-44:6: missing pipe argument error @37:12-43:12: expected [stream[A]] (array) but found stream[{ B with src_lon: F, src_lon: float, src_lat: E, src_lat: float, route_id: string, longitude: float, latitude: float, hop: int, dst_lon: D, dst_lon: float, dst_lat: C, dst_lat: float, }] (argument tables) error @37:12-43:12: expected stream[A] but found { B with src_lon: F, src_lon: float, src_lat: E, src_lat: float, route_id: string, longitude: float, latitude: float, hop: int, dst_lon: D, dst_lon: float, dst_lat: C, dst_lat: float, } (record) (argument tables)


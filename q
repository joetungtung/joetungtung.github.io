import "influxdata/influxdb/schema"

routes =
  from(bucket: "SOC")
    |> range(start: -7d)
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> schema.fieldsAsCols()
    |> map(fn: (r) => ({
        r with
        src_lat: if exists r.src_lat then float(v: r.src_lat) else 0.0,
        src_lon: if exists r.src_lon then float(v: r.src_lon) else 0.0,
        dst_lat: if exists r.dst_lat then float(v: r.dst_lat) else 0.0,
        dst_lon: if exists r.dst_lon then float(v: r.dst_lon) else 0.0,
    }))
    |> filter(fn: (r) => r.src_lat != 0.0 and r.src_lon != 0.0 and r.dst_lat != 0.0 and r.dst_lon != 0.0)
    |> keep(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
    |> group(columns: [])     // 變成單一資料框（非多資料框）
    |> limit(n: 3)            // 只取 3 條，方便測試

routes
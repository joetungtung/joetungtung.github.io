import "influxdata/influxdb/schema"

base =
  from(bucket: "SOC")
    |> range(start: -24h)
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

routes =
  base
    |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
    |> count(column: "_time")                 // 用 _time 當計數依據
    |> rename(columns: {_value: "events"})
    |> group()
    |> sort(columns: ["events"], desc: true)
    |> limit(n: 50)

routes
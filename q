import "influxdata/influxdb/schema"

from(bucket: "SOC")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> schema.fieldsAsCols()
  |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon","src","dst"])
  |> limit(n: 10)
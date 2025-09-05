import "influxdata/influxdb/schema"

schema.fieldKeys(
  bucket: "SOC",
  predicate: (r) => r._measurement == "arcsight_event"
)


import "influxdata/influxdb/schema"

schema.tagKeys(
  bucket: "SOC",
  predicate: (r) => r._measurement == "arcsight_event"
)




from(bucket: "SOC")
  |> range(start: 0)               // 先不踩時間窗地雷
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> keep(columns: ["_time","_field","_value","src","dst","src_lat","src_lon","dst_lat","dst_lon"])
  |> limit(n: 20)
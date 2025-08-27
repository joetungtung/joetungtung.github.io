from(bucket: "SOC")
  |> range(start: $__timeFrom(), stop: $__timeTo())
  |> filter(fn:(r)=> r._measurement=="arcsight_event")
  |> filter(fn:(r)=> r._field=="bytes")   // 只用 bytes 代表事件存在
  |> drop(columns: ["_start","_stop"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 100)
  |> keep(columns: ["_time","attacker_address","target_address","agent_name","target_geo_country_name","_value"])
  |> rename(columns: {_value:"bytes"})





from(bucket: "SOC")
  |> range(start: $__timeFrom(), stop: $__timeTo())
  |> filter(fn:(r)=> r._measurement=="arcsight_event")
  |> filter(fn:(r)=> r._field=="bytes")
  |> aggregateWindow(every: 1m, fn: sum, createEmpty: false)
  |> yield(name: "bytes_per_min")





from(bucket: "SOC")
  |> range(start: $__timeFrom(), stop: $__timeTo())
  |> filter(fn:(r)=> r._measurement=="arcsight_event")
  |> filter(fn:(r)=> r._field=="bytes")
  |> group(columns: ["target_geo_country_name"])
  |> sum(column: "_value")
  |> rename(columns: {_value: "bytes"})
  |> top(n: 10, columns: ["bytes"])
  |> keep(columns: ["target_geo_country_name","bytes"])
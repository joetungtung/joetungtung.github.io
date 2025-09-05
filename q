import "influxdata/influxdb/schema"

schema.fieldKeys(
  bucket: "SOC",
  predicate: (r) => r._measurement == "arcsight_event",
  start: -7d
)




import "influxdata/influxdb/schema"
schema.tagKeys(bucket:"SOC", predicate:(r)=> r._measurement=="arcsight_event", start:-7d)



from(bucket:"SOC")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> filter(fn: (r) => r._field =~ /^(dst_lat|dst_lon|src_lat|src_lon)$/)
  |> limit(n: 20)





import "influxdata/influxdb/schema"

from(bucket:"SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> schema.fieldsAsCols()
  // 把字串數字安全地轉成 float；空值給 0
  |> map(fn: (r) => ({
      r with
      dst_lat: if exists r.dst_lat and string(v:r.dst_lat) <> "" then float(v:r.dst_lat) else 0.0,
      dst_lon: if exists r.dst_lon and string(v:r.dst_lon) <> "" then float(v:r.dst_lon) else 0.0,
      src_lat: if exists r.src_lat and string(v:r.src_lat) <> "" then float(v:r.src_lat) else 0.0,
      src_lon: if exists r.src_lon and string(v:r.src_lon) <> "" then float(v:r.src_lon) else 0.0
  }))
  |> filter(fn: (r) => r.dst_lat != 0.0 and r.dst_lon != 0.0)   // 至少目的地要有座標
  |> keep(columns: ["_time","src","dst","src_lat","src_lon","dst_lat","dst_lon"])
  |> limit(n: 500)



|> map(fn: (r) => ({
  r with
  src_lat: if r.src_lat == 0.0 then 25.04 else r.src_lat,
  src_lon: if r.src_lon == 0.0 then 121.53 else r.src_lon
}))





import "influxdata/influxdb/schema"

from(bucket: "SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> schema.fieldsAsCols()
  // 正常化：字串空白/ 'nan' 變成 0，再轉成 float
  |> map(fn: (r) => ({
      r with
      dst_lat: if exists r.dst_lat and string(v: r.dst_lat) != "" and string(v: r.dst_lat) != "nan" then float(v: r.dst_lat) else 0.0,
      dst_lon: if exists r.dst_lon and string(v: r.dst_lon) != "" and string(v: r.dst_lon) != "nan" then float(v: r.dst_lon) else 0.0,
      src_lat: if exists r.src_lat and string(v: r.src_lat) != "" and string(v: r.src_lat) != "nan" then float(v: r.src_lat) else 0.0,
      src_lon: if exists r.src_lon and string(v: r.src_lon) != "" and string(v: r.src_lon) != "nan" then float(v: r.src_lon) else 0.0,
      src:     if exists r.src and string(v: r.src) != "" and string(v: r.src) != "nan" then string(v: r.src) else ""
  }))
  // 只保留源、目的座標都齊全的列
  |> filter(fn: (r) => r.dst_lat != 0.0 and r.dst_lon != 0.0 and r.src_lat != 0.0 and r.src_lon != 0.0 and r.src != "")
  |> keep(columns: ["_time","src","dst","src_lat","src_lon","dst_lat","dst_lon"])
  |> limit(n: 100)






invalid: compilation failed: error @10:61-10:62: invalid expression: invalid token for primary expression: GT error @11:61-11:62: invalid expression: invalid token for primary expression: GT error @12:61-12:62: invalid expression: invalid token for primary expression: GT error @13:61-13:62: invalid expression: invalid token for primary expression: GT


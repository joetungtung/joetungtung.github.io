import "influxdata/influxdb/schema"

base =
from(bucket: "SOC")
  |> range(start: -48h)                                    // 不要用 start: 0，先限制時間避免超重
  |> filter(fn: (r) =>
      r._measurement == "arcsight_event" and
      (r._field == "src_lat" or r._field == "src_lon" or
       r._field == "dst_lat" or r._field == "dst_lon"))
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
  // 強制轉成數值，並過濾掉 0 / 空值 / 超出地理範圍的雜訊
  |> map(fn: (r) => ({
      r with
      src_lat: float(v: r.src_lat),  src_lon: float(v: r.src_lon),
      dst_lat: float(v: r.dst_lat),  dst_lon: float(v: r.dst_lon),
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

routes =
base
  // 不要對 _time 做 count（會出 “unsupported aggregate column type time”）
  // 用一個常數欄位來計數，再做 sum
  |> map(fn: (r) => ({ r with events: 1.0 }))
  |> group(columns: ["src_lat","src_lon","dst_lat","dst_lon"])
  |> sum(column: "events")
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 200)                                         // 先限量，畫線會順很多
  // 某些 Grafana 版本會自動識別下列欄名，順手改成它愛的名字
  |> rename(columns: {
      src_lat: "sourceLatitude",  src_lon: "sourceLongitude",
      dst_lat: "destinationLatitude", dst_lon: "destinationLongitude"
  })

routes
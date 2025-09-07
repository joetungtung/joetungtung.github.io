import "influxdata/influxdb/schema"

// 1) 取基礎資料（可調時間窗）
base =
from(bucket: "SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> schema.fieldsAsCols()
  // 如果「台灣」是 tag（常見），用這條；不是就註解掉換下一條
  |> filter(fn: (r) => r.target_geo_country_name == "Taiwan")
  // 若「台灣」是 field，改用下一行：
  // |> filter(fn: (r) => exists r.target_geo_country_name and r.target_geo_country_name == "Taiwan")
  |> filter(fn: (r) =>
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon
  )
  // 轉成 float，避免字串/整數型別讓地圖無法吃
  |> map(fn: (r) => ({
      r with
      src_lat: float(v: r.src_lat),
      src_lon: float(v: r.src_lon),
      dst_lat: float(v: r.dst_lat),
      dst_lon: float(v: r.dst_lon),
  }))
  // 過濾掉 0 / NaN（避免落在 (0,0)）
  |> filter(fn: (r) =>
      r.src_lat != 0.0 and r.src_lon != 0.0 and
      r.dst_lat != 0.0 and r.dst_lon != 0.0
  )
  // 控制量（視需要調整）
  |> limit(n: 2000)

// 2) 變兩列：hop=0（來源點）、hop=1（目的點）
src =
  base
    |> keep(columns: ["_time","src_lat","src_lon"])
    |> rename(columns: {src_lat: "latitude", src_lon: "longitude"})
    // 用 time 當 group id（也可換成你的 event_id）
    |> map(fn: (r) => ({ gid: string(v: r._time), hop: 0, latitude: r.latitude, longitude: r.longitude }))

dst =
  base
    |> keep(columns: ["_time","dst_lat","dst_lon"])
    |> rename(columns: {dst_lat: "latitude", dst_lon: "longitude"})
    |> map(fn: (r) => ({ gid: string(v: r._time), hop: 1, latitude: r.latitude, longitude: r.longitude }))

// 3) 合併為路徑表（每條路徑 = 同一 gid 的 hop=0 → hop=1）
routes =
  union(tables: [src, dst])
    |> group(columns: ["gid"])
    |> sort(columns: ["hop"], desc: false)
    |> keep(columns: ["gid","hop","latitude","longitude"])
    // 再保險限制一次（避免點數爆表）
    |> limit(n: 4000)

// 4) 輸出
routes
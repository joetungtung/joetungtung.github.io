import "influxdata/influxdb/schema"

// 1) 取出你要的四個座標欄位，轉成數值並過濾 0/缺值
base =
  from(bucket: "SOC")
    |> range(start: -12h)                      // 視需求調整時間窗
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
      exists r.src_lat and exists r.src_lon and exists r.dst_lat and exists r.dst_lon and
      r.src_lat != 0.0 and r.src_lon != 0.0 and r.dst_lat != 0.0 and r.dst_lon != 0.0
    )

// 2) 把一筆事件拆成兩個點，並用相同的 route_id 串起來
srcPts =
  base
    |> keep(columns: ["_time","src_lat","src_lon"])
    |> rename(columns: {src_lat: "latitude", src_lon: "longitude"})
    |> map(fn: (r) => ({ r with route_id: string(v: r._time) }))

dstPts =
  base
    |> keep(columns: ["_time","dst_lat","dst_lon"])
    |> rename(columns: {dst_lat: "latitude", dst_lon: "longitude"})
    |> map(fn: (r) => ({ r with route_id: string(v: r._time) }))

// 3) 合併，依 route_id 分組並按時間排序（每條線 = 一個事件）
union(tables: [srcPts, dstPts])
  |> group(columns: ["route_id"])
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: 2000)  // 太多就提高時間窗或加 limit，避免點數爆表
import "influxdata/influxdb/schema"

// 1) 先把原始資料變成欄位表格（含經緯度），在時間範圍內
base =
  from(bucket: "SOC")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> schema.fieldsAsCols()                                  // 轉成寬表，拿得到 dst_lat 等欄位
    |> keep(columns: ["_time","src_lat","src_lon","dst_lat","dst_lon"])
    // 轉成浮點、同時保留 _time 以便當作配對用的 gid
    |> map(fn: (r) => ({
        _time: r._time,
        src_lat: float(v: r.src_lat),
        src_lon: float(v: r.src_lon),
        dst_lat: float(v: r.dst_lat),
        dst_lon: float(v: r.dst_lon),
    }))
    // 基本清洗：排除 0 或 NaN
    |> filter(fn: (r) =>
        exists r.src_lat and exists r.src_lon and
        exists r.dst_lat and exists r.dst_lon and
        r.src_lat != 0.0 and r.src_lon != 0.0 and
        r.dst_lat != 0.0 and r.dst_lon != 0.0
    )

// 2) 拆成起點與終點兩份表，並加上 hop / gid
src =
  base
    |> keep(columns: ["_time","src_lat","src_lon"])
    |> rename(columns: {src_lat: "latitude", src_lon: "longitude"})
    |> map(fn: (r) => ({ r with hop: 0, gid: string(v: r._time) }))
    |> keep(columns: ["latitude","longitude","hop","gid"])

dst =
  base
    |> keep(columns: ["_time","dst_lat","dst_lon"])
    |> rename(columns: {dst_lat: "latitude", dst_lon: "longitude"})
    |> map(fn: (r) => ({ r with hop: 1, gid: string(v: r._time) }))
    |> keep(columns: ["latitude","longitude","hop","gid"])

// 3) 合併成 Route layer 可用格式（同一 gid 會有兩列：hop=0 與 hop=1）
union(tables: [src, dst])
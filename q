import "influxdata/influxdb/schema"

// 先把資料整成想要的欄位、型別
base =
  from(bucket: "SOC")
    |> range(start: -6h)                                   // 需要就改時間窗
    |> filter(fn: (r) => r._measurement == "arcsight_event")
    |> filter(fn: (r) => r.target_geo_country_name == "Taiwan")   // 只保留「目的地＝台灣」
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

// 拆成來源點與目的點兩條 stream
src =
  base
    |> map(fn: (r) => ({
        r with
        hop: 0,
        latitude:  r.src_lat,
        longitude: r.src_lon,
        route_id:  string(v: r.src_lat) + "," + string(v: r.src_lon) + "=>" +
                   string(v: r.dst_lat) + "," + string(v: r.dst_lon)
    }))

dst =
  base
    |> map(fn: (r) => ({
        r with
        hop: 1,
        latitude:  r.dst_lat,
        longitude: r.dst_lon,
        route_id:  string(v: r.src_lat) + "," + string(v: r.src_lon) + "=>" +
                   string(v: r.dst_lat) + "," + string(v: r.dst_lon)
    }))

// 合併、依 route 分組，並確保「來源在前、目的在後」
routes =
  union(tables: [src, dst])
    |> group(columns: ["route_id"])
    |> sort(columns: ["hop"], desc: false)
    |> keep(columns: ["_time","latitude","longitude","hop","route_id"])

// 最終輸出
routes





from(bucket:"SOC")
|> range(start:-12h)
|> filter(fn:(r)=> r._measurement=="arcsight_event")
|> filter(fn:(r)=> r.target_geo_country_name=="Taiwan")
|> schema.fieldsAsCols()
|> map(fn:(r)=> ({
    r with hasSrc: if exists r.src_lat and exists r.src_lon and r.src_lat!=0.0 and r.src_lon!=0.0 then "ok" else "missing"
}))
|> group(columns:["hasSrc","attacker_geo_country_name"])
|> count(column:"_time")
|> rename(columns:{_value:"events"})
|> sort(columns:["hasSrc","events"], desc:true)
|> limit(n:50)






base =
  from(bucket:"SOC")
  |> range(start:-12h)
  |> filter(fn:(r)=> r._measurement=="arcsight_event")
  |> filter(fn:(r)=> r.target_geo_country_name=="Taiwan")
  |> schema.fieldsAsCols()
  |> keep(columns:["_time","src_lat","src_lon","dst_lat","dst_lon"])

src = base |> map(fn:(r)=> ({ r with hop:0, latitude:float(v:r.src_lat), longitude:float(v:r.src_lon) }))
dst = base |> map(fn:(r)=> ({ r with hop:1, latitude:float(v:r.dst_lat), longitude:float(v:r.dst_lon) }))

union(tables:[src,dst])
|> group(columns:["_time"])
|> sort(columns:["hop"], desc:false)
|> limit(n:200)




import "influxdata/influxdb/schema"

from(bucket:"SOC")
|> range(start:-7d)
|> filter(fn:(r)=> r._measurement=="arcsight_event")
|> filter(fn:(r)=> r.target_geo_country_name=="Taiwan")
|> schema.fieldsAsCols()
|> map(fn:(r)=> ({
    r with
    hasSrc: if exists r.src_lat and exists r.src_lon and
               r.src_lat != 0.0 and r.src_lon != 0.0
            then "ok" else "missing",
    one: 1.0                               // ← 做一個常數欄位
}))
|> group(columns:["hasSrc","attacker_geo_country_name"])
|> sum(column:"one")                        // ← 用 sum(one) 當事件數
|> rename(columns:{one:"events"})
|> sort(columns:["hasSrc","events"], desc:true)
|> limit(n:50)









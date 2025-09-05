// 取最近 24h，可自行改成 $__timeFrom()/To()
from(bucket: "SOC")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  // 確保兩端國名與座標都存在
  |> filter(fn: (r) =>
      exists r.src and r.src != "" and
      exists r.dst and r.dst != "" and
      exists r.src_lat and exists r.src_lon and
      exists r.dst_lat and exists r.dst_lon
  )
  // 以 (src, dst) 成對彙總事件數
  |> group(columns: ["src","dst"])
  |> count()
  // 把對應的座標帶下來（在每個 group 拿第一筆即可）
  |> first(column: "src_lat")
  |> first(column: "src_lon")
  |> first(column: "dst_lat")
  |> first(column: "dst_lon")
  // 清理欄位名稱（把 _value 改名成 events）
  |> rename(columns: {_value: "events"})
  // 解除 group key 讓欄位變成真正欄位
  |> group(columns: [])
  // 僅保留地圖需要的欄位
  |> keep(columns: ["src","dst","events","src_lat","src_lon","dst_lat","dst_lon"])
  // 排序 + 取 Top N，避免太多線
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 200)
from(bucket: "SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  // 只保留有國家的資料
  |> filter(fn: (r) => exists r.attacker_geo_country_name and exists r.target_geo_country_name)
  // 以「來源國、目的國」分組後計數
  |> group(columns: ["attacker_geo_country_name", "target_geo_country_name"])
  |> count()
  // 轉成 Geomap 想要的欄位名
  |> rename(columns: {
      attacker_geo_country_name: "src",
      target_geo_country_name: "dst",
      _value: "events"
  })
  // 清掉空字串或 "nan" 垃圾值（視你的資料而定）
  |> filter(fn: (r) => r.src != "" and r.dst != "" and r.src != "nan" and r.dst != "nan")
  // 保留必要欄位、排序、取前 N 名
  |> keep(columns: ["src","dst","events"])
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 50)





from(bucket:"SOC")
  |> range(start: -12h)
  |> filter(fn:(r)=> r._measurement=="arcsight_event")
  |> filter(fn:(r)=> exists r.attacker_geo_country_name and exists r.target_geo_country_name)
  |> group(columns:["attacker_geo_country_name","target_geo_country_name"])
  |> count()
  |> rename(columns:{attacker_geo_country_name:"src", target_geo_country_name:"dst", _value:"events"})
  |> group(columns:["src"])
  |> sum(column:"events")
  |> keep(columns:["src","events"])
  |> sort(columns:["events"], desc:true)
  |> limit(n:10)
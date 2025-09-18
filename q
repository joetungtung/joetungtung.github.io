import "influxdata/influxdb/schema"
import "strings"

// 小工具：清理國名（去空白、排除空字串與 "nan"）
clean = (name) => {
  n = strings.trimSpace(v: name)
  return if n == "" or strings.toLower(v: n) == "nan" then "" else n
}

// ========= src 缺經緯度 by 攻擊國家 =========
src_missing =
from(bucket: "SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> schema.fieldsAsCols()
  |> map(fn: (r) => ({ r with _country: clean(name: r.attacker_geo_country_name) }))
  |> filter(fn: (r) => r._country != "")
  |> map(fn: (r) => ({ r with _flag: if (exists r.src_lat and exists r.src_lon) then 0 else 1 }))
  |> group(columns: ["_country"])
  |> sum(column: "_flag")
  |> map(fn: (r) => ({ country: r._country, role: "src", missing_count: r._flag }))
  |> keep(columns: ["country","role","missing_count"])

// ========= dst 缺經緯度 by 目標國家 =========
dst_missing =
from(bucket: "SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> schema.fieldsAsCols()
  |> map(fn: (r) => ({ r with _country: clean(name: r.target_geo_country_name) }))
  |> filter(fn: (r) => r._country != "")
  |> map(fn: (r) => ({ r with _flag: if (exists r.dst_lat and exists r.dst_lon) then 0 else 1 }))
  |> group(columns: ["_country"])
  |> sum(column: "_flag")
  |> map(fn: (r) => ({ country: r._country, role: "dst", missing_count: r._flag }))
  |> keep(columns: ["country","role","missing_count"])

// ========= 角色分開的排行榜（src/dst） =========
union(tables: [src_missing, dst_missing])
  |> sort(columns: ["missing_count","country","role"], desc: true)
  |> limit(n: 200)
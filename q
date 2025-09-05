from(bucket: "SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> filter(fn: (r) => exists r.attacker_geo_country_name and exists r.target_geo_country_name)
  |> group(columns: ["attacker_geo_country_name","target_geo_country_name"])
  |> count()
  |> group()
  |> rename(columns: {
      attacker_geo_country_name: "src",
      target_geo_country_name: "dst",
      _value: "events"
  })
  |> keep(columns: ["src","dst","events"])
  |> drop(columns: ["_start","_stop","_measurement","_field"])
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 100)
from(bucket: "SOC")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> group(columns: ["attacker_geo_country_name", "target_geo_country_name"])
  |> count()
  |> rename(columns: {
      attacker_geo_country_name: "src",
      target_geo_country_name: "dst",
      _value: "events"
  })
  |> sort(columns: ["events"], desc: true)
  |> limit(n: 50)
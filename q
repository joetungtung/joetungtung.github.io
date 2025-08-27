from(bucket: "SOC")
  |> range(start: $__timeFrom(), stop: $__timeTo())
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> keep(columns: ["_time", "attacker_geo_country_name"])
  |> group(columns: ["attacker_geo_country_name"])
  |> count()
  |> rename(columns: {
      attacker_geo_country_name: "country",
      _value: "events"
  })
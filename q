from(bucket: "SOC")
  |> range(start: $__timeFrom(), stop: $__timeTo())
  |> filter(fn: (r) => r._measurement == "arcsight_event")
  |> filter(fn: (r) => r.attacker_address == "192.168.1.10")  // 換成你要追的 IP
  |> aggregateWindow(every: 1m, fn: count, createEmpty: false)
  |> yield(name: "events_per_min")
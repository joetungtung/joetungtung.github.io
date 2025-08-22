[INGEST] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming\test.csv
[DEBUG] columns: ['event_time', 'src_ip', 'dst_ip', 'protocol', 'severity', 'bytes', 'device', 'message']
D:\Joe\Develop\GrafanaInfluxdb\Autoimport\arcsight_ingest.py:64: UserWarning: The argument 'infer_datetime_format' is deprecated and will be removed in a future version. A strict version of it is now the default, see https://pandas.pydata.org/pdeps/0004-consistent-to-datetime-parsing.html. You can safely remove this argument.
  return pd.to_datetime(xs, utc=True, errors="coerce", infer_datetime_format=True)
[ERROR] ingest failed: 'Pandas' object has no attribute '__event_time__'
[MOVE] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming\test.csv -> D:\Joe\Develop\GrafanaInfluxdb\Autoimport\failed\test.csv

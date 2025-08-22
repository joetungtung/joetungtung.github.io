(venv) D:\Joe\Develop\GrafanaInfluxdb\Autoimport>python arcsight_ingest.py
[WATCHING] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming
[TARGET] InfluxDB: http://localhost:8086, org=LINE BANK SOC, bucket=SOC, measurement=arcsight_event
[INGEST] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming\test.csv
[DEBUG] columns: ['event_time', 'src_ip', 'dst_ip', 'protocol', 'severity', 'bytes', 'device', 'message']
[ERROR] ingest failed: name 'pick_time_column' is not defined
[MOVE] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming\test.csv -> D:\Joe\Develop\GrafanaInfluxdb\Autoimport\failed\test.csv

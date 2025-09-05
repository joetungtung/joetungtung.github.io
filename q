(venv) D:\Joe\Develop\GrafanaInfluxdb\Autoimport>Offical_Arcsight_Ingest.py
[WATCHING] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming
[TARGET] InfluxDB: http://127.0.0.1:8086, org=LINE BANK SOC, bucket=SOC, measurement=arcsight_event
[INGEST] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming\DDoS_5min_8-26-25 11-31-27 AM.csv
[DEBUG] columns(normalized): ['end_time', 'start_time', 'name', 'device_vendor', 'attacker_address', 'attacker_port', 'agent_type', 'transport_protocol', 'agent_severity', 'device_action', 'attacker_geo_location_info', 'attacker_geo_country_name', 'target_address', 'target_port', 'target_geo_location_info', 'target_geo_country_name', 'manager_receipt_time', 'agent_id', 'agent_name']
[DEBUG] using time column: manager_receipt_time
[DEBUG] ts_min = 2025-08-26 03:26:38+00:00 ts_max = 2025-08-26 03:31:28+00:00 rows = 350
[DEBUG] tag_cols used: ['device_vendor', 'agent_name', 'agent_type', 'agent_id', 'transport_protocol', 'device_action', 'attacker_geo_country_name', 'target_geo_country_name', 'attacker_address', 'attacker_port', 'target_address', 'target_port']
[ERROR] ingest failed: (422)
Reason: Unprocessable Entity
HTTP response headers: HTTPHeaderDict({'Content-Type': 'application/json; charset=utf-8', 'X-Influxdb-Build': 'OSS', 'X-Influxdb-Version': 'v2.7.12', 'X-Platform-Error-Code': 'unprocessable entity', 'Date': 'Fri, 05 Sep 2025 09:04:10 GMT', 'Content-Length': '1140'})
HTTP response body: {"code":"unprocessable entity","message":"failure writing points to database: partial write: dropped 350 points outside retention policy of duration 72h0m0s - oldest point arcsight_event,agent_id=3rLPKhHABABC+0glav-tIYQ\\=\\=,agent_name=Radware_UDP515,agent_type=syslog,attacker_address=64.59.150.138,attacker_geo_country_name=Canada,attacker_port=40787.0,device_action=forward,device_vendor=Radware,target_address=122.147.229.201,target_geo_country_name=Taiwan,target_port=53.0,transport_protocol=UDP at 2025-08-26T03:26:38Z dropped because it violates a Retention Policy Lower Bound at 2025-09-02T09:04:10.7684903Z, newest point arcsight_event,agent_id=3rLPKhHABABC+0glav-tIYQ\\=\\=,agent_name=Radware_UDP515,agent_type=syslog,attacker_address=178.32.193.255,attacker_geo_country_name=France,attacker_port=0.0,device_action=drop,device_vendor=Radware,target_address=218.210.53.3,target_geo_country_name=Taiwan,target_port=0.0,transport_protocol=TCP at 2025-08-26T03:31:28Z dropped because it violates a Retention Policy Lower Bound at 2025-09-02T09:04:10.7684903Z dropped=350 for database: 610c26001281c704 for retention policy: autogen"}

[MOVE] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming\DDoS_5min_8-26-25 11-31-27 AM.csv -> D:\Joe\Develop\GrafanaInfluxdb\Autoimport\failed\DDoS_5min_8-26-25 11-31-27 AM.csv
[INGEST] D:\Joe\Develop\GrafanaInfluxdb\Autoimport\incoming\DDoS_5min_8-26-25 11-11-27 AM.csv
[DEBUG] columns(normalized): ['end_time', 'start_time', 'name', 'device_vendor', 'attacker_address', 'attacker_port', 'agent_type', 'transport_protocol', 'agent_severity', 'device_action', 'attacker_geo_location_info', 'attacker_geo_country_name', 'target_address', 'target_port', 'target_geo_location_info', 'target_geo_country_name', 'manager_receipt_time', 'agent_id', 'agent_name']
[DEBUG] using time column: manager_receipt_time
[DEBUG] ts_min = 2025-08-26 03:06:48+00:00 ts_max = 2025-08-26 03:11:28+00:00 rows = 305
[DEBUG] tag_cols used: ['device_vendor', 'agent_name', 'agent_type', 'agent_id', 'transport_protocol', 'device_action', 'attacker_geo_country_name', 'target_geo_country_name', 'attacker_address', 'attacker_port', 'target_address', 'target_port']

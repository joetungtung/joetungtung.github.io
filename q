D:\Joe\Develop\GrafanaInfluxdb\influxdb2CLI>.\influx.exe bucket update --id 822d46d43317c3c6 --retention 72h --shard-group-duration 24h --token TUNNT9zHsfHU2nFhjxL63i8HMpqLpSGv5J5hzrq9x-79DAxmTaOv5EbAr31OZaaz1zVrN1PjG4paGZJuvII57Q== --org "LINE BANK SOC"
Incorrect Usage: flag provided but not defined: -org

NAME:
   influx bucket update - Update bucket

USAGE:
   influx bucket update [command options] [arguments...]

COMMON OPTIONS:
   --host value                     HTTP address of InfluxDB [%INFLUX_HOST%]
   --skip-verify                    Skip TLS certificate chain and host name verification [%INFLUX_SKIP_VERIFY%]
   --configs-path value             Path to the influx CLI configurations [%INFLUX_CONFIGS_PATH%]
   --active-config value, -c value  Config name to use for command [%INFLUX_ACTIVE_CONFIG%]
   --http-debug
   --json                           Output data as JSON [%INFLUX_OUTPUT_JSON%]
   --hide-headers                   Hide the table headers in output data [%INFLUX_HIDE_HEADERS%]
   --token value, -t value          Token to authenticate request [%INFLUX_TOKEN%]

OPTIONS:
   --name value, -n value         New name to set on the bucket [%INFLUX_BUCKET_NAME%]
   --id value, -i value           The bucket ID
   --description value, -d value  New description to set on the bucket
   --retention value, -r value    New retention duration to set on the bucket, or 0 for infinite
   --shard-group-duration value   New shard group duration to set on the bucket, or 0 to have the server calculate a value

Error: flag provided but not defined: -org

#!/bin/bash
curl -X PUT "https://hostname.region.cloud.elastic-cloud.com:443/_ingest/pipeline/timestamp_pipeline?pretty" -H 'Content-Type: application/json' \
     -H "Authorization: ApiKey XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" -d'
{
  "description" : "Adds a timestamp to documents",
  "processors" : [
    {
      "set": {
        "field": "es_ingest_timestamp",
        "value": "{{_ingest.timestamp}}"
      }
    }
  ]
}'


curl -X POST "https://hostname.region.cloud.elastic-cloud.com:443/_bulk?pipeline=timestamp_pipeline&pretty" \
  -H "Authorization: ApiKey XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" \
  -H "Content-Type: application/json" \
  -d'
{ "index" : { "_index" : "telemetry_1" } }
{"timestamp": "2024-02-25T03:00:24.878377Z", "device_id": "device012", "temperature": 20.27, "humidity": 35.51, "pressure": 991.74, "altitude": 316.9, "battery_level": 62.38}
'


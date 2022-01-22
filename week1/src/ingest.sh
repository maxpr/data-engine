#!/bin/bash

sleep 15s

URL_TAXI="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

python  src/ingest_data.py \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL_TAXI}

URL_ZONES="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

python src/ingest_data.py \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi \
  --table_name=zones \
  --url=${URL_ZONES}
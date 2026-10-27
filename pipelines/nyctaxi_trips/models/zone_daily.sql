-- Dataset zone_daily of pipeline:playground3-1027/9f9f2767-f6b2-4e36-8910-f788c6ee6f8c: file:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/pipelines/nyctaxi/transformations/gold_zone_daily.sql line 3.
{{ config(materialized='table') }}

SELECT pickup_date, pickup_zip, COUNT(*) AS trips, ROUND(SUM(fare_amount), 2) AS total_fare, ROUND(AVG(trip_distance), 2) AS avg_distance, ROUND(AVG(duration_min), 1) AS avg_duration_min, ROUND(APPROX_PERCENTILE(avg_mph, 0.5), 1) AS median_mph FROM trips_clean GROUP BY pickup_date, pickup_zip

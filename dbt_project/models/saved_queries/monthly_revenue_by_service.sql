

SELECT
  DATETIME_TRUNC(pickup_datetime, month) AS metric_time__month
  , service_type AS trip_id__service_type
  , SUM(total_amount) AS gross_revenue
FROM {{ ref('fct_taxi_trips') }}
GROUP BY
  metric_time__month
  , trip_id__service_type
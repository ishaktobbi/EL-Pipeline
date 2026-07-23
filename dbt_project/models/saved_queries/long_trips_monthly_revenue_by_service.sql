
SELECT
  metric_time__month
  , trip_id__service_type
  , SUM(gross_revenue) AS gross_revenue

FROM (
  SELECT
    DATETIME_TRUNC(pickup_datetime, month) AS metric_time__month
    , service_type AS trip_id__service_type
    , total_amount AS gross_revenue
  FROM {{ ref('fct_taxi_trips') }}
  WHERE trip_distance > 10
) subq_2

GROUP BY
  metric_time__month
  , trip_id__service_type
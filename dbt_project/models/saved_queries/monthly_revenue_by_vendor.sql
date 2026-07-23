

SELECT
  DATETIME_TRUNC(sem_fct_taxi_trips_src_10000.pickup_datetime, month) AS metric_time__month
  -- here we specify for the semantics, because we create here a table from this model
  , COALESCE(sem_dim_vendors_src_10000.vendor_name , 'NOT SPECIFIED IN LKP FILE') AS vendor__vendor_name
  , SUM(sem_fct_taxi_trips_src_10000.total_amount) AS gross_revenue
FROM {{ ref('fct_taxi_trips') }} sem_fct_taxi_trips_src_10000
LEFT OUTER JOIN
     {{ ref('dim_vendors') }} sem_dim_vendors_src_10000
ON
  sem_fct_taxi_trips_src_10000.vendor_id = sem_dim_vendors_src_10000.vendor_id
GROUP BY
  metric_time__month
  , vendor__vendor_name
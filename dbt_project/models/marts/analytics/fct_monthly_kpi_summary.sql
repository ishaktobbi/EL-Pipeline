{{ config(
    materialized='table',
    partition_by={
      "field": "metric_month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['service_type', 'payment_method'],
    tags=['dashboard']
) }}

select
    -- 1. Date Granularity
    date(date_trunc(fct.pickup_datetime, month)) as metric_month,
    
    -- 2. Human-Readable Dimensions Joined from Dim Tables
    fct.service_type as service_type,               -- e.g., 'yellow' or 'green'
    p.payment_method as payment_method,               -- e.g., 'Credit card', 'Cash'
    
    -- 3. Pre-calculated Aggregated Metrics
    count(fct.trip_id) as total_trips,
    sum(fct.passenger_count) as total_passengers,
    sum(fct.total_amount) as gross_revenue,
    avg(fct.fare_amount) as avg_fare_per_trip

from {{ ref('fct_taxi_trips') }} fct

-- 🟢 Join Dimension Tables inside dbt!

left join {{ ref('dim_payment_methods') }} p
    on fct.payment_type_id = p.payment_type_id

group by 1, 2, 3
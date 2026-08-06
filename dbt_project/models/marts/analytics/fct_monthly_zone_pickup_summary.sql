{{ config(
    materialized='table',
    partition_by={
      "field": "metric_month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['service_type', 'borough', 'zone'],
    tags=['dashboard']
) }}

select
    -- 1. Date Granularity
    date(date_trunc(fct.pickup_datetime, month)) as metric_month,

    fct.service_type as service_type,               -- e.g., 'yellow' or 'green'
    
    loc.zone as zone,               -- e.g., 'Midtown'
    loc.borough as borough,         -- e.g., 'Manhattan'

    CONCAT(loc.zone, ", ", loc.borough, ", New York, USA") as zone_full_address,    --- e.g., 'Midtown, Manhattan, New York, USA'

    count(fct.trip_id) as total_trips          -- ◄ Used for Map Density!

from {{ ref('fct_taxi_trips') }} fct

-- 🟢 Join Dimension Tables inside dbt!
left join {{ ref('dim_locations') }} loc
    on fct.pickup_location_id = loc.location_id

group by 1, 2, 3, 4, 5
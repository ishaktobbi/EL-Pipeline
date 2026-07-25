with source as (

    select * from {{ source('raw_nyc_taxi', 'yellow_taxi_trips') }}

    -- BigQuery uses these variables to prune the GCS bucket *before* executing any cleaning logic.
    where 
        year = CAST('{{ var("run_year") }}' AS INT64) and 
        month = CAST('{{ var("run_month") }}' AS INT64)  -- we need to add a default values in yaml dbt_project
),

staged as (

    select
        -- Identifiers (Cast to Int64 or String)
        safe_cast(VendorID as int64) as vendor_id,
        safe_cast(RatecodeID as int64) as rate_code_id,
        safe_cast(PULocationID as int64) as pickup_location_id,
        safe_cast(DOLocationID as int64) as dropoff_location_id,
        safe_cast(payment_type as int64) as payment_type_id,
        
        -- Timestamps (Convert raw strings into actual BigQuery Timestamps)
        safe_cast(tpep_pickup_datetime as datetime) as pickup_datetime,
        safe_cast(tpep_dropoff_datetime as datetime) as dropoff_datetime,
        
        -- Trip Info
        safe_cast(passenger_count as int64) as passenger_count,
        safe_cast(trip_distance as float64) as trip_distance,
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        
        -- Financials (Cast strings to Float64 or Numeric)
        safe_cast(fare_amount as float64) as fare_amount,
        safe_cast(extra as float64) as extra,
        safe_cast(mta_tax as float64) as mta_tax,
        safe_cast(tip_amount as float64) as tip_amount,
        safe_cast(tolls_amount as float64) as tolls_amount,
        safe_cast(improvement_surcharge as float64) as improvement_surcharge,
        safe_cast(total_amount as float64) as total_amount,
        safe_cast(congestion_surcharge as float64) as congestion_surcharge,

        safe_cast('{{ var("run_year") }}' as INT64) as year_ingested,
        safe_cast('{{ var("run_month") }}' as INT64) as month_ingested,


    from source


)

select * from staged

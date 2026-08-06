with source as (

    SELECT * ,
        -- Construct partition key from Hive partitions (e.g., '2024-03')
        CONCAT(CAST(year AS STRING), '-', LPAD(CAST(month AS STRING), 2, '0')) AS partition_key
    FROM {{ source('raw_nyc_taxi', 'yellow_taxi_trips') }} 

   
    -- BigQuery uses these variables to prune the GCS bucket *before* executing any cleaning logic.
    -- where 
    --     year = CAST('{{ var("run_year") }}' AS INT64) and 
    --     month = CAST('{{ var("run_month") }}' AS INT64)  -- we need to add a default values in yaml dbt_project
),

source_polished as (
    SELECT r.*
    FROM source r
    -- Reference the external control source!
    INNER JOIN {{ source('partition_control', 'ingestion_partition_control') }} c
        ON r.partition_key = c.partition_key
    WHERE c.status = 'LOADED'
),

staged as (

    select
        PARSE_DATE('%Y-%m-%d', CONCAT(partition_key, '-01')) AS partition_date,
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

        -- Structural Alignment Placeholder (Column only exists in Green taxi data)
        CAST(NULL AS FLOAT64) AS ehail_fee,
        CAST(NULL AS INT64) AS trip_type_id,

        partition_key


    from source_polished


)

select * from staged
